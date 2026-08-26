//! The dequeue loop: a supervisor, and a worker task per queue.
//!
//! This is what makes an `ENQUEUED` row run. Everything under it already existed — the claim is
//! one transaction in the system database, and turning a claimed row into a running workflow is
//! [`dispatch`](crate::dispatch) — so what lives here is the loop that asks, the cadence it asks
//! at, and the count it has to keep to ask correctly.
//!
//! **A supervisor, and a worker per queue**, which is Go's arrangement and holds harder in Rust
//! where a task is cheaper than a goroutine. The supervisor sweeps once a second: transition
//! `DELAYED` rows, rebuild the queue set, spawn a worker for anything missing one. A worker reads
//! its queue's configuration from the published set on every pass, which is what lets a limit
//! change at runtime — the whole point of the starter app's Queues tab — and stops itself when its
//! queue leaves the set.
//!
//! `DELAYED` is transitioned by the supervisor rather than by each worker because the statement
//! names no queue: running it per worker would multiply one identical write by the number of
//! queues.
//!
//! Every task here is registered with the executor's task set, so shutdown reaches queue workers
//! exactly as it reaches workflows — aborting them without writing anything durable, which leaves
//! a claimed-but-unstarted workflow `PENDING` for the next executor to recover.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::Instrument;

use crate::dbos::Executor;
use crate::dispatch::dispatch;
use crate::queue::DEFAULT_POLLING_INTERVAL;
use crate::sysdb;
use crate::sysdb::INTERNAL_QUEUE;
use crate::sysdb::types::{Applications, QueueRecord, ResolvedLimits, Submission, WorkflowFilter};
use crate::workflow::spawn_tracked;

/// How often the supervisor rebuilds the queue set and transitions delayed workflows.
///
/// Distinct from a queue's own polling interval, which is how often a *worker* asks for work:
/// this is how often the set of workers is brought back in line with the table.
const SUPERVISOR_INTERVAL: Duration = Duration::from_secs(1);

/// The ceiling a contended worker's polling interval backs off to.
const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(120);

/// What a contended pass multiplies the polling interval by.
const BACKOFF_FACTOR: f64 = 2.0;

/// What a clean pass multiplies it by, walking it back towards the queue's own interval.
const SCALEBACK_FACTOR: f64 = 0.9;

/// The band every wait is jittered into.
///
/// **Not decorative.** A fleet that started together polls together forever without it, and every
/// one of those polls contends with the others for the same rows.
const JITTER: (f64, f64) = (0.95, 1.05);

/// How many of a queue's workflows this process is currently running.
///
/// The dequeue takes this as an argument because the database cannot know it — a `PENDING` row
/// says which executor claimed it, but not whether that executor's task is still alive — and
/// asking would be a second round trip on the hot path.
///
/// **Keyed by queue and by queue-and-partition**, because the two worker-concurrency limits are
/// enforced at different scopes and both are answered locally. A workflow on a partitioned queue
/// is counted under both keys, so neither count has to be derived from the other.
#[derive(Default)]
pub(crate) struct Running(Mutex<HashMap<Key, i64>>);

/// What a local tally is kept under: a queue, or a partition of one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Key {
    Queue(String),
    Partition(String, String),
}

impl Running {
    /// Counts one workflow as running on `queue` until the returned slot is dropped.
    ///
    /// A partition key counts it twice — once for the queue, once for the partition — and the
    /// slot releases both together.
    fn claim(self: &Arc<Self>, queue: &str, partition: Option<&str>) -> Slot {
        let mut keys = vec![Key::Queue(queue.to_owned())];
        if let Some(partition) = partition {
            keys.push(Key::Partition(queue.to_owned(), partition.to_owned()));
        }
        let mut counts = self.0.lock().expect("the running tally is poisoned");
        for key in &keys {
            *counts.entry(key.clone()).or_default() += 1;
        }
        drop(counts);
        Slot {
            running: Arc::clone(self),
            keys,
        }
    }

    /// What this process is running from `queue` right now, across every partition.
    fn count(&self, queue: &str) -> i64 {
        self.get(&Key::Queue(queue.to_owned()))
    }

    /// What this process is running from one partition of `queue` right now.
    fn count_for_partition(&self, queue: &str, partition: &str) -> i64 {
        self.get(&Key::Partition(queue.to_owned(), partition.to_owned()))
    }

    fn get(&self, key: &Key) -> i64 {
        self.0
            .lock()
            .expect("the running tally is poisoned")
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

/// One workflow's place in the local tallies, released when the workflow's task ends.
///
/// Held by the spawned execution rather than by the runner, which is what makes the release
/// correct without anything having to observe the workflow finishing: the task owns it, so it
/// goes when the task does — whether that is a return, a panic, or shutdown aborting it.
pub(crate) struct Slot {
    running: Arc<Running>,
    keys: Vec<Key>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut counts = match self.running.0.lock() {
            Ok(counts) => counts,
            // A poisoned tally means some other thread panicked holding it. Undercounting here
            // would let this queue over-dequeue forever; leaving the count alone is the safer
            // failure, and the panic that poisoned it is already reported.
            Err(_) => return,
        };
        for key in &self.keys {
            if let Some(count) = counts.get_mut(key) {
                *count -= 1;
                if *count <= 0 {
                    counts.remove(key);
                }
            }
        }
    }
}

/// The queue set the supervisor publishes and the workers read.
type Queues = Arc<Mutex<HashMap<String, QueueRecord>>>;

/// Starts the dequeue loop's supervisor. Called once, by `launch`.
pub(crate) fn spawn(executor: Arc<Executor>) {
    spawn_tracked(
        &executor,
        {
            let executor = Arc::clone(&executor);
            supervise(executor)
        }
        .instrument(tracing::info_span!("queues")),
    );
}

/// Reconciles the queue set once a second, spawning and respawning workers.
async fn supervise(executor: Arc<Executor>) {
    let queues: Queues = Arc::default();
    let running: Arc<Running> = Arc::default();
    let mut workers: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    // Once, not once a second: the row is a standing misconfiguration, so every sweep would find
    // it again. TypeScript keeps a `conflictWarned` set for its own queue-name collision warning
    // for the same reason.
    let mut warned_internal = false;

    loop {
        // Centrally, and before the set is rebuilt: a row whose delay expired this tick should be
        // visible to the dequeue that follows it rather than a tick later.
        match executor.sysdb().transition_delayed_workflows().await {
            Ok(0) => {}
            Ok(moved) => tracing::debug!(workflows = moved, "delayed workflows are now enqueued"),
            Err(error) => {
                tracing::warn!(error = %error, "could not transition delayed workflows");
            }
        }

        let names = refresh_queue_set(&executor, &queues, &mut warned_internal).await;

        // A worker that returned — its queue was deleted, or stopped being listened to — is
        // forgotten here, so the loop below respawns it if the queue comes back.
        workers.retain(|_, handle| !handle.is_finished());
        for name in names {
            if workers.contains_key(&name) {
                continue;
            }
            tracing::debug!(queue = name, "starting a worker for the queue");
            let handle = spawn_tracked(
                &executor,
                poll_queue(
                    Arc::clone(&executor),
                    name.clone(),
                    Arc::clone(&queues),
                    Arc::clone(&running),
                )
                .instrument(tracing::info_span!("queue", queue = %name)),
            );
            workers.insert(name, handle);
        }

        tokio::time::sleep(SUPERVISOR_INTERVAL).await;
    }
}

/// Rebuilds and publishes the set of queues this process runs workers for, returning their names.
///
/// The set is *read and published* here; spawning and reaping the workers that serve it is the
/// supervisor's, which is why this is not called a reconcile — it drives nothing.
///
/// **From the table, never from what this instance registered.** A process that called
/// `register_queue` for nothing still polls every queue its application owns, which is what lets a
/// fleet share one backlog. The scope is this application's rows plus the unclaimed ones — a peer
/// application's queue is not this one's to poll.
///
/// **A transient read failure keeps the previous set rather than emptying it**, which is Go's
/// behaviour and the only safe one: publishing an empty set would stop every worker, and the next
/// tick would start them all again.
///
/// **[`Config::listen_queues`](crate::Config::listen_queues) narrows it here, after the read.**
/// Intersecting rather than querying for the named queues is what makes the filter dynamic for
/// free: a listened queue registered after launch appears the moment its row does, and one deleted
/// disappears, without the filter needing to know either happened. The internal queue is added
/// before the filter and never subject to it.
///
/// `warned_internal` is the supervisor's, so the warning about a stored internal-queue row is
/// emitted once for the life of the executor rather than on every sweep.
async fn refresh_queue_set(
    executor: &Arc<Executor>,
    queues: &Queues,
    warned_internal: &mut bool,
) -> Vec<String> {
    let listed = match executor.sysdb().list_queues(&Applications::Unset).await {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(error = %error, "could not list queues; keeping the current set");
            return queues
                .lock()
                .expect("the queue set is poisoned")
                .keys()
                .cloned()
                .collect();
        }
    };

    // The internal queue is always present and is not a row — see `INTERNAL_QUEUE`. Inserted
    // first, and a stored row under the name is skipped below rather than allowed to replace it.
    let mut current: HashMap<String, QueueRecord> =
        HashMap::from([(INTERNAL_QUEUE.to_owned(), internal_queue())]);
    let listened = executor.listen_queues();
    for queue in listed {
        if queue.name == INTERNAL_QUEUE {
            // **Skipped, and said out loud once.** Rust's `register_queue` refuses this name, but
            // nothing else does: every peer implementation's client accepts it, and this crate's
            // own `sysdb::upsert_queue` is public and unguarded. Honouring the row would put a
            // concurrency limit or a polling interval on the queue `resume` and `fork` land on —
            // a knob whose only meaning is to throttle recovery from outside the application.
            if !*warned_internal {
                *warned_internal = true;
                tracing::warn!(
                    queue = INTERNAL_QUEUE,
                    "the queues table holds a row for the engine's internal queue; its stored \
                     limits are ignored. Delete the row: it can only throttle `resume` and `fork`"
                );
            }
            continue;
        }
        if let Some(listened) = listened
            && !listened.contains(&queue.name)
        {
            continue;
        }
        current.insert(queue.name.clone(), queue);
    }

    let names = current.keys().cloned().collect();
    *queues.lock().expect("the queue set is poisoned") = current;
    names
}

/// The engine's own queue, which `resume` and `fork` put work on.
///
/// No row, no limits, and the default cadence. Inserting a row at launch instead would make every
/// application in the fleet race to create the same one, and would hand an operator a knob whose
/// only meaning is "break resume".
///
/// It carries no concurrency default, which is a choice rather than an oversight: none of the four
/// references gives it one either, so an executor that re-enqueues a large backlog dequeues all of
/// it.
fn internal_queue() -> QueueRecord {
    QueueRecord {
        name: INTERNAL_QUEUE.to_owned(),
        concurrency: None,
        worker_concurrency: None,
        rate_limit: None,
        priority_enabled: false,
        partition_queue: false,
        partition_concurrency: None,
        partition_worker_concurrency: None,
        partition_rate_limit: None,
        polling_interval: DEFAULT_POLLING_INTERVAL,
        application_name: None,
    }
}

/// Polls one queue until its row leaves the published set, or shutdown aborts the task.
async fn poll_queue(executor: Arc<Executor>, name: String, queues: Queues, running: Arc<Running>) {
    // Started from the queue's own interval, then adapted. Held across iterations, which is the
    // point: a contended queue stays backed off rather than rediscovering the contention.
    let mut interval = DEFAULT_POLLING_INTERVAL;

    loop {
        // **Re-read every iteration**, which is what makes a limit changed at runtime take effect
        // without a restart. The worker holds a name, not a record, for exactly this reason.
        let Some(queue) = queues
            .lock()
            .expect("the queue set is poisoned")
            .get(&name)
            .cloned()
        else {
            tracing::info!("the queue is no longer registered; stopping its worker");
            return;
        };

        // The queue's own interval is the floor and the ceiling is derived from it, so both move
        // when the row does. Clamped rather than reset: a backed-off worker keeps its backoff.
        let floor = queue.polling_interval;
        let ceiling = floor.max(MAX_POLLING_INTERVAL);
        interval = interval.clamp(floor, ceiling);

        let contended = poll_once(&executor, &queue, &running).await;

        interval = if contended {
            interval.mul_f64(BACKOFF_FACTOR).min(ceiling)
        } else {
            interval.mul_f64(SCALEBACK_FACTOR).max(floor)
        };
        tokio::time::sleep(jitter(interval)).await;
    }
}

/// How many more workflows this process may start from a queue, or `None` for unlimited.
///
/// Local by construction: worker concurrency is the limit a process can answer without asking the
/// database. A per-partition worker limit of zero pauses this worker outright — nothing this crate
/// registers can hold one, since validation wants at least 1, but a peer's row can.
fn worker_budget(limits: &ResolvedLimits, running: i64) -> Option<i64> {
    if limits.partition_worker_concurrency.is_some_and(|w| w <= 0) {
        return Some(0);
    }
    limits
        .worker_concurrency
        .map(|worker| (i64::from(worker) - running).max(0))
}

/// One dequeue and the dispatch of whatever it claimed. Reports whether it met contention.
///
/// **Three shapes, which is the split TypeScript makes.** An unpartitioned queue is one claim. A
/// partitioned queue whose only limit is one-at-a-time-per-key is a single batched sweep, which is
/// what keeps a thousand partitions costing one round trip rather than a thousand. Any other
/// partitioned queue walks its partitions one at a time, because the limits it carries have to be
/// counted within each key — and in a shuffled order, so a queue with more partitions than budget
/// does not starve the ones sorting last.
async fn poll_once(executor: &Arc<Executor>, queue: &QueueRecord, running: &Arc<Running>) -> bool {
    let limits = queue.resolved_limits();

    if !limits.is_partitioned() {
        return match executor
            .sysdb()
            .start_queued_workflows(
                queue,
                executor.executor_id(),
                executor.application_version(),
                None,
                running.count(&queue.name),
                0,
            )
            .await
        {
            Ok(claimed) => {
                dispatch_claimed(executor, queue, None, running, claimed).await;
                false
            }
            Err(error) => report_dequeue_error(&error),
        };
    }

    // Snapshot once. Dispatch is asynchronous, so re-reading between partitions would count this
    // poll's own claims twice — once in the tally and once in `claimed`.
    let already_running = running.count(&queue.name);
    let budget = worker_budget(&limits, already_running);
    if budget == Some(0) {
        return false;
    }

    // The batched path, and the only one that does not count: see
    // `start_queued_partitioned_workflows` for why these four conditions are the whole of its
    // precondition.
    if limits.partition_concurrency == Some(1)
        && limits.concurrency.is_none()
        && limits.rate_limit.is_none()
        && limits.partition_rate_limit.is_none()
    {
        return match executor
            .sysdb()
            .start_queued_partitioned_workflows(
                queue,
                executor.executor_id(),
                executor.application_version(),
                budget,
            )
            .await
        {
            Ok(claimed) => {
                // The sweep returns one head per partition and does not say which; each claimed
                // row carries its own key, so the tally is credited from the rows themselves.
                dispatch_claimed(executor, queue, None, running, claimed).await;
                false
            }
            Err(error) => report_dequeue_error(&error),
        };
    }

    let partitions = match executor.sysdb().get_queue_partitions(&queue.name).await {
        Ok(partitions) => shuffled(partitions),
        Err(error) => return report_dequeue_error(&error),
    };

    let mut claimed_here = 0i64;
    for partition in partitions {
        if worker_budget(&limits, already_running + claimed_here) == Some(0) {
            break;
        }
        let claimed = match executor
            .sysdb()
            .start_queued_workflows(
                queue,
                executor.executor_id(),
                executor.application_version(),
                Some(&partition),
                already_running + claimed_here,
                running.count_for_partition(&queue.name, &partition),
            )
            .await
        {
            Ok(claimed) => claimed,
            // A peer holds this partition's rows. Skipping just this key is the point of walking
            // them separately — one contended partition is not a reason to back the whole queue
            // off, and the next poll shuffles into a different order anyway.
            Err(error) if is_contention(&error) => {
                tracing::debug!(
                    partition,
                    "a peer is mid-dequeue on this partition; skipping it"
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(error = %error, partition, "could not dequeue from the partition");
                continue;
            }
        };
        claimed_here += i64::try_from(claimed.len()).unwrap_or(i64::MAX);
        dispatch_claimed(executor, queue, Some(&partition), running, claimed).await;
    }
    false
}

/// Turns a failed dequeue into the "was it contention" answer the caller backs off on.
fn report_dequeue_error(error: &sysdb::Error) -> bool {
    if is_contention(error) {
        // Not a failure at this layer: a peer holds the rows this dequeue wanted to lock, which
        // is the system working. It costs an interval, and says nothing louder.
        tracing::debug!(error = %error, "a peer is mid-dequeue; backing off");
        return true;
    }
    tracing::warn!(error = %error, "could not dequeue from the queue");
    false
}

/// Fisher-Yates, so a walk visits partitions in a different order each poll.
///
/// Starvation is the reason rather than fairness in the abstract: a worker whose budget runs out
/// part way through would otherwise always spend it on whichever keys sort first, and the tail of
/// a large partition set would never be reached. TypeScript shuffles here for the same reason.
///
/// Seeded from a v4 UUID for the reason [`jitter`] draws one: the randomness comes from a
/// dependency this crate already has rather than one added for two uses. Seeded once per walk and
/// advanced by xorshift, so a large partition set does not cost an entropy draw per swap.
fn shuffled(mut keys: Vec<String>) -> Vec<String> {
    let mut state = (uuid::Uuid::new_v4().as_u128() as u64) | 1;
    for i in (1..keys.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        keys.swap(i, (state % (i as u64 + 1)) as usize);
    }
    keys
}

/// Reads the claimed workflows in one round trip and starts each, crediting the local tally.
async fn dispatch_claimed(
    executor: &Arc<Executor>,
    queue: &QueueRecord,
    partition: Option<&str>,
    running: &Arc<Running>,
    claimed: Vec<String>,
) {
    if claimed.is_empty() {
        return;
    }

    // **One round trip for the whole batch**, which is why the claim returns ids rather than
    // rows: Python's `start_dequeued_workflows` reads every claimed status at once and then
    // dispatches each, and the system database was built for that shape.
    let ids: Vec<&str> = claimed.iter().map(String::as_str).collect();
    let rows = match executor
        .sysdb()
        .list_workflows(&WorkflowFilter {
            workflow_ids: ids,
            load_output: false,
            ..WorkflowFilter::default()
        })
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            // The rows stay `PENDING` with this executor's id on them, which is what recovery is
            // for. Nothing is lost; this tick is.
            tracing::warn!(
                error = %error,
                claimed = claimed.len(),
                "could not read the claimed workflows; they stay PENDING for recovery"
            );
            return;
        }
    };
    if rows.len() != claimed.len() {
        tracing::warn!(
            claimed = claimed.len(),
            found = rows.len(),
            "some claimed workflows have no row"
        );
    }

    tracing::debug!(workflows = rows.len(), "dequeued workflows");
    for row in rows {
        let workflow_id = row.workflow_id.clone();
        // The row's own key rather than the one being swept, so the batched path — which names no
        // partition and claims across all of them — still credits each tally correctly.
        let partition = partition.or(row.queue_partition_key.as_deref());
        // Claimed before the dispatch, so the next iteration's counts include it even if this one
        // is still starting. `dispatch` drops the slot itself if it does not spawn.
        let slot = running.claim(&queue.name, partition);
        if let Err(error) = dispatch(executor, row, Submission::Dequeue, Some(slot)).await {
            tracing::warn!(
                workflow_id,
                error = %error,
                "could not start the dequeued workflow; it stays PENDING for recovery"
            );
        }
    }
}

/// Whether a failed dequeue means a peer was mid-dequeue rather than something being wrong.
///
/// **`55P03` by code, not by class.** A `NOWAIT` conflict is `lock_not_available`, and the
/// backend classifier works by SQLSTATE class prefix — class `55` is not class `40`, so a lock
/// conflict arrives as [`BackendErrorKind::Permanent`](crate::sysdb::BackendErrorKind::Permanent)
/// and never reaches the retry layer. That is the right call for the retry layer, which cannot
/// know that asking again later is exactly what this caller does; it just means the runner has to
/// recognise the code itself. Serialization failures (class `40`) do not appear here at all —
/// `start_queued_workflows` retries those internally.
fn is_contention(error: &sysdb::Error) -> bool {
    matches!(
        error,
        sysdb::Error::Backend(backend) if backend.sqlstate.as_deref() == Some("55P03")
    )
}

/// Spreads a wait over [`JITTER`], so a fleet that started together stops polling in lockstep.
///
/// The randomness comes from a v4 UUID for the reason `sysdb::retry`'s own jitter does: `uuid` is
/// already a dependency, and one number per poll does not justify another.
fn jitter(interval: Duration) -> Duration {
    let (low, high) = JITTER;
    let bits = uuid::Uuid::new_v4().as_u128() as u32;
    let factor = low + (high - low) * (f64::from(bits) / f64::from(u32::MAX).next_up());
    interval.mul_f64(factor)
}
