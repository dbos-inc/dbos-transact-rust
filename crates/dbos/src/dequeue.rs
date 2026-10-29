//! The dequeue loop: a supervisor, and a worker task per queue.
//!
//! This is what makes an `ENQUEUED` row run. The claim itself is one transaction in the system
//! database and turning a claimed row into a running workflow is [`dispatch`] below, so what lives
//! here is the loop that asks, the cadence it asks at, and the count it has to keep to ask
//! correctly.
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

use crate::error::Error;
use crate::instance::Executor;
use crate::queue::DEFAULT_POLLING_INTERVAL;
use crate::sysdb;
use crate::sysdb::INTERNAL_QUEUE;
use crate::sysdb::types::{
    AppScope, DatabaseTime, QueueRecord, Timestamp, WorkflowFilter, WorkflowRecord, WorkflowStatus,
};
use crate::workflow::{MAX_RECOVERY_ATTEMPTS, spawn_execution, spawn_tracked};

/// How often the supervisor rebuilds the queue set and transitions delayed workflows.
///
/// Distinct from a queue's own polling interval, which is how often a *worker* asks for work:
/// this is how often the set of workers is brought back in line with the table.
const SUPERVISOR_INTERVAL: Duration = Duration::from_secs(1);

/// The ceiling the polling interval of a worker that keeps losing serialization conflicts backs
/// off to.
const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(120);

/// What a pass that lost a serialization conflict multiplies the polling interval by.
///
/// See [`Contention`] for why only that kind of contention backs off.
const BACKOFF_FACTOR: f64 = 2.0;

/// What every other pass multiplies it by, walking it back towards the queue's own interval —
/// including a pass that found the rows locked by a peer.
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
    let listed = match executor.sysdb().list_queues(&AppScope::Unset).await {
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

        let polled = poll_once(&executor, &queue, &running).await;
        tracing::trace!(claims = polled.claims, "polled the queue");
        let backs_off = polled.contention.is_some_and(Contention::backs_off);

        interval = if backs_off {
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
fn worker_budget(queue: &QueueRecord, running: i64) -> Option<i64> {
    if queue.partition_worker_concurrency.is_some_and(|w| w <= 0) {
        return Some(0);
    }
    queue
        .worker_concurrency
        .map(|worker| (i64::from(worker) - running).max(0))
}

/// What one poll of a queue met.
#[derive(Debug, Default)]
struct Polled {
    /// The contention that ended it, if any.
    contention: Option<Contention>,
    /// How many claim transactions it ran — one per partition visited, when it walked them.
    claims: usize,
}

impl Polled {
    fn new(contention: Option<Contention>, claims: usize) -> Self {
        Self { contention, claims }
    }
}

/// One dequeue and the dispatch of whatever it claimed. Reports the contention it met, if any, and
/// how many claims it took to get there.
///
/// **Three shapes, which is the split TypeScript makes.** An unpartitioned queue is one claim. A
/// partitioned queue whose only limit is one-at-a-time-per-key is a single batched sweep, which is
/// what keeps a thousand partitions costing one round trip rather than a thousand. Any other
/// partitioned queue walks its partitions one at a time, because the limits it carries have to be
/// counted within each key — and in a shuffled order, so a queue with more partitions than budget
/// does not starve the ones sorting last.
///
/// **The walk is bounded by the queue-wide limits as well as by the worker's own.** Each visit is
/// a claim transaction, at a stricter isolation whenever the queue carries a shared limit, and with
/// a queue-wide limit spent every one of them claims nothing, so a queue with tens of thousands of waiting partitions would otherwise pay
/// that many transactions a poll to learn one fact. The queue's room under its global concurrency
/// is read once up front: none means no partition is visited at all, and the walk ends as soon as
/// this poll has claimed it. A claim that reports a queue-wide limit closed — a peer filled it
/// since, or the rate limit's window is full — ends the walk too, since every other key would
/// report the same.
async fn poll_once(
    executor: &Arc<Executor>,
    queue: &QueueRecord,
    running: &Arc<Running>,
) -> Polled {
    // One token per claim: every row a claim takes is owned by it.
    let owner_xid = || uuid::Uuid::new_v4().to_string();
    if !queue.is_partitioned() {
        let owner_xid = owner_xid();
        return match executor
            .sysdb()
            .start_queued_workflows(
                queue,
                executor.executor_id(),
                executor.app_version(),
                &owner_xid,
                None,
                running.count(&queue.name),
                0,
            )
            .await
        {
            Ok(claimed) => {
                dispatch_claimed(
                    executor,
                    queue,
                    None,
                    running,
                    claimed.workflow_ids,
                    &owner_xid,
                )
                .await;
                Polled::new(None, 1)
            }
            Err(error) => Polled::new(report_dequeue_error(&error), 1),
        };
    }

    // Snapshot once. Dispatch is asynchronous, so re-reading between partitions would count this
    // poll's own claims twice — once in the tally and once in `claimed`.
    let already_running = running.count(&queue.name);
    let budget = worker_budget(queue, already_running);
    if budget == Some(0) {
        return Polled::default();
    }

    // The batched path, and the only one that does not count: see
    // `start_queued_partitioned_workflows` for why these four conditions are the whole of its
    // precondition.
    if queue.partition_concurrency == Some(1)
        && queue.concurrency.is_none()
        && queue.rate_limit.is_none()
        && queue.partition_rate_limit.is_none()
    {
        let owner_xid = owner_xid();
        return match executor
            .sysdb()
            .start_queued_partitioned_workflows(
                queue,
                executor.executor_id(),
                executor.app_version(),
                &owner_xid,
                budget,
            )
            .await
        {
            Ok(claimed) => {
                // The sweep returns one head per partition and does not say which; each claimed
                // row carries its own key, so the tally is credited from the rows themselves.
                dispatch_claimed(executor, queue, None, running, claimed, &owner_xid).await;
                Polled::new(None, 1)
            }
            Err(error) => Polled::new(report_dequeue_error(&error), 1),
        };
    }

    // The room left under the queue's global concurrency, across every executor: `None` when it
    // has none. One count rather than one per partition, and a snapshot rather than a promise —
    // each claim below counts again in its own transaction, so a peer that spends the room in the
    // meantime costs this walk a claim, not an overrun.
    let global_room = match queue.concurrency {
        None => None,
        Some(concurrency) => {
            match executor
                .sysdb()
                .count_pending_queue_workflows(&queue.name)
                .await
            {
                Ok(pending) => Some((i64::from(concurrency) - pending).max(0)),
                Err(error) => return Polled::new(report_dequeue_error(&error), 0),
            }
        }
    };
    // Not even listed: with the queue full, every partition would claim nothing.
    if global_room == Some(0) {
        return Polled::default();
    }

    let partitions = match executor.sysdb().get_queue_partitions(&queue.name).await {
        Ok(partitions) => shuffled(partitions),
        Err(error) => return Polled::new(report_dequeue_error(&error), 0),
    };

    let mut claimed_here = 0i64;
    let mut claims = 0usize;
    for partition in partitions {
        if worker_budget(queue, already_running + claimed_here) == Some(0)
            || global_room.is_some_and(|room| claimed_here >= room)
        {
            break;
        }
        claims += 1;
        let owner_xid = owner_xid();
        let claimed = match executor
            .sysdb()
            .start_queued_workflows(
                queue,
                executor.executor_id(),
                executor.app_version(),
                &owner_xid,
                Some(&partition),
                already_running + claimed_here,
                running.count_for_partition(&queue.name, &partition),
            )
            .await
        {
            Ok(claimed) => claimed,
            // A peer holds this partition's rows, or committed to them first. Skipping just this
            // key is the point of walking them separately — one contended partition is not a
            // reason to back the whole queue off, whichever kind of contention it was, and the next
            // poll shuffles into a different order anyway.
            Err(error) if contention(&error).is_some() => {
                tracing::debug!(
                    partition,
                    error = %error,
                    "a peer is mid-dequeue on this partition; skipping it"
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(error = %error, partition, "could not dequeue from the partition");
                continue;
            }
        };
        claimed_here += i64::try_from(claimed.workflow_ids.len()).unwrap_or(i64::MAX);
        dispatch_claimed(
            executor,
            queue,
            Some(&partition),
            running,
            claimed.workflow_ids,
            &owner_xid,
        )
        .await;
        if let Some(stop) = claimed.stopped_by
            && stop.spans_partitions()
        {
            tracing::debug!(
                partition,
                ?stop,
                "a queue-wide limit has no room left; ending the partition walk"
            );
            break;
        }
    }
    Polled::new(None, claims)
}

/// Logs a failed dequeue at the level it deserves, and reports the contention it was, if any.
fn report_dequeue_error(error: &sysdb::Error) -> Option<Contention> {
    let contention = contention(error);
    match contention {
        // Not a failure at this layer: a peer was dequeuing from the same rows, which is the
        // system working. It costs a poll, and says nothing louder.
        Some(_) => tracing::debug!(error = %error, "a peer is mid-dequeue; polling again later"),
        None => tracing::warn!(error = %error, "could not dequeue from the queue"),
    }
    contention
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
    owner_xid: &str,
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
        .list_workflows(
            &WorkflowFilter {
                workflow_ids: ids,
                load_output: false,
                ..WorkflowFilter::default()
            },
            // The dequeue loop is nobody's step.
            None,
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            // The rows stay `PENDING` with this executor's id on them, and recovery re-enqueues
            // them when this executor next launches; until then they count against the queue's
            // concurrency. Nothing is lost; this tick is, and the rows wait for the restart.
            tracing::warn!(
                error = %error,
                claimed = claimed.len(),
                "could not read the claimed workflows; they stay PENDING until this executor \
                 restarts"
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

    // **A reading of the database's clock, for timing the claimed deadlines.** The claim set each
    // one on that clock, and a wait for it is measured from a reading of it rather than this
    // host's wall clock. One per batch, and none for a batch with no deadline in it, which times
    // nothing. A wait measured from it ends late by the reading's return trip, never early.
    let clock = if rows.iter().any(|row| row.deadline.is_some()) {
        timing_clock(executor.sysdb().database_time().await)
    } else {
        local_clock()
    };

    // **Dispatched in claim order, not in the order the read came back.** The claim sorts its
    // batch into dequeue order and says so, because that order is what `priority` is for; the
    // read that follows is a `list_workflows`, which sorts by `created_at`. Walking the rows as
    // they arrive would therefore undo the sort — and `dispatch` awaits a round trip apiece, so
    // on a prioritised queue the workflow that ranked first would start last, one round trip per
    // row late. Indexing the rows and walking the claimed ids is how Python keeps the order too.
    let mut by_id: HashMap<String, _> = rows
        .into_iter()
        .map(|row| (row.workflow_id.clone(), row))
        .collect();
    for id in &claimed {
        // A claimed id with no row was counted in the warning above; there is nothing to start.
        let Some(row) = by_id.remove(id) else {
            continue;
        };
        let workflow_id = row.workflow_id.clone();
        // The row's own key rather than the one being swept, so the batched path — which names no
        // partition and claims across all of them — still credits each tally correctly.
        let partition = partition.or(row.queue_partition_key.as_deref());
        // Claimed before the dispatch, so the next iteration's counts include it even if this one
        // is still starting. `dispatch` drops the slot itself if it does not spawn.
        let slot = running.claim(&queue.name, partition);
        if let Err(error) = dispatch(executor, row, owner_xid, clock, Some(slot)).await {
            tracing::warn!(
                workflow_id,
                error = %error,
                "could not start the dequeued workflow; it stays PENDING until this executor \
                 restarts"
            );
        }
    }
}

/// Submits one workflow from the row that describes it, if this executor still can and should.
///
/// A dequeue has claimed the row and read it; what remains is to reconstruct the call the row
/// describes — look the registration up by the row's own name, check the row is still `PENDING`,
/// and spawn — and that is what this does. It takes the row rather than
/// fetching it because the claim already read every row it took in one round trip, which is the
/// reason [`start_queued_workflows`](crate::sysdb::SystemDatabase::start_queued_workflows) returns
/// ids.
///
/// `owner_xid` is the claim's token. The execution presents it on every write, so if the row is
/// handed to another execution since the claim (a resume, a recovery), this one's first write is
/// refused. That is the only check of ownership here: a row claimed again before the read is
/// `PENDING` and runs, and its first step's body runs before that write is refused.
///
/// **The registry is consulted after the row is claimed, and that is harmless rather than
/// load-bearing.** The claim has already flipped the row to `PENDING`, counted the attempt, and
/// stamped `executor_id` before this is reached, so there is nothing for the order to save. What
/// it does mean is that a row this process cannot run is never parked, however many attempts it
/// accrues — the subject of the upstream note below.
///
/// `slot` is this workflow's place in its queue's local running tally. It travels into the spawned
/// execution so the tally is released when the workflow's task ends — and is dropped here,
/// releasing it immediately, on every path that does not spawn.
///
/// Returning `Ok(())` covers every reason not to run that is not a fault: no name, no
/// registration, a row already claimed, a row already finished, a parked row. The caller logs a
/// genuine `Err` and moves on to the next id — a sweep must not strand every workflow behind one
/// bad row.
async fn dispatch(
    executor: &Arc<Executor>,
    row: WorkflowRecord,
    owner_xid: &str,
    clock: DatabaseTime,
    slot: Option<Slot>,
) -> crate::Result<()> {
    let Some(key) = row.qualified_name() else {
        tracing::warn!(
            workflow_id = row.workflow_id,
            "the row names no workflow; skipped"
        );
        return Ok(());
    };
    let workflow_id = row.workflow_id;
    if !executor.workflows().contains_key(&key) {
        // Logged and skipped, never fatal: the code that knew this workflow was removed or
        // renamed, and the row waits for a launch that recognises it.
        //
        // TODO(dbos-team): UPSTREAM item 24, the cap this skip never reaches. The claim already
        // counted this dispatch — `start_queued_workflows` does `recovery_attempts + 1` — and
        // the cap below is the only place that reads that count and parks the row, so returning
        // here skips it: the row is claimed, counted, skipped, re-enqueued by
        // recovery, and claimed again, with nothing ever parking it. All four implementations
        // have the same hole, and for them the order is forced because the budget is read off the
        // registration whose absence is the problem. Here it is `MAX_RECOVERY_ATTEMPTS`, a
        // constant, so this could check the cap first; it does not, because a lone port that
        // parks rows its peers leave alone is the worse divergence.
        tracing::warn!(
            workflow_id,
            workflow = %key,
            "no workflow is registered under the row's name; it stays where it is"
        );
        return Ok(());
    }

    match admission(row.status, row.recovery_attempts) {
        Admission::Run => {}
        Admission::NotPending => {
            tracing::warn!(
                workflow_id,
                status = %row.status,
                "the dequeued workflow is no longer pending; it is not started"
            );
            return Ok(());
        }
        Admission::Park => {
            match executor
                .sysdb()
                .dead_letter_workflows(&[&workflow_id], row.recovery_attempts)
                .await
            {
                Ok(parked) if !parked.is_empty() => tracing::warn!(
                    workflow_id,
                    attempts = row.recovery_attempts,
                    limit = MAX_RECOVERY_ATTEMPTS,
                    "the workflow exceeded its recovery attempts and is parked"
                ),
                Ok(_) => {}
                Err(error) => return Err(Error::SystemDatabase(error)),
            }
            return Ok(());
        }
    }

    tracing::debug!(
        workflow_id,
        workflow = %key,
        attempt = row.recovery_attempts,
        "running the workflow"
    );
    // Detached: submitted workflows run concurrently and the caller moves on. The handle is not
    // awaited by anyone, which is exactly the case the execution layer's own logging covers.
    // The stored deadline, so a workflow gets what is *left* of its budget rather than the whole
    // of it again — and one submitted after its expiry cancels at once instead of running on
    // unbounded.
    spawn_execution(
        executor,
        key,
        workflow_id,
        owner_xid.to_owned(),
        row.input,
        // The row's own tag, which is how its input was written — by this SDK or another.
        row.serialization,
        crate::workflow::Deadline::of(row.deadline, clock),
        slot,
    );
    Ok(())
}

/// How a dequeue lost a race to a peer, as opposed to failing.
///
/// Both kinds reach the poll loop as they happened: the dequeue replays neither. A lock conflict is
/// [`BackendErrorKind::Permanent`](crate::sysdb::BackendErrorKind::Permanent) to the retry layer,
/// and the dequeue turns conflict replay off, so a serialization failure is returned on the first
/// attempt. Asking again later is exactly what this caller does, and it is the caller that knows
/// how much later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Contention {
    /// `55P03` `lock_not_available`: the `NOWAIT` lock a rate-limited dequeue takes found a peer
    /// holding the rows. The peer commits in milliseconds, so the next poll finds them free, and
    /// backing off would only leave waiting work unclaimed.
    LockNotAvailable,
    /// `40001` `serialization_failure`: a dequeue running at repeatable read or serialisable lost
    /// to a peer that committed first. On PostgreSQL that is a dequeue raised to keep a shared
    /// budget consistent; on CockroachDB, whose default isolation is serialisable, any dequeue.
    /// Contenders can keep colliding on every poll, so the worker backs off to spread them out.
    SerializationFailure,
}

impl Contention {
    /// Whether this contention should lengthen the polling interval.
    fn backs_off(self) -> bool {
        self == Contention::SerializationFailure
    }
}

/// The contention a failed dequeue was, or `None` if something is actually wrong.
///
/// By exact code. `40P01`, a deadlock, is deliberately not contention: nothing in the dequeue
/// expects one, so it is a real error and is logged as one.
fn contention(error: &sysdb::Error) -> Option<Contention> {
    let sysdb::Error::Backend(backend) = error else {
        return None;
    };
    match backend.sqlstate.as_deref() {
        Some("55P03") => Some(Contention::LockNotAvailable),
        Some("40001") => Some(Contention::SerializationFailure),
        _ => None,
    }
}

/// Spreads a wait over [`JITTER`], so a fleet that started together stops polling in lockstep.
///
/// The randomness comes from a v4 UUID for the reason `sysdb::retry`'s own jitter does: `uuid` is
/// already a dependency, and one number per poll does not justify another.
fn jitter(interval: Duration) -> Duration {
    let (low, high) = JITTER;
    interval.mul_f64(low + (high - low) * random_unit())
}

/// A uniformly random number in `[0, 1)`, for jitter.
///
/// From a v4 UUID, which this crate already depends on, rather than from `rand` added for jitter
/// alone. Shared with the scheduler's jitter.
pub(crate) fn random_unit() -> f64 {
    let bits = uuid::Uuid::new_v4().as_u128() as u32;
    f64::from(bits) / f64::from(u32::MAX).next_up()
}

/// What a dispatch does with a claimed row, decided on the batch read's copy of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Started.
    Run,
    /// Left alone. **Only a `PENDING` row can own its outcome**: a row that moved on since the
    /// claim (cancelled, resumed, or finished by another execution) would run for nothing, and a
    /// resumed one is worse than that: `resume_workflows` puts it back on its queue as `ENQUEUED`,
    /// so running it here while the next sweep claims it again is one workflow executing twice.
    NotPending,
    /// Parked, once it has been dispatched more often than allowed. The claim counted this
    /// attempt; a workflow that keeps being recovered without finishing is crashing its
    /// executors, and running it again would crash another. The park's guard on the count leaves
    /// a row alone that a resume reset meanwhile. A row another claim has taken since is parked
    /// too, and rightly: that claim counted one more attempt, so its own dispatch would park it.
    Park,
}

/// The status first, so a row that is no longer `PENDING` is left as it is whatever its count.
fn admission(status: WorkflowStatus, recovery_attempts: i64) -> Admission {
    if status != WorkflowStatus::Pending {
        Admission::NotPending
    } else if recovery_attempts > MAX_RECOVERY_ATTEMPTS + 1 {
        Admission::Park
    } else {
        Admission::Run
    }
}

/// The clock a batch's deadlines are timed from: the database's reading, or, if the read failed
/// outright, this host's.
///
/// The fallback trades accuracy for progress. Skew between the two clocks moves the deadlines of
/// this one batch, where holding the rows back instead would leave them `PENDING` under a live
/// executor, counting against the queue's concurrency, until it next launched.
///
/// **A lost connection never reaches it.** The read is retried until the database answers, like
/// every system-database call and like the batch read just before it, so an unreachable database
/// holds the batch there rather than here. Nothing dispatched sooner would get further: its first
/// checkpoint would wait on the same database.
fn timing_clock(reading: Result<DatabaseTime, sysdb::Error>) -> DatabaseTime {
    reading.unwrap_or_else(|error| {
        tracing::warn!(
            error = %error,
            "could not read the database's clock; the claimed deadlines are timed from this host's"
        );
        local_clock()
    })
}

/// A reading of this host's wall clock, in the shape a database reading takes.
fn local_clock() -> DatabaseTime {
    DatabaseTime::read(Timestamp::now().as_epoch_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sysdb::{BackendError, BackendErrorKind};

    fn backend(sqlstate: &str, kind: BackendErrorKind) -> sysdb::Error {
        sysdb::Error::Backend(BackendError {
            message: "boom".to_owned(),
            sqlstate: Some(sqlstate.to_owned()),
            kind,
        })
    }

    /// The two ways of losing a dequeue race are told apart, and only a serialization failure
    /// lengthens the polling interval.
    #[test]
    fn only_a_serialization_failure_backs_off() {
        let locked = contention(&backend("55P03", BackendErrorKind::Permanent));
        assert_eq!(locked, Some(Contention::LockNotAvailable));
        assert!(!locked.unwrap().backs_off());

        let conflict = contention(&backend("40001", BackendErrorKind::Conflict));
        assert_eq!(conflict, Some(Contention::SerializationFailure));
        assert!(conflict.unwrap().backs_off());
    }

    /// A deadlock is not something the dequeue expects, so it is an error rather than contention,
    /// as is anything that is not a backend failure at all.
    #[test]
    fn a_deadlock_is_not_contention() {
        assert_eq!(
            contention(&backend("40P01", BackendErrorKind::Conflict)),
            None
        );
        assert_eq!(
            contention(&backend("23505", BackendErrorKind::Permanent)),
            None
        );
        assert_eq!(
            contention(&sysdb::Error::Malformed("bad row".to_owned())),
            None
        );
    }

    /// **A claimed row that has been claimed again since runs, but records nothing.** It is
    /// `PENDING` under another claim's token, so its body runs and its outcome write is refused;
    /// the row stays the owner's, and the owner's run is the one that records the outcome.
    #[tokio::test]
    async fn a_row_claimed_again_since_records_nothing() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..crate::Config::new("dispatch-test", db.url())
        });
        let runs = Arc::new(AtomicU32::new(0));
        let dbos_runs = Arc::clone(&runs);
        dbos.register_workflow("counted", move |()| {
            let runs = Arc::clone(&dbos_runs);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok::<_, crate::Error>(())
            }
        })
        .unwrap();
        dbos.launch().await.expect("launch failed");
        let executor = dbos.executor("test").expect("launched");
        executor
            .sysdb()
            .init_workflow(
                &crate::sysdb::types::NewWorkflow {
                    name: Some("counted"),
                    input: Some("null"),
                    serialization: Some("rust_serde"),
                    ..crate::sysdb::types::NewWorkflow::new("wf-reclaimed")
                },
                None,
            )
            .await
            .unwrap();
        let row = || async {
            executor
                .sysdb()
                .get_workflow("wf-reclaimed")
                .await
                .unwrap()
                .unwrap()
        };
        let owner = row().await.owner_xid.expect("a started row is owned");

        dispatch(
            &executor,
            row().await,
            "an-earlier-claim",
            local_clock(),
            None,
        )
        .await
        .unwrap();
        let ran = |n: u32| {
            let runs = Arc::clone(&runs);
            async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while runs.load(Ordering::SeqCst) < n {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "run {n} never started"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        ran(1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let held = row().await;
        assert_eq!(
            held.status,
            WorkflowStatus::Pending,
            "a stale claim settled it"
        );
        assert_eq!(held.owner_xid.as_deref(), Some(owner.as_str()));

        dispatch(&executor, row().await, &owner, local_clock(), None)
            .await
            .unwrap();
        ran(2).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while row().await.status != WorkflowStatus::Success {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the owner's run never recorded its outcome"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        dbos.shutdown().await;
    }

    /// A launched executor that polls no queue of its own accord, with `queue` registered and one
    /// workflow waiting on each of `partitions` partitions of it.
    ///
    /// `listen_queues` is empty so the only polls of the queue are the test's own, which is what
    /// lets a test count the claims one poll makes.
    async fn partitioned_backlog(
        queue: crate::sysdb::types::NewQueue<'_>,
        partitions: usize,
    ) -> (
        crate::DBOS,
        Arc<Executor>,
        dbos_test_support::TestDatabase,
        QueueRecord,
    ) {
        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            listen_queues: Some(Vec::new()),
            ..crate::Config::new("walk-test", db.url())
        });
        dbos.register_workflow("noop", |()| async { Ok::<_, crate::Error>(()) })
            .unwrap();
        dbos.launch().await.expect("launch failed");
        let executor = dbos.executor("test").expect("launched");
        let sysdb = executor.sysdb();
        sysdb
            .upsert_queue(&queue, crate::sysdb::types::OnExistingQueue::Update)
            .await
            .unwrap();
        for n in 0..partitions {
            let id = format!("wf-waiting-{n}");
            let partition = format!("p-{n}");
            sysdb
                .init_workflow(
                    &crate::sysdb::types::NewWorkflow {
                        name: Some("noop"),
                        input: Some("null"),
                        serialization: Some("rust_serde"),
                        queue_name: Some(queue.name),
                        queue_partition_key: Some(&partition),
                        application_version: Some(executor.app_version()),
                        ..crate::sysdb::types::NewWorkflow::new(&id)
                    },
                    None,
                )
                .await
                .unwrap();
        }
        let registered = sysdb.get_queue(queue.name).await.unwrap().unwrap();
        (dbos, executor, db, registered)
    }

    /// Puts a workflow on `partition` of `queue` that started moments ago and is still running,
    /// as a peer's claim would have left it: it holds a slot of the queue's concurrency and rate
    /// limit, and of the partition's.
    async fn started_elsewhere(
        executor: &Executor,
        db: &dbos_test_support::TestDatabase,
        queue: &str,
        partition: &str,
    ) {
        executor
            .sysdb()
            .init_workflow(
                &crate::sysdb::types::NewWorkflow {
                    name: Some("noop"),
                    input: Some("null"),
                    serialization: Some("rust_serde"),
                    application_version: Some(executor.app_version()),
                    ..crate::sysdb::types::NewWorkflow::new("wf-started-elsewhere")
                },
                None,
            )
            .await
            .unwrap();
        sqlx::query(
            r#"UPDATE "dbos"."workflow_status"
               SET queue_name = $1, queue_partition_key = $2, rate_limited = TRUE,
                   started_at_epoch_ms = (EXTRACT(epoch FROM now()) * 1000)::bigint
               WHERE workflow_uuid = 'wf-started-elsewhere'"#,
        )
        .bind(queue)
        .bind(partition)
        .execute(&db.pool().await)
        .await
        .unwrap();
    }

    /// **A partitioned queue whose global concurrency is already spent visits no partition**,
    /// however many are waiting: the one pending count up front shows there is no room, so the
    /// poll neither lists the partitions nor pays a claim transaction for each of them.
    #[tokio::test]
    async fn a_walk_with_the_queue_wide_concurrency_spent_visits_no_partition() {
        let queue = crate::sysdb::types::NewQueue {
            concurrency: Some(1),
            partition_concurrency: Some(1),
            ..crate::sysdb::types::NewQueue::new("walked")
        };
        let (dbos, executor, db, registered) = partitioned_backlog(queue, 50).await;
        started_elsewhere(&executor, &db, "walked", "elsewhere").await;

        let polled = poll_once(&executor, &registered, &Arc::new(Running::default())).await;
        assert_eq!(polled.claims, 0, "no partition should have been visited");
        assert_eq!(polled.contention, None);
        dbos.shutdown().await;
    }

    /// **A spent queue-wide rate limit ends the walk on its first claim.** The pending count
    /// cannot see a rate limit, so one claim is paid to learn it; that claim reports the limit,
    /// and since it spans every partition the rest are not visited.
    #[tokio::test]
    async fn a_walk_with_the_queue_wide_rate_limit_spent_stops_after_one_claim() {
        let queue = crate::sysdb::types::NewQueue {
            rate_limit: Some(crate::sysdb::types::RateLimit {
                limit: 1,
                period: Duration::from_secs(3600),
            }),
            partition_concurrency: Some(1),
            ..crate::sysdb::types::NewQueue::new("walked")
        };
        let (dbos, executor, db, registered) = partitioned_backlog(queue, 50).await;
        started_elsewhere(&executor, &db, "walked", "elsewhere").await;

        let polled = poll_once(&executor, &registered, &Arc::new(Running::default())).await;
        assert_eq!(
            polled.claims, 1,
            "the first claim's stop should end the walk"
        );
        assert_eq!(polled.contention, None);
        dbos.shutdown().await;
    }

    /// **A partition at its own limit does not end the walk.** Only a limit counted across the
    /// whole queue says anything about the next key, so a walk that meets a full partition skips
    /// it and goes on to claim from every other one.
    #[tokio::test]
    async fn a_full_partition_does_not_end_the_walk() {
        // A queue-wide concurrency with room to spare keeps this queue off the batched sweep, so
        // the poll walks its partitions one claim at a time.
        let queue = crate::sysdb::types::NewQueue {
            concurrency: Some(100),
            partition_concurrency: Some(1),
            ..crate::sysdb::types::NewQueue::new("walked")
        };
        let (dbos, executor, db, registered) = partitioned_backlog(queue, 5).await;
        // Exact only where every claim delivers what it is entitled to. On CockroachDB a claim can
        // come back short of a row enqueued moments ago, leaving one of the open partitions'
        // rows waiting through no fault of the walk.
        if matches!(db.backend(), dbos_test_support::Backend::Cockroach) {
            eprintln!("skipped on CockroachDB: a claim may under-deliver; see UPSTREAM item 7");
            dbos.shutdown().await;
            return;
        }
        // `p-0` is at its one-at-a-time limit; the other four partitions are open.
        started_elsewhere(&executor, &db, "walked", "p-0").await;

        let polled = poll_once(&executor, &registered, &Arc::new(Running::default())).await;
        assert_eq!(
            polled.claims, 5,
            "every partition visited, the full one included, wherever the shuffle put it"
        );
        assert_eq!(polled.contention, None);
        let waiting: Vec<String> = sqlx::query_scalar(
            r#"SELECT workflow_uuid FROM "dbos"."workflow_status"
               WHERE queue_name = 'walked' AND status = 'ENQUEUED'"#,
        )
        .fetch_all(&db.pool().await)
        .await
        .unwrap();
        assert_eq!(
            waiting,
            ["wf-waiting-0"],
            "only the full partition's row should still be waiting"
        );
        dbos.shutdown().await;
    }

    /// **A walk stops once it has claimed the room the queue had**, rather than visiting the
    /// remaining partitions to be told each time that the queue is now full.
    #[tokio::test]
    async fn a_walk_stops_once_the_queue_wide_room_is_claimed() {
        let queue = crate::sysdb::types::NewQueue {
            concurrency: Some(2),
            partition_concurrency: Some(1),
            ..crate::sysdb::types::NewQueue::new("walked")
        };
        let (dbos, executor, db, registered) = partitioned_backlog(queue, 50).await;
        // Exact only where every claim delivers what it is entitled to. On CockroachDB a claim can
        // come back short of a row enqueued moments ago, and each short claim costs the walk one
        // more visit; the walk is still bounded there, just not to this count.
        if matches!(db.backend(), dbos_test_support::Backend::Cockroach) {
            eprintln!("skipped on CockroachDB: a claim may under-deliver; see UPSTREAM item 7");
            dbos.shutdown().await;
            return;
        }

        let polled = poll_once(&executor, &registered, &Arc::new(Running::default())).await;
        assert_eq!(
            polled.claims, 2,
            "one claim per unit of room, and none after it is spent"
        );
        assert_eq!(polled.contention, None);
        // Counted as no longer waiting rather than as pending: the claimed workflows are
        // dispatched, and may already have finished.
        let claimed: i64 = sqlx::query_scalar(
            r#"SELECT count(*) FROM "dbos"."workflow_status"
               WHERE queue_name = 'walked' AND status <> 'ENQUEUED'"#,
        )
        .fetch_one(&db.pool().await)
        .await
        .unwrap();
        assert_eq!(claimed, 2, "exactly the room the queue had");
        dbos.shutdown().await;
    }

    /// A row that moved on since the claim is not started, and is not parked either, however many
    /// attempts it carries; a `PENDING` one runs up to the limit and is parked past it.
    #[test]
    fn a_claimed_row_runs_only_while_pending_and_within_its_attempts() {
        let limit = MAX_RECOVERY_ATTEMPTS + 1;
        for status in [
            WorkflowStatus::Cancelled,
            WorkflowStatus::Enqueued,
            WorkflowStatus::Success,
        ] {
            assert_eq!(admission(status, 1), Admission::NotPending, "{status}");
            assert_eq!(
                admission(status, limit + 1),
                Admission::NotPending,
                "{status}"
            );
        }
        assert_eq!(admission(WorkflowStatus::Pending, 1), Admission::Run);
        assert_eq!(admission(WorkflowStatus::Pending, limit), Admission::Run);
        assert_eq!(
            admission(WorkflowStatus::Pending, limit + 1),
            Admission::Park
        );
    }

    /// A failed read of the database's clock times the batch from this host's instead, so its
    /// deadlines still run out rather than holding the rows back.
    #[test]
    fn a_failed_clock_read_times_from_this_host() {
        let in_a_minute = Timestamp::from_epoch_ms(Timestamp::now().as_epoch_ms() + 60_000);
        let expires =
            timing_clock(Err(sysdb::Error::Malformed("no clock".to_owned()))).reaches(in_a_minute);
        let left = expires.saturating_duration_since(std::time::Instant::now());
        assert!(
            (Duration::from_secs(58)..=Duration::from_secs(60)).contains(&left),
            "{left:?}"
        );

        let read = DatabaseTime::read(0);
        assert_eq!(timing_clock(Ok(read)), read, "a reading is used as it is");
    }
}
