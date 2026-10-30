//! Running a workflow durably.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::AbortHandle;
use tracing::Instrument;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::connection::Connection;
use crate::context::Ctx;
use crate::error::{DurableError, EncodedError, Error, Failure, Result, refusal_detail};
use crate::handle::{ChildResultPlacement, WorkflowHandle};
use crate::instance::Executor;
use crate::registry::{QualifiedWorkflowName, WorkflowRef};
use crate::serialization::{EncodedOutput, PayloadEncoding, SerializationFormat, encode_input};
use crate::sysdb::types::{
    AwaitedOutcome, DatabaseTime, InitWorkflowCaller, NewWorkflow, Outcome, OutcomeWrite,
    StepCaller, StepTiming, Timestamp, WorkflowIdReusePolicy, WorkflowInitResult,
    decode_step_value, encode_step_value, validate_positive_duration,
};

/// Attempts before a workflow is parked as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
///
/// The default every implementation shares. It only counts recoveries and dequeues; starting a
/// workflow fresh never approaches it.
pub(crate) const MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// The tasks of one [`Role`] this executor started and has not seen finish.
///
/// Abort handles to reach them with, and a count of how many are still alive to wait on. Nothing
/// here holds a `JoinHandle`: a caller awaiting its own workflow holds that, and a caller that
/// dropped its future is no longer waiting though the workflow is still running — which is exactly
/// the case shutdown has to reach. The count stands in for joining, and is what lets shutdown mean
/// "quiet" rather than "told to stop".
#[derive(Default)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    /// Woken as each task's future is dropped, so [`all_ended`](Tasks::all_ended) can wait for the
    /// last one out without polling for it.
    ended: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    /// Each task's abort handle, with what to call it in a log: the workflow id, for an execution.
    running: Vec<(AbortHandle, String)>,
    /// How many spawned futures exist and have not been dropped.
    live: usize,
    /// Set by [`abort_all`](Tasks::abort_all). A task registered after the sweep is cancelled on
    /// arrival rather than added to a list nothing will read again.
    closed: bool,
}

impl Tasks {
    /// Records a running task, dropping the handles of any that have since finished.
    ///
    /// Pruning on insert rather than on completion keeps this to one lock and no bookkeeping ids.
    /// The cost is that a finished task's handle lingers until the next one starts, which is a
    /// pointer and a label, and the benefit is that a workflow whose caller dropped its future is
    /// still reachable by shutdown — which is the case that matters.
    ///
    /// A task arriving after the sweep is aborted here instead. That is the race between spawning
    /// and registering: a workflow started as shutdown ran would otherwise never be cancelled and
    /// would outlive the shutdown meant to stop it.
    fn insert(&self, handle: AbortHandle, label: String) {
        let mut state = self.lock();
        if state.closed {
            drop(state);
            handle.abort();
            return;
        }
        state.running.retain(|(handle, _)| !handle.is_finished());
        state.running.push((handle, label));
    }

    /// Counts a task about to be spawned.
    fn arrived(&self) {
        self.lock().live += 1;
    }

    /// Un-counts one whose future has been dropped, waking a shutdown waiting for the last.
    ///
    /// Runs from a `Drop`, including one unwinding out of a panicking workflow, so it takes care
    /// not to panic itself: the count cannot legitimately go below zero — one guard per task,
    /// dropped once — and saturating rather than wrapping keeps a bug here from becoming an abort.
    fn departed(&self) {
        {
            let mut state = self.lock();
            state.live = state.live.saturating_sub(1);
        }
        self.ended.notify_waiters();
    }

    /// The labels of the tasks still running, for a shutdown that is waiting on them to say what
    /// it is waiting on.
    pub(crate) fn running(&self) -> Vec<String> {
        self.lock()
            .running
            .iter()
            .filter(|(handle, _)| !handle.is_finished())
            .map(|(_, label)| label.clone())
            .collect()
    }

    /// Waits for every task to end on its own, up to `deadline`, reporting whether they all did.
    ///
    /// Nothing is cancelled, at the deadline or before it: what is still running is left to
    /// [`abort_all`](Self::abort_all).
    pub(crate) async fn ended_by(&self, deadline: tokio::time::Instant) -> bool {
        tokio::time::timeout_at(deadline, self.all_ended())
            .await
            .is_ok()
    }

    /// Cancels every task still running and waits for them to stop, returning the labels of those
    /// it cancelled.
    ///
    /// A cancelled workflow's row stays `PENDING`, which is the point: it is one a later executor
    /// recovers, so an abrupt shutdown loses no work. Writing a terminal status here would be the
    /// bug — it would mark as finished something that never finished.
    ///
    /// **The wait is the half that makes this mean anything.** `abort` is not synchronous; it
    /// schedules cancellation at the task's next yield point, so without waiting, shutdown returns
    /// while the bodies it cancelled may still be running. The wait is unbounded, matching what
    /// `close` already does with its listener: a task that ignores cancellation should be a hang
    /// that gets fixed rather than a warning logged forever.
    pub(crate) async fn abort_all(&self) -> Vec<String> {
        let cancelled = {
            let mut state = self.lock();
            state.closed = true;
            let mut cancelled = Vec::new();
            for (handle, label) in state.running.drain(..) {
                if !handle.is_finished() {
                    handle.abort();
                    cancelled.push(label);
                }
            }
            cancelled
        };
        self.all_ended().await;
        cancelled
    }

    /// Waits until no task's future is left alive.
    async fn all_ended(&self) {
        loop {
            // Registered before the count is read, so a task ending in between is not missed:
            // `notify_waiters` wakes only those already waiting.
            let ended = self.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            // Bound to a `let` so the guard is released before the await, not held across it.
            let live = self.lock().live;
            if live == 0 {
                return;
            }
            ended.await;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What a spawned task does for the executor, which decides when shutdown stops it.
///
/// Each role is its own [`Tasks`], because shutdown treats them in turn rather than all at once:
/// the producers stop, the workflows drain, and only then are the monitors stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// Starts workflows in this process, or enqueues them: the dequeue loop and the scheduler.
    /// Stopped first, so the set being drained cannot grow.
    Producer,
    /// Runs a workflow, or a child's start on its parent's behalf. What shutdown drains.
    Workflow,
    /// Watches workflows: the timeout sweep. Stopped after the drain, because a workflow still
    /// running may be waiting on it to record its timeout.
    Monitor,
}

/// Keeps a spawned task counted for as long as its future exists.
///
/// Built *before* the spawn and moved into the future, which is the whole trick: it is then
/// dropped however the task ends — completed, panicked, or cancelled before its first poll. A
/// guard created inside the body would miss that last case entirely and leave
/// [`abort_all`](Tasks::abort_all) waiting forever on a task that never started.
struct TaskGuard(Arc<Executor>, Role);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.tasks(self.1).departed();
    }
}

/// Spawns `future` on the executor's runtime, counted and reachable by shutdown.
///
/// Two things every spawn here needs and both are easy to get subtly wrong alone: the abort handle
/// is what lets shutdown *reach* the task, and the guard is what lets it *wait* for it. Doing them
/// in one place is also what orders them correctly — the guard exists before the spawn, so a
/// shutdown racing this one either aborts the task through `insert` or waits for it through the
/// count, and never misses it through both.
///
/// `label` is what shutdown calls the task when it reports one it is waiting on or cancelled: the
/// workflow id, for an execution.
pub(crate) fn spawn_tracked<F>(
    executor: &Arc<Executor>,
    role: Role,
    label: String,
    future: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let tasks = executor.tasks(role);
    tasks.arrived();
    let guard = TaskGuard(Arc::clone(executor), role);
    let task = executor.runtime().clone().spawn(async move {
        let _guard = guard;
        future.await
    });
    tasks.insert(task.abort_handle(), label);
    task
}

/// Logs a panic escaping a workflow body, from inside the unwind.
///
/// A panic is deliberately not an outcome — nothing is recorded and the row stays `PENDING`, the
/// same as a crash — but it must not be *silent*. Tokio parks the panic in the `JoinHandle`, and a
/// caller that dropped its future never joins it, so without this the only evidence would be the
/// panic hook's stderr line: no workflow id, nothing about the durable consequence.
///
/// A drop guard because the unwind is the only place the case can be observed: an async block's
/// live locals are dropped as the panic unwinds out of `poll`, with `std::thread::panicking()`
/// true, and the workflow span still entered.
///
/// `panicking()` is the whole of the discrimination, which is why there is nothing to defuse. The
/// guard is dropped on every path — completion, panic, and the abort that shutdown issues — and
/// only the middle one is inside an unwind.
struct PanicLog<'a> {
    workflow_id: &'a str,
}

impl Drop for PanicLog<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(
                workflow_id = self.workflow_id,
                "the workflow body panicked: no outcome is recorded, and the row stays PENDING \
                 for a later executor to recover"
            );
        }
    }
}

/// What a caller may say about a run, beyond the input.
///
/// A struct rather than a builder, matching how `sysdb` spells optional arguments; the common case
/// pays nothing because [`run`](WorkflowRef::run) and [`start`](WorkflowRef::start) keep their
/// no-option forms. Deliberately **not** `#[non_exhaustive]` — that would forbid the
/// `..Default::default()` form this type is built around. Adding a field stays non-breaking for
/// every caller who wrote it.
#[derive(Debug, Clone, Default)]
pub struct RunOptions<'a> {
    /// The workflow's id, in place of a generated one.
    ///
    /// A caller-supplied id is an idempotency key: starting the same id twice joins the workflow
    /// already running — or already finished — rather than failing the second caller, unless
    /// [`workflow_id_reuse_policy`](Self::workflow_id_reuse_policy) says to refuse.
    pub workflow_id: Option<&'a str>,
    /// What the start does when the id already names another caller's workflow: join it (the
    /// default), or refuse with [`WorkflowIdInUse`](crate::sysdb::Error::WorkflowIdInUse).
    /// [`Reject`](WorkflowIdReusePolicy::Reject) needs a [`workflow_id`](Self::workflow_id), and
    /// is refused as [`Error::InvalidArgument`] without one.
    pub workflow_id_reuse_policy: WorkflowIdReusePolicy,
    /// How long the whole workflow may take, and whether it takes a parent's budget when it names
    /// none of its own. Defaults to [`Timeout::Inherit`].
    ///
    /// **Durable, and unlike a step timeout it cancels rather than fails.** The budget becomes a
    /// wall-clock deadline stored on the row, so it survives a crash: a workflow recovered with
    /// two minutes left has two minutes, not the whole budget again. On expiry the row goes
    /// `CANCELLED` and callers awaiting it get [`Error::WorkflowCancelled`], which is what all four
    /// references do. Every executor of the application sweeps for expired deadlines, so the row is
    /// cancelled whether or not anything is running it: a queued workflow nobody has dequeued, or
    /// one whose executor died, times out too.
    ///
    /// A step's [`timeout`](crate::StepOptions::timeout) bounds one attempt and is recorded as a
    /// step failure; this bounds everything and is not the workflow's *outcome* at all — a
    /// cancelled workflow was interrupted, not wrong.
    pub timeout: Timeout,

    /// Caller-supplied attributes, stored as JSON on the workflow's row.
    ///
    /// Searchable metadata: a tenant, a request id, a trace context. Plain JSON, never the
    /// configured [`Serializer`](crate::Serializer) — the column is read by containment and by
    /// every other implementation, so a workflow's own payload encoding has no say in it.
    ///
    /// **Not inherited by a child**, which starts with whatever its own call names. TypeScript
    /// says the same of its `attributes` in as many words; nothing propagates here because a child
    /// is started through this same type.
    pub attributes: Option<&'a serde_json::Map<String, serde_json::Value>>,
}

/// What a caller may say about a start, beyond the input.
///
/// [`RunOptions`] plus a [`queue`](Self::queue), and that one extra field is why the two are
/// separate types. Enqueueing means some *other* executor runs the workflow, at some later time;
/// running means this process executes it and waits. There is no coherent reading of the two
/// together — a queued `run_with` could only block on work this process is not doing, or silently
/// ignore the queue — so the signature refuses it rather than the body, and no `run_with` caller
/// has to read about a field they cannot use.
///
/// Every `RunOptions` [converts](RunOptions) into one of these, because
/// [`run_with`](WorkflowRef::run_with) *is* [`start_with`](WorkflowRef::start_with) plus an await.
#[derive(Debug, Clone, Default)]
pub struct StartOptions<'a> {
    /// The workflow's id, in place of a generated one, and an idempotency key — see
    /// [`RunOptions::workflow_id`].
    pub workflow_id: Option<&'a str>,
    /// What the start does when the id already names another caller's workflow — see
    /// [`RunOptions::workflow_id_reuse_policy`].
    pub workflow_id_reuse_policy: WorkflowIdReusePolicy,
    /// How long the whole workflow may take, and what it does about a parent's budget — see
    /// [`RunOptions::timeout`].
    pub timeout: Timeout,
    /// A queue to leave this workflow on, instead of running it here.
    ///
    /// The workflow is recorded `ENQUEUED` and **not started**: whichever executor next polls that
    /// queue claims it and runs it, under whatever limits the queue carries. The handle returned is
    /// a polling one, because the process that asked is usually not the process that runs it.
    ///
    /// Everything an enqueue can ask for lives in [`Enqueue`] rather than beside this field, which
    /// is what makes the five queue-only options unstatable without a queue — see that type.
    pub queue: Option<Enqueue<'a>>,

    /// Caller-supplied attributes, stored as JSON on the workflow's row.
    ///
    /// Searchable metadata: a tenant, a request id, a trace context. Plain JSON, never the
    /// configured [`Serializer`](crate::Serializer) — the column is read by containment and by
    /// every other implementation, so a workflow's own payload encoding has no say in it.
    ///
    /// **Not inherited by a child**, which starts with whatever its own call names — see
    /// [`RunOptions::attributes`].
    pub attributes: Option<&'a serde_json::Map<String, serde_json::Value>>,
}

/// A queue to leave a workflow on, and what to ask of it.
///
/// **The queue-only options are nested here rather than sitting beside
/// [`StartOptions::queue`](StartOptions::queue), and that is the whole design.** A deduplication
/// id, a priority, a partition key, a delay and a [`duplication_policy`](Self::duplication_policy)
/// each mean nothing without a queue: Go checks all five at start and returns `InvalidOptionError`
/// for each, including the policy (`workflow.go`), which is five runtime errors describing states
/// its type system allowed it to build. Owning them from the queue makes the same five
/// unrepresentable — there is no queue-less value here to hang them on. Two rules survive as
/// refusals at start, because no shape can take them:
///
/// - **A [`priority`](Self::priority) must be between 1 and [`i32::MAX`].** `0` is the stored
///   sentinel for unprioritised, so accepting it would give that state a second spelling that
///   reads like a real priority.
/// - **[`DuplicationPolicy::ReturnExisting`] needs a
///   [`deduplication_id`](Self::deduplication_id)**, because a policy for resolving a collision is
///   meaningless where nothing can collide. Python and Go refuse the same pair
///   (`_enqueue_options.py`, `workflow.go`), and keeping the two apart is what makes the ordinary
///   enqueue — a key, no policy — the short one to write.
///
/// TypeScript groups the same options into an `EnqueueOptions` bag (`system_database.ts`), which is
/// the nearest precedent; Go and Python keep them flat on their options struct.
///
/// The name is the address — a [`Queue`](crate::Queue) receipt is not needed to enqueue onto one,
/// and a queue registered by a peer is as valid a destination as one registered here.
/// [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE) is a legitimate destination too, and is where
/// `resume` and `fork` put work.
///
/// ```no_run
/// # async fn f(workflow: &dbos::WorkflowRef<(), String>) -> dbos::Result<()> {
/// workflow.start_with((), dbos::StartOptions {
///     queue: Some(dbos::Enqueue {
///         deduplication_id: Some("order-42"),
///         ..dbos::Enqueue::new("demo-queue")
///     }),
///     ..Default::default()
/// }).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enqueue<'a> {
    /// The queue's name, which is its address.
    pub name: &'a str,
    /// A key no two waiting workflows on this queue may share.
    ///
    /// The key is held only while the workflow is waiting — it is cleared when the workflow
    /// finishes — so it deduplicates a *backlog*, not a history: enqueueing the same key again
    /// after the first one completed is a new workflow, not a duplicate.
    ///
    /// A second enqueue under a held key is refused, unless [`duplication_policy`](Self::duplication_policy)
    /// asks to join the holder instead.
    ///
    /// **Held across the whole queue, partitions included.** On a partitioned queue a key held by a
    /// workflow in one partition blocks the same key in every other, and under
    /// [`DuplicationPolicy::ReturnExisting`] the caller joins the holder whichever partition it is
    /// in. For a key scoped to one partition, put the partition key into it, for example
    /// `format!("{tenant}:{key}")`.
    pub deduplication_id: Option<&'a str>,
    /// Dequeue order among this queue's waiting workflows, **lower first**.
    ///
    /// Every queue honours it: the dequeue orders a queue's waiting workflows by priority, and
    /// by `created_at` among equal priorities.
    ///
    /// **`None` is not "priority zero".** The column's `0` is the references' sentinel for
    /// *unprioritised*, and it sorts ahead of every explicit priority — TypeScript says so in as
    /// many words (*"starting from 1 ~ 2,147,483,647. Default 0 (highest priority)"*). Letting a
    /// caller write `Some(0)` would spell one state two ways, so the range starts at 1 and `None`
    /// is the only way to say "unprioritised".
    pub priority: Option<u32>,
    /// Which partition of a partitioned queue this workflow belongs to.
    ///
    /// A partitioned queue runs at most one workflow per partition at a time, so the key is the
    /// unit of ordering: work sharing a key runs in sequence, and work under different keys runs
    /// concurrently.
    ///
    /// **Required on a partitioned queue, and not checked.** A partitioned queue is dequeued one
    /// partition at a time, and its partitions are the keys present on its rows, so a workflow
    /// enqueued on one without a key is in no partition and is never dequeued. The enqueue does not
    /// read the queue's configuration to catch this, since that would cost a round trip on every
    /// enqueue. On an unpartitioned queue a key is stored and ignored.
    ///
    /// A [`deduplication_id`](Self::deduplication_id) set beside it is held across the whole
    /// queue, not within this partition.
    pub partition_key: Option<&'a str>,
    /// How long to hold the workflow before it may be dequeued at all.
    ///
    /// The row goes in `DELAYED` rather than `ENQUEUED` and the supervisor moves it across when
    /// the delay expires — [`NewWorkflow::initial_status`](crate::sysdb::types::NewWorkflow::initial_status)
    /// derives that from this field's presence.
    ///
    /// **A duration, not an instant**, and the wall-clock moment is stamped by the database rather
    /// than computed here: the system database writes it against the same clock it writes
    /// `created_at` with, so a caller's clock skew never reaches the row.
    pub delay: Option<Duration>,

    /// What to do when the [`deduplication_id`](Self::deduplication_id) is already held.
    ///
    /// Named as Python and TypeScript name it — `duplication_policy` and `duplicationPolicy`,
    /// beside their `deduplication_id` — rather than shortened; Go says `DeduplicationPolicy`.
    ///
    /// **Its own field rather than riding inside the key**, though the two are meaningless apart
    /// and keeping them separate costs a rule this type has to refuse at start. Folding them into
    /// one value makes the invalid pair unrepresentable, but only by making the *ordinary* enqueue
    /// name a policy it does not care about: every implementation defaults to rejecting, and every
    /// reference's queue tutorial leads with a bare deduplication id, keeping `return-existing`
    /// for the singleton-workflow section that follows. The common case stays a key and nothing
    /// else.
    pub duplication_policy: DuplicationPolicy,
    /// How the enqueued workflow's input, output and error are encoded.
    ///
    /// [`SerializationFormat::Default`] takes the workflow's registration —
    /// [`WorkflowOptions::serialization`](crate::WorkflowOptions::serialization) — and
    /// [`SerializationFormat::Portable`] or [`SerializationFormat::Native`] overrides it for this
    /// one enqueue.
    pub serialization: SerializationFormat,
}

/// What an enqueue does when its [`deduplication_id`](Enqueue::deduplication_id) is already held.
///
/// Only meaningful with a key: an enqueue with no deduplication id has nothing to collide with,
/// and asking for [`ReturnExisting`](Self::ReturnExisting) without one is refused rather than
/// ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DuplicationPolicy {
    /// Refuse the enqueue, reporting the collision.
    ///
    /// The default, and what every implementation defaults to. A caller who did not think about
    /// collisions should hear that the key was taken, rather than silently joining a workflow
    /// whose arguments are somebody else's.
    ///
    /// **Started from inside a workflow, the error is recorded at the parent's step**, so a replay
    /// of the parent gets the same error and starts nothing, even once the holder has finished and
    /// the key is free.
    #[default]
    Reject,
    /// Hand back a handle to the workflow already holding the key.
    ///
    /// **Idempotent enqueue**: the first caller's workflow is the one that runs, and every later
    /// caller waits on it instead of being told no — so this caller's arguments are discarded,
    /// which is the half worth knowing. Three implementations offer the choice — TypeScript's
    /// `duplicationPolicy` (`dbos.ts`), Python's `duplication_policy` (`_context.py`) and Go's
    /// `DeduplicationPolicy` (`workflow.go`) — and all three require a key, as this does. Java is
    /// the one that always rejects, though it has the same lookup underneath: its `DebouncerClient`
    /// hand-rolls the join with `findDeduplicationHolder`. All three document it as how a
    /// *singleton workflow* is written.
    ///
    /// The key is held only while the holder is *waiting*, so this joins a backlog, not a history:
    /// once the holder has finished, the same key enqueues a new workflow.
    ///
    /// **Started from inside a workflow, the parent's launch record points at the workflow that was
    /// joined**, not at the id this call derived — so a replay resolves to the same workflow rather
    /// than trying to start the child again. Go records the same mapping at the same reserved step
    /// id, for the same stated reason (`workflow.go`).
    ///
    /// **A joined workflow is a child by one measure and not by the other, deliberately.** The
    /// relationship is stored twice: `operation_outputs.child_workflow_id` at the parent's step,
    /// which is a pointer from one *position* and is what a replay reads, and
    /// `workflow_status.parent_workflow_id` on the child's own row, which names its one owner and
    /// is what [`get_workflow_children`](crate::sysdb::SystemDatabase::get_workflow_children) and
    /// the cascading forms of cancel and delete walk. A join writes only the first, and cannot
    /// write the second: the holder already has an owner, the column holds one value where any
    /// number of callers may join one workflow, and re-parenting it would let *this* parent's
    /// cascade cancel or delete a workflow somebody else created and is waiting on. So a joined
    /// workflow is resolved by the parent's replay and named among its steps, but is not listed
    /// among its children and does not go down with it. Go's join path records the same one of the
    /// two.
    ReturnExisting,
}

impl<'a> Enqueue<'a> {
    /// A plain enqueue onto `name`, asking for nothing else.
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            deduplication_id: None,
            priority: None,
            partition_key: None,
            delay: None,
            // The enum's `#[default]`, not a second copy of it: "reject unless asked" is stated
            // once, where the variants are.
            duplication_policy: DuplicationPolicy::default(),
            serialization: SerializationFormat::Default,
        }
    }

    /// Rejects an enqueue no queue could honour.
    ///
    /// **Only what the shape could not rule out.** Nesting these options under the queue already
    /// makes "a delay with no queue" and its three siblings unbuildable, so what is left is the
    /// rules that are about the options themselves:
    ///
    /// - **[`DuplicationPolicy::ReturnExisting`] needs a deduplication id**, since with no key
    ///   there is no collision to resolve.
    /// - **A priority must be at least 1.** `0` is the stored sentinel for unprioritised, so
    ///   accepting it would give that state a second spelling that reads like a real priority;
    ///   the references' documented range starts at 1 for the same reason. The ceiling is
    ///   [`i32::MAX`] because the column is a signed 32-bit integer.
    /// - **A delay must be more than zero, and fit in milliseconds as a 64-bit integer.** A zero
    ///   delay is a caller that meant `None`. Neither rule reads a clock, so a replay decides it as
    ///   the first run did. A delay that fits is never refused for the instant it reaches: the
    ///   system database resolves it against its own clock and saturates at the largest instant,
    ///   a release that never comes. The system database also refuses everything here again for
    ///   its own callers, reported as the same [`Error::InvalidArgument`] by [`refused_start`].
    ///
    /// Each is an [`Error::InvalidArgument`] to [`ENQUEUE_OPERATION`] naming the queue, through
    /// [`start_refusal`].
    pub(crate) fn validate(&self) -> Result<()> {
        let refused = |message: String| start_refusal(Some(self.name), message);
        let refuse = |message: String| Err(refused(message));

        if self.duplication_policy == DuplicationPolicy::ReturnExisting
            && self.deduplication_id.is_none()
        {
            return refuse(
                "`DuplicationPolicy::ReturnExisting` needs a `deduplication_id`: with no key there is \
                 no collision to resolve"
                    .to_owned(),
            );
        }
        if let Some(priority) = self.priority {
            if priority == 0 {
                return refuse(
                    "`priority` must be at least 1; use `None` for an unprioritised workflow"
                        .to_owned(),
                );
            }
            if priority > i32::MAX as u32 {
                return refuse(format!(
                    "`priority` must be at most {}, got {priority}",
                    i32::MAX
                ));
            }
        }
        if let Some(delay) = self.delay {
            validate_positive_duration("delay", delay).map_err(|error| {
                Error::from_refusal(error, |field, detail| {
                    refused(refusal_detail(field, &detail))
                })
            })?;
        }
        Ok(())
    }

    /// The stored priority: the sentinel `0` when unprioritised.
    ///
    /// Infallible because [`validate`](Self::validate) has already ruled out everything that would
    /// not fit, which is why this takes no `Result` and the cast cannot wrap.
    pub(crate) fn stored_priority(&self) -> i32 {
        self.priority.map_or(0, |priority| priority as i32)
    }
}

/// What a start that runs at once is refused as. A start onto a queue, and with it every client
/// enqueue, is refused as [`ENQUEUE_OPERATION`].
pub(crate) const START_OPERATION: &str = "start a workflow";

/// What a start onto a queue is refused as; see [`START_OPERATION`].
pub(crate) const ENQUEUE_OPERATION: &str = "enqueue a workflow";

/// A start's refusal of one of its arguments: an [`Error::InvalidArgument`] to
/// [`ENQUEUE_OPERATION`] when it names a queue, with the queue in the detail because nothing
/// else in the refusal says which enqueue it was, and to [`START_OPERATION`] when it does not.
fn start_refusal(queue: Option<&str>, detail: String) -> Error {
    match queue {
        Some(queue) => Error::InvalidArgument {
            operation: ENQUEUE_OPERATION.into(),
            detail: format!("queue `{queue}`: {detail}"),
        },
        None => Error::InvalidArgument {
            operation: START_OPERATION.into(),
            detail,
        },
    }
}

/// A start's error, with the system database's refusal of its arguments reported as the start's
/// own, through [`start_refusal`].
///
/// Every value of the row the caller supplied and the insert refuses — an empty `workflow_id`,
/// `deduplication_id` or `partition_key`, a duration too long to store — named as the start's API
/// spells it, which is not always the column's name. A child start records such a refusal at its
/// parent's step as the system database's error, so a replay reads it back in the same form and
/// this maps it the same way. **The refusal is what replays; its wording is not recorded.** The
/// operation and the queue in the detail come from the call making it, so a replay fails as the
/// first run did, with the same variant, field and reason, and names its own queue if its options
/// name a different one.
pub(crate) fn refused_start(queue: Option<&str>, error: Error) -> Error {
    match error {
        Error::SystemDatabase(crate::sysdb::Error::InvalidInput { field, detail }) => {
            start_refusal(queue, refusal_detail(api_field(&field), &detail))
        }
        other => other,
    }
}

/// The name a start's API gives a column the system database refuses by name: the queue's
/// [`name`](Enqueue::name) and [`partition_key`](Enqueue::partition_key), and a client's
/// `workflow` and [`app_version`](crate::EnqueueOptions::app_version). Every other column is
/// spelled as the API spells it.
fn api_field(field: &str) -> &str {
    match field {
        "queue_name" => "name",
        "queue_partition_key" => "partition_key",
        "name" => "workflow",
        "application_version" => "app_version",
        other => other,
    }
}

/// Refuses a start's arguments that no row could hold, or that mean nothing together:
/// [`WorkflowIdReusePolicy::Reject`] without a `workflow_id`, its enqueue's, through
/// [`Enqueue::validate`], and a timeout of zero or one too long to store as milliseconds. Each is
/// an [`Error::InvalidArgument`], through [`start_refusal`].
///
/// A reuse policy is about an id the caller chose, so `Reject` needs one: with none, a root start
/// has a fresh id nothing can have taken, and a child's is derived from its parent's, an id the
/// caller never asked to be new.
///
/// The one check every surface that starts or enqueues a workflow makes before it writes
/// anything — [`WorkflowRef::start_with`] before it takes a step id, and the client's enqueues
/// before they encode the input — so a refusal costs no row and no step id. None of it reads a
/// clock, so a replay decides it as the first run did. The system database refuses the same
/// values again for its own callers.
pub(crate) fn validate_start(
    enqueue: Option<&Enqueue<'_>>,
    timeout: Option<Duration>,
    workflow_id: Option<&str>,
    reuse_policy: WorkflowIdReusePolicy,
) -> Result<()> {
    if reuse_policy == WorkflowIdReusePolicy::Reject && workflow_id.is_none() {
        return Err(start_refusal(
            enqueue.map(|enqueue| enqueue.name),
            "`WorkflowIdReusePolicy::Reject` needs a `workflow_id`: the policy is about an id the \
             caller chose"
                .to_owned(),
        ));
    }
    if let Some(enqueue) = enqueue {
        enqueue.validate()?;
    }
    if let Some(timeout) = timeout {
        validate_positive_duration("timeout", timeout).map_err(|error| {
            Error::from_refusal(error, |field, detail| {
                start_refusal(
                    enqueue.map(|enqueue| enqueue.name),
                    refusal_detail(field, &detail),
                )
            })
        })?;
    }
    Ok(())
}

/// A new workflow row with its queue-shaped half filled in: the five columns an [`Enqueue`]
/// decides, and what they are when there is no queue.
///
/// The base both literals that create a workflow build on —
/// [`WorkflowRef::start_with`](WorkflowRef::start_with), where a queue is one option among
/// several, and [`Client::enqueue_with`](crate::Client::enqueue_with), where there is always one.
/// Written once because the two must agree about what a queue owns: which columns it fills, and
/// that `priority` is a `NOT NULL` column whose unprioritised value is the sentinel `0` — zero
/// also being what a workflow that was never enqueued stores, since it has no order to keep.
///
/// The status is not among them. A queued row goes in `ENQUEUED` rather than `PENDING`, and a
/// [`delay`](Enqueue::delay) makes it `DELAYED`, but `initial_status` derives both from these
/// columns rather than a caller stating them.
pub(crate) fn new_row<'a>(workflow_id: &'a str, enqueue: Option<&Enqueue<'a>>) -> NewWorkflow<'a> {
    NewWorkflow {
        queue_name: enqueue.map(|enqueue| enqueue.name),
        deduplication_id: enqueue.and_then(|enqueue| enqueue.deduplication_id),
        priority: enqueue.map_or(0, Enqueue::stored_priority),
        queue_partition_key: enqueue.and_then(|enqueue| enqueue.partition_key),
        delay: enqueue.and_then(|enqueue| enqueue.delay),
        ..NewWorkflow::new(workflow_id)
    }
}

/// Everything a start needs after the point it can no longer be abandoned, owned outright.
///
/// [`StartOptions`] borrows — an id, a queue's name, a partition key are all `&'a str` — and the
/// half of a start that writes rows runs in a task of its own, which outlives the future that
/// spawned it and so can hold nothing borrowed from it. This is that half's copy, taken while the
/// caller's future is still there to take it from.
///
/// **Owned, and encoded too**, which is the one thing the name understates: the payloads are
/// serialised here rather than in the task, so a value that cannot be serialised fails the call
/// itself. Encoding is the one part of a start that can fail without touching the database, and it
/// would be a poor trade to answer that from a task the caller may already have stopped listening
/// to.
pub(crate) struct OwnedStart {
    /// The id the child will have, resolved before the task is spawned — see
    /// [`child_workflow_id`] for why it is settled at the call rather than in the task.
    pub(crate) workflow_id: String,
    pub(crate) workflow_id_reuse_policy: WorkflowIdReusePolicy,
    pub(crate) timeout: Timeout,
    pub(crate) queue: Option<OwnedEnqueue>,
    pub(crate) input: Option<String>,
    /// How `input` is encoded, and so how the workflow's outcome will be.
    pub(crate) encoding: PayloadEncoding,
    pub(crate) attributes: Option<String>,
    /// When the start began, which the parent's record of it is stamped with. Read here rather
    /// than in the task so that it measures the call and not the wait for a runtime thread.
    pub(crate) started_at: Timestamp,
    /// Marks the row a debounced one, which only a [`Debouncer`](crate::Debouncer) asks for.
    pub(crate) debounce: Option<Debounced>,
    /// The application the row is written for, in place of the executor's own. Only a
    /// [`Debouncer`](crate::Debouncer) targeting another application sets it.
    pub(crate) app_name: Option<String>,
}

/// What makes a fresh enqueue a debounced one: the row goes in with `is_debounced` set, so the
/// release from `DELAYED` clears its deduplication id, and with a debounce timeout the insert
/// resolves into the cap no later bounce may push its release past.
///
/// The row's deduplication id and delay are an ordinary enqueue's, set on its [`Enqueue`]; this
/// carries only the two columns no public option can write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Debounced {
    /// How far past the insert a bounce may move the release, or `None` for no cap. A duration,
    /// which the insert resolves on the database clock along with the delay.
    pub(crate) timeout: Option<Duration>,
}

/// An [`Enqueue`] with its strings owned, for the same reason [`OwnedStart`] is: see there.
pub(crate) struct OwnedEnqueue {
    name: String,
    deduplication_id: Option<String>,
    priority: Option<u32>,
    partition_key: Option<String>,
    delay: Option<Duration>,
    duplication_policy: DuplicationPolicy,
    serialization: SerializationFormat,
}

impl From<&Enqueue<'_>> for OwnedEnqueue {
    fn from(enqueue: &Enqueue<'_>) -> Self {
        Self {
            name: enqueue.name.to_owned(),
            deduplication_id: enqueue.deduplication_id.map(str::to_owned),
            priority: enqueue.priority,
            partition_key: enqueue.partition_key.map(str::to_owned),
            delay: enqueue.delay,
            duplication_policy: enqueue.duplication_policy,
            serialization: enqueue.serialization,
        }
    }
}

impl OwnedEnqueue {
    /// The borrowed form back, so everything below this point reads one type.
    ///
    /// An inherent `as_*` rather than [`AsRef`] or [`Borrow`](std::borrow::Borrow), which both
    /// have to hand back a reference: an [`Enqueue`] is a value *made* of references, so there is
    /// nothing here to lend. `EncodedSendMessage::as_sysdb_message` in `message.rs` is the same
    /// shape for the same reason.
    fn as_enqueue(&self) -> Enqueue<'_> {
        Enqueue {
            name: &self.name,
            deduplication_id: self.deduplication_id.as_deref(),
            priority: self.priority,
            partition_key: self.partition_key.as_deref(),
            delay: self.delay,
            duplication_policy: self.duplication_policy,
            serialization: self.serialization,
        }
    }
}

/// What an insert did: wrote this call's row, or resolved a collision by joining someone else's.
pub(crate) enum Submitted {
    /// The row this call wrote, and what the system database decided about it.
    Created(WorkflowInitResult),
    /// The workflow already holding the deduplication key, which this call joined instead of
    /// writing a row of its own. Only reachable under [`DuplicationPolicy::ReturnExisting`].
    Joined(String),
}

/// Inserts a new workflow, resolving a deduplication collision the way the caller asked.
///
/// A loop, because [`DuplicationPolicy::ReturnExisting`] answers a collision by reading who holds the
/// key, and the holder can finish between those two statements — leaving the key free and this
/// call with nothing to join, so it tries the insert again. Everything the insert depends on is
/// decided by the caller before the first attempt, so a retry writes the same row rather than a
/// differently-derived one.
///
/// Shared by the two callers that create workflows — [`WorkflowRef::start_with`] and
/// [`Client::enqueue_with`](crate::Client::enqueue_with) — which differ in what they do with the
/// answer and not in how they get it: a start has a parent's launch to record and a body to spawn,
/// a client has neither.
pub(crate) async fn init_or_join(
    conn: &Connection,
    new: &NewWorkflow<'_>,
    policy: DuplicationPolicy,
    reuse_policy: WorkflowIdReusePolicy,
    caller: Option<InitWorkflowCaller<'_>>,
) -> Result<Submitted> {
    loop {
        match conn.sysdb().init_workflow(new, caller, reuse_policy).await {
            Ok(initialized) => return Ok(Submitted::Created(initialized)),
            // **A key another workflow holds, and a caller who asked to join it.** The insert lost
            // on the partial unique index over `(queue_name, deduplication_id)`; the holder's id
            // is the answer, and a handle to it is what [`DuplicationPolicy::ReturnExisting`] promises.
            Err(crate::sysdb::Error::QueueDeduplicated {
                queue_name,
                deduplication_id,
                ..
            }) if policy == DuplicationPolicy::ReturnExisting => {
                match conn
                    .sysdb()
                    .get_deduplication_key_holder(&queue_name, &deduplication_id)
                    .await
                    .map_err(Error::SystemDatabase)?
                {
                    Some(holder) => {
                        tracing::debug!(
                            workflow_id = holder,
                            deduplication_id,
                            "the deduplication key is held; the handle joins its holder"
                        );
                        return Ok(Submitted::Joined(holder));
                    }
                    // The holder finished between the conflict and this read, so the key is free
                    // again: retry the insert rather than report a collision with a workflow that
                    // is over. Python and TypeScript both loop here.
                    None => continue,
                }
            }
            Err(error) => return Err(Error::SystemDatabase(error)),
        }
    }
}

/// Caller-supplied attributes as the row stores them: JSON text, or nothing.
///
/// Plain `serde_json`, never the configured serializer — the column is read by containment and by
/// every other implementation, so a workflow's payload encoding has no say in it. A
/// [`serde_json::Map`] is an object by construction, which is the shape `validate_attributes`
/// insists on downstream; the error arm is here because `to_string` is fallible in principle, not
/// because a map can trip it.
pub(crate) fn encode_attributes(
    attributes: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<String>> {
    attributes
        .map(serde_json::to_string)
        .transpose()
        .map_err(|source| Error::encode_failed("attributes", source))
}

impl<'a> From<RunOptions<'a>> for StartOptions<'a> {
    /// A run is a start that nobody queued.
    fn from(options: RunOptions<'a>) -> Self {
        Self {
            workflow_id: options.workflow_id,
            workflow_id_reuse_policy: options.workflow_id_reuse_policy,
            timeout: options.timeout,
            attributes: options.attributes,
            queue: None,
        }
    }
}

/// How long a workflow may take, and — started from inside another one — what it does about the
/// deadline its parent is under.
///
/// **Three states rather than an `Option<Duration>`, because a child has three things to say and
/// only one of them is silence.** Saying nothing and saying "no limit" are different instructions
/// once there is a parent budget to inherit, and an `Option` collapses them: `None` would have to
/// mean both, and inheritance is the more useful default, so an unbounded child would have been
/// inexpressible.
///
/// **All four references carry these three states**, which is why they are an enum here rather
/// than a convention:
///
/// - **Java** is the same shape under the same first name: a sealed `Timeout` permitting
///   `Timeout.Inherit`, `Timeout.None` and `Timeout.Explicit` (`workflow/Timeout.java`), resolved
///   at `DBOSContext.resolveTimeoutAndDeadline` — where the `None` case clears the propagated
///   deadline *and* the timeout, exactly as [`None`](Self::None) does here. This is the one place
///   Java **is** the model, variant names included; where it is not is its user-facing
///   `withDeadline` and the precedence that follows from it, which Rust lacks.
/// - **TypeScript** spells the three as `number | null | undefined` (`context.ts`) and branches
///   on the middle one under the comment *"Detach child deadline if a null timeout is configured"*
///   (`dbos.ts`, and again at `enqueue_workflow.ts`).
/// - **Python** reaches them through `SetWorkflowTimeout(None)`, whose `__enter__` clears the
///   propagated deadline as well as the timeout (`_context.py`).
/// - **Go** gets all three for free, because its deadline rides on a `context` a caller may
///   decline to pass on.
///
/// **The variant names are Java's**, so the two SDKs spell the same three states the same way and
/// a reader crossing between them has nothing to translate. `Timeout::None` does sit next to the
/// `Option`s this crate is written in, which is worth a moment's care at a call site and worth
/// less than the parity.
///
/// Outside a workflow there is nothing to inherit, so [`Inherit`](Self::Inherit) and
/// [`None`](Self::None) mean the same thing there: no deadline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Timeout {
    /// Say nothing about the budget: a child takes its parent's deadline, a root runs unbounded.
    ///
    /// The default, and the reason the default is this rather than [`None`](Self::None):
    /// a workflow factored out of a parent's body should stay inside the budget that body was
    /// under, without its call site having to say so.
    #[default]
    Inherit,
    /// Run for as long as it likes, **even under a parent that has a deadline**.
    ///
    /// The state an `Option<Duration>` cannot express, and the reason this is an enum: `Inherit`
    /// is a caller who said nothing, and this is a caller who decided. `Timeout.None` in Java,
    /// a `null` timeout in TypeScript, `SetWorkflowTimeout(None)` in Python.
    None,
    /// A budget, counted from now.
    ///
    /// **Replaces an inherited deadline rather than being bounded by it**, even when it is the
    /// longer of the two — so a child given more time than its parent has left outlives its
    /// parent. That is Python's and TypeScript's rule and their shared comment (*"If a timeout is
    /// explicitly specified, use it over any propagated deadline"*), and it is defensible for the
    /// reason it reads: an explicit timeout on one specific child is a statement about that child.
    /// Go takes the earlier of the two instead, but not by design — `context.WithTimeout` composes
    /// as a minimum and nobody wrote a precedence rule.
    Explicit(Duration),
}

impl From<Duration> for Timeout {
    fn from(timeout: Duration) -> Self {
        Self::Explicit(timeout)
    }
}

impl Timeout {
    /// The budget to record on the row, which only [`Explicit`](Self::Explicit) has.
    ///
    /// Neither of the others is a budget: `Inherit` takes an *instant* from its parent and
    /// `None` takes nothing, and a row's `workflow_timeout_ms` is what the database turns into a
    /// deadline: the insert, for a workflow that starts now, and the dequeue's claim, for a queued
    /// one. Writing one for either would hand those a budget the caller never asked for.
    pub(crate) fn budget(self) -> Option<Duration> {
        match self {
            Self::Explicit(timeout) => Some(timeout),
            Self::Inherit | Self::None => Option::None,
        }
    }
}

impl<P, R, E> WorkflowRef<P, R, E>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    E: DurableError,
{
    /// Runs this workflow durably and waits for its result.
    ///
    /// [`start`](Self::start) followed by [`result`](crate::WorkflowHandle::result) — one
    /// mechanism, packaged as the common case. The workflow is recorded before its body starts,
    /// so a process that dies mid-run leaves a `PENDING` row a later executor recovers. Awaiting
    /// is a convenience over the durable run rather than the thing that makes it durable: drop
    /// this future and the workflow carries on.
    ///
    /// Called from inside a running workflow this runs a **child** of it, and takes two of the
    /// parent's step ids rather than one — see [`start_with`](Self::start_with).
    pub fn run(&self, input: P) -> PendingRun<'_, R, E> {
        self.run_with(input, RunOptions::default())
    }

    /// [`run`](Self::run), with something to say about how it starts.
    ///
    /// Takes [`RunOptions`] rather than [`StartOptions`]: a workflow cannot be both queued and
    /// waited for here, so there is no queue to name.
    ///
    /// **Both ids are claimed here, the await's immediately behind the start's**, which is what
    /// makes a set of runs built and then driven together deterministic: the pairs come out
    /// `{start, await}, {start, await}, ..` in the order the calls were *written*, whatever order
    /// the children then finish in. Claiming the await's id when the start's future resolved
    /// would interleave the pairs by completion instead, and a replay does not reproduce that.
    /// The await needs no handle to be placed — where a call stands is decided by the ambient
    /// context and the connection, neither of which the child has any say in — which is the whole
    /// of why the second id can be taken before the first call has run.
    pub fn run_with<'a>(&'a self, input: P, options: RunOptions<'a>) -> PendingRun<'a, R, E> {
        let name: Arc<str> = Arc::from(self.name());
        let options: StartOptions<'a> = options.into();
        // Carried into the future by `placed`, as [`start_with`](Self::start_with) carries its
        // own: a run that could not be placed claimed neither id, so there is no await to place
        // either.
        let built = self.place(&options).map(StartPlacement::into_parts);
        // Immediately behind the start's, and before anything runs — which is why it is taken
        // here rather than inside the run, and why it is taken from the placement's own executor
        // rather than by asking the instance a second time.
        let awaiting = built
            .as_ref()
            .ok()
            .map(|(executor, _)| ChildResultPlacement::of(executor.connection()));
        // The start's placement stands for both halves, which is sound because the two differ
        // only in the id they hold: `check_here` decides on the execution and the step marker,
        // and the await was placed in the same context an instant later. Holding the start's is
        // what keeps [`PendingStep::step_id`] reporting the id a reader would expect of a run —
        // the position the pair begins at.
        PendingWorkflow(PendingStep::placed(
            name,
            built,
            move |executor, placement| async move {
                let handle = self
                    .started(StartPlacement::of(executor, placement), input, options)
                    .await
                    .map_err(Error::lift)?;
                // Matched rather than `?`-ed, so the engine-channel `Result` does not survive as
                // a temporary across the await below — which is what would make this future
                // non-`Send` for a workflow error type that is not. The `None` cannot arise: this
                // run is only reached where the build succeeded, which is where the await was
                // placed.
                let awaiting = match awaiting {
                    Some(Ok(awaiting)) => awaiting,
                    Some(Err(refused)) => return Err(refused.lift()),
                    None => {
                        unreachable!("a run that could not be placed never reaches its own body")
                    }
                };
                handle.settle(awaiting).await
            },
        ))
    }

    /// Everything a start settles **when it is written**: the instance that will serve it, that
    /// the enqueue is one a queue could honour, and which of the parent's step ids it occupies.
    ///
    /// Split out of [`start_with`](Self::start_with) for the third of those. The id has to come
    /// from the parent's counter at the call rather than at the first poll, or a set of starts
    /// driven together is numbered by whichever the combinator reaches first and a replay does not
    /// reproduce it — so the half of a start that decides *where* it stands has to be a plain
    /// function, and only the half that writes rows may be deferred into the future.
    ///
    /// **The checks ahead of the placement are ahead of it on purpose**: a start refused for its
    /// instance, its enqueue or its timeout must move no counter, or the refusal shifts every later step
    /// of the parent onto the wrong replay slot.
    ///
    /// The placement itself is [`StepPlacement::of`]'s, shared with every other library step in
    /// the crate — including the await this start's [`run`](Self::run) pairs it with. Two of its
    /// answers mean something particular to a start, and both are four-of-four:
    ///
    /// - **Inside a step is an error, not a plain call.** A step is a leaf whose checkpoint stands
    ///   for everything its body did, so an id-allocating call inside one would shift every later
    ///   step onto the wrong replay slot. A nested *step* degrades to a plain call because there is
    ///   a plain version of it; there is no undurable version of starting a child. That is why
    ///   [`StepPlacement::InsideStep`] — which every other producer takes as "run this plainly" —
    ///   is turned into [`Error::InsideStep`] here.
    /// - **A `WorkflowRef` from another instance is [`Error::WrongInstance`].** The step id would
    ///   come from this workflow's counter and the start record would be written through the other
    ///   instance's system database, landing where the workflow that allocated it cannot see it.
    ///   `get_event` refuses the same combination for the same reason.
    fn place(&self, options: &StartOptions<'_>) -> Result<StartPlacement> {
        let executor = self.dbos().executor(START_OPERATION)?;
        // Borrowed rather than moved, because `options` is read again by the run, and **validated
        // before anything is written and before the counter moves**: an enqueue no queue could
        // honour should cost a round trip, not a row and not a step id.
        validate_start(
            options.queue.as_ref(),
            options.timeout.budget(),
            options.workflow_id,
            options.workflow_id_reuse_policy,
        )?;
        let placement = StepPlacement::of(executor.connection(), "starting a workflow")?;
        if matches!(placement, StepPlacement::InsideStep { .. }) {
            return Err(Error::InsideStep {
                operation: "starting a workflow".into(),
            });
        }
        Ok(StartPlacement {
            executor,
            placement,
        })
    }

    /// Starts this workflow durably and returns a handle to it, without waiting.
    ///
    /// Called from inside a running workflow this starts a **child** of it — see
    /// [`start_with`](Self::start_with) for what that records and what it costs.
    pub fn start(&self, input: P) -> PendingStart<'_, R, E> {
        self.start_with(input, StartOptions::default())
    }

    /// [`start`](Self::start), with something to say about how.
    ///
    /// Returns as soon as the workflow is recorded and spawned. If the id is already owned —
    /// another process is running it, or a previous run finished it — the handle joins the
    /// existing run rather than this being an error: the id is an idempotency key, and honouring
    /// it is the promise.
    ///
    /// **A start answers in the child's error channel by default**, though it can only fail in the
    /// engine's terms — there is no application error to report, since nothing the application
    /// wrote has run yet, and the *workflow's* failures come out of the handle. Declaring a
    /// channel at all is what lets `start(..).await?` sit in a workflow body beside every other
    /// call rather than needing a [`lift`](Error::lift) the caller has to remember; the await and
    /// the run already answered this way, so the start was the odd one out. A parent whose own
    /// error type is *not* its child's says so with [`PendingStart::lift`], which re-declares the
    /// channel before the await — the one case the default cannot cover, since no conversion
    /// exists between two application channels.
    ///
    /// # Child workflows
    ///
    /// **Called from inside a running workflow, this starts a *child* of it.** The ambient context
    /// is what decides, so there is no separate `start_child` and factoring a workflow body out
    /// into its own workflow does not change how its call sites are written. What changes is what
    /// the engine records:
    ///
    /// - **The child's id is `{parent_id}-{step_id}`**, taken from the parent's step counter,
    ///   unless [`workflow_id`](StartOptions::workflow_id) assigns one. Derived rather than random
    ///   so that a parent recovered mid-run re-derives the same id, finds the child it already
    ///   started, and adopts it instead of starting a second one.
    /// - **The start is a checkpoint of the parent.** A replayed parent gets a handle to the
    ///   recorded child without starting anything — whether that child is still running, finished
    ///   while the parent was dead, or is itself awaiting recovery.
    /// - **Awaiting the handle is a second checkpoint**, recorded as `DBOS.get_result` (see
    ///   [`WorkflowHandle::result`]). So [`run`](Self::run) spends two step ids, and a parent that
    ///   calls it twice has children `{parent}-0` and `{parent}-2`.
    /// - **The child inherits the parent's deadline** as the same instant. A
    ///   [`Timeout::Explicit`] of its own replaces that deadline — even a longer one, so such a
    ///   child outlives its parent — and [`Timeout::None`] declines it outright, which is the
    ///   only way to say "no limit" under a parent that has one.
    /// - **Starting a workflow from inside a step is [`Error::InsideStep`].** A step is a leaf, and
    ///   an id-allocating call inside one would shift every later step onto the wrong replay slot.
    ///
    /// A child is a durable workflow in its own right rather than a piece of the parent's future:
    /// it keeps running if that future is dropped, and it recovers on its own.
    ///
    /// ## Fanning out
    ///
    /// Children **run** concurrently — each is its own task — and a parent may start them and
    /// await them concurrently too. Both calls take their step id from the parent's counter
    /// *where they are written*, so the numbering follows source order however the futures are
    /// then driven, and a replay that builds the same calls in the same order meets the same ids.
    ///
    /// The loop is still the plainest way to write it, and it costs nothing in wall-clock: each
    /// [`start`](Self::start) returns as soon as the child is recorded and spawned, so the parent
    /// takes about as long as the slowest child rather than the sum.
    ///
    /// ```no_run
    /// # async fn fan_out(child: dbos::WorkflowRef<u32, u32>) -> dbos::Result<u32> {
    /// let mut handles = Vec::new();
    /// for n in 0..3 {
    ///     handles.push(child.start(n).await?);
    /// }
    /// let mut total = 0;
    /// for handle in handles {
    ///     total += handle.result().await?;
    /// }
    /// # Ok(total) }
    /// ```
    ///
    /// **A `join!` over the starts — or over the awaits, or over whole
    /// [`run`](Self::run)s — is sound**, for the reason a `join!` over
    /// [`step`](crate::step())s is: `join!` builds every branch before polling any, which is
    /// exactly the order the ids were claimed in. What is *not* sound is building a start in one
    /// workflow and polling it in another, or across a step-body boundary — an id is a claim on
    /// one position in one execution, and a start carried somewhere that cannot honour it is
    /// refused as [`Error::StepBuiltElsewhere`] rather than run.
    ///
    /// **A start that is polled and then dropped still starts the child.** Dropping a future
    /// cannot undo a committed row, so the half that writes rows runs detached and finishes on its
    /// own. The caller loses the handle and nothing else: the child exists, its start is recorded
    /// against this parent, and a replay of this position joins it rather than starting a second.
    /// A start that is *never* polled writes nothing at all, and has still spent its step id.
    ///
    /// That is what makes an *interrupted* start harmless; it is not licence to race one. **A
    /// `select!` or a `timeout` over a start is forbidden** for a reason drops do not reach: a
    /// race stops at the first branch that is ready, so whether this branch was polled at all
    /// follows the timing of another — and a child that exists on one execution and not on its
    /// replay is a side effect no replay reproduces. Start outside the race and race what
    /// *observes* the result; [`PendingWorkflow`] has the whole of it.
    pub fn start_with<'a>(&'a self, input: P, options: StartOptions<'a>) -> PendingStart<'a, R, E> {
        // The workflow's bare name, which is what the start records against the parent and
        // therefore what a refusal should call this call.
        let name: Arc<str> = Arc::from(self.name());
        // **A refusal is carried into the future rather than returned**, which is what `placed`
        // does with a build that failed — so `start(..).await?` reads as it always did and a
        // caller has one operator to write rather than two. Nothing was claimed on the way to any
        // of `place`'s refusals, which is what makes a start that never stood anywhere safe to
        // leave unplaced.
        let built = self.place(&options).map(StartPlacement::into_parts);
        // **Left in the engine's channel rather than lifted here**, which is what makes
        // [`PendingStart::lift`] sound: the error this can answer with is engine-only whatever
        // channel the type declares, so re-declaring it loses nothing. The lift happens once, at
        // the poll, in whichever channel the caller asked for.
        PendingStart {
            starting: PendingWorkflow(PendingStep::placed(
                name,
                built,
                move |executor, placement| async move {
                    self.started(StartPlacement::of(executor, placement), input, options)
                        .await
                },
            )),
            channel: PhantomData,
        }
    }

    /// The start itself, once something polls it: everything that writes a row.
    ///
    /// Split from [`place`](Self::place) at the line between deciding and doing. Nothing here
    /// allocates a step id — the parent's counter moved when the call was written — so this is
    /// free to be deferred, and has to be: a start that ran at the call would make
    /// `let handle = child.start(..)` start a child.
    ///
    /// **The writing half runs in a task, and this awaits it.** A start creates a durable object
    /// part-way through its own future, and dropping a future does not undo a committed row —
    /// so a start driven by anything that abandons a branch, a `select!` or a `timeout`, could be
    /// dropped between creating a child and spawning it and leave a workflow that exists, is
    /// recorded, and that nothing is running until the process next launches. Detached, the drop
    /// takes the [`JoinHandle`](tokio::task::JoinHandle) and leaves the work: the child is
    /// created, its start is recorded, and it runs. What the caller loses by dropping is the
    /// handle, which is [`select_workflow!`](macro@crate::select_workflow)'s existing rule — a
    /// workflow runs whether or not anything is watching it — rather than a new hazard of its own.
    ///
    /// Spawned through [`spawn_tracked`], so shutdown reaches it — and what an abort leaves
    /// behind depends on where it lands. Before the transaction commits, it rolls back and the
    /// start simply did not happen, which is what shutdown means everywhere else in the crate.
    /// After it, the child exists and is recorded, and the caller is told it was
    /// [`Interrupted`](Error::Interrupted) rather than told it never started; the arm below says
    /// what that costs, which is nothing — the child is `PENDING` and recovers, and a replay of
    /// this position joins it.
    ///
    /// Answers in the engine's channel and is lifted by its caller, because everything below can
    /// only fail in the engine's terms.
    async fn started(
        &self,
        placed: StartPlacement,
        input: P,
        options: StartOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        // Taken before the spawn, while the borrowed options are still here to take it from — and
        // the encoding with it, so a payload that cannot be serialised fails this call rather than
        // a task the caller may have stopped listening to.
        // Resolved while the placement is still whole, since deriving an id needs the parent it
        // is derived from — and resolved *here* rather than in the task so that this call can name
        // the workflow whatever becomes of the task.
        let workflow_id = child_workflow_id(options.workflow_id, placed.parent().as_ref());
        // Held by the task for as long as it exists, so a parent waits for its start before
        // recording its outcome, and with it giving up the token the start writes under. See
        // `Ctx::starts`. Taken before the spawn and moved into the task, so one aborted before its
        // first poll still gives it back.
        let tracked = placed.parent().map(|parent| parent.ctx.starts().token());
        let (executor, placement) = placed.into_parts();
        // The registration's choice, unless an enqueue makes its own, and the configured
        // serializer where neither does. A child does not inherit its parent's: what it is encoded
        // in is a property of the workflow, not of who started it.
        let registered =
            PayloadEncoding::resolve(self.serialization(), executor.serializer().encoding());
        let encoding = match &options.queue {
            Some(enqueue) => PayloadEncoding::resolve(enqueue.serialization, registered),
            None => registered,
        };
        let request = OwnedStart {
            workflow_id: workflow_id.clone(),
            workflow_id_reuse_policy: options.workflow_id_reuse_policy,
            timeout: options.timeout,
            queue: options.queue.as_ref().map(OwnedEnqueue::from),
            input: Some(encode_input(&input, &encoding)?),
            encoding,
            attributes: encode_attributes(options.attributes)?,
            started_at: Timestamp::now(),
            debounce: None,
            app_name: None,
        };
        // The caller's span rather than one of its own: this is the same start, on another
        // thread, and its logs belong where the call was written.
        let creating = create::<R, E>(
            Arc::clone(&executor),
            placement,
            self.qualified_name().clone(),
            request,
        )
        .instrument(tracing::Span::current());
        let creating = async move {
            let _tracked = tracked;
            creating.await
        };

        let queue = options.queue.as_ref().map(|enqueue| enqueue.name);
        // Labelled apart from the execution it spawns, which carries the bare id: both can be
        // running at once, and shutdown's logs would otherwise name the workflow twice.
        let label = format!("start {workflow_id}");
        match spawn_tracked(&executor, Role::Workflow, label, creating).await {
            Ok(started) => started.map_err(|error| refused_start(queue, error)),
            // Only shutdown aborts this task, and what the abort leaves behind depends on where
            // it landed. Before the transaction committed, nothing was written and the start did
            // not happen. After it, the child exists and is recorded against this parent — what
            // `InitWorkflowCaller` makes atomic is the pair of rows, not the caller's knowledge of
            // them, and an abort between the commit and this answer is a start the caller was
            // never told about. Nothing is lost either way: the child is `PENDING` and recovers,
            // and a replay of this position finds the record and joins it rather than starting a
            // second. So this reports what is true of both — that this caller stopped being the
            // one waiting, which is what `Interrupted` says everywhere else.
            Err(join) if join.is_cancelled() => Err(Error::Interrupted { workflow_id }),
            Err(join) => std::panic::resume_unwind(join.into_panic()),
        }
    }
}

/// The id the child will have: the one the caller named, or one derived from where the start
/// stands.
///
/// Settled at the call rather than in the task that writes the row, because the task can be
/// aborted by shutdown and the caller still has to be told *which* start was interrupted. The
/// answer is the same either way — nothing below reads anything the task could have changed.
///
/// A dedup that joins an existing holder runs under a different id than this one, and that is not
/// a disagreement: this is the id the start would create, which is exactly what a caller learns
/// from and what a replay of this position derives again.
fn child_workflow_id(chosen: Option<&str>, parent: Option<&Parent<'_>>) -> String {
    match (chosen, parent) {
        // An application-assigned id wins over the derivation, in every reference.
        (Some(id), _) => id.to_owned(),
        // **`{parent}-{step_id}`, and it must be derived rather than random**: a recovered
        // parent re-derives the same id, so the launch is idempotent even when the crash
        // landed between creating the child and recording it. Rust's step ids are zero-based
        // (Go, TypeScript and Java; Python is the one-based outlier — see UPSTREAM item 19),
        // so a first child is `parent-0` here and in three of the four.
        (None, Some(parent)) => format!("{}-{}", parent.workflow_id(), parent.step_id),
        (None, None) => uuid::Uuid::new_v4().to_string(),
    }
}

/// [`child_workflow_id`] for a start whose placement is all the caller has: a workflow it is a
/// child of where the placement claimed an id, and a root start everywhere else.
pub(crate) fn placed_workflow_id(chosen: Option<&str>, placement: &StepPlacement) -> String {
    match placement {
        StepPlacement::Recorded { ctx, step_id } => child_workflow_id(
            chosen,
            Some(&Parent {
                ctx,
                step_id: *step_id,
            }),
        ),
        StepPlacement::Outside
        | StepPlacement::InsideStep { .. }
        | StepPlacement::ClientConnection => child_workflow_id(chosen, None),
    }
}

/// Everything a start writes, run detached from the future that asked for it.
///
/// The half of [`WorkflowRef::started`](WorkflowRef::started) that touches the database, as a free
/// function because a task outlives the reference that spawned it: what it needs of the
/// [`WorkflowRef`] is the key, and that it takes by value.
///
/// Nothing here is abandoned part-way by a caller losing interest. That is the whole point of it
/// being here — see [`started`](WorkflowRef::started) — and it is why the order below can be read
/// as a sequence rather than as a set of states a drop might leave behind.
pub(crate) async fn create<R, E>(
    executor: Arc<Executor>,
    placement: StepPlacement,
    key: QualifiedWorkflowName,
    request: OwnedStart,
) -> Result<WorkflowHandle<R, E>> {
    let placed = StartPlacement::of(executor, placement);
    let executor = &placed.executor;
    // Taken apart rather than read through a borrow: the row below wants the payload and the
    // attributes by reference and then hands the payload on to the run, so owning them here is
    // what keeps a start from copying its own arguments.
    let OwnedStart {
        workflow_id,
        workflow_id_reuse_policy,
        timeout,
        queue,
        input,
        encoding,
        attributes,
        // The caller's reading, taken before this task existed: the step spans the start as the
        // *caller* saw it, not the wait for a runtime thread to pick the task up.
        started_at,
        debounce,
        app_name,
    } = request;
    let owned_queue = queue.as_ref().map(OwnedEnqueue::as_enqueue);
    let enqueue = owned_queue.as_ref();
    // **The ambient context is what makes this a child.** Every reference overloads the same
    // call rather than adding a `start_child`, so factoring a workflow body out into its own
    // workflow does not change how its call sites are written — and a workflow started from
    // outside one is unaffected by everything below.
    let parent = placed.parent();

    // A replay of this position finds the child it already started, or the error its start
    // returned, instead of trying the start again.
    if let Some(parent) = &parent
        && let Some(child) = parent.check_child(executor, key.name()).await?
    {
        tracing::debug!(
            parent_workflow_id = parent.workflow_id(),
            step_id = parent.step_id,
            workflow_id = child,
            "the child workflow was already started; the handle joins it"
        );
        // The start record is all this run read: the child's own row belongs to the earlier
        // run that made it, and was never in front of this one. So an absent row is left as
        // "not yet", the same answer an id from outside the process gets.
        return Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            child,
            false,
        ));
    }

    // **A deadline is written here only when it is inherited.** An explicit timeout is written as
    // a budget, and the database turns it into a deadline on its own clock: the insert does, for a
    // workflow that starts now, and the dequeue's claim does for a queued one, so the wait in the
    // queue is not part of the budget. On the database's clock because every executor that later
    // runs, recovers or sweeps the workflow compares against that clock, so a starter whose clock
    // is off can neither shorten the budget nor stretch it. Persisting the deadline rather than
    // recomputing it on recovery is the whole point of a durable timeout: a workflow given an hour
    // that crashes after fifty minutes has ten left, not another hour.
    let deadline = match (timeout, &parent) {
        // **An explicit timeout replaces an inherited deadline.** So a child given longer than its
        // parent has left outlives its parent: an explicit timeout on a specific child is a
        // statement about that child, and the alternative would silently ignore what the caller
        // asked for. `Timeout::None` is a caller who decided not to inherit one either.
        (Timeout::Explicit(_) | Timeout::None, _) => None,
        // **Inherited as an instant, not as a budget**, which is what makes it a deadline the
        // parent and the child genuinely share: both `select!`s fire at the same moment, in
        // different tasks and possibly in different processes, with no signal passing between
        // them. A propagated deadline is the cancellation cascade, and needs no other one.
        // **Inherited even onto a queue**, and this is not an oversight in Python's code: its
        // `_get_timeout_deadline` branches on the queue only inside the explicit-timeout arm,
        // and returns the propagated deadline unconditionally otherwise. The difference is
        // what the two mean. A budget is a promise about how long the *work* may take, so the
        // queue wait cannot count against it; an inherited deadline is an instant a parent is
        // already bound by, and a child does not escape it by being queued.
        (Timeout::Inherit, Some(parent)) => parent.deadline(),
        (Timeout::Inherit, None) => None,
    };

    // A row written for another application carries no version: this executor's version means
    // nothing to that application's executors, and an unversioned row is run by its latest.
    let for_another_app = app_name
        .as_deref()
        .is_some_and(|name| name != executor.app_name());
    let new = NewWorkflow {
        name: Some(key.name()),
        class_name: key.class_name(),
        config_name: key.config_name(),
        input: input.as_deref(),
        serialization: Some(encoding.name()),
        executor_id: Some(executor.executor_id()),
        application_name: Some(app_name.as_deref().unwrap_or(executor.app_name())),
        application_version: (!for_another_app).then(|| executor.app_version()),
        // Only a budget is written: an inherited deadline is an *instant* and has no
        // budget behind it, and `Timeout::None` has neither. The column is what the insert
        // and a dequeue's claim compute a deadline from, so filling it in for either would
        // hand them a budget nobody asked for.
        timeout: timeout.budget(),
        deadline,
        attributes: attributes.as_deref(),
        is_debounced: debounce.is_some(),
        debounce_timeout: debounce.and_then(|debounce| debounce.timeout),
        // The queue's five columns, and nothing below spawns the row they describe: a queue's
        // whole point is that the process which asks is not necessarily the one that runs.
        ..new_row(&workflow_id, enqueue)
    };

    let initialized = match init_or_join(
        executor.connection(),
        &new,
        enqueue.map_or(DuplicationPolicy::Reject, |enqueue| {
            enqueue.duplication_policy
        }),
        workflow_id_reuse_policy,
        // **The parent's record of the start travels with the row, so the two commit
        // together.** A start is one durable act: either this parent started this child and
        // both rows say so, or neither exists. Written as two statements it had a window —
        // and not only a crash window, since a start is a future and a combinator that races
        // one may drop it part-way — in which a child existed that nothing in its parent
        // pointed at. `None` is a root start, which has no parent and no step to record.
        parent.as_ref().map(|parent| InitWorkflowCaller {
            parent_workflow_id: parent.workflow_id(),
            step_id: parent.step_id,
            // The step name is the child workflow's bare name; see `Parent::record_child`,
            // which records the joined case under the same one.
            step_name: key.name(),
            started_at,
            owner_xid: parent.owner_xid(),
        }),
    )
    .await
    {
        Ok(Submitted::Created(initialized)) => initialized,
        // **The start's error is recorded at the parent's step**, so a replay gets it too. See
        // [`should_record_start_error`] for which errors, and why each one needs the record.
        Err(Error::SystemDatabase(error)) if should_record_start_error(&error) => {
            return Err(match &parent {
                Some(parent) => {
                    parent
                        .record_start_error(executor, error, key.name(), started_at)
                        .await
                }
                None => Error::SystemDatabase(error),
            });
        }
        Err(error) => return Err(error),
        // **The start records the workflow that was joined**, not the id this call derived,
        // so a replay of this position resolves to the same workflow instead of trying to
        // start a child that was never created. Go records the same mapping at the same
        // reserved step id.
        //
        // Only this record, and not the joined workflow's own `parent_workflow_id`: it has an
        // owner already, and a cascade following that column must not reach a workflow this
        // parent merely joined. [`DuplicationPolicy::ReturnExisting`] states the asymmetry.
        //
        // A crash between the join and this write is harmless, and for a different reason than
        // the one below: nothing was written by the losing insert, so a replay simply asks
        // again. It joins the same holder if the key is still held, and starts a workflow of
        // its own if the holder has since finished — which is what the policy means at that
        // moment, since the key deduplicates a backlog rather than a history.
        Ok(Submitted::Joined(holder)) => {
            if let Some(parent) = &parent {
                parent
                    .record_child(executor, &holder, key.name(), started_at)
                    .await?;
            }
            // The holder's row was just read to find it, so a missing one from here is a
            // deletion rather than a row still to come.
            return Ok(WorkflowHandle::polling(
                Arc::clone(executor.connection()),
                holder,
                true,
            ));
        }
    };

    // The start record is not written here: `init_or_join` carried it into the transaction
    // that created the child, which is what makes the pair atomic. What remains below only
    // decides which handle to hand back.

    // **Enqueued, so this process is not the one running it.** A polling handle is the honest
    // answer even when this executor turns out to dequeue it moments later: nothing local is
    // waiting on, and the row is the only thing that knows where the workflow got to.
    if let Some(enqueue) = enqueue {
        tracing::debug!(
            workflow_id,
            queue = enqueue.name,
            "the workflow is enqueued"
        );
        // This call wrote the row, so the handle it hands back can say what an id from
        // outside cannot: a row that goes missing was deleted.
        return Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            workflow_id,
            true,
        ));
    }

    // Someone else created this row: the id was supplied and a previous call made it. Joining
    // rather than erroring is what makes a retried request idempotent.
    if !initialized.should_execute {
        tracing::debug!(
            workflow_id,
            "another caller created the workflow; the handle joins it"
        );
        // Park-and-adopt, one frame out from the one in `execute`: `init_workflow` just read this
        // row, and it is the case the references pass their own flag on — Python parks its unowned
        // dispatch with `fail_if_missing=True` (`_core.py`) and Go its lost start race
        // (`workflow.go`).
        return Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            workflow_id,
            true,
        ));
    }

    // The row's encoding, which is this caller's own: it created the row.
    let serialization = initialized.serialization;
    // The deadline the *database* holds, not the one this caller offered: `init_workflow`
    // turns a budget into an instant against its own clock, and an existing row keeps the
    // deadline it already had rather than taking a new one from a joining caller.
    let task = spawn_execution(
        executor,
        key.clone(),
        workflow_id.clone(),
        // The row was inserted `PENDING`, so its creator owns it from the start.
        initialized.creator_xid,
        input,
        serialization,
        Deadline::of(initialized.deadline, initialized.clock),
        // A fresh start is not a dequeue, so it holds no queue's slot.
        None,
    );
    Ok(WorkflowHandle::local(
        Arc::clone(executor.connection()),
        workflow_id,
        task,
    ))
}

/// A call that creates a workflow, has claimed the step ids that costs, and has started nothing.
///
/// What [`run`](WorkflowRef::run) hands back as [`PendingRun`], and what a
/// [`start`](WorkflowRef::start) is built on — [`PendingStart`] holds one of these and declares
/// the channel it reports in. Awaiting either starts the workflow, so `child.start(x).await?` and
/// `child.run(x).await?` read exactly as they would if both were `async fn`s.
///
/// **A newtype over [`PendingStep`] rather than a `PendingStep`, because a durable race must not
/// take one**, and the reason is not that dropping one leaves anything broken. It does not: dropped
/// before its first poll it writes nothing, and dropped after one it finishes on its own — see
/// [`start_with`](WorkflowRef::start_with). The reason is that a race would make a **workflow's
/// existence depend on poll order**. A durable race polls its branches in source order and stops
/// at the first that is ready, so a branch that creates a workflow may or may not have been polled
/// before the winner answered — and a child that exists on one execution and not the next is a
/// side effect no replay reproduces. A branch that only *observes* a workflow
/// ([`WorkflowHandle::result`](crate::WorkflowHandle::result)) has no such problem, and is what a
/// race should hold.
///
/// **Start outside the race, then race what observes it.** For workflows alone that is
/// [`select_workflow!`](macro@crate::select_workflow), which spends one id on the wait and one on
/// the winner however many handles it is given; where a workflow is raced against a step it is the
/// handle's [`result`](crate::WorkflowHandle::result) that belongs in the arm. Either way the
/// workflows are created before the race, and the race decides only which one is watched.
///
/// **The type states the rule; it does not enforce it.** This implements [`Future`], as anything
/// awaited must, and `tokio::select!` takes any future — so a race over a start compiles, and no
/// diagnostic stands between a body and the hazard above. What the newtype buys is narrower and
/// still worth having: a start is not a [`PendingStep`], so nothing in the crate that is typed on
/// one can be handed a call that creates a workflow, and a reader looking at a raced branch finds
/// the rule on the type the branch names.
///
/// **`Unpin`, and the check the inner [`PendingStep`] makes on every poll is inherited** — this
/// delegates its `poll` rather than reaching past it, so a start is held to the workflow it was
/// written in exactly as a step is.
#[must_use = "a start or a run that is not awaited never starts the workflow, and has still spent \
              the step ids it claimed; await it"]
pub struct PendingWorkflow<'a, T, E = crate::EngineOnly>(PendingStep<'a, T, E>);

/// A [`start`](WorkflowRef::start) that has claimed its step id and has started nothing.
///
/// One id, and it answers with a handle: the workflow's outcome is the handle's to give, through
/// [`result`](crate::WorkflowHandle::result), which claims an id of its own where it is written.
///
/// **Two error types, because a start deals in two.** `E` is the *child's* — it is what the handle
/// will report when the child finishes, and none of it is in play yet. `C` is the channel this
/// call itself answers in, and a start can only fail in the engine's terms: the child's body has
/// not run, so there is no application error for it to have. One parameter for both would force a
/// parent to fail the way its child does — and a parent whose error type differs from its child's
/// would then have no way to report a start failure at all, since `?` cannot convert between two
/// application channels and [`Error::lift`] starts from [`EngineOnly`](crate::EngineOnly).
///
/// `C` **defaults to `E`**, which is the common case written without saying anything: a workflow
/// starting a child that fails the way it does writes `child.start(x).await?` and nothing else.
/// Where the two differ, [`lift`](Self::lift) re-declares the channel before the await.
#[must_use = "a start that is not awaited never starts the workflow, and has still spent the step \
              id it claimed; await it"]
pub struct PendingStart<'a, R, E = crate::EngineOnly, C = E> {
    /// The start itself, in the engine's channel — see [`PendingWorkflow`] for why a start is a
    /// newtype and not a [`PendingStep`].
    starting: PendingWorkflow<'a, WorkflowHandle<R, E>, crate::EngineOnly>,
    /// The channel the caller asked to hear about a failure in, which is a claim about *reporting*
    /// and not about what is stored: nothing of `C` is ever constructed, since the error being
    /// re-declared is engine-only. `fn() -> C` rather than `C` so the marker adds no drop
    /// obligation and no `Send`/`Sync` bound of its own.
    channel: PhantomData<fn() -> C>,
}

impl<'a, R, E, C> PendingStart<'a, R, E, C> {
    /// A start built from a placed call that answers with a handle, for a durable call other than
    /// [`start_with`](WorkflowRef::start_with) that creates a workflow — a
    /// [`Debouncer`](crate::Debouncer)'s.
    pub(crate) fn from_step(
        step: PendingStep<'a, WorkflowHandle<R, E>, crate::EngineOnly>,
    ) -> Self {
        Self {
            starting: PendingWorkflow(step),
            channel: PhantomData,
        }
    }

    /// The id this start claimed when it was written, or `None` if it claimed none.
    ///
    /// [`PendingWorkflow::step_id`]'s, forwarded.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.starting.step_id()
    }

    /// Re-declares which channel this start answers in.
    ///
    /// **For a parent whose error type is not its child's.** `child.start(x).await?` needs the
    /// start to fail the way the *parent* does; by default it fails the way the child does, which
    /// is right whenever the two agree and impossible to convert when they do not. This says so
    /// before the await, where the declaration still can be changed:
    ///
    /// ```ignore
    /// let handle = shipping.start(order).lift().await?;   // in a workflow that fails as `Billing`
    /// ```
    ///
    /// Nothing is converted and nothing can be lost: what the future holds is an engine error
    /// whatever this type declares, so this swaps a marker and the [`Error::lift`] at the poll
    /// does the rest. It is [`Error::lift`]'s own argument, made one step earlier — with the
    /// child's errors untouched, since those come out of the handle rather than out of this.
    pub fn lift<C2>(self) -> PendingStart<'a, R, E, C2> {
        PendingStart {
            starting: self.starting,
            channel: PhantomData,
        }
    }
}

impl<R, E, C> Future for PendingStart<'_, R, E, C> {
    type Output = Result<WorkflowHandle<R, E>, C>;

    /// Delegated to the start, and lifted into `C` on the way out — the one place the declared
    /// channel is paid for.
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Every field is `Unpin` — the marker holds nothing — so there is nothing here for pinning
        // to protect.
        Pin::new(&mut self.get_mut().starting)
            .poll(cx)
            .map(|started| started.map_err(Error::lift))
    }
}

impl<R, E, C> std::fmt::Debug for PendingStart<'_, R, E, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PendingStart").field(&self.starting).finish()
    }
}

/// A [`run`](WorkflowRef::run) that has claimed its two step ids and has started nothing.
///
/// **Two ids, the start's and the await's**, claimed in that order and before either has run —
/// which is what makes a `join!` over runs yield `{start, await}` pairs in the order the calls
/// were written rather than pairs interleaved by whichever child finished first. That second id is
/// the other half of why a run is not a [`PendingStep`]: everything a race could hold spends
/// exactly one.
pub type PendingRun<'a, R, E = crate::EngineOnly> = PendingWorkflow<'a, R, E>;

impl<T, E> PendingWorkflow<'_, T, E> {
    /// The id this call claimed when it was written, or `None` if it claimed none.
    ///
    /// [`PendingStep::step_id`]'s, forwarded. For a [`PendingRun`] that is the *start's* id — the
    /// position the pair begins at — since the await's follows it directly.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.0.step_id()
    }
}

impl<T, E> Future for PendingWorkflow<'_, T, E> {
    type Output = Result<T, E>;

    /// Delegated, which is what makes the inner check apply: see the type's documentation.
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Every field is `Unpin`, so there is nothing here for pinning to protect.
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl<T, E> std::fmt::Debug for PendingWorkflow<'_, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PendingWorkflow").field(&self.0).finish()
    }
}

/// Where a start stands, decided when the call was written.
///
/// Built once per start, because building it *allocates a step id* — the parent's counter moves
/// whether or not the start ends up creating anything, which is what keeps a replay aligned with
/// the run it is replaying. Holding the executor beside the placement is what makes it one value:
/// the two are decided together, the placement is decided *against* that executor's connection,
/// and handing the run one of them without the other would let it write through an instance the
/// placement never agreed to.
struct StartPlacement {
    /// The instance that will serve the start, resolved before anything could fail.
    executor: Arc<Executor>,
    placement: StepPlacement,
}

impl StartPlacement {
    /// The two halves back together, once the run that deferred them is polled.
    ///
    /// The inverse of [`into_parts`](Self::into_parts): [`PendingStep::placed`] carries a build's
    /// placement beside whatever else the run will need, and for a start that is the executor the
    /// placement was decided against — so the pair travels apart and arrives back here.
    fn of(executor: Arc<Executor>, placement: StepPlacement) -> Self {
        Self {
            executor,
            placement,
        }
    }

    /// The executor and the placement, in the shape [`PendingStep::placed`] takes a build in.
    ///
    /// Kept as a named type either side of that rather than passed around as a bare pair, because
    /// what makes a start's placement sound is that the executor is *the one it was decided
    /// against* — handing the run one without the other would let it write through an instance the
    /// placement never agreed to.
    fn into_parts(self) -> (Arc<Executor>, StepPlacement) {
        (self.executor, self.placement)
    }

    /// The workflow this start is a child of, or `None` where it is a root.
    ///
    /// A start is a child exactly where it claimed an id, so the placement already answered this
    /// and nothing here decides it again.
    fn parent(&self) -> Option<Parent<'_>> {
        match &self.placement {
            StepPlacement::Recorded { ctx, step_id } => Some(Parent {
                ctx,
                step_id: *step_id,
            }),
            // `Outside` is a root start, which is the common case. `InsideStep` never reaches
            // here — [`WorkflowRef::place`] turns it into [`Error::InsideStep`] — and
            // `ClientConnection` cannot arise at all, since the connection the placement was
            // taken against is this instance's own and a client's is never that.
            StepPlacement::Outside
            | StepPlacement::InsideStep { .. }
            | StepPlacement::ClientConnection => None,
        }
    }
}

/// Whether a child start's `error` is recorded at the parent's step, so a replay gets the same
/// error rather than trying the start again.
///
/// Each of these depends on another workflow's row, which can be gone by the time of a replay:
///
/// - [`QueueDeduplicated`](crate::sysdb::Error::QueueDeduplicated): the key is held only while its
///   holder waits, so once the holder finishes the same start would go ahead.
/// - [`ConflictingWorkflow`](crate::sysdb::Error::ConflictingWorkflow): the chosen child id names a
///   workflow of another function, class or config, and once that row is deleted the same start
///   would go ahead.
/// - [`WorkflowIdInUse`](crate::sysdb::Error::WorkflowIdInUse): the start refused an id that was
///   taken, and once that row is deleted the same start would go ahead.
///
/// One more is recorded although it depends only on the start's arguments:
///
/// - [`InvalidInput`](crate::sysdb::Error::InvalidInput): a value of the row the insert refuses.
///   The replay would refuse it the same way, so recording it changes nothing, and it stays in
///   the list so a record an earlier version wrote still reads back as a start error. A delay
///   that once could be refused by the clock no longer can: the insert resolves it on the
///   database clock and saturates.
///
/// Without the record, the replay would start the child and the parent would take the other branch
/// from the one it took after the error. Every other error a start can return is one of:
///
/// - an error that depends only on the start's own arguments, and so replays the same without a
///   record;
/// - database trouble, which a replay should meet afresh;
/// - a signal about this execution (cancelled, superseded, replaying changed code), which ends the
///   run rather than answering the start.
fn should_record_start_error(error: &crate::sysdb::Error) -> bool {
    matches!(
        error,
        crate::sysdb::Error::QueueDeduplicated { .. }
            | crate::sysdb::Error::ConflictingWorkflow { .. }
            | crate::sysdb::Error::WorkflowIdInUse { .. }
            | crate::sysdb::Error::InvalidInput { .. }
    )
}

/// The variants [`should_record_start_error`] accepts, by the name a recorded one is stored under.
///
/// A [`sysdb::Error`](crate::sysdb::Error) is stored as an object whose one key is its variant's
/// name, so [`check_child`](Parent::check_child) can tell a recorded start error from a plain
/// step's error by that key, before reading it as the type. A unit test keeps this list and the
/// predicate in step.
const RECORDED_START_ERRORS: [&str; 4] = [
    "QueueDeduplicated",
    "ConflictingWorkflow",
    "WorkflowIdInUse",
    "InvalidInput",
];

/// Whether `value`, a recorded step error, has the shape of a recorded start error.
fn is_recorded_start_error(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == 1
            && object
                .keys()
                .all(|key| RECORDED_START_ERRORS.contains(&key.as_str()))
    })
}

/// The workflow a child is being started from: the context it runs in, and the step id the start
/// occupies.
///
/// A borrowed view over a [`StartPlacement`] rather than a value of its own. Everything a child
/// needs of its parent — the id it derives from, the deadline it inherits — is on the context that
/// claimed the id, so copying any of it out would be two spellings of one fact.
struct Parent<'a> {
    ctx: &'a Ctx,
    step_id: i32,
}

impl Parent<'_> {
    /// The parent's workflow id, which is what a child's derived id and its row both point back
    /// at.
    fn workflow_id(&self) -> &str {
        self.ctx.workflow_id()
    }

    /// The token the parent's execution owns it by, which every write recording the start
    /// presents: a parent that has been handed off starts nothing.
    fn owner_xid(&self) -> &str {
        self.ctx.owner_xid()
    }

    /// The parent's own deadline, for the child to inherit when it asks for no budget of its own.
    fn deadline(&self) -> Option<Timestamp> {
        self.ctx.deadline()
    }

    /// Reads back a start recorded at this position, if this parent has run this far before.
    ///
    /// `check_step` compares the recorded name, so a mismatch here is already
    /// [`UnexpectedStep`](crate::sysdb::Error::UnexpectedStep) before this sees it. What is left
    /// is what the row holds:
    ///
    /// - **a child id**: the start went ahead, and the replay joins that child;
    /// - **a recorded error**: the start failed, and the replay returns the same error without
    ///   trying the start again. See [`record_start_error`](Self::record_start_error);
    /// - **a recorded error this build can't decode**, or a recorded start error it can't read
    ///   as one: the decode error, naming this step;
    /// - **neither**, or an error that [`should_record_start_error`] doesn't name: a plain step
    ///   wrote the row, which means the parent's code changed (`step("charge")` became a child
    ///   workflow named `charge`), and starting a child now would give this position two meanings
    ///   across two runs.
    async fn check_child(&self, executor: &Executor, step_name: &str) -> Result<Option<String>> {
        let Some(recorded) = executor
            .sysdb()
            .check_step(self.workflow_id(), self.step_id, step_name)
            .await
            .map_err(Error::SystemDatabase)?
        else {
            return Ok(None);
        };
        if let Some(child) = recorded.child_workflow_id {
            return Ok(Some(child));
        }
        if let Some(encoded) = recorded.error.as_deref() {
            let caller = StepCaller {
                workflow_id: self.workflow_id(),
                step_id: self.step_id,
                codec: executor.serializer(),
                owner_xid: self.owner_xid(),
            };
            let value: serde_json::Value =
                decode_step_value(&recorded, encoded, caller, step_name, "an error")
                    .map_err(Error::SystemDatabase)?;
            // Told apart by shape before the typed read, so a start error this build can't read
            // is reported as that, naming the step, rather than as a plain step's row.
            if is_recorded_start_error(&value) {
                let error = serde_json::from_value::<crate::sysdb::Error>(value).map_err(|e| {
                    Error::SystemDatabase(crate::sysdb::Error::Malformed(format!(
                        "workflow {} step {} ({step_name}) has an error this build cannot read: \
                         {e}",
                        self.workflow_id(),
                        self.step_id,
                    )))
                })?;
                tracing::debug!(
                    parent_workflow_id = self.workflow_id(),
                    step_id = self.step_id,
                    %error,
                    "the child workflow start failed; the replay returns the same error"
                );
                return Err(Error::SystemDatabase(error));
            }
        }
        Err(Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
            workflow_id: self.workflow_id().to_owned(),
            step_id: self.step_id,
            expected: format!("a child workflow start of {step_name}"),
            recorded: format!("a plain step named {step_name}"),
        }))
    }

    /// Records a failed start's error at this position, and gives back the error the caller
    /// should see.
    ///
    /// The insert that failed wrote nothing, so the record goes in on its own. A crash before
    /// it leaves nothing recorded and nothing observed, and the replay tries the start again as a
    /// first run would.
    ///
    /// - **Recorded**: the error is returned, and [`check_child`](Self::check_child)
    ///   returns it again on a replay.
    /// - **The record fails**: that failure is returned instead, and the position stays
    ///   unrecorded, since the record is one transaction. A position recorded already is
    ///   [`StepNondeterminism`](crate::sysdb::Error::StepNondeterminism), and
    ///   a parent no longer this execution's is
    ///   [`WorkflowConflict`](crate::sysdb::Error::WorkflowConflict). Database trouble leaves the
    ///   parent `PENDING` rather than ending it.
    /// - **The error can't be encoded**: a custom serializer can fail for its own reasons. The
    ///   encoding failure is returned and the error logged, so the position stays unrecorded
    ///   rather than the parent acting on an answer nothing recorded.
    async fn record_start_error(
        &self,
        executor: &Executor,
        error: crate::sysdb::Error,
        step_name: &str,
        started_at: Timestamp,
    ) -> Error {
        let recorded = match encode_step_value(&error, executor.serializer(), "an error") {
            Ok(recorded) => recorded,
            Err(encode_error) => {
                tracing::warn!(
                    parent_workflow_id = self.workflow_id(),
                    step_id = self.step_id,
                    %error,
                    %encode_error,
                    "the child workflow start failed, but its error could not be encoded \
                     to record it"
                );
                return Error::SystemDatabase(encode_error);
            }
        };
        // Read once, outside the retry inside `record_step`, so a retried write carries the same
        // completion time and is recognised as this call's own.
        let timing = StepTiming {
            started_at,
            completed_at: Timestamp::now(),
        };
        match executor
            .sysdb()
            .record_step(
                self.workflow_id(),
                self.step_id,
                step_name,
                Outcome::Error(&recorded.value),
                recorded.serialization.as_deref(),
                Some(timing),
                self.owner_xid(),
            )
            .await
        {
            Ok(()) => Error::SystemDatabase(error),
            Err(record_error) => Error::SystemDatabase(record_error),
        }
    }

    /// Records the start, so the replay above finds it — for the **joined** case alone.
    ///
    /// A start that creates its child records it inside the transaction that creates it, by
    /// handing [`init_workflow`](crate::sysdb::SystemDatabase::init_workflow) a
    /// [`InitWorkflowCaller`]. This is the other arm, where the insert lost the deduplication key
    /// and there is no transaction of this call's to join: the holder's row belongs to whoever
    /// created it, and all that is left to write is the mapping. Nothing was created here, so
    /// nothing is left dangling if this write never happens — which is what makes a separate
    /// statement sound in this arm and not in the other.
    ///
    /// The mapping only, never the holder's own `parent_workflow_id`: that column travels on
    /// [`InitWorkflowCaller`] and so cannot be reached from here at all, which is the asymmetry
    /// [`DuplicationPolicy::ReturnExisting`] describes, held by the shape rather than by care.
    ///
    /// **The step name is the workflow's bare name**, not the `name`/`class_name`/`config_name`
    /// triple that identifies it — and that is four of four rather than a narrowing, including both
    /// references that also carry a class and a config. Python records `get_dbos_func_name(func)`
    /// and hands the other two to `_init_workflow` separately (`_core.py`); Java records
    /// `workflowName` beside a `className` on the status row (`DBOSExecutor.java`); Go has one name
    /// to record. The qualification belongs to the child's own row, which the `init_workflow` call
    /// above fills in — this column is the *parent's* step listing, where the name is what a reader
    /// is looking for.
    async fn record_child(
        &self,
        executor: &Executor,
        child_workflow_id: &str,
        step_name: &str,
        started_at: Timestamp,
    ) -> Result<()> {
        executor
            .sysdb()
            .record_child_workflow(
                self.workflow_id(),
                child_workflow_id,
                self.step_id,
                step_name,
                Some(started_at),
                self.owner_xid(),
            )
            .await
            .map_err(Error::SystemDatabase)
    }
}

/// Spawns the workflow body on the executor's runtime and registers it for shutdown to reach.
///
/// Spawned, not awaited in place: the workflow's life is the executor's, not the caller's, so
/// dropping any future that observes the returned handle must not stop the run. Both submitters go
/// through here — a caller's `run`, which awaits the handle, and recovery, which does not.
///
/// The span travels with the task, so everything the workflow logs — the engine's own events and
/// the application's — carries the workflow id without threading it anywhere.
///
/// `slot` is carried, never read: a dequeued workflow holds its place in its queue's local running
/// tally for as long as this task lives, so the release happens on a return, a panic, or shutdown
/// aborting it, without anything having to watch for the end. The one earlier release is an
/// execution another one has overtaken, which [`adopt_superseded`] gives up before it waits. Every
/// other submitter passes `None`.
///
/// `owner_xid` is the token the execution owns the workflow by: the creator token of a direct
/// start that inserted the row, or the token of the claim that dequeued it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_execution(
    executor: &Arc<Executor>,
    key: QualifiedWorkflowName,
    workflow_id: String,
    owner_xid: String,
    input: Option<String>,
    serialization: Option<String>,
    deadline: Option<Deadline>,
    slot: Option<crate::dequeue::Slot>,
) -> tokio::task::JoinHandle<std::result::Result<EncodedOutput, Failure>> {
    let span = tracing::info_span!("workflow", workflow_id = %workflow_id, name = %key);
    spawn_tracked(
        executor,
        Role::Workflow,
        workflow_id.clone(),
        {
            let executor = Arc::clone(executor);
            async move {
                // The row's tag, which says how its input was written and so how the outcome is
                // written back. A tag this SDK cannot read is an input it cannot decode, and ends
                // the execution as an undecodable input does: with nothing recorded.
                let encoding = PayloadEncoding::of(serialization.as_deref(), executor.serializer())
                    .map_err(|error| {
                        tracing::warn!(
                            %error,
                            "the workflow's input is in an encoding this SDK cannot read; it \
                             records no outcome"
                        );
                        Failure::Control(error)
                    })?;
                let ctx = Ctx::new(
                    Arc::clone(&executor),
                    &workflow_id,
                    owner_xid,
                    deadline.map(|deadline| deadline.at),
                    encoding.clone(),
                );
                let _panic_log = PanicLog {
                    workflow_id: &workflow_id,
                };
                execute(
                    &executor,
                    &key,
                    &workflow_id,
                    input,
                    encoding,
                    ctx,
                    deadline.map(|deadline| deadline.expires),
                    slot,
                )
                .await
            }
        }
        .instrument(span),
    )
}

/// A workflow's deadline, and when it passes on this process's monotonic clock.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadline {
    /// The instant the row holds, which the context carries for children to inherit.
    at: Timestamp,
    /// When the database's clock reaches `at`, measured from a reading of that clock, so the wait
    /// doesn't depend on this host's wall clock agreeing with the database's.
    expires: Instant,
}

impl Deadline {
    /// A deadline the database set, timed from a reading of the database's clock: the one
    /// `init_workflow` returned with it, or a dequeue's reading for its batch.
    pub(crate) fn of(at: Option<Timestamp>, clock: DatabaseTime) -> Option<Self> {
        at.map(|at| Self {
            at,
            expires: clock.reaches(at),
        })
    }
}

/// Runs `body` until it finishes or its deadline passes.
///
/// The local timer is what interrupts a body blocked outside any step, which no durable check would
/// reach. When it wins, the body is dropped and [`time_out`] records the timeout.
///
/// **Shutdown deliberately doesn't go through here.** It aborts the task outright, so no code of
/// ours runs and no row is written, and the workflow is recovered. The deadline and shutdown use
/// different mechanisms rather than a shared hook, so there is no cancellation cause to inspect.
///
/// `expires` is when the database's clock reaches the deadline, on this process's monotonic clock
/// (see [`DatabaseTime`]). The deadline is an *instant* rather than a budget, and that is what
/// makes it survive a crash: a workflow recovered with two minutes left waits two minutes, not the
/// whole timeout again. A deadline already in the past yields a zero wait, which is the correct
/// reading of a workflow recovered after its expiry.
async fn run_until_deadline<F>(expires: Option<Instant>, body: F) -> Ended
where
    F: Future<Output = std::result::Result<EncodedOutput, Failure>>,
{
    let Some(expires) = expires else {
        return Ended::Body(body.await);
    };

    // Biased so a body that finished in the same instant keeps its outcome: it did complete, and
    // recording a completed workflow as cancelled would discard work that was actually done.
    tokio::select! {
        biased;
        outcome = body => Ended::Body(outcome),
        () = tokio::time::sleep_until(expires.into()) => Ended::DeadlinePassed,
    }
}

/// How an execution ended, and so whether its caller has anything left to record.
enum Ended {
    /// The body ran to a conclusion. That conclusion is this execution's outcome and is written.
    Body(std::result::Result<EncodedOutput, Failure>),
    /// The local timer fired and the body was dropped. What is left is to record the timeout.
    DeadlinePassed,
}

/// Records the timeout of a workflow whose local timer fired, and settles on what the row holds.
///
/// **Cancelled only if the database's clock says the deadline has passed**, the same test the
/// timeout sweep makes, and only while this execution still owns the workflow. The timer is measured on that clock, so the two normally agree; the test is
/// what keeps a timer that fires a moment early, inside a reading's millisecond, from cancelling a
/// workflow early.
///
/// Whatever the cancel finds, the outcome is what the row records: this cancel, or what settled the
/// row first (the sweep, an explicit cancel, or another execution, including one resumed meanwhile,
/// whose outcome this then waits for rather than overwrites). A row the cancel left unsettled is
/// waited on until the sweep records its timeout.
///
/// The queue slot is given up first: the body is gone, and waiting here does no work a queue's
/// concurrency limit should count.
async fn time_out(
    executor: &Executor,
    workflow_id: &str,
    owner_xid: &str,
    slot: Option<crate::dequeue::Slot>,
) -> std::result::Result<EncodedOutput, Failure> {
    drop(slot);
    match executor
        .sysdb()
        .cancel_if_timed_out(workflow_id, owner_xid)
        .await
    {
        Ok(true) => {
            tracing::info!(
                workflow_id,
                "the workflow exceeded its deadline and is cancelled"
            );
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(workflow_id, %error, "could not record the deadline cancellation");
        }
    }
    // This execution read the row to run it, so an absence now is a delete, not a race.
    executor.connection().adopt(workflow_id, true).await
}

/// Runs the body with a context ambient and records what it did.
///
/// `expires` is when the context's deadline passes, on this process's monotonic clock: the
/// deadline travels with the context, for the children it starts to inherit, and `expires` is the
/// same instant measured from a reading of the database's clock. `slot` is the queue slot a
/// dequeued execution holds, kept for as long as this runs.
#[allow(clippy::too_many_arguments)]
async fn execute(
    executor: &Executor,
    key: &QualifiedWorkflowName,
    workflow_id: &str,
    input: Option<String>,
    encoding: PayloadEncoding,
    ctx: Ctx,
    expires: Option<Instant>,
    slot: Option<crate::dequeue::Slot>,
) -> std::result::Result<EncodedOutput, Failure> {
    let workflow = executor
        .workflows()
        .get(key)
        .ok_or_else(|| {
            Failure::Control(Error::NotRegistered {
                workflow: key.clone(),
            })
        })?
        .body
        .clone();

    // Kept for the outcome write, which only lands while this execution still owns the workflow.
    let owner_xid = ctx.owner_xid().to_owned();
    let starts = ctx.starts().clone();
    let outcome = match run_until_deadline(
        expires,
        Ctx::scope(ctx, async move {
            let outcome = async {
                let encoded = workflow(input, encoding.clone()).await?;
                Ok(EncodedOutput { encoded, encoding })
            }
            .await;
            // **A start the body dropped may still be writing under this execution's token**,
            // which the outcome write gives up. Its write would then be refused with nobody
            // awaiting it, and the child would never exist, so the outcome waits for every start
            // the body began. Inside the deadline, so a start that stalls past it cannot hold the
            // workflow open: the run times out as any other would, and a start refused after
            // that is refused for a cancelled parent.
            starts.drained().await;
            outcome
        }),
    )
    .await
    {
        Ended::Body(outcome) => outcome,
        Ended::DeadlinePassed => return time_out(executor, workflow_id, &owner_xid, slot).await,
    };

    let write = match &outcome {
        Ok(output) => {
            executor
                .sysdb()
                .record_workflow_outcome(
                    workflow_id,
                    Outcome::Output(output.encoded.as_deref()),
                    &owner_xid,
                )
                .await
        }
        // A write was refused because the workflow was handed to another execution (cancelled,
        // resumed, or recovered), so this one is superseded: the same position as
        // `OutcomeWrite::AlreadyFinished` below, one level down. It is a fact about ownership,
        // not a failure of the workflow, so the caller gets the outcome the owner records rather
        // than an error for a workflow that may well succeed.
        Err(Failure::Control(Error::SystemDatabase(crate::sysdb::Error::WorkflowConflict {
            workflow_id: conflicted,
        }))) if conflicted == workflow_id => {
            tracing::warn!(
                workflow_id,
                "the workflow is no longer owned by this execution; waiting for the owner's outcome"
            );
            return adopt_superseded(executor, workflow_id, slot).await;
        }
        // A control signal is not the workflow's outcome, so this execution writes nothing
        // terminal. Where that leaves the row depends on the signal — PENDING for a later executor
        // after a shutdown or a failed write, already CANCELLED when the signal is a cancellation
        // raised elsewhere and observed by a preemptible step — so the line reports what this
        // execution did and does not claim a status it has not read. Warned rather than
        // debug-logged, because the caller may have dropped its future: this can be the only
        // evidence.
        Err(Failure::Control(control)) => {
            tracing::warn!(
                error = %control,
                "a control signal ended this execution: it records no outcome of its own"
            );
            return outcome;
        }
        Err(Failure::Recorded(recorded)) => {
            executor
                .sysdb()
                .record_workflow_outcome(workflow_id, Outcome::Error(&recorded.encoded), &owner_xid)
                .await
        }
    }
    .map_err(|error| {
        tracing::warn!(
            error = %error,
            "could not record the workflow's outcome: the row stays PENDING"
        );
        Failure::Control(Error::SystemDatabase(error))
    })?;

    match write {
        OutcomeWrite::Recorded => {
            match &outcome {
                Ok(_) => tracing::debug!("the workflow completed; its output is recorded"),
                Err(_) => tracing::debug!("the workflow failed; its error is recorded"),
            }
            outcome
        }
        // Another run finished this workflow while this one was working. It is superseded, so what
        // it computed is not the answer — the recorded outcome is, and every caller must agree on
        // which one that is.
        OutcomeWrite::AlreadyFinished => {
            tracing::warn!(
                workflow_id,
                "another execution recorded this workflow's outcome first"
            );
            adopt_superseded(executor, workflow_id, slot).await
        }
    }
}

/// Returns the outcome another execution records, for an execution that execution has overtaken.
///
/// The superseded execution writes nothing: what it computed is not the answer, and every caller
/// must agree on the one that is.
///
/// **The queue slot goes first.** It counts work this process is doing, and waiting on another
/// execution is not work — while a dequeued execution's result has no reader at all, so holding the
/// slot would stop this process dequeuing from that queue for as long as the winner runs.
async fn adopt_superseded(
    executor: &Executor,
    workflow_id: &str,
    slot: Option<crate::dequeue::Slot>,
) -> std::result::Result<EncodedOutput, Failure> {
    drop(slot);
    // Park-and-adopt, the path every reference passes its own flag on: this execution has read the
    // row, whether it created it, recovered it or dequeued it, so a missing one has been deleted.
    executor.connection().adopt(workflow_id, true).await
}

impl Connection {
    /// Reads back the outcome of a workflow this caller does not own.
    ///
    /// On the connection rather than on an executor because that is all it needs — a row read and
    /// the interval to re-ask at — and because both surfaces reach it: a `WorkflowHandle` polls
    /// through here whether it came from a running application or from a
    /// [`Client`](crate::Client).
    pub(crate) async fn adopt(
        &self,
        workflow_id: &str,
        fail_if_missing: bool,
    ) -> std::result::Result<EncodedOutput, Failure> {
        let outcome = self
            .sysdb()
            .await_workflow_result(workflow_id, self.outcome_poll_interval(), fail_if_missing)
            .await
            .map_err(|error| {
                Failure::Control(match error {
                    // Only reachable with `fail_if_missing`, so this is a row the caller had
                    // already seen: it was deleted mid-wait. Reported as the same absence
                    // [`WorkflowHandle::status`] reports rather than buried in a system database
                    // failure, and for a single id because a single id is what was awaited — the
                    // plural variant belongs to the calls that take a list.
                    crate::sysdb::Error::NonExistentWorkflow { .. } => Error::WorkflowNotFound {
                        workflow_id: workflow_id.to_owned(),
                    },
                    other => Error::SystemDatabase(other),
                })
            })?;
        // The row's tag says how to read the outcome, whoever wrote it; one this SDK cannot read is
        // reported rather than handed on to be misread.
        let encoding = |serialization: Option<String>| {
            PayloadEncoding::of(serialization.as_deref(), self.serializer())
                .map_err(Failure::Control)
        };
        match outcome {
            AwaitedOutcome::Succeeded {
                output,
                serialization,
            } => Ok(EncodedOutput {
                encoded: output,
                encoding: encoding(serialization)?,
            }),
            // Handed back encoded, for the caller to decode into its own error type — the adopting
            // caller knows what that is and this function does not.
            AwaitedOutcome::Failed {
                error,
                serialization,
            } => Err(Failure::Recorded(EncodedError {
                encoded: error,
                encoding: encoding(serialization)?,
            })),
            AwaitedOutcome::Cancelled => Err(Failure::Control(Error::WorkflowCancelled {
                workflow_id: workflow_id.to_owned(),
            })),
            AwaitedOutcome::Parked { recovery_attempts } => {
                Err(Failure::Control(Error::MaxRecoveryAttemptsExceeded {
                    workflow_id: workflow_id.to_owned(),
                    recovery_attempts,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    /// A delay or a timeout the row cannot hold is refused with the start's other argument
    /// checks, which run before the start takes a step id.
    #[test]
    fn a_start_refuses_a_delay_or_timeout_the_row_cannot_hold() {
        let fits = Duration::from_secs(3600);
        assert!(
            Enqueue {
                delay: Some(fits),
                ..Enqueue::new("q")
            }
            .validate()
            .is_ok()
        );
        assert!(
            validate_start(
                None,
                Some(fits),
                None,
                WorkflowIdReusePolicy::ReturnExisting
            )
            .is_ok()
        );
        assert!(validate_start(None, None, None, WorkflowIdReusePolicy::ReturnExisting).is_ok());

        let delay = Enqueue {
            delay: Some(Duration::MAX),
            ..Enqueue::new("q")
        }
        .validate();
        assert!(
            matches!(&delay, Err(Error::InvalidArgument { operation, detail })
                if operation == ENQUEUE_OPERATION
                    && detail == "queue `q`: `delay` must fit in milliseconds as a 64-bit integer"),
            "got {delay:?}"
        );
        // Fits in milliseconds, however far past now it lands: the system database resolves it
        // on its own clock and saturates at the largest instant.
        assert!(
            Enqueue {
                delay: Some(Duration::from_millis(i64::MAX as u64)),
                ..Enqueue::new("q")
            }
            .validate()
            .is_ok()
        );
        // The enqueue's own checks run through the same function.
        let both = validate_start(
            Some(&Enqueue {
                delay: Some(Duration::ZERO),
                ..Enqueue::new("q")
            }),
            Some(fits),
            None,
            WorkflowIdReusePolicy::ReturnExisting,
        );
        assert!(
            matches!(&both, Err(Error::InvalidArgument { operation, detail })
                if operation == ENQUEUE_OPERATION && detail.contains("`delay`")),
            "got {both:?}"
        );

        // A queued start's timeout is the enqueue's argument, and names its queue.
        let queued = validate_start(
            Some(&Enqueue::new("q")),
            Some(Duration::MAX),
            None,
            WorkflowIdReusePolicy::ReturnExisting,
        );
        assert!(
            matches!(&queued, Err(Error::InvalidArgument { operation, detail })
                if operation == ENQUEUE_OPERATION
                    && detail == "queue `q`: `timeout` must fit in milliseconds as a 64-bit integer"),
            "got {queued:?}"
        );

        let timeout = validate_start(
            None,
            Some(Duration::MAX),
            None,
            WorkflowIdReusePolicy::ReturnExisting,
        );
        assert!(
            matches!(&timeout, Err(Error::InvalidArgument { operation, detail })
                if operation == START_OPERATION
                    && detail == "`timeout` must fit in milliseconds as a 64-bit integer"),
            "got {timeout:?}"
        );

        // Zero is refused here too, with the system database's own wording, rather than once the
        // start has taken its step id.
        let delay = Enqueue {
            delay: Some(Duration::ZERO),
            ..Enqueue::new("q")
        }
        .validate();
        assert!(
            matches!(&delay, Err(Error::InvalidArgument { operation, detail })
                if operation == ENQUEUE_OPERATION
                    && detail == "queue `q`: `delay` must be a positive, non-zero duration"),
            "got {delay:?}"
        );
        let timeout = validate_start(
            None,
            Some(Duration::ZERO),
            None,
            WorkflowIdReusePolicy::ReturnExisting,
        );
        assert!(
            matches!(&timeout, Err(Error::InvalidArgument { operation, detail })
                if operation == START_OPERATION
                    && detail == "`timeout` must be a positive, non-zero duration"),
            "got {timeout:?}"
        );
    }

    /// The names [`check_child`](Parent::check_child) recognises a recorded start error by are the
    /// variants [`should_record_start_error`] records, and no others.
    #[test]
    fn the_recorded_start_error_names_match_the_errors_that_are_recorded() {
        let errors = [
            crate::sysdb::Error::QueueDeduplicated {
                workflow_id: "w".to_owned(),
                queue_name: "q".to_owned(),
                deduplication_id: "d".to_owned(),
            },
            crate::sysdb::Error::ConflictingWorkflow {
                workflow_id: "w".to_owned(),
                detail: "d".to_owned(),
            },
            crate::sysdb::Error::WorkflowIdInUse {
                workflow_id: "w".to_owned(),
                status: crate::sysdb::types::WorkflowStatus::Success,
                name: Some("n".to_owned()),
            },
            crate::sysdb::Error::InvalidInput {
                field: "f".into(),
                detail: "d".to_owned(),
            },
            crate::sysdb::Error::NonExistentWorkflow {
                workflow_ids: vec!["w".to_owned()],
            },
        ];
        for error in &errors {
            let value = serde_json::to_value(error).unwrap();
            assert_eq!(
                is_recorded_start_error(&value),
                should_record_start_error(error),
                "{value}"
            );
        }
        let recorded = errors
            .iter()
            .filter(|error| should_record_start_error(error))
            .count();
        assert_eq!(recorded, RECORDED_START_ERRORS.len());
    }

    /// A subscriber that keeps every event's level and message, and nothing else.
    ///
    /// Hand-rolled because the crate has no `tracing-subscriber` dependency, and one test does not
    /// justify one: the trait is eight methods, six of which do nothing here.
    struct Capture(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        use std::fmt::Write;
                        let _ = write!(self.0, "{value:?}");
                    }
                }
            }
            let mut message = Message(format!("{} ", event.metadata().level()));
            event.record(&mut message);
            self.0.lock().unwrap().push(message.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// The premise the guard is built on, pinned: an async block's live locals are dropped during
    /// the panic's unwind out of `poll`, so the guard fires while `std::thread::panicking()` is
    /// still true. Were a runtime change to defer that drop past the catch, the error line would
    /// vanish silently — this is the test that notices.
    ///
    /// `#[tokio::test]` runs a current-thread runtime, which is what lets a thread-local
    /// subscriber observe a spawned task.
    #[tokio::test]
    async fn a_panic_in_a_spawned_task_is_logged_from_the_unwind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(Capture(Arc::clone(&events)));

        let task = tokio::spawn(async {
            let _log = PanicLog {
                workflow_id: "wf-boom",
            };
            // Held across an await point, as the real guard is.
            tokio::task::yield_now().await;
            panic!("a bug, not an outcome");
        });
        let joined = task.await.expect_err("the task must panic");
        assert!(joined.is_panic());

        let events = events.lock().unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.starts_with("ERROR") && e.contains("panicked")),
            "the unwind must produce the error event: {events:?}"
        );
    }

    /// The half that makes shutdown mean "stopped" rather than "told to stop": the sweep does not
    /// finish while a task it cancelled is still on its way out.
    #[tokio::test]
    async fn abort_all_waits_until_every_task_has_departed() {
        let tasks = Arc::new(Tasks::default());
        tasks.arrived();

        let mut sweep = {
            let tasks = Arc::clone(&tasks);
            tokio::spawn(async move { tasks.abort_all().await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut sweep)
                .await
                .is_err(),
            "the sweep returned while a task was still live"
        );

        tasks.departed();
        tokio::time::timeout(Duration::from_secs(10), sweep)
            .await
            .expect("the sweep never noticed the last task leaving")
            .expect("the sweep panicked");
    }

    /// The drain's wait: it gives up at the deadline without cancelling anything, and reports a
    /// set that has emptied as ended.
    #[tokio::test]
    async fn ended_by_gives_up_at_the_deadline_and_cancels_nothing() {
        let tasks = Tasks::default();
        tasks.arrived();
        let task = tokio::spawn(std::future::pending::<()>());
        tasks.insert(task.abort_handle(), "wf-slow".to_owned());

        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        assert!(
            !tasks.ended_by(deadline).await,
            "a live task was reported ended"
        );
        assert!(!task.is_finished(), "the wait cancelled the task");
        assert_eq!(tasks.running(), ["wf-slow"]);

        task.abort();
        tasks.departed();
        assert!(tasks.ended_by(tokio::time::Instant::now()).await);
    }

    /// The race between spawning a workflow and registering it: one that arrives after the sweep
    /// has run is cancelled here, rather than outliving the shutdown meant to stop it.
    #[tokio::test]
    async fn a_task_arriving_after_the_sweep_is_cancelled() {
        let tasks = Tasks::default();
        tasks.abort_all().await;

        let task = tokio::spawn(std::future::pending::<()>());
        tasks.insert(task.abort_handle(), "late".to_owned());

        let joined = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the late task was never cancelled");
        assert!(joined.expect_err("it must be cancelled").is_cancelled());
    }

    /// The defused path: a task that completes normally logs nothing.
    #[tokio::test]
    async fn a_completed_task_logs_no_panic() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(Capture(Arc::clone(&events)));

        tokio::spawn(async {
            let _panic_log = PanicLog {
                workflow_id: "wf-fine",
            };
            tokio::task::yield_now().await;
        })
        .await
        .expect("the task must complete");

        let events = events.lock().unwrap();
        assert!(events.is_empty(), "{events:?}");
    }

    /// A start built in one workflow and polled in another is refused, having started nothing.
    ///
    /// The id a start claims is a claim on one position in one execution, so carrying the value
    /// somewhere else is the mistake the check exists for — and a start is the case where running
    /// it anyway costs the most: it would create a durable child, spawn it, and record it against
    /// a parent whose counter that id means nothing in. Two contexts over one executor is what a
    /// smuggling looks like from the inside, and `Ctx::scope` is the only way to hold both.
    ///
    /// A unit test rather than an integration one because nothing else can enter two workflows
    /// from one call stack: an integration test would have to move the value between two
    /// registered bodies, which needs a channel and a `WorkflowRef` outliving both.
    #[tokio::test]
    async fn a_start_built_in_another_workflow_is_refused() {
        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..crate::Config::new("smuggled-start", db.url())
        });
        let child = dbos
            .register_workflow("child", |n: u32| async move { Ok::<u32, crate::Error>(n) })
            .expect("registration failed");
        dbos.launch().await.expect("launch failed");
        let executor = dbos.executor("test").expect("launched");

        // Built where the id means something: the first step of `wf-donor`. The lint reads an
        // `async` block returning a future as a missing `.await`, which is exactly what this is:
        // the point is to build the start inside the donor's context and carry it out unpolled.
        #[allow(clippy::async_yields_async)]
        let smuggled = Ctx::scope(
            Ctx::new(
                Arc::clone(&executor),
                "wf-donor",
                crate::context::TEST_OWNER,
                None,
                crate::serialization::PayloadEncoding::Native,
            ),
            async { child.start(1) },
        )
        .await;
        assert_eq!(smuggled.step_id(), Some(0), "the donor's counter moved");

        let thief = Ctx::new(
            Arc::clone(&executor),
            "wf-thief",
            crate::context::TEST_OWNER,
            None,
            crate::serialization::PayloadEncoding::Native,
        );
        match Ctx::scope(thief, smuggled).await {
            Err(Error::StepBuiltElsewhere {
                step,
                built,
                polled,
            }) => {
                assert_eq!((step.as_str(), &*built), ("child", "in workflow wf-donor"));
                assert_eq!(&*polled, "in workflow wf-thief");
            }
            other => panic!("expected a built-elsewhere refusal, got {other:?}"),
        }

        // Refused before anything was written, which is the half that matters: the alternative to
        // this check is a child running with a start record nobody can find.
        assert!(
            executor
                .sysdb()
                .get_workflow("wf-donor-0")
                .await
                .expect("read failed")
                .is_none(),
            "the refused start created no child"
        );

        dbos.shutdown().await;
    }
}
