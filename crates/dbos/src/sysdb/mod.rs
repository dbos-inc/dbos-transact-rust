//! The DBOS system database.
//!
//! This layer owns the schema, its migrations, and every query against it. It is
//! deliberately free of any dependency on the execution engine — no registry, no
//! contexts, no workflow types — so that it compiles on its own with
//! `--no-default-features`. CI enforces that, and it is what keeps a second backend
//! (SQLite) a second implementation rather than a rewrite.

/// The system schema used when configuration does not name one.
///
/// Every implementation defaults to `dbos`, so a database migrated by one is found by the
/// others without being told where to look. The name is configurable because some deployments
/// keep DBOS's tables somewhere else, but changing it is a deployment-wide decision: every
/// application sharing the database has to agree.
pub const DEFAULT_SCHEMA: &str = "dbos";

/// The queue a workflow is resumed onto when the caller names none.
///
/// Every implementation uses this exact string, and a resumed workflow must land on a queue that
/// another SDK's dequeuer also polls.
pub const INTERNAL_QUEUE: &str = "_dbos_internal_queue";

/// The topic a message with no topic is filed under.
///
/// A sentinel rather than `NULL`, because a receiver selects on `topic = $1` and nothing equals
/// `NULL`. All four implementations write this exact string, so a message sent by one is found by
/// another — it is a cross-SDK constant, not an encoding this layer is free to change.
pub const NULL_TOPIC: &str = "__null__topic__";

/// The value written to a stream to mark it closed.
///
/// A sentinel entry rather than a column, so closing is an ordinary append and a reader learns
/// of it in the same pass that reads the values. Python, Java and Go all write exactly this
/// string, so it is a cross-SDK constant.
pub const STREAM_CLOSED: &str = "__DBOS_STREAM_CLOSED__";

/// Partitions a single partitioned sweep will look at.
///
/// A bound on the work one transaction does, not on what is eventually dequeued: partitions past
/// the cap are picked up by the next poll, because the `PENDING` gate keeps a partition ineligible
/// only while its head is running. Every implementation uses this number.
pub const PARTITIONED_DEQUEUE_SWEEP_CAP: u32 = 8192;

pub mod error;
pub mod migrations;
pub(crate) mod notify;
pub mod postgres;
pub mod retry;
pub mod types;

use async_trait::async_trait;

// Re-exported: `sysdb::Error` is how the rest of the crate and its callers name it, and moving
// the definition to a file should not move the path.
pub use error::{BackendError, BackendErrorKind, Error};

use std::time::Duration;

use types::step_names;
use types::{
    ApplicationRowCounts, Applications, AwaitedOutcome, Debounce, DebounceRequest, EncodedValue,
    EventRecord, Fork, ForkOptions, ForkPoint, GetEventCaller, InitWorkflowCaller, Message,
    NewQueue, NewSchedule, NewWorkflow, NotificationRecord, OnExistingQueue, Outcome, OutcomeWrite,
    QueueRecord, QueueUpdate, RenameBatching, RenameFrom, ScheduleFilter, ScheduleRecord,
    ScheduleStatus, ScheduleUpdate, StepRecord, StepTiming, StreamRead, StreamRecord, Submission,
    Timestamp, VersionInfo, WorkflowDelay, WorkflowFilter, WorkflowInitResult, WorkflowRecord,
    WrittenBy,
};

/// Everything the engine needs from the system database.
///
/// **No method mentions a driver type.** No pool, no row, no `sqlx::Postgres` — each
/// implementation owns its connections privately. That is what keeps a second backend a second
/// implementation rather than a rewrite, and what would let a host language call this across an
/// FFI boundary.
///
/// Payloads cross as already-encoded strings; see [`types`].
///
/// The methods that must be atomic with the step recording them take a `caller: Option<(&str,
/// i32)>` — a workflow id and step id — and own the transaction internally, rather than taking a
/// caller's connection the way Python and TypeScript do. [`get_event`](SystemDatabase::get_event)
/// takes a [`GetEventCaller`] instead — the same thing plus the step its deadline is recorded
/// under — because there the caller is optional and a wrapped `Option` is what reads. It is named
/// for its one method rather than for blocking in general: `recv` blocks too and takes plain
/// parameters, because its caller is required and its two step ids are not optional together.
///
/// TODO(dbos-team): UPSTREAM item 13, the shape itself. Threading a `PoolClient` or
/// `sa.Connection` through the system database makes atomicity the call site's job to remember,
/// and is the part that would not survive a language-neutral core — a host can pass two strings
/// and an integer, not a connection. Nothing is broken either way; worth the team having seen it.
#[async_trait]
pub trait SystemDatabase: Send + Sync {
    /// Records a workflow, reconciling with any row already under that id.
    ///
    /// The same id submitted twice is one workflow, which is what makes a retried enqueue safe.
    /// Reconciling is not a no-op, though — three things happen on conflict:
    ///
    /// - **Recovery attempts are counted**, but only when the existing row is not merely queued
    ///   and only when the caller is recovering or dequeuing. Passing the limit parks the
    ///   workflow as [`types::WorkflowStatus::MaxRecoveryAttemptsExceeded`] and errors.
    /// - **The executor is re-stamped**, unless this is an enqueue — a queued workflow has no
    ///   executor yet, and claiming one would be wrong.
    /// - **A different function under the same id is an error.** Name, class, and config must
    ///   match; a differing queue is only a warning, since requeueing elsewhere is legitimate.
    ///
    /// `max_recovery_attempts` of `None` disables parking entirely. [`Submission`] says why the
    /// workflow is being submitted, and so whether it may claim a row another owner holds.
    ///
    /// The owner identity behind the single-execution guard is generated in here rather than
    /// passed in, and generated once per call — before any retry the implementation makes. A
    /// retry that generated a fresh identity after a lost commit acknowledgement would fail to
    /// recognise its own write and conclude another executor owned the row.
    ///
    /// **`caller` makes the child's row and the parent's record of it one write.** A start from
    /// inside a workflow is a step of that workflow, and as two statements the pair has a gap: a
    /// crash between them — or a dropped future, since a start is a future and any combinator that
    /// races one may drop it — leaves a child workflow that exists, carries a
    /// `parent_workflow_id`, and has nothing in the parent pointing at it.
    ///
    /// A replaying parent survives that, and it is worth being exact about why: the child's id is
    /// derived from the parent's and this step's, so the replay re-derives it, finds the row owned,
    /// and joins the child it already made rather than starting a second one. What does *not*
    /// survive is the pair coming apart in the other direction. If the record fails permanently
    /// where the row succeeded, the caller is told its start failed while the child exists and is
    /// `PENDING` — a workflow that will run, reported as one that never began. And a parent that
    /// never replays, because it finished or because the losing branch of a race dropped the start,
    /// leaves that child unreferenced by anything but its own parent column.
    ///
    /// Passing the caller closes both: the two rows commit together or neither does. `None` is a
    /// root start, which has no parent to record against.
    ///
    /// This is the shape [`fork_workflows`](Self::fork_workflows) and the management calls already
    /// use — the checkpoint committing with the operation it records — reaching the one operation
    /// that creates a workflow.
    async fn init_workflow(
        &self,
        workflow: &NewWorkflow,
        max_recovery_attempts: Option<i64>,
        submission: Submission,
        caller: Option<InitWorkflowCaller<'_>>,
    ) -> Result<WorkflowInitResult, Error>;

    /// Reads one workflow, or `None` if there is no such id.
    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowRecord>, Error>;

    /// Reads the workflows matching a filter, oldest first unless told otherwise.
    ///
    /// This is one query with every filter folded into its `WHERE` clause, not a scan the caller
    /// narrows. `WorkflowFilter::default()` therefore returns the whole table, and callers that
    /// mean to page should say so with [`WorkflowFilter::limit`].
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// read and its step checkpoint **commit together**, so what a replay reads back is exactly
    /// the snapshot the first execution saw — see [`fork_workflows`](Self::fork_workflows).
    async fn list_workflows(
        &self,
        filter: &WorkflowFilter<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<WorkflowRecord>, Error>;

    /// Every workflow descended from this one, at any depth.
    ///
    /// Excludes the workflow itself. Walks level by level rather than recursing in SQL, which is
    /// what all four implementations do.
    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, Error>;

    /// Records a terminal outcome, but only while the workflow is still running.
    ///
    /// The status gate is the point. Two executors can believe they own the same workflow — one
    /// recovering after the other was presumed dead — and whichever finishes second must not
    /// overwrite the first's result. Returning which happened lets the loser adopt the recorded
    /// outcome instead of reporting its own.
    ///
    /// The terminal status comes from the [`Outcome`] rather than being passed separately, so a
    /// success carrying an error is unrepresentable. No implementation treats the two as
    /// independent — see [`Outcome`].
    ///
    /// **Losing here is a value; losing in [`record_step`](Self::record_step) is an error.** The
    /// two look parallel and deliberately are not. A workflow whose outcome was recorded by someone
    /// else has simply been superseded, and the right move is to adopt what is stored — routine
    /// enough to be a return value. A step recorded by someone else means two executions of one
    /// workflow are live at the same moment, which every implementation raises on.
    async fn record_workflow_outcome(
        &self,
        workflow_id: &str,
        outcome: Outcome<'_>,
    ) -> Result<OutcomeWrite, Error>;

    /// Waits for a workflow to finish and reports how it did.
    ///
    /// The read counterpart of [`record_workflow_outcome`](Self::record_workflow_outcome): what one
    /// run records, this is how everyone else finds out. A workflow handle's result is this call,
    /// and so is the adopt half of park-and-adopt — a run that loses the status-gated outcome write
    /// reads back the outcome that won.
    ///
    /// **It polls, and every implementation does.** There is no wakeup for workflow completion in
    /// any of the four: no channel carries it and no trigger publishes it, so the only way to learn
    /// that a status has changed is to look. `poll_interval` is how often, and the caller supplies
    /// it because the engine's configuration owns that number.
    ///
    /// **No timeout**, following Python and Go. A wait ends when the workflow does, and a caller
    /// that wants to stop sooner drops the future.
    ///
    /// **A missing row means "not inserted yet", and `fail_if_missing` is how a caller says
    /// otherwise.** This waits for a workflow to *finish*, and by default a row that is not there
    /// is one that has not appeared yet — a caller holding an id from outside this process, waiting
    /// on whoever owns it to enqueue it, is the case that default serves. A caller that knows the
    /// row must already exist passes `true` and gets [`Error::NonExistentWorkflow`] instead of
    /// polling for a row that will never reappear.
    ///
    /// All four references draw the line in the same place and in the same words — Python's
    /// `fail_if_missing` (`_sys_db.py:1885`), Go's and Java's required `failIfMissing`,
    /// TypeScript's optional one — and they all give the same reason for it: *"The row is known to
    /// have existed (this run inserted or read it), so a missing row means it was deleted: fail
    /// fast rather than polling for a row that will never reappear."* Python, Go and TypeScript
    /// pass `true` only where a run parks on its own outcome; **Java also passes it at a handle**,
    /// on a start that finds the row already `SUCCESS` (`DBOSExecutor.java:1914`), and carries the
    /// flag on the handle itself — `WorkflowHandleDBPoll`, defaulting to `false`, *"for handles
    /// built from a `workflow_status` row that was just read"*.
    ///
    /// **This engine takes Java's shape and applies the reason wherever it holds**, which is more
    /// call sites than any reference has. The test is whether the caller has had the row in front
    /// of it, not which internal route it arrived by. Everyone's park sites are here — the adopt
    /// path in `workflow.rs`, the deadline cancellation beside it, and the cancellation poller in
    /// `step.rs` — and so is every handle minted by a call that wrote or read the row: a start, an
    /// enqueue, a join onto a workflow another execution already owns. Java stops after the
    /// already-finished start and lets the rest wait; the other three let every handle wait. What
    /// still waits here is what cannot do better: an id this process has only been handed
    /// ([`Client::retrieve_workflow`](crate::Client::retrieve_workflow)) or holds only a launch
    /// record for (a parent replaying a child it started), where "deleted" and "not yet" are the
    /// same observation.
    ///
    /// **The hazard the default carries is real**: a workflow deleted while somebody awaits it
    /// hangs that waiter, because such a wait cannot tell "not yet" from "never again". Deleting is
    /// an ordinary operation, reachable from Console and Conductor. Rust waits where the references
    /// wait, but only on the handles that have never seen the row, where all four wait on every
    /// handle.
    ///
    /// **The fix for what remains is a bound on the wait, and here the caller already has one.**
    /// Go offers `WithHandleTimeout` and TypeScript a durable `timeoutSeconds`, both of which a
    /// caller has to know to pass; Python and Java have nothing to pass, and a blocked
    /// `get_result` stays blocked. A Rust caller wraps the future in `tokio::time::timeout` or
    /// simply drops it, and the poll stops — the same escape the *no timeout* note above describes,
    /// applied to the one case that can still wait forever. That is why this method takes no
    /// timeout of its own: the language supplies the bound the references had to add parameters
    /// for.
    ///
    /// TODO(dbos-team): UPSTREAM item 17.
    ///
    /// Cancellation and dead-lettering are reported as values rather than errors; see
    /// [`AwaitedOutcome`] for why that is not merely convenient.
    ///
    /// **Each poll takes a connection for the length of a query**, so this waits under the polling
    /// concurrency cap — half the pool by default, configured by
    /// [`Settings::polling_concurrency`](postgres::Settings::polling_concurrency). Python caps the
    /// same waits together at half its pool with `sys_db_polling_concurrency`, and TypeScript has
    /// the equivalent. A hundred uncapped waiters are a hundred queries per interval, which empties
    /// the pool and starves the control plane — leaving those waiters blocked on writes that can no
    /// longer happen.
    ///
    /// The permit covers the query and not the wait: it is taken inside the retried region and
    /// released before the interval sleep, so a waiter parked between polls holds nothing. A cap on
    /// concurrent *waiters* rather than concurrent queries would deadlock at the first pool's worth
    /// of them.
    async fn await_workflow_result(
        &self,
        workflow_id: &str,
        poll_interval: Duration,
        fail_if_missing: bool,
    ) -> Result<AwaitedOutcome, Error>;

    /// Waits until one of these workflows has settled, and reports which.
    ///
    /// "Settled" is the *status* leaving `PENDING`, `ENQUEUED` and `DELAYED` — the same three
    /// TypeScript's `awaitFirstWorkflowId` and Python's `check_first_workflow_id` exclude. So a
    /// cancelled or dead-lettered workflow counts as settled and can be the one reported, which is
    /// deliberate in all three: the caller asked which race finished first, not which succeeded,
    /// and it has [`await_workflow_result`](Self::await_workflow_result) to ask the second question
    /// with. Reporting only successes would leave a caller waiting out its whole fan-out because
    /// one member was cancelled.
    ///
    /// **An id naming no row is waited for, never refused.** There is no `fail_if_missing` here
    /// and neither reference has one: a set of ids is settled by whichever member appears first,
    /// and an id whose enqueue has not committed yet is indistinguishable from one that will never
    /// exist. A caller who needs the stricter reading holds one id and uses
    /// [`await_workflow_result`](Self::await_workflow_result).
    ///
    /// **Duplicate ids are fine, and nothing above requires otherwise either.** `ANY` de-duplicates
    /// on its own and `LIMIT 1` answers with an id, which names one workflow however many entries
    /// pointed at it — so a repeat is invisible here and stays invisible all the way out to
    /// [`select_workflow`](crate::DBOS::select_workflow), whose answer is that same id. Python and
    /// TypeScript both reject a repeated id at their own surface, but only because they return a
    /// *handle* and key a map by id to find it; neither constraint survives the translation.
    ///
    /// Empty input is [`Error::InvalidInput`], not a wait that never ends — a caller's value the
    /// layer will not act on, rather than a stored one it could not read. Python raises
    /// `ValueError` at the same spot; a query over an empty array matches nothing forever, which
    /// is the worst possible reading of "wait for one of nothing".
    ///
    /// **Two members that settle in the same interval are a tie this cannot break**, and the
    /// winner between them is arbitrary. There is no `ORDER BY`: the poll asks for one settled row
    /// and takes whichever the plan yields first, which can vary with statistics and physical row
    /// order. TypeScript and Python both do exactly this — neither orders its own `LIMIT 1` — so a
    /// Rust waiter and a TypeScript one on the same database are arbitrary in the same way rather
    /// than differently.
    ///
    /// It is also the honest answer, because **the poll interval is the resolution at which this
    /// call can observe finishing at all**. Two candidate orderings look like improvements and are
    /// not:
    ///
    /// - `ORDER BY completed_at` would be a lie for the one status hardest to reason about. The
    ///   dead-letter transition sets neither `completed_at` nor `updated_at`, so a parked member
    ///   sorts on a stale or absent value and would systematically win or lose by where `NULLS`
    ///   were put — not by when it stopped.
    /// - `ORDER BY workflow_uuid` would be stable and meaningless: it turns *which finished first*
    ///   into *which sorts first* whenever more than one is ready, biasing every tie toward
    ///   `task-0` in a fan-out named that way. Stable nondeterminism reads as a guarantee and is
    ///   not one, which is worse than visible arbitrariness.
    ///
    /// **What makes the arbitrariness harmless is the checkpoint above this layer**, not anything
    /// here. [`select_workflow`](crate::DBOS::select_workflow) records the winner, so a replay
    /// reads it back rather than racing again and cannot take a different branch. A caller outside
    /// a workflow has nothing recorded and may well see a different winner from a second call over
    /// the same settled set — which is right, because it asked a question about *now*.
    ///
    /// **One row, not one per id**, and it polls under the same concurrency cap
    /// [`await_workflow_result`](Self::await_workflow_result) waits under and for the same reason
    /// — a fan-out waiting on N workflows through N separate result waits is N queries per
    /// interval, where this is one whatever N is. That is the efficiency the call exists for as
    /// much as the semantics.
    async fn await_first_workflow_id(
        &self,
        workflow_ids: &[&str],
        poll_interval: Duration,
    ) -> Result<String, Error>;

    /// Waits until every one of these workflows has settled.
    ///
    /// The all-form of [`await_first_workflow_id`](Self::await_first_workflow_id), with the same
    /// definition of settled and the same treatment of an id that names no row — so this waits out
    /// a mistyped id rather than reporting it, exactly as TypeScript's `awaitWorkflowIds` does.
    ///
    /// **Each pass asks only about the ids still outstanding**, which is what keeps a long fan-out
    /// from re-reading the whole set every interval once most of it has finished. TypeScript
    /// narrows the same way, against a `Set` it deletes from.
    ///
    /// **Duplicates are harmless**, as they are in the first-form: settling is a property of an id
    /// rather than a choice between ids, so a repeated id is simply satisfied twice. Nothing above
    /// removes them either — [`join_workflows`](crate::DBOS::join_workflows) passes the caller's
    /// slice through as it was given — so an implementation must expect them.
    ///
    /// **Nothing above checkpoints this wait**, where
    /// [`await_first_workflow_id`](Self::await_first_workflow_id) is recorded when a workflow makes
    /// it: an all-wait decides nothing, so a replay simply asks again and is answered by a set that
    /// has already settled. TypeScript records its `waitAll` and this does not — the
    /// free [`join_workflows`](crate::join_workflows()) sets out why.
    ///
    /// Narrowing the array it sends is then an implementation's own business rather than a
    /// contract: the Postgres one holds its outstanding ids in a `HashSet`, as TypeScript holds
    /// its own in a `Set`, so a repeat collapses on the way in and each pass removes what it
    /// watched settle without scanning what it did not.
    ///
    /// Empty input returns at once. Nothing to wait for is a satisfied wait, and TypeScript
    /// short-circuits an empty handle list the same way — where an empty *first*-wait has no
    /// answer to give and is refused.
    async fn await_workflow_ids(
        &self,
        workflow_ids: &[&str],
        poll_interval: Duration,
    ) -> Result<(), Error>;

    /// Moves a delayed workflow's release time.
    ///
    /// Only touches a `DELAYED` row. A workflow that has already been released is running or
    /// queued, and pushing its delay out would not recall it.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together** — see
    /// [`fork_workflows`](Self::fork_workflows), which explains what that buys and why this whole
    /// surface takes it.
    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay: WorkflowDelay,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Puts a running workflow back on its queue, reporting whether it moved.
    ///
    /// For an executor that claimed a queued workflow and then could not run it. Only applies to
    /// a `PENDING` row that has a queue to return to.
    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, Error>;

    /// Replaces a workflow's attributes. `None` clears them.
    ///
    /// A replacement rather than a merge, matching every implementation.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together** — see
    /// [`fork_workflows`](Self::fork_workflows), which explains what that buys and why this whole
    /// surface takes it.
    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Returns this executor's abandoned workflows to a queue, so any peer may run them.
    ///
    /// **Recovery's whole write.** A `PENDING` row whose executor is gone goes back to `ENQUEUED`
    /// and is dispatched by whichever executor next polls its queue, rather than being executed by
    /// the process that found it. Python, TypeScript and Go all recover this way; it is what makes
    /// a recovery sweep idempotent, lets a fleet share one backlog, and turns "how many at once"
    /// into a question the queue answers.
    ///
    /// A workflow already on a queue goes back to **its own** queue; only one that was never
    /// queued lands on `recovery_queue`, which callers set to [`INTERNAL_QUEUE`].
    ///
    /// **`executor_ids` is what makes a repeat harmless.** Once any live executor dequeues one of
    /// these rows, the claim stamps its own executor id, so a second sweep naming the dead
    /// executor matches nothing rather than tearing a running workflow off its runner.
    ///
    /// **Scoped by application version and by application.** A workflow started under different
    /// code must not be resumed by an executor running this version, because its recorded steps
    /// may no longer line up. And `executor_id` defaults to `"local"` — Rust follows Go here — so
    /// two applications running on one machine present the same executor to this query; unscoped,
    /// each would re-enqueue the other's workflows and go on to run functions it has never heard
    /// of. Migration 7's `owner_xid` does not help, because a recovery sweep is looking for
    /// workflows whose owner is *gone*.
    ///
    /// Returns the ids that actually moved. An empty `executor_ids` moves nothing.
    async fn reenqueue_for_recovery(
        &self,
        executor_ids: &[&str],
        application_version: &str,
        recovery_queue: &str,
    ) -> Result<Vec<String>, Error>;

    /// Releases delayed workflows whose time has come, returning how many moved.
    ///
    /// **Clears the deduplication id of debounced workflows in the same statement.** That id is a
    /// debounce key held only while the workflow is `DELAYED`; once released the workflow is
    /// committed to running, and a later debounce with the same key must start a fresh workflow
    /// rather than bounce this one. Python does this and explains it; **Java does not**, and its
    /// version predates the column.
    async fn transition_delayed_workflows(&self) -> Result<u64, Error>;

    /// Cancels workflows, returning the ids that actually moved.
    ///
    /// A workflow that has already finished is left alone rather than reported as an error —
    /// cancelling something that is already over is a no-op, not a mistake. The returned ids are
    /// those that were still running, so a caller that needs to know can compare.
    ///
    /// Cancelling also clears the queue assignment, the deduplication key, and the start time,
    /// so a cancelled workflow cannot be dequeued and cannot hold a deduplication key against a
    /// later workflow that wants it.
    ///
    /// With `cancel_children`, the cascade **cancels each level before discovering the next**,
    /// and repeats until it finds nothing new. Cancelling a parent first is what stops it
    /// spawning more children behind the walk — Java's comment on the same loop reads "cancel
    /// level-by-level so newly-spawned children are also caught".
    ///
    /// Go instead collects the whole subtree and cancels it in one statement. That is one round
    /// trip rather than one per level, but it walks the tree while every workflow in it is still
    /// running, so a child spawned during the walk is missed. Python and Java both interleave,
    /// and this follows them.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together** — see
    /// [`fork_workflows`](Self::fork_workflows), which explains what that buys and why this whole
    /// surface takes it.
    ///
    /// **The cascade is one transaction, which buys atomicity and not exclusion.** A failure
    /// partway through leaves the tree untouched rather than half-cancelled — that is the part it
    /// buys. It does not stop a child being spawned behind the walk: at READ COMMITTED these
    /// writes are invisible to the workflows they cancel until the commit, and nothing constrains
    /// `parent_workflow_id`, so a parent that has not yet learned it is cancelled can commit a new
    /// child after the walk has read its level. That child keeps running.
    ///
    /// What bounds the window is the interleaving: each level is read after the level above it is
    /// written, so the deeper the tree the later the last read, and a child committed before that
    /// read is caught. Only the commit actually stops a parent, at its next step boundary.
    ///
    /// **Python and Java get more from the same order than this does**, and the difference is the
    /// transaction. Python commits each level (`_sys_db.py:1129`, a `with self.engine.begin()` per
    /// level, its child reads outside any transaction), so a parent reading its own status between
    /// levels finds `CANCELLED` and stops spawning mid-walk. Wrapping the cascade to commit it with
    /// the step checkpoint — which is what this whole surface does — trades that visibility for
    /// atomicity. Worth knowing before treating either half as free.
    async fn cancel_workflows(
        &self,
        workflow_ids: &[&str],
        cancel_children: bool,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<String>, Error>;

    /// Re-enqueues workflows, returning the ids that actually moved.
    ///
    /// Resuming clears the recovery-attempt count and the deadline: the workflow is being given
    /// a fresh start, and holding it to a deadline set before it was parked would fail it
    /// immediately. `queue_name` defaults to [`INTERNAL_QUEUE`].
    ///
    /// Unlike [`cancel_workflows`](Self::cancel_workflows), an id with no row behind it is an
    /// [`Error::NonExistentWorkflow`]. The two differ because a zero-row update cannot tell
    /// "already finished" from "never existed", and here the distinction matters: resuming an id
    /// that was mistyped should say so rather than silently do nothing. Python draws the same
    /// line, and for the same reason.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together** — see
    /// [`fork_workflows`](Self::fork_workflows), which explains what that buys and why this whole
    /// surface takes it.
    ///
    /// TODO(dbos-team): UPSTREAM item 27. The only guard is that the row is not terminal, which is
    /// the predicate all five share — so `PENDING` passes, and resuming a workflow that is
    /// executing right now re-enqueues it underneath its own execution. The next sweep claims the
    /// row and dispatches it, and two executions of one id run concurrently: neither is told about
    /// the other, and the running one is not cancelled, so nothing stops it at its next step. It
    /// cannot simply be tightened here, because `PENDING` means *owned*, not *running*, and the
    /// row cannot say whether the owner is alive — a workflow left `PENDING` by a dead node is the
    /// case an operator most wants to resume by hand.
    async fn resume_workflows(
        &self,
        workflow_ids: &[&str],
        queue_name: Option<&str>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<String>, Error>;

    /// Deletes workflows and everything hanging off them.
    ///
    /// Steps, notifications, events, and streams go with the row: the schema declares
    /// `ON DELETE CASCADE` on every child table, so one `DELETE` is the whole operation.
    ///
    /// Unlike [`cancel_workflows`](Self::cancel_workflows), the descendants are collected first
    /// and deleted in one statement rather than level by level: the cascade does the work that
    /// cancelling needs a statement per level for. Interleaving would buy nothing here, because
    /// what stops a parent spawning is the delete itself, and that is one statement whichever
    /// order the tree was read in.
    ///
    /// Python takes no `delete_children` flag and always deletes only what it is given. Java and
    /// Go have it, and this follows them.
    ///
    /// **No status guard, deliberately.** A running workflow is deleted like any other — Go's
    /// comment on the same statement reads "Delete all matching workflows regardless of their
    /// state", and all four behave that way. Naming an id is an operator saying *this one, now*,
    /// and refusing would leave no way to clear a workflow that is wedged.
    ///
    /// The guard belongs to garbage collection instead, which sweeps by age rather than by id
    /// and so must never take out live work: all four exclude `PENDING`, `ENQUEUED`, and
    /// `DELAYED` there. That method is not built yet.
    ///
    /// The consequence for a caller: an executor running a deleted workflow finds its row gone
    /// at the next step boundary and fails with [`Error::NonExistentWorkflow`], rather than
    /// being stopped cleanly. [`cancel_workflows`](Self::cancel_workflows) is the graceful form.
    ///
    /// Takes `&[&str]` rather than `&[String]`, as the other bulk methods do: a caller holding
    /// owned ids converts by copying pointers, where the reverse would allocate.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together** — see
    /// [`fork_workflows`](Self::fork_workflows), which explains what that buys and why this whole
    /// surface takes it.
    ///
    /// The descendants are collected *before* the transaction, unlike
    /// [`cancel_workflows`](Self::cancel_workflows)'s cascade, because there is no race a
    /// transaction could close: a parent stops spawning when its row goes, which is after the walk
    /// rather than during it, and moving the walk inside would not change that at READ COMMITTED. A
    /// child committed between the walk and the delete survives its parent, with
    /// `parent_workflow_id` naming a row that is gone — nothing constrains that column. Deleting a
    /// tree that is still running is inherently that: [`cancel_workflows`](Self::cancel_workflows)
    /// first is what makes it a tree that has stopped.
    async fn delete_workflows(
        &self,
        workflow_ids: &[&str],
        delete_children: bool,
        caller: Option<(&str, i32)>,
    ) -> Result<u64, Error>;

    /// Forks workflows, each resuming from its own start step.
    ///
    /// A fork is a *new* workflow that inherits its source's identity — name, inputs, roles,
    /// attributes — and the recorded results of every step below
    /// [`Fork::start_step`]. Those steps replay instead of running, so the fork reaches the start
    /// step in the state the original was in when it got there, and runs on from a point that
    /// already happened. Re-running a failed workflow against fixed code is what this is for.
    ///
    /// Returns the forked ids in the order given, including any that were generated.
    ///
    /// **The fork is enqueued, not started.** Its status is `ENQUEUED` on
    /// [`INTERNAL_QUEUE`] unless [`ForkOptions::queue_name`] says otherwise, so whichever
    /// executor next polls that queue runs it. All four references do this: the process asking
    /// for a fork is usually an operator's tool, not a host that can run the workflow.
    ///
    /// **Four tables are copied, not one.** Steps come from `operation_outputs`; a workflow that
    /// published events or wrote streams before its start step must find them again, so
    /// `workflow_events_history`, `workflow_events`, and `streams` are copied too. All are
    /// bounded by `function_id < start_step`, and the events *current* value is rebuilt from the
    /// history rather than copied from the source — copying it would carry forward a value set
    /// after the fork point.
    ///
    /// Batch because the callers are batch: forking from failure takes a list, and forking a
    /// tree of workflows has to write the whole tree at once for
    /// [`ForkOptions::replacement_children`] to name ids that exist. Go and Python take the batch
    /// form directly; Java and TypeScript expose a single-workflow wrapper over one.
    ///
    /// Fails with [`Error::NonExistentWorkflow`] if any source is missing, and writes nothing —
    /// a partially applied batch would leave forks whose siblings never existed.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing the forking. Given
    /// one, the forks and their step checkpoint **commit together**, and a replay returns the ids
    /// the first run generated rather than forking again. That atomicity matters more here than
    /// anywhere else on the management surface: a fork's id is generated by the call, so a replay
    /// that found no checkpoint would write a *second* fork under a second id and run the work
    /// twice. Cancelling or deleting the same workflow twice reaches the same end state; forking
    /// it twice does not.
    ///
    /// **This is ahead of the references.** All four route their management calls through a
    /// non-transactional step wrapper — Python's `call_function_as_step`, TypeScript's
    /// `runInternalStep`, Go's `RunAsStep`, Java's `runDbosFunctionAsStep` — which leaves exactly
    /// that window open. TypeScript has the transactional variant and spends it on schedules;
    /// this crate has it too, and spends it here as well.
    async fn fork_workflows(
        &self,
        forks: &[Fork<'_>],
        options: &ForkOptions<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<String>, Error>;

    /// Forks workflows from a step this works out for each of them.
    ///
    /// Named for what it does: Python, Java and TypeScript all call this `fork_from_failure`,
    /// but three of [`ForkPoint`]'s four cases have nothing to do with failure. Go calls it
    /// `ForkFrom` for the same reason.
    ///
    /// [`fork_workflows`](Self::fork_workflows) with the start step computed rather than given:
    /// the caller says *from the failure* or *from the step called `charge_card`*, and each
    /// workflow's own history decides where that is. Ids are always generated, since a caller
    /// who is not choosing the step is not choosing the id either.
    ///
    /// Fails with [`Error::NoForkPoint`] if any workflow has nothing at the point asked for, and
    /// writes nothing. [`ForkPoint::Step`] is exempt: it names a position directly, so there is
    /// nothing to look up and nothing to be missing.
    ///
    /// `caller` works as it does on [`fork_workflows`](Self::fork_workflows), and covers the
    /// lookup as well as the write: the resolve runs inside the same transaction, so a replay
    /// never asks where the fork point was. That is what lets a replayed fork answer for a source
    /// whose steps have since been deleted — the recorded ids are the answer, and nothing is
    /// looked up to produce them.
    async fn fork_from(
        &self,
        workflow_ids: &[&str],
        point: ForkPoint<'_>,
        options: &ForkOptions<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<String>, Error>;

    /// Delivers one message to a workflow.
    ///
    /// Records the step as `"DBOS.send"`, which is what makes this a separate method from
    /// [`send_messages`](Self::send_messages) rather than a caller with a one-element slice: the
    /// step name is the API surface the caller reached for, and a batch of one is still a batch.
    /// Python and Java pass their name down from the same two surfaces
    /// (`function_name="DBOS.send"` versus `"DBOS.send_bulk"`; `"DBOS.send"` versus
    /// `"DBOS.sendBulk"`), and neither infers it from a count.
    ///
    /// Everything else is [`send_messages`](Self::send_messages)'s, which this shares an
    /// implementation with — the transaction, the fork fan-out, the replay skip, the two kinds of
    /// idempotency, and the foreign key that refuses a destination that does not exist.
    async fn send_message(
        &self,
        message: &Message<'_>,
        serialization: Option<&str>,
        caller: Option<(&str, i32)>,
        send_to_forks: bool,
    ) -> Result<(), Error>;

    /// Delivers many messages to workflows, in one transaction.
    ///
    /// Records the step as `"DBOS.sendBulk"` however many messages it is given, one and none
    /// included — see [`send_message`](Self::send_message) for why the name is the method rather
    /// than the count. Python writes `DBOS.send_bulk` here and Java `DBOS.sendBulk`; Java's
    /// spelling is taken because the rest of this constant family is camelCase already.
    ///
    /// `caller` is the sending workflow and the step id to record against.
    ///
    /// **Two independent kinds of idempotency, for two different callers.** `caller` makes a
    /// whole batch idempotent for a *workflow*: a replay finds the step recorded and sends
    /// nothing.
    /// [`Message::idempotency_key`] makes one message idempotent for *anyone*, by deriving the
    /// row's primary key from it so a duplicate is discarded by the database. A sender outside a
    /// workflow has no step, and the key is all it has.
    ///
    /// With `send_to_forks`, each message also reaches every workflow recursively forked from its
    /// destination. The fork set is resolved inside the same transaction as the insert, so it
    /// cannot be made stale by a fork created while the send is in flight.
    ///
    /// Fails with [`Error::NonExistentWorkflow`] if a destination does not exist — the foreign
    /// key catches it, so a message can never be left addressed to nothing.
    async fn send_messages(
        &self,
        messages: &[Message<'_>],
        serialization: Option<&str>,
        caller: Option<(&str, i32)>,
        send_to_forks: bool,
    ) -> Result<(), Error>;

    /// Takes the oldest message sent to a workflow, waiting up to `timeout` for one to arrive.
    ///
    /// The read counterpart of [`send_messages`](Self::send_messages), and the consuming one: a
    /// message is delivered exactly once, marked `consumed` rather than deleted so that what a
    /// workflow was sent stays visible to export and audit.
    ///
    /// **The caller is not optional, unlike [`get_event`](Self::get_event)'s** — which is why the
    /// three parts are plain parameters here and a [`GetEventCaller`] there. All four
    /// implementations require a workflow, and the workflow receiving *is* the workflow calling, so
    /// `workflow_id` is the destination and the step owner at once. A client outside a workflow has
    /// [`get_all_notifications`](Self::get_all_notifications) to read with and no way to consume,
    /// which is the right shape: consuming without a step to record it against would lose the
    /// message on any retry.
    ///
    /// The struct is what makes `get_event`'s *optional* caller readable; with nothing optional to
    /// wrap it would buy only naming. Python, TypeScript and Java each carry a caller type for
    /// `get_event` and pass `recv`'s three flat, which is three implementations reaching the same
    /// place — Java's is even named `GetEventCaller`.
    ///
    /// `step_id` records the receive, so a replay returns the message rather than taking another.
    /// `timeout_step_id` records the deadline, so a recovery resumes it rather than restarting the
    /// timeout.
    ///
    /// `topic` of `None` is the default topic, stored as the same sentinel
    /// [`send_messages`](Self::send_messages) writes — a real string rather than SQL `NULL`,
    /// because nothing equals `NULL` and a receiver selecting on it would never find its own
    /// message.
    ///
    /// **Two concurrent receivers on one (workflow, topic) is
    /// [`Error::ConcurrentRecv`].** One message goes to one of them, so the other can only wait out
    /// its timeout and report nothing — which the sender cannot distinguish from not having sent.
    /// Python and Go reject it; TypeScript and Java allow it. Erring towards the error is the
    /// recoverable direction: a layer above can swallow one this layer raises, and cannot
    /// manufacture one it never raised. The guard is per process; two receivers in *different*
    /// processes are arbitrated at the database by the consuming statement, which is the case
    /// recovery actually produces.
    ///
    /// **Absence is a value.** `Ok(None)` means nothing was waiting when the deadline passed. Go
    /// raises a timeout error, in its engine rather than at this layer.
    ///
    /// The waiting, the two checkpoints and the polling cap are exactly
    /// [`get_event`](Self::get_event)'s; what differs is that the message is consumed and recorded
    /// in one transaction, since a message taken but not recorded would be lost outright rather
    /// than merely re-read.
    async fn recv(
        &self,
        workflow_id: &str,
        step_id: i32,
        timeout_step_id: i32,
        topic: Option<&str>,
        timeout: Duration,
    ) -> Result<Option<EncodedValue>, Error>;

    /// Appends a value to a workflow's stream.
    ///
    /// The offset is allocated by the insert itself, as `MAX(offset) + 1` for the key — so two
    /// writers racing produce a primary-key collision rather than two entries at one offset, and
    /// the loser retries onto the next offset.
    ///
    /// **Computing it inside the insert narrows that race but does not close it.** Under `READ
    /// COMMITTED` two concurrent statements can evaluate `MAX(offset)` to the same value and one
    /// still loses the key, so the retry is what makes this correct and the single statement only
    /// makes it rarer. Python and Go compose it this way; Java and TypeScript read the offset in
    /// a separate round trip first, holding the window open for longer.
    ///
    /// `written_by` decides whether the write is itself a durable step; see [`WrittenBy`].
    ///
    /// **Writing to a closed stream is allowed here.** Go rejects it, alone among the four, at
    /// the cost of a query per write. A reader stops at the sentinel, so an entry appended after
    /// it is invisible rather than corrupting — and a workflow that writes after closing has a
    /// bug this layer cannot fix by refusing one of the two writes.
    async fn write_stream(
        &self,
        workflow_id: &str,
        step_id: i32,
        key: &str,
        value: &str,
        serialization: Option<&str>,
        written_by: WrittenBy,
    ) -> Result<(), Error>;

    /// Marks a stream closed, so a reader knows no more values are coming.
    ///
    /// An ordinary append of [`STREAM_CLOSED`], which is why closing is durable and replayable
    /// on the same terms as any other write. Always a workflow-level step: a stream is closed by
    /// the workflow that owns it.
    async fn close_stream(&self, workflow_id: &str, step_id: i32, key: &str) -> Result<(), Error>;

    /// Releases the connections this backend holds.
    ///
    /// Idempotent, and on the trait rather than the concrete type because shutdown reaches the
    /// system database only through a trait object. Java's `SystemDatabase` and Go's `SysDB`
    /// both expose the same.
    async fn close(&self);

    /// Reads a recorded step, or `None` if it has not run.
    ///
    /// `check` rather than `get`, following all three references, because this is a replay gate
    /// and not a lookup: unlike [`get_workflow`](Self::get_workflow), it can reject
    /// the caller outright. A `get_` that raises [`Error::WorkflowCancelled`] would be a
    /// surprise in exactly the place a caller can least afford one.
    ///
    /// This is the whole of durable execution in one call: on replay, a step that returns
    /// `Some` is skipped and its recorded result used instead of running it again.
    ///
    /// Two checks happen first, and both are the point rather than defensive noise:
    ///
    /// - **A cancelled workflow raises [`Error::WorkflowCancelled`].** Cancelling only sets a
    ///   status; a workflow already in flight finds out here, at its next step boundary.
    /// - **A step recorded under a different name raises [`Error::UnexpectedStep`].** The
    ///   `step_id` is just a counter, so if the workflow's code changed between runs, step 3
    ///   of the replay is not step 3 of the original — and using its result would be silently
    ///   wrong rather than merely stale.
    async fn check_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error>;

    /// Records a step's result, which no later execution may overwrite.
    ///
    /// [`StepTiming::completed_at`] is the concurrency control, not merely a timestamp. The
    /// insert conflicts if the step is already recorded, and when both sides carry a completion
    /// time it is what separates the two reasons that might be:
    ///
    /// - **the same time** — this caller's own write, acknowledged but not observed. Retrying it
    ///   is correct and the call succeeds.
    /// - **a different time** — another execution got there first, which is
    ///   [`Error::StepAlreadyRecorded`].
    ///
    /// So a caller that retries must pass the same `timing` both times — hold it in a variable
    /// rather than building it at the call site inside a retry loop. Python spells the rule out
    /// where it does the same thing: *"Outside the retry: the conflict check compares the stored
    /// completion to ours."*
    ///
    /// A step recorded with no `timing` gets no such detection — there is nothing to compare, so
    /// a duplicate write is accepted rather than reported. That is the trade for omitting it, and
    /// it is Java's behaviour whenever its end time is null.
    ///
    /// Recording also **re-stamps the workflow's executor id**, because an executor that runs a
    /// step is by definition the one running the workflow.
    async fn record_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error>;

    /// Reads a workflow's steps in execution order.
    ///
    /// `load_output` off leaves `output` and `error` `None`, as on
    /// [`WorkflowFilter`] and with the same caveat: absent and
    /// not-asked-for look identical.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// read and its step checkpoint **commit together**, so what a replay reads back is exactly
    /// the snapshot the first execution saw — see [`fork_workflows`](Self::fork_workflows).
    async fn list_workflow_steps(
        &self,
        workflow_id: &str,
        load_output: bool,
        limit: Option<i64>,
        offset: Option<i64>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<StepRecord>, Error>;

    /// Checkpoints a durable sleep, returning the instant to wake at.
    ///
    /// This layer records the wake time; **it does not wait.** The caller sleeps until the
    /// returned instant, and a replay gets the *original* wake time back rather than starting the
    /// clock again — which is the whole point. A workflow that slept an hour and crashed after
    /// fifty minutes has ten left, not sixty.
    ///
    /// The wake time is the step's recorded output, stored as epoch milliseconds in portable
    /// JSON. That combination is deliberate and matches neither reference exactly: Java stores
    /// milliseconds through the *workflow's* serializer, Python stores epoch *seconds* through
    /// the portable one. Milliseconds because every other instant in this schema is
    /// milliseconds; portable because the value is a plain number this layer both writes and
    /// reads, so nothing is served by making it legible only to Rust.
    ///
    /// The step's `completed_at` is stamped at the **wake time**, which is in the future when
    /// the row is written — so a timeline shows an hour's sleep as an hour rather than as an
    /// instant. Nothing in execution or recovery reads that column; it is for step aggregates,
    /// metrics, and Conductor. **All four references do the same**; Go's is `withCompletedAt`
    /// (#442).
    ///
    /// The deadline [`get_event`](Self::get_event) registers is the same checkpoint with the
    /// opposite stamping, since a read that answers in milliseconds under a minute's timeout has
    /// not taken a minute. Python and TypeScript expose that as a flag on this method; here it is
    /// the implementation's business, because no caller of *this* method wants it — a caller
    /// registering a deadline is calling the blocking read, not recording a sleep by hand.
    async fn record_sleep(
        &self,
        workflow_id: &str,
        step_id: i32,
        duration: Duration,
    ) -> Result<Timestamp, Error>;

    /// Publishes a key/value on a workflow, for another workflow to read.
    ///
    /// Writes **three** rows in one transaction: the current value in `workflow_events`, a
    /// per-step row in `workflow_events_history`, and the step record. The transaction is not
    /// optional — if the step record committed without the event write, a replay would find the
    /// step already done, skip the write, and lose the value permanently.
    ///
    /// `step_id` is required rather than optional because `workflow_events_history` keys on it:
    /// `(workflow_uuid, key, function_id)` is its primary key and the column is `NOT NULL`, so
    /// there is no history row to write without one. TypeScript reaches the same place by
    /// rejecting `DBOS.setEvent` outside a workflow.
    ///
    /// Setting the same key twice replaces the current value and adds a history row, so the
    /// history is what a fork copies forward and the current value is what a reader sees.
    async fn set_event(
        &self,
        workflow_id: &str,
        step_id: i32,
        key: &str,
        value: &str,
        serialization: Option<&str>,
    ) -> Result<(), Error>;

    /// Reads a key another workflow published, waiting up to `timeout` for it to appear.
    ///
    /// The read counterpart of [`set_event`](Self::set_event). `workflow_id` is the workflow being
    /// read *from*; `caller`, if any, is the workflow doing the reading.
    ///
    /// **Absence is a value, not an error.** `Ok(None)` means the key was not there when the
    /// deadline passed, which is indistinguishable from the key never being set — the same answer
    /// Python and TypeScript return. Go raises a timeout error instead, but it does so in its
    /// engine, above the layer this trait describes, and an error is the one thing a caller cannot
    /// synthesise if it wanted the other shape.
    ///
    /// **How it waits.** Subscribe, look, wait a bounded interval, look again. The loop is what
    /// delivers: a wakeup only ever says "look again", never what changed, so with no wakeups at
    /// all this still returns as soon as the next interval comes round. That is not a degraded
    /// mode — CockroachDB has no `LISTEN`/`NOTIFY`, so it is every SDK's Cockroach configuration
    /// and this crate's CI.
    ///
    /// **The caller's cancellation is the caller's business.** A `get_event` in a workflow
    /// cancelled mid-wait runs to its deadline here; TypeScript re-checks the caller's status every
    /// interval, which costs a second query per pass to do what dropping the future does for free.
    /// Python does not check either. Same position as
    /// [`await_workflow_result`](Self::await_workflow_result): a caller that wants to stop sooner
    /// drops the future.
    ///
    /// **Inside a workflow it is two checkpoints, not one.** `caller.step_id` records the read, so
    /// a replay returns the value the first run saw rather than waiting again — including the
    /// `None` a timeout produced, which is a result like any other. `caller.timeout_step_id`
    /// records the deadline as a `DBOS.sleep` before the first wait, so a recovery resumes the
    /// original deadline instead of restarting the timeout. It is stamped complete now rather than
    /// at the deadline — see [`record_sleep`](Self::record_sleep) for the distinction. The deadline
    /// is recorded whether or not the value happens to be there already, so the steps a run records
    /// do not depend on how a race went.
    ///
    /// Outside a workflow there is neither: the deadline is the wall clock, nothing is recorded,
    /// and the look that ended the wait is the whole answer.
    ///
    /// **Each look takes a connection**, so they run under the polling concurrency cap — the same
    /// one [`await_workflow_result`](Self::await_workflow_result) waits under, and for the same
    /// reason. The step record is not a poll and does not run under it: it happens once, after the
    /// waiting is over.
    async fn get_event(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
        caller: Option<GetEventCaller<'_>>,
    ) -> Result<Option<EncodedValue>, Error>;

    /// Every message sent to a workflow, oldest first, consumed or not.
    ///
    /// Receiving marks `consumed` rather than deleting, so this reports what a workflow was sent
    /// and not merely what is still waiting. Export and audit want the former.
    async fn get_all_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationRecord>, Error>;

    /// Every key a workflow has published, with its current value.
    ///
    /// The *current* value: `set_event` upserts, so a key set twice appears once. The per-step
    /// history lives in `workflow_events_history`, which this does not read.
    async fn get_all_events(&self, workflow_id: &str) -> Result<Vec<EventRecord>, Error>;

    /// Reads one offset of a stream, with the producing workflow's status.
    ///
    /// The read counterpart of [`write_stream`](Self::write_stream), and **the only one of the
    /// three reads here that does not block.** It looks once and reports what it found. The waiting
    /// belongs to the engine's `read_stream`, which loops this over rising offsets and is what a
    /// caller actually reaches for; this is the one indexed read underneath it.
    ///
    /// That split is Python's and TypeScript's, and their method is named exactly this. Go returns
    /// every entry from `offset` onward in one call instead — the only implementation that batches,
    /// and the only one whose database layer has to decide how much of a stream to buy at once.
    ///
    /// **One statement, so the value and the status share a snapshot**, which is the reason this
    /// returns [`StreamRead`] rather than a value: a reader deciding whether to wait needs both,
    /// and needs them to agree. A `LEFT JOIN` from `workflow_status`, so a workflow with nothing at
    /// the offset still reports its status; matching the offset exactly keeps it a single lookup
    /// on the `(workflow_uuid, key, offset)` primary key.
    ///
    /// **A missing workflow is [`Error::NonExistentWorkflow`].** Python and TypeScript report a
    /// null status instead and their engines raise immediately on it, which is the same answer one
    /// layer up; this crate already spells a missing workflow that way in
    /// [`await_workflow_result`](Self::await_workflow_result).
    ///
    /// Nothing at the offset is `value: None` rather than an error — an offset a producer has not
    /// reached yet is the ordinary case, and the reason a reader waits.
    ///
    /// **Each call takes a connection**, and the loop above makes it a poll, so it runs under the
    /// polling concurrency cap like the other two. Python and TypeScript both say so at this exact
    /// method.
    ///
    /// No caller, no step, no deadline. A stream read is not a checkpoint: the loop that drives it
    /// records one, and each offset re-read on replay yields what it yielded before.
    async fn read_stream_value(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i32,
    ) -> Result<StreamRead, Error>;

    /// Every stream entry a workflow has written, grouped by key and in stream order.
    async fn get_all_stream_entries(&self, workflow_id: &str) -> Result<Vec<StreamRecord>, Error>;

    /// Registers an application version, or leaves an existing one alone.
    ///
    /// Idempotent on the name: launching the same version twice registers it once. The generated
    /// `version_id` is not the identity callers use — `version_name` is, and it is what workflow
    /// rows store.
    ///
    /// `application_name` names the application the version belongs to; `None` means this
    /// handle's own. A version already held by a *different* application is
    /// [`Error::RegisteredByAnother`] rather than a silent takeover — for a caller that has a name
    /// to be refused under. A handle with no application name of its own is let through instead,
    /// and registers nothing: the claim is guarded on the row being unowned and the insert
    /// declines on conflict, so the peer's row stands as it was. That branch is a shared one; see
    /// UPSTREAM item 26 on `resolve_owning_application` for the writes where it is *not*
    /// harmless.
    async fn create_application_version(
        &self,
        version_name: &str,
        application_name: Option<&str>,
    ) -> Result<(), Error>;

    /// Every registered version this handle's application can see, latest first.
    ///
    /// Takes no target, unlike the three around it: a listing is what *this* application has
    /// registered plus the unclaimed, and a caller wanting a peer's asks for the peer's latest.
    /// Python and TypeScript draw the line in the same place.
    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>, Error>;

    /// The version with the highest timestamp, or `None` if none are registered.
    ///
    /// **Latest by `version_timestamp`, not by creation.** Moving a version's timestamp is how a
    /// deployment is promoted or rolled back, which is what
    /// [`update_application_version_timestamp`](Self::update_application_version_timestamp) is
    /// for — so the newest row is not necessarily the current one.
    ///
    /// `None` rather than an error: an empty registry is what a database looks like before any
    /// application has launched. Java also returns null here; Python and Go raise, because their
    /// callers ask only where a version must already exist. That is a caller's invariant, not
    /// this layer's, and it matches [`get_workflow`](Self::get_workflow) returning `None`.
    ///
    /// `application_name` names whose latest to read; `None` means this handle's own. Naming a
    /// peer is what a schedule fire needs: a schedule carries its owner, and its runs are enqueued
    /// against *that* application's latest version, whichever handle happens to fire it.
    async fn get_latest_application_version(
        &self,
        application_name: Option<&str>,
    ) -> Result<Option<VersionInfo>, Error>;

    /// Moves a version's timestamp, which is how the latest version is chosen.
    ///
    /// `application_name` names whose version to promote; `None` means this handle's own.
    /// Promoting a *different* application's is [`Error::RegisteredByAnother`]: moving a timestamp
    /// is how a deployment is rolled forward or back, so it must not move one a peer is running
    /// on.
    ///
    /// **That guard needs the caller to have a name.** A handle with no application name of its
    /// own resolves to the row's holder and retimes it, a peer's included — the shared behaviour
    /// UPSTREAM item 26 asks all five implementations to settle, and what a default-configured
    /// client does today.
    ///
    /// **A name that matches nothing is `Ok`, not [`Error::NotRegistered`].** The write moves no
    /// row and says so to nobody, so a misspelled rollback reports success; UPSTREAM item 2.
    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp: Timestamp,
        application_name: Option<&str>,
    ) -> Result<(), Error>;

    /// Registers a queue, reporting whether this call created it.
    ///
    /// `false` means the row was already there, whether or not [`OnExistingQueue`] changed it.
    /// Callers use that to tell a first registration from a restart.
    ///
    /// **A name already held by another application is [`Error::RegisteredByAnother`] in either
    /// mode**, for a caller with an application name of its own. A queue name addresses one row
    /// across every application sharing the database, so taking it would redirect a peer's work;
    /// ownership moves only by [`rename_application`](Self::rename_application).
    ///
    /// **A nameless caller is not refused, and [`OnExistingQueue::Update`] then replaces a peer's
    /// stored limits** while leaving the owner column alone. Shared with every implementation, and
    /// unresolved: UPSTREAM item 26 on `resolve_owning_application`.
    async fn upsert_queue(
        &self,
        queue: &NewQueue<'_>,
        on_existing: OnExistingQueue,
    ) -> Result<bool, Error>;

    /// Claims up to a queue's worth of enqueued workflows for this executor, returning what it got.
    ///
    /// The whole of dequeueing, in one transaction: rate limit, concurrency, version eligibility,
    /// selection and claim. Callers poll this; an empty result means nothing was available *this
    /// tick*, which is the ordinary case rather than an error.
    ///
    /// **Only this application's workflows, plus unclaimed ones**, and claiming is part of the
    /// same statement that starts them — see [`types::Applications`]. An unclaimed workflow is
    /// taken by whichever application dequeues it first, which is how work enqueued by a nameless
    /// client finds a runner.
    ///
    /// Limits narrow what is taken, rate limits first — a queue already at one returns nothing
    /// without selecting anything — then the concurrency limits, whose tightest budget wins:
    ///
    /// - **Rate limit**, against the starts in the trailing window.
    /// - **Worker concurrency**, against `local_running_count` — what this process is already
    ///   running, which it knows without asking the database.
    /// - **Global concurrency**, against the `PENDING` count across every executor.
    ///
    /// **`partition_key` narrows the selection and adds a second scope; it does not move the
    /// first.** Given one, the three per-partition limits apply as well, counted within that key
    /// alone — `partition_local_running_count` is this process's share of it — while the three
    /// above go on counting the whole queue. A queue can therefore hold both, and the caller
    /// sweeping one partition still cannot spend a budget that belongs to the queue.
    ///
    /// Which limit sits at which scope is [`QueueRecord::resolved_limits`]'s answer, not the
    /// columns': a row a peer wrote with the deprecated `partition_queue` flag keeps its
    /// per-partition numbers in the queue-wide columns, and enforcing those queue-wide would admit
    /// one workflow for the whole queue where the flag promised one per key.
    ///
    /// A queue with global concurrency or a rate limit runs at `REPEATABLE READ` and locks with
    /// `NOWAIT`, so every executor sees a consistent count rather than a partial one; without
    /// them it stays at `READ COMMITTED` and uses `SKIP LOCKED`. A `NOWAIT` conflict therefore
    /// surfaces as a backend error rather than an empty result — a peer is mid-dequeue, and the
    /// caller's next poll is the retry. Both references do the same.
    ///
    /// `application_version` is this executor's. A workflow with no version recorded is eligible
    /// only when this executor is running the *latest* registered version, so a rolling deploy
    /// does not hand unversioned work to the code being replaced.
    async fn start_queued_workflows(
        &self,
        queue: &QueueRecord,
        executor_id: &str,
        application_version: &str,
        partition_key: Option<&str>,
        local_running_count: i64,
        partition_local_running_count: i64,
    ) -> Result<Vec<String>, Error>;

    /// The partitions of a queue that currently have work waiting.
    ///
    /// Only partitions this application could dequeue from — a peer's are not this caller's to
    /// poll. Ordered, and each key appears once.
    async fn get_queue_partitions(&self, queue_name: &str) -> Result<Vec<String>, Error>;

    /// Claims the head-of-line workflow of every partition at once, returning what it got.
    ///
    /// The partitioned counterpart to
    /// [`start_queued_workflows`](Self::start_queued_workflows), and a different shape rather
    /// than a variation: instead of taking *n* workflows from one queue, it takes *one* from each
    /// partition in a single transaction, so a queue with a thousand partitions costs one sweep
    /// rather than a thousand polls.
    ///
    /// **Only valid for a queue whose one limit is partition concurrency 1**, which is
    /// [`Error::InvalidInput`] otherwise — no queue-wide concurrency, and neither rate limit. That
    /// restriction is what makes the sweep safe without counting: every worker ranks each
    /// partition's head identically, and the `PENDING` gate admits at most one row per partition,
    /// so the limit is enforced by the data rather than by a count. Anything else a queue can
    /// carry has to be counted within each key, which is what walking the partitions one at a time
    /// through [`start_queued_workflows`](Self::start_queued_workflows) is for. TypeScript splits
    /// the same two ways, on the same four conditions.
    ///
    /// `partition_worker_concurrency` is deliberately not among them: it is at least 1, and a
    /// partition already capped at one workflow across the whole fleet cannot exceed one in any
    /// single process, so it could never bind here.
    ///
    /// `max_tasks` is the caller's own remaining budget — its worker concurrency less what it is
    /// already running — and bounds the sweep alongside
    /// [`PARTITIONED_DEQUEUE_SWEEP_CAP`], so a process near its limit does not claim heads it must
    /// immediately sit on. `None` is unbounded; `Some(0)` returns without a query.
    ///
    /// At most [`PARTITIONED_DEQUEUE_SWEEP_CAP`]
    /// partitions per sweep; the rest arrive on later polls.
    async fn start_queued_partitioned_workflows(
        &self,
        queue: &QueueRecord,
        executor_id: &str,
        application_version: &str,
        max_tasks: Option<i64>,
    ) -> Result<Vec<String>, Error>;

    /// Reads one queue by name, or `None` if it is not registered.
    ///
    /// Unscoped, like every read addressed by name: the caller has asked about that queue, and a
    /// peer's is still the answer to the question.
    async fn get_queue(&self, name: &str) -> Result<Option<QueueRecord>, Error>;

    /// Reads the registered queues, scoped to the applications asked for.
    ///
    /// A search rather than an address, so [`Applications::Unset`] means this handle's own plus
    /// the unclaimed ones — see [`types::Applications`].
    async fn list_queues(&self, applications: &Applications<'_>)
    -> Result<Vec<QueueRecord>, Error>;

    /// Changes the fields of a registered queue that an update names, leaving the rest.
    ///
    /// **Read, check and write in one transaction**, which is what `validate` is for. The row is
    /// read under `FOR UPDATE`, the update is applied to it, the result is handed to `validate`,
    /// and the write lands before the lock is released — so a limit judged against a row that has
    /// since moved cannot be stored. Go manages the same thing with the `mutate` callback its
    /// `UpdateQueueConfig` takes; Python and TypeScript read and write separately, and two
    /// operators changing different limits at once can leave a pair neither asked for.
    ///
    /// **The transaction does not leave this layer.** `validate` is handed the row as stored and
    /// the row as the update would leave it, and says yes or no; it does no I/O of its own and
    /// never sees a connection. Both, because some rules are about the transition rather than the
    /// destination — whether a limit may be set at all can depend on what the row already is. It
    /// must also be free of side effects, because a retried attempt calls it again against the row
    /// that attempt read. Callers with nothing to check pass a closure that always succeeds.
    ///
    /// The second argument is [`QueueUpdate::apply_to`]'s result rather than the update, because a
    /// limit is rarely wrong on its own and usually wrong only beside another already stored. It
    /// refuses by returning an error, and [`Error::InvalidInput`] is the variant for that — the
    /// caller is expected to recognise its own refusal coming back.
    ///
    /// An update naming nothing writes nothing, `updated_at` included, and is not validated: the
    /// stored row stands unexamined, which is what both references do rather than treating an
    /// empty update as an error. A caller assembling one from optional inputs should not have to
    /// check whether any survived.
    ///
    /// [`Error::NotRegistered`] if the name matches nothing, like
    /// [`update_schedule`](Self::update_schedule) — the row has to be read to check against it, so
    /// its absence is known here rather than inferred from a row count.
    ///
    /// Returns the row as written, which spares the caller a read back that a later transaction
    /// would have answered anyway.
    ///
    /// Unscoped, like the other reads and writes addressed by queue name. Ownership is not
    /// updatable: see [`QueueUpdate`].
    async fn update_queue(
        &self,
        name: &str,
        update: &QueueUpdate,
        validate: &(
             dyn for<'r, 's> Fn(&'r QueueRecord, &'s QueueRecord) -> Result<(), Error> + Send + Sync
         ),
    ) -> Result<QueueRecord, Error>;

    /// Extends a debounced workflow's delay and replaces its inputs, or reports who holds the key.
    ///
    /// A debounce coalesces repeated requests onto one delayed workflow: each call pushes the
    /// release further out and overwrites the inputs, so the workflow eventually runs once with
    /// the most recent ones. The new delay is **capped at the workflow's debounce deadline**, so
    /// a steady stream of requests cannot postpone it forever.
    ///
    /// Matching includes the workflow's name, class and configured instance, so a key collision
    /// between unrelated workflows — the classic `"a" + "b-c"` against `"a-b" + "c"`, or two
    /// configured instances of one class — never overwrites another workflow's inputs. It falls
    /// through to [`Debounce::Held`] instead, which describes the holder well enough for a caller
    /// to tell a collision from a coincidence.
    ///
    /// TODO(dbos-team): UPSTREAM item 11. No reference keys a debounce on all three parts of a
    /// workflow's identity — TypeScript matches name and class, Python the name alone — so there a
    /// bounce for one configured instance can extend another's workflow and replace its inputs.
    /// This implementation matches all three.
    ///
    /// [`DebounceRequest::application_name`] is the application the bounce acts *for*; `None` means
    /// this handle's own. Only that application's holders and unclaimed ones are extended, and an
    /// unclaimed one is claimed in the same statement — left unclaimed, every peer would coalesce
    /// onto the one workflow and the last inputs would win. `caller` names the workflow step this
    /// runs as, when a workflow is doing the bouncing. Given one, the bounce and its step
    /// checkpoint **commit together**: a crash can never leave one without the other, which on
    /// recovery would bounce work that had already been bounced. A replay returns what the first
    /// run decided rather than bouncing again.
    ///
    /// Python and TypeScript get that atomicity by passing a database connection down from
    /// `call_txn_as_step`. This layer names no driver type, so it takes the step instead and owns
    /// the transaction — the same shape as [`send_messages`](Self::send_messages).
    async fn debounce_delayed_workflow(
        &self,
        request: &DebounceRequest<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Debounce, Error>;

    /// The workflow currently holding a deduplication key, if any.
    ///
    /// The caller for this is an enqueue that **lost a race and wants to adopt the winner**:
    /// submitting under a key another workflow holds fails on the unique index, and a
    /// return-the-existing-one policy then asks who won and reports that id instead of erroring
    /// (`client.ts:430`, `Debouncer.java:322`). `None` means the holder finished between the
    /// conflict and this read — the key is free again and the caller should retry the insert
    /// rather than treat it as an error.
    ///
    /// Narrow on purpose: the id alone. A bounce needs to know far more about the holder, but it
    /// reads that for itself — see [`types::Debounce::Held`].
    ///
    /// Unscoped, because a deduplication key is an address rather than a search: migration 27's
    /// partial index makes `(queue_name, deduplication_id)` unique wherever the key is set, so at
    /// most one row can match. That the index is global rather than per-application is the same
    /// shared-database question queue names raise; it is not this method's to answer.
    async fn get_deduplication_key_holder(
        &self,
        queue_name: &str,
        deduplication_id: &str,
    ) -> Result<Option<String>, Error>;

    /// Removes a queue from the registry.
    ///
    /// Only the registration. Workflows already enqueued keep their `queue_name`.
    ///
    /// Whether anything still dequeues them depends on the layer above: an implementation that
    /// also keeps queues declared in code carries on polling one it declared, while this crate's
    /// engine builds its worker set from these rows alone, so deleting the row strands the backlog
    /// until the queue is registered again.
    async fn delete_queue(&self, name: &str) -> Result<(), Error>;

    /// Registers a schedule, failing if the name is taken.
    ///
    /// [`Error::AlreadyRegistered`] when this application already holds the name, and
    /// [`Error::RegisteredByAnother`] when a peer does — the same distinction queues draw, since
    /// `schedule_name` is unique across every application sharing the database. A caller with no
    /// application name of its own gets the first of those for a peer's row rather than the
    /// second, because nothing refuses it and the unique index is what stops the insert; nothing
    /// is overwritten either way. UPSTREAM item 26.
    ///
    /// The cron expression is stored, not parsed. Java validates it here and the other three do
    /// not; validating belongs with the scheduler that has to interpret it, not with the layer
    /// that only persists it.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together**, and a replay returns what the first run
    /// decided rather than doing it again. TypeScript and Python get that atomicity by passing a
    /// database connection down from their step wrapper (`runTransactionalInternalStep`,
    /// `dbos.ts:359`); this layer names no driver type, so it takes the step instead and owns the
    /// transaction — the same shape as [`send_messages`](Self::send_messages) and
    /// [`debounce_delayed_workflow`](Self::debounce_delayed_workflow).
    async fn create_schedule(
        &self,
        schedule: &NewSchedule<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Registers a schedule, updating its definition if the name is already registered.
    ///
    /// What a redeployment calls. **Only the definition moves**; `schedule_id`, `status` and
    /// `last_fired_at` are kept from the existing row, so re-applying an unchanged schedule is a
    /// no-op and re-applying a changed one does not resume a schedule the operator paused or
    /// forget where it had got to.
    ///
    /// An unclaimed row is claimed in the same statement, and a peer's is
    /// [`Error::RegisteredByAnother`] — again only for a caller that has a name. A nameless one
    /// replaces the peer's definition instead, which is UPSTREAM item 26.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it, with the same
    /// meaning it has on [`create_schedule`](Self::create_schedule) — but on weaker precedent.
    /// **No reference runs this as a step.** TypeScript has no counterpart at all: its upsert is
    /// inlined in `applySchedules`, which takes no connection. Python's `upsert_schedule`
    /// (`_sys_db.py:5761`) does take one, but only ever from `apply_schedules`, which is a plain
    /// transaction rather than a step. The parameter is here because the signature is Python's and
    /// a schedule registered from inside a workflow wants the same atomicity its siblings get, not
    /// because a reference does it.
    async fn upsert_schedule(
        &self,
        schedule: &NewSchedule<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Registers a whole set of schedules in one transaction.
    ///
    /// What a process calls at startup with everything it declares: either the registry matches the
    /// deployment or none of it moved. Each entry is an [`upsert_schedule`](Self::upsert_schedule),
    /// so the definitions land and the runtime state survives.
    ///
    /// **Runtime state is taken at face value**, not refused and not normalised: a `status` or
    /// `last_fired_at` seeds a fresh row and is dropped by the conflict clause on an existing one,
    /// so re-applying cannot resume a schedule an operator paused. Whether a *declaration* should
    /// carry either is a question for whoever builds one — the references never ask it here,
    /// because their public `applySchedules` takes a type with no such fields and the layer above
    /// hardcodes both. Java normalises at this layer instead, forcing `ACTIVE` and a null
    /// `lastFiredAt` onto every entry.
    ///
    /// **No step, unlike its siblings.** This is a startup call, made before the process runs any
    /// workflow, and TypeScript's takes no connection and is not step-wrapped either. Python has
    /// no method here at all — its `apply_schedules` is a loop over
    /// [`upsert_schedule`](Self::upsert_schedule) one layer up (`_dbos.py:3119`).
    async fn apply_schedules(&self, schedules: &[NewSchedule<'_>]) -> Result<(), Error>;

    /// Reads one schedule, or `None` if there is no such name.
    ///
    /// Unscoped: a schedule name addresses a row across every application sharing the database,
    /// so this is an identity read.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// read and its step checkpoint **commit together**, and a replay returns what the first run
    /// saw rather than reading again — see [`create_schedule`](Self::create_schedule) for the
    /// shape.
    ///
    /// A workflow that branches on a schedule must see the same schedule on replay, whatever an
    /// operator changed in between, which is why a read records a step at all.
    async fn get_schedule(
        &self,
        name: &str,
        caller: Option<(&str, i32)>,
    ) -> Result<Option<ScheduleRecord>, Error>;

    /// Reads the schedules matching a filter.
    ///
    /// A search, so [`ScheduleFilter::applications`] defaults to this handle's own plus the
    /// unclaimed rather than to every application's.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// read and its step checkpoint **commit together**, and a replay returns what the first run
    /// saw rather than reading again — see [`create_schedule`](Self::create_schedule) for the
    /// shape.
    async fn list_schedules(
        &self,
        filter: &ScheduleFilter<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<ScheduleRecord>, Error>;

    /// Changes a registered schedule's definition.
    ///
    /// [`Error::NotRegistered`] if the name matches nothing, including when the update itself is
    /// empty — a typo should not read as success. TypeScript is explicit about both; the other
    /// three have no such method.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together**, and a replay returns what the first run
    /// decided rather than doing it again — see [`create_schedule`](Self::create_schedule) for
    /// the shape.
    async fn update_schedule(
        &self,
        name: &str,
        update: &ScheduleUpdate<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Pauses or resumes a schedule.
    ///
    /// One method rather than the pair Python and Java expose, which are their two calls to
    /// exactly this — TypeScript already collapses it the same way.
    ///
    /// [`Error::NotRegistered`] if the name matches nothing. **All four implementations are
    /// silent here**, so pausing a schedule that does not exist reads as success in every one of
    /// them. Raising is the recoverable direction: a layer above can swallow an error it does not
    /// want, and no layer above can manufacture one this layer never raised.
    ///
    /// TODO(dbos-team): UPSTREAM item 12. Nothing but TypeScript's `updateSchedule` checks the
    /// row count, so a misspelled schedule name reports success, which an operator cannot tell
    /// from a schedule that is now paused.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together**, and a replay returns what the first run
    /// decided rather than doing it again — see [`create_schedule`](Self::create_schedule) for
    /// the shape.
    ///
    /// Pausing and resuming record **different step names**, as they are different calls in the
    /// references, so a replay of one is never mistaken for the other.
    async fn set_schedule_status(
        &self,
        name: &str,
        status: ScheduleStatus,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error>;

    /// Records when a schedule last fired.
    ///
    /// Written by the scheduler after each firing, and read back to decide where a backfill
    /// resumes. **Silent when the name matches nothing**, unlike the writes above: the scheduler
    /// races an operator's delete, and a schedule removed between firing and this write is a
    /// benign outcome rather than an error the loop has to absorb.
    ///
    /// The column is **text, not epoch milliseconds** — the one time column in the schema that
    /// is — so this takes an instant and formats it, rather than taking a string and trusting the
    /// caller to have picked a spelling the others read. Each implementation picked a different
    /// one: Python `datetime.isoformat()`, TypeScript `Date.toISOString()`, Go `RFC3339Nano`,
    /// Java `Instant.toString()`. [`Timestamp::to_iso8601`](types::Timestamp::to_iso8601) writes
    /// TypeScript's, the only fixed-width one of the four.
    ///
    /// [`ScheduleRecord::last_fired_at`](types::ScheduleRecord::last_fired_at) is an instant too,
    /// read through [`Timestamp::parse_iso8601`](types::Timestamp::parse_iso8601), which accepts
    /// all four spellings. A stored value that is not an instant at all is
    /// [`Error::Malformed`] rather than silently a schedule that never fired — the same treatment
    /// an unrecognised status gets, since both mean the row holds something this build cannot
    /// read.
    ///
    /// **No step, unlike its siblings**, and for the same reason it is silent about a missing row:
    /// the scheduler loop writes this after every firing, and a loop is not a workflow step.
    /// Neither TypeScript nor Python takes a connection here.
    async fn update_schedule_last_fired_at(
        &self,
        name: &str,
        last_fired_at: Timestamp,
    ) -> Result<(), Error>;

    /// Removes a schedule from the registry.
    ///
    /// Silent if the name matches nothing, as all four implementations are. Workflows the
    /// schedule already fired are ordinary workflows and are untouched.
    ///
    /// `caller` names the workflow step this runs as, when a workflow is doing it. Given one, the
    /// write and its step checkpoint **commit together**, and a replay returns what the first run
    /// decided rather than doing it again — see [`create_schedule`](Self::create_schedule) for
    /// the shape.
    async fn delete_schedule(&self, name: &str, caller: Option<(&str, i32)>) -> Result<(), Error>;

    /// Gives `new_name` ownership of the rows a [`RenameFrom`] selects.
    ///
    /// **The application being renamed must be stopped**, or its own dequeues race this and
    /// re-claim rows behind it. Nothing here can enforce that.
    ///
    /// Two phases, and the split is the design. Queues, schedules, versions and **in-flight**
    /// workflows (`PENDING`, `ENQUEUED`, `DELAYED`) move in one transaction, because a half-renamed
    /// application dequeues work whose version row it can no longer see. Terminal workflows and
    /// their steps then move in batches: they can be an entire history, and they scope only
    /// observability and garbage collection, so they may lag without anything observing a
    /// half-renamed state.
    ///
    /// Re-running after a failure resumes rather than restarting — every statement is an idempotent
    /// re-own, and the batches are key ranges rather than offsets.
    ///
    /// `new_name` must satisfy [`types::is_valid_application_name`], and must differ from the name
    /// being renamed.
    async fn rename_application(
        &self,
        source: RenameFrom<'_>,
        new_name: &str,
        batching: RenameBatching,
    ) -> Result<ApplicationRowCounts, Error>;

    /// Records that a step started a child workflow.
    ///
    /// A step row with a `child_workflow_id` and no result: the child's outcome lives on the
    /// child's own row, and duplicating it here would give a replay two places to disagree.
    ///
    /// **Not a call through [`record_step`](Self::record_step)**, because the two
    /// resolve a duplicate write differently. That one compares the completion time; this one
    /// compares the **child id**, since the timestamp here spans only the launch and a retry
    /// would stamp a new one — while the child id it is recording is necessarily the same. A
    /// *different* child at the same position is nondeterminism in the parent, and is reported
    /// as [`Error::StepAlreadyRecorded`]. Python splits the two for exactly this reason; Java
    /// reaches it by passing null timestamps and skipping the comparison.
    async fn record_child_workflow(
        &self,
        parent_workflow_id: &str,
        child_workflow_id: &str,
        step_id: i32,
        step_name: &str,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error>;

    /// Reads back a recorded child-workflow await, or `None` if the parent has not got this far.
    ///
    /// The replay gate for [`record_child_result`](Self::record_child_result), and the reason a
    /// parent that already learned its child's outcome does not wait for it a second time. It takes
    /// no step name for the same reason that one does not.
    ///
    /// Provided rather than implemented: it is [`check_step`](Self::check_step) under a name that
    /// is stored contract rather than a backend's to choose, so a backend has nothing of its own to
    /// add and no opportunity to disagree about the name.
    async fn check_child_result(
        &self,
        parent_workflow_id: &str,
        step_id: i32,
    ) -> Result<Option<StepRecord>, Error> {
        self.check_step(parent_workflow_id, step_id, step_names::GET_RESULT)
            .await
    }

    /// Records what a child workflow returned, as a step of the parent that awaited it.
    ///
    /// **The parent's second checkpoint for one child.** The first — `record_child_workflow` —
    /// records the *launch* and deliberately carries no result, because the child's outcome lives
    /// on the child's own row. This one records that the parent *observed* that outcome, which is
    /// a different fact and one only the parent can state: it is what lets a replayed parent
    /// continue from a value it already has instead of waiting on a workflow that may since have
    /// been forked, deleted, or restarted.
    ///
    /// Unlike its sibling, this **is** [`record_step`](Self::record_step) with a child id attached,
    /// and it resolves a duplicate write the same way — by comparing the completion timestamp. It
    /// is a separate method only so the child id stays off that signature, which every ordinary
    /// step would then pass `None` to.
    ///
    /// The step name is not a parameter for the same reason it is not one on
    /// [`record_sleep`](Self::record_sleep): `"DBOS.getResult"` is what all four implementations
    /// write — Python's `_sys_db.py`, Go's `StepName`, TypeScript's and Java's the same — so a step
    /// listing reads alike whichever SDK ran the parent, which is what Conductor renders. A caller
    /// that could choose would be choosing wrong.
    async fn record_child_result(
        &self,
        parent_workflow_id: &str,
        step_id: i32,
        child_workflow_id: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error>;
}
