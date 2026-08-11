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

pub mod error;
pub mod migrations;
pub mod postgres;
pub mod retry;
pub mod types;

use async_trait::async_trait;

// Re-exported: `sysdb::Error` is how the rest of the crate and its callers name it, and moving
// the definition to a file should not move the path.
pub use error::{BackendError, BackendErrorKind, Error};

use std::time::Duration;

use types::{
    EventRecord, Fork, ForkOptions, ForkPoint, Message, NewWorkflow, NotificationRecord, Outcome,
    OutcomeWrite, StepRecord, StepTiming, StreamRecord, Submission, Timestamp, VersionInfo,
    WorkflowDelay, WorkflowFilter, WorkflowInitResult, WorkflowRecord,
};

/// Everything the engine needs from the system database.
///
/// **No method mentions a driver type.** No pool, no row, no `sqlx::Postgres` — each
/// implementation owns its connections privately. That is what keeps a second backend a second
/// implementation rather than a rewrite, and what would let a host language call this across an
/// FFI boundary.
///
/// Payloads cross as already-encoded strings; see [`types`].
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
    async fn init_workflow(
        &self,
        workflow: &NewWorkflow,
        max_recovery_attempts: Option<i64>,
        submission: Submission,
    ) -> Result<WorkflowInitResult, Error>;

    /// Reads one workflow, or `None` if there is no such id.
    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowRecord>, Error>;

    /// Reads the workflows matching a filter, oldest first unless told otherwise.
    ///
    /// This is one query with every filter folded into its `WHERE` clause, not a scan the caller
    /// narrows. `WorkflowFilter::default()` therefore returns the whole table, and callers that
    /// mean to page should say so with [`WorkflowFilter::limit`].
    async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowRecord>, Error>;

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
    /// **Losing here is a value; losing in [`record_step`](Self::record_step) is an error.** The two look parallel and deliberately are not. A workflow whose outcome was
    /// recorded by someone else has simply been superseded, and the right move is to adopt what
    /// is stored — routine enough to be a return value. A step recorded by someone else means
    /// two executions of one workflow are live at the same moment, which every implementation
    /// raises on.
    async fn record_workflow_outcome(
        &self,
        workflow_id: &str,
        outcome: Outcome<'_>,
    ) -> Result<OutcomeWrite, Error>;

    /// Moves a delayed workflow's release time.
    ///
    /// Only touches a `DELAYED` row. A workflow that has already been released is running or
    /// queued, and pushing its delay out would not recall it.
    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay: WorkflowDelay,
    ) -> Result<(), Error>;

    /// Puts a running workflow back on its queue, reporting whether it moved.
    ///
    /// For an executor that claimed a queued workflow and then could not run it. Only applies to
    /// a `PENDING` row that has a queue to return to.
    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, Error>;

    /// Replaces a workflow's attributes. `None` clears them.
    ///
    /// A replacement rather than a merge, matching every implementation.
    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
    ) -> Result<(), Error>;

    /// Workflows this executor left `PENDING`, which recovery picks up.
    ///
    /// Scoped by application version as well as executor: a workflow started under different code
    /// must not be resumed by an executor running this version, because its recorded steps may no
    /// longer line up.
    async fn get_pending_workflows(
        &self,
        executor_id: &str,
        application_version: &str,
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
    async fn cancel_workflows(
        &self,
        workflow_ids: &[&str],
        cancel_children: bool,
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
    async fn resume_workflows(
        &self,
        workflow_ids: &[&str],
        queue_name: Option<&str>,
    ) -> Result<Vec<String>, Error>;

    /// Deletes workflows and everything hanging off them.
    ///
    /// Steps, notifications, events, and streams go with the row: the schema declares
    /// `ON DELETE CASCADE` on every child table, so one `DELETE` is the whole operation.
    ///
    /// Unlike [`cancel_workflows`](Self::cancel_workflows), the descendants are collected first
    /// and deleted in one statement rather than level by level. Cancelling interleaves so a
    /// parent cannot spawn behind the walk; a deleted parent cannot spawn at all.
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
    /// `DELAYED` there. That method is task 3.8 and is not built yet.
    ///
    /// The consequence for a caller: an executor running a deleted workflow finds its row gone
    /// at the next step boundary and fails with [`Error::NonExistentWorkflow`], rather than
    /// being stopped cleanly. [`cancel_workflows`](Self::cancel_workflows) is the graceful form.
    ///
    /// Takes `&[&str]` rather than `&[String]`, as the other bulk methods do: a caller holding
    /// owned ids converts by copying pointers, where the reverse would allocate.
    async fn delete_workflows(
        &self,
        workflow_ids: &[&str],
        delete_children: bool,
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
    async fn fork_workflows(
        &self,
        forks: &[Fork<'_>],
        options: &ForkOptions<'_>,
    ) -> Result<Vec<String>, Error>;

    /// Forks workflows from a step this works out for each of them.
    ///
    /// Named for what it does rather than what it was first for: Python, Java and TypeScript all
    /// call this `fork_from_failure`, from when the failed step was the only place it could
    /// start. Three of [`ForkPoint`]'s four cases have nothing to do with failure. Go reached the
    /// same conclusion and calls it `ForkFrom`.
    ///
    /// [`fork_workflows`](Self::fork_workflows) with the start step computed rather than given:
    /// the caller says *from the failure* or *from the step called `charge_card`*, and each
    /// workflow's own history decides where that is. Ids are always generated, since a caller
    /// who is not choosing the step is not choosing the id either.
    ///
    /// Fails with [`Error::NoForkPoint`] if any workflow has nothing at the point asked for, and
    /// writes nothing. [`ForkPoint::Step`] is exempt: it names a position directly, so there is
    /// nothing to look up and nothing to be missing.
    async fn fork_from(
        &self,
        workflow_ids: &[&str],
        point: ForkPoint<'_>,
        options: &ForkOptions<'_>,
    ) -> Result<Vec<String>, Error>;

    /// Delivers messages to workflows, in one transaction.
    ///
    /// One method rather than the `send`/`send_bulk` pair the plan lists, because the batch form
    /// is the primitive and the single form is a caller with one message. Python says so in its
    /// own docstring — its `send_bulk` "is the single implementation underlying both `DBOS.send`
    /// and `send_bulk`, inside and outside a workflow" — and Java exposes only `sendBulk` too.
    ///
    /// `caller` is the sending workflow and the step id to record against. The step *name* is
    /// derived from the batch size rather than passed in — `"DBOS.send"` for a single message,
    /// `"DBOS.sendBulk"` for any other count — because that is what distinguishes the two API
    /// surfaces the references record it under.
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
    async fn list_workflow_steps(
        &self,
        workflow_id: &str,
        load_output: bool,
        limit: Option<i64>,
        offset: Option<i64>,
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
    /// metrics, and Conductor.
    ///
    /// Java does the same unconditionally. Go never projects, so its sleeps look instantaneous.
    /// Python alone distinguishes the two with a `project_completion_time` flag, leaving it off
    /// for `recv` and `get_event`, which register a deadline they may abandon early and would
    /// otherwise be recorded as having waited the whole timeout.
    ///
    /// TODO: revisit when `recv` and `get_event` land. They are the only callers that would ever
    /// want the other behaviour, and both are blocked on Stage 4's notifier. A first pass modelled
    /// Python's flag as a `SleepKind` enum and dropped it: a 1-of-4 divergence, over a column
    /// nothing executes on, decided before either caller existed. Decide it with them.
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

    /// Every stream entry a workflow has written, grouped by key and in stream order.
    async fn get_all_stream_entries(&self, workflow_id: &str) -> Result<Vec<StreamRecord>, Error>;

    /// Registers an application version, or leaves an existing one alone.
    ///
    /// Idempotent on the name: launching the same version twice registers it once. The generated
    /// `version_id` is not the identity callers use — `version_name` is, and it is what workflow
    /// rows store.
    async fn create_application_version(&self, version_name: &str) -> Result<(), Error>;

    /// Every registered version, latest first.
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
    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>, Error>;

    /// Moves a version's timestamp, which is how the latest version is chosen.
    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp: Timestamp,
    ) -> Result<(), Error>;

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
}
