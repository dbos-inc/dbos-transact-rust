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

pub mod migrations;
pub mod postgres;
pub mod retry;
pub mod runner;
pub mod types;

use async_trait::async_trait;

use types::{NewWorkflow, Timestamp, WorkflowFilter, WorkflowRecord, WorkflowStatus};

/// What went wrong talking to the system database.
///
/// Deliberately not `sqlx::Error`: that type names a specific driver, and this trait has to be
/// implementable by a backend that does not use one.
#[derive(Debug)]
pub enum Error {
    /// The database rejected or could not serve the request.
    Backend(BackendError),
    /// A stored value could not be understood — an unrecognised status, a missing column.
    ///
    /// Usually means the database was written by an implementation that knows something this
    /// one does not.
    Malformed(String),
    /// A workflow with this id already exists, running a different function.
    ///
    /// Reusing an id for different work is a programming error, not a race: the id is how
    /// every implementation decides two attempts are the same workflow.
    ConflictingWorkflow {
        /// The id submitted twice.
        workflow_id: String,
        /// Which part disagreed, and how.
        detail: String,
    },
    /// A caller supplied a value the layer will not store.
    ///
    /// Distinct from [`Error::Malformed`], which is about values already *in* the database.
    /// This one never reaches the database at all.
    InvalidInput {
        /// The field at fault.
        field: &'static str,
        /// What was wrong with it.
        detail: String,
    },
    /// One or more of the named workflows do not exist.
    NonExistentWorkflow {
        /// The ids with no row behind them.
        workflow_ids: Vec<String>,
    },
    /// The workflow has been recovered too many times and is now parked.
    MaxRecoveryAttemptsExceeded {
        /// The parked workflow.
        workflow_id: String,
        /// The limit it passed.
        limit: i64,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Backend(e) => write!(f, "system database error: {e}"),
            Error::Malformed(m) => write!(f, "unexpected value in the system database: {m}"),
            Error::ConflictingWorkflow {
                workflow_id,
                detail,
            } => write!(f, "workflow {workflow_id} already exists: {detail}"),
            Error::InvalidInput { field, detail } => write!(f, "invalid {field}: {detail}"),
            Error::NonExistentWorkflow { workflow_ids } => {
                write!(f, "no such workflow: {}", workflow_ids.join(", "))
            }
            Error::MaxRecoveryAttemptsExceeded { workflow_id, limit } => write!(
                f,
                "workflow {workflow_id} exceeded {limit} recovery attempts"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// A failure the database or its driver reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    /// What the driver said.
    pub message: String,
    /// The SQLSTATE, when the failure came from the database rather than the connection.
    pub sqlstate: Option<String>,
    /// Whether waiting and asking again could succeed.
    pub kind: BackendErrorKind,
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.sqlstate {
            Some(code) => write!(f, "{} ({code})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

/// Whether a backend failure is worth asking again about.
///
/// Classification is the backend's job, not the retry loop's: SQLSTATEs are Postgres's, and a
/// SQLite backend would decide on message text instead. Python and Go both split it this way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    /// The connection failed, or the server cannot serve requests right now.
    ///
    /// Retried by default, and the one class [`retry::RetryPolicy`] can be told to give up on:
    /// a caller that would rather see the failure than block can opt out.
    Connection,
    /// Contention — a serialization failure or a deadlock.
    ///
    /// Always retried, whatever the policy says. The database is working; it asked this
    /// transaction to step aside so another could commit, and not asking again loses the write.
    Transient,
    /// The database understood the request and rejected it.
    ///
    /// A syntax error, a constraint violation, a missing table. Asking again cannot help.
    Permanent,
}

/// The result of trying to record a workflow's final outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeWrite {
    /// This process recorded the outcome.
    Recorded,
    /// Another process got there first, and the row is already terminal.
    ///
    /// Not an error: the caller has lost a race it was allowed to lose, and should adopt the
    /// recorded outcome rather than overwrite it.
    AlreadyFinished,
}

/// What the caller is doing, which decides how an existing row is treated.
#[derive(Debug, Clone, Copy)]
pub struct InitWorkflowStatus<'a> {
    /// The workflow to record.
    pub workflow: &'a NewWorkflow,
    /// Dead-letter threshold. `None` disables parking entirely.
    pub max_recovery_attempts: Option<i64>,
    /// This attempt is recovering a workflow a dead executor left behind.
    pub is_recovery: bool,
    /// This attempt is dequeuing, which tells the caller it owns a workflow that was enqueued.
    ///
    /// Kept apart from `is_recovery` because the references do, even though the two currently
    /// have the same effect here: both count against the recovery budget and both may claim a
    /// row another owner holds, which would be theft from a fresh start.
    pub is_dequeue: bool,
}

impl<'a> InitWorkflowStatus<'a> {
    /// A first attempt at a workflow, with no dead-letter limit.
    pub fn new(workflow: &'a NewWorkflow) -> Self {
        Self {
            workflow,
            max_recovery_attempts: None,
            is_recovery: false,
            is_dequeue: false,
        }
    }
}

/// What the database said about a workflow after initialising it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowInitResult {
    /// The stored status, which is the existing one when the row was already there.
    pub status: WorkflowStatus,
    /// Recovery attempts recorded against the workflow, after this one.
    pub recovery_attempts: i64,
    /// The workflow's absolute expiry, as the database now holds it.
    ///
    /// Reported back because it may not be the one offered: a timeout is turned into a deadline
    /// against the database layer's clock, and an existing row keeps the deadline it already had.
    pub deadline: Option<Timestamp>,
    /// The serialization format actually stored.
    ///
    /// May differ from what the caller offered: the first writer decides the format, and every
    /// later attempt has to read the payloads that are actually there.
    pub serialization: Option<String>,
    /// Whether this caller should go on to run the workflow.
    ///
    /// `false` means another owner holds it and this attempt is not a recovery — so the row is
    /// recorded, but running it would be a second execution.
    pub should_execute: bool,
}

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
    ///   workflow as [`WorkflowStatus::MaxRecoveryAttemptsExceeded`] and errors.
    /// - **The executor is re-stamped**, unless this is an enqueue — a queued workflow has no
    ///   executor yet, and claiming one would be wrong.
    /// - **A different function under the same id is an error.** Name, class, and config must
    ///   match; a differing queue is only a warning, since requeueing elsewhere is legitimate.
    ///
    /// The owner identity behind the single-execution guard is generated in here rather than
    /// passed in, and generated once per call — before any retry the implementation makes. A
    /// retry that generated a fresh identity after a lost commit acknowledgement would fail to
    /// recognise its own write and conclude another executor owned the row.
    async fn init_workflow_status(
        &self,
        input: InitWorkflowStatus<'_>,
    ) -> Result<WorkflowInitResult, Error>;

    /// Reads one workflow, or `None` if there is no such id.
    async fn get_workflow_status(&self, workflow_id: &str)
    -> Result<Option<WorkflowRecord>, Error>;

    /// Reads the workflows matching a filter, oldest first unless told otherwise.
    ///
    /// This is one query with every filter folded into its `WHERE` clause, not a scan the caller
    /// narrows. `WorkflowFilter::default()` therefore returns the whole table, and callers that
    /// mean to page should say so with [`WorkflowFilter::limit`].
    async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowRecord>, Error>;

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
        workflow_ids: &[String],
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
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> Result<Vec<String>, Error>;

    /// Replaces a workflow's attributes. `None` clears them.
    ///
    /// A replacement rather than a merge, matching every implementation.
    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
    ) -> Result<(), Error>;

    /// Records a terminal outcome, but only while the workflow is still running.
    ///
    /// The status gate is the point. Two executors can believe they own the same workflow — one
    /// recovering after the other was presumed dead — and whichever finishes second must not
    /// overwrite the first's result. Returning which happened lets the loser adopt the recorded
    /// outcome instead of reporting its own.
    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<OutcomeWrite, Error>;
}
