//! What can go wrong talking to the system database.
//!
//! Split from the trait itself because the error surface is shared: the retry layer classifies
//! on it, both input types validate into it, and a second backend will construct it without
//! seeing any of the trait's method signatures.

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
    /// The workflow has been cancelled, so its steps must not run.
    ///
    /// Raised by the step-replay check rather than by cancellation itself: cancelling only sets
    /// a status, and a workflow already in flight learns about it the next time it asks.
    WorkflowCancelled {
        /// The cancelled workflow.
        workflow_id: String,
    },
    /// A step at this position was recorded under a different name.
    ///
    /// Means the workflow's code changed between the original run and this replay, so the
    /// recorded results no longer line up with the steps asking for them.
    UnexpectedStep {
        /// The workflow being replayed.
        workflow_id: String,
        /// The position that disagreed.
        step_id: i32,
        /// The step asking.
        expected: String,
        /// The step recorded there.
        recorded: String,
    },
    /// Another execution recorded this step first.
    ///
    /// Two executors believed they owned one workflow. Distinguished from this caller's own
    /// retry by the completion timestamp, which a retry repeats and a rival does not.
    StepAlreadyRecorded {
        /// The workflow whose step was taken.
        workflow_id: String,
        /// The position that was already filled.
        step_id: i32,
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
            Error::WorkflowCancelled { workflow_id } => {
                write!(f, "workflow {workflow_id} is cancelled")
            }
            Error::UnexpectedStep {
                workflow_id,
                step_id,
                expected,
                recorded,
            } => write!(
                f,
                "workflow {workflow_id} step {step_id} was recorded as {recorded:?}, \
                 but {expected:?} was expected"
            ),
            Error::StepAlreadyRecorded {
                workflow_id,
                step_id,
            } => write!(
                f,
                "workflow {workflow_id} step {step_id} was already recorded by another execution"
            ),
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
    /// Retried by default, and the one class [`super::retry::RetryPolicy`] can be told to give up on:
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
