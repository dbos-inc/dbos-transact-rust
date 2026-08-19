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
    /// A second `recv` on a (workflow, topic) another is already waiting on.
    ///
    /// One message goes to one receiver, so a second waiter can only wait out its timeout and
    /// report that nothing arrived — indistinguishable, to the workflow that sent it, from nothing
    /// having been sent. Python and Go reject it too; both reuse their generic workflow-conflict
    /// error, which this crate's [`ConflictingWorkflow`](Error::ConflictingWorkflow) is not — that
    /// one says the workflow already exists, and here it existing is the premise.
    ///
    /// **In-process only.** Two receivers in different processes never meet, and are arbitrated at
    /// the database instead; see `consume_message`.
    ConcurrentRecv {
        /// The workflow being received on, which is also the workflow calling.
        workflow_id: String,
        /// The topic, or `None` for the default one.
        topic: Option<String>,
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
    /// A queued workflow already holds this deduplication key.
    ///
    /// Raised only by workflow creation, and only when the caller supplied a deduplication id:
    /// the primary-key conflict is absorbed by `ON CONFLICT (workflow_uuid)`, so the sole unique
    /// violation left is the partial index on `(queue_name, deduplication_id)`. Python asserts
    /// exactly that at its own raise site.
    ///
    /// An answer rather than a failure — the caller asked to enqueue work that is already
    /// enqueued — so it is never retried.
    QueueDeduplicated {
        /// The workflow that could not be enqueued.
        workflow_id: String,
        /// The queue it was destined for.
        ///
        /// Not optional: the index is plain `UNIQUE`, so its NULLs are distinct and two rows
        /// with no queue never collide. A violation therefore proves both rows had one.
        queue_name: String,
        /// The key already held.
        deduplication_id: String,
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
    /// A workflow has no step to fork from.
    ///
    /// It exists, but nothing is recorded at the point asked for: no steps at all, or none under
    /// the name given. Forking anyway would restart it from the beginning, which is a different
    /// request from the one made.
    NoForkPoint {
        /// The workflows with nothing to fork from.
        workflow_ids: Vec<String>,
        /// The step name that matched nothing, when one was named.
        step_name: Option<String>,
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
    /// A name is already registered by this same application.
    ///
    /// Distinct from [`Error::RegisteredByAnother`], which is a collision *between* applications
    /// and cannot be resolved here. This one the caller can resolve: update the registration, or
    /// register under a different name.
    AlreadyRegistered {
        /// What kind of thing it is, capitalised for a message: `"Schedule"`.
        kind: &'static str,
        /// The name already taken.
        name: String,
    },
    /// A write addressed a name with no row behind it.
    NotRegistered {
        /// What kind of thing it is, capitalised for a message: `"Schedule"`.
        kind: &'static str,
        /// The name that matched nothing.
        name: String,
    },
    /// A named thing in the system database is already registered by another application.
    ///
    /// Queue, schedule and version names address a row across every application sharing the
    /// database, so two applications cannot hold the same one. This is not the library's to
    /// resolve: taking the row would redirect a peer's work, and ignoring the write would leave
    /// this application pointing at a row it does not own.
    ///
    /// The usual causes are a genuine collision between two applications, and an application that
    /// was renamed without its rows being moved — which is what `rename_application` is for.
    RegisteredByAnother {
        /// What kind of thing it is, capitalised for a message: `"Queue"`, `"Application version"`.
        kind: &'static str,
        /// The contested name.
        name: String,
        /// The application that holds it.
        holder: String,
        /// The application that tried to take it, if it had a name.
        claimant: Option<String>,
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
            Error::ConcurrentRecv { workflow_id, topic } => match topic {
                Some(topic) => write!(
                    f,
                    "workflow {workflow_id} is already receiving on topic {topic}"
                ),
                None => write!(f, "workflow {workflow_id} is already receiving"),
            },
            Error::InvalidInput { field, detail } => write!(f, "invalid {field}: {detail}"),
            Error::QueueDeduplicated {
                workflow_id,
                queue_name,
                deduplication_id,
            } => write!(
                f,
                "workflow {workflow_id} (queue: {queue_name}, \
                 deduplication id: {deduplication_id}) is already enqueued"
            ),
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
            Error::NoForkPoint {
                workflow_ids,
                step_name,
            } => match step_name {
                Some(name) => write!(
                    f,
                    "no step named {name} in workflows {}",
                    workflow_ids.join(", ")
                ),
                None => write!(f, "no steps in workflows {}", workflow_ids.join(", ")),
            },
            Error::NonExistentWorkflow { workflow_ids } => {
                write!(f, "no such workflow: {}", workflow_ids.join(", "))
            }
            Error::MaxRecoveryAttemptsExceeded { workflow_id, limit } => write!(
                f,
                "workflow {workflow_id} exceeded {limit} recovery attempts"
            ),
            // The remedy is in the message because there is no way to act on this from code:
            // whichever cause it is, a person has to choose a name or move the rows.
            Error::AlreadyRegistered { kind, name } => {
                write!(f, "{kind} {name:?} is already registered")
            }
            Error::NotRegistered { kind, name } => {
                write!(f, "{kind} {name:?} is not registered")
            }
            Error::RegisteredByAnother {
                kind,
                name,
                holder,
                claimant,
            } => {
                let lower = kind.to_lowercase();
                write!(
                    f,
                    "{kind} {name:?} is already registered by application {holder:?} in this \
                     system database, and {lower} names must be unique across the applications \
                     sharing one"
                )?;
                if let Some(claimant) = claimant {
                    write!(
                        f,
                        ": either give {claimant:?} a different {lower} name, or, if \
                         {holder:?} was renamed to {claimant:?}, move its rows first"
                    )?;
                }
                Ok(())
            }
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
