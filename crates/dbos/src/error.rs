//! The error channel for the execution engine.
//!
//! [`Error`] is what every engine operation returns, and it is deliberately an ordinary
//! `std::error::Error`: an application's own error type is expected to hold one, usually as a
//! `#[error(transparent)] DBOS(#[from] dbos::Error)` variant, and `#[from]` needs the trait.
//!
//! **There is no blanket `impl<E: std::error::Error> From<E> for Error`**, and the reason is a
//! language constraint rather than a preference: such an impl collides with `core`'s reflexive
//! `impl<T> From<T> for T` for as long as `Error` is itself a `std::error::Error`. `anyhow` buys
//! its blanket conversion by *not* implementing the trait, and that trade is closed here. An
//! application error therefore becomes ours through an explicit conversion at the site that needs
//! one, and ours becomes an application's through `#[from]`.

/// The result of an engine operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything the engine can fail with.
///
/// `#[non_exhaustive]` because the twenty codes the other implementations share (§4.6) arrive with
/// the phases that raise them; matching callers need a wildcard arm from the start rather than a
/// breaking change later. This is the attribute used where it belongs — on an enum, whose variants
/// grow — as opposed to on an options struct, where it would forbid `..Default::default()`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An operation needing a running executor was called before [`launch`](crate::DBOS::launch).
    ///
    /// Names the operation, following Java's `ensureLaunched(caller)`: the useful half of this
    /// error is which call was too early, and a bare "not launched" makes the developer find it.
    #[error("cannot {operation} before DBOS is launched")]
    NotLaunched {
        /// The operation that was called too early.
        operation: &'static str,
    },

    /// An operation that must happen before launch was called after it.
    ///
    /// Registration is the case that matters: the executor takes a snapshot of the registry at
    /// launch, so a workflow registered afterwards would be invisible to recovery and to dequeue.
    #[error("cannot {operation} after DBOS is launched")]
    AlreadyLaunched {
        /// The operation that was called too late.
        operation: &'static str,
    },

    /// The configuration could not be used.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Two workflows were registered under one identity.
    ///
    /// Uniqueness is on the whole `(name, class_name, config_name)` triple. A name resolving to
    /// two functions is a workflow that recovers as the wrong one, which is why this is an error
    /// rather than a last-registration-wins.
    #[error("a workflow is already registered as {key}")]
    AlreadyRegistered {
        /// The identity that was registered twice.
        key: String,
    },

    /// A value could not be encoded for the database.
    #[error("could not serialize the workflow {what}")]
    Serialization {
        /// Which value: `argument`, `result`.
        what: &'static str,
        /// The underlying failure.
        #[source]
        source: serde_json::Error,
    },

    /// A value from the database could not be decoded.
    ///
    /// Usually a signature that changed under a workflow already in flight — the row holds what
    /// the old code wrote, and the new code cannot read it.
    #[error("could not deserialize the workflow {what}")]
    Deserialization {
        /// Which value: `argument`, `result`.
        what: &'static str,
        /// The underlying failure.
        #[source]
        source: serde_json::Error,
    },

    /// A workflow was started that this executor has no registration for.
    ///
    /// In-process this cannot happen — a `WorkflowRef` comes from a registration. It is reachable
    /// through recovery and dequeue, where the name comes from a row and the code that registered
    /// it may be gone.
    #[error("no workflow is registered as {key}")]
    NotRegistered {
        /// The identity that was looked up.
        key: String,
    },

    /// The workflow was cancelled by shutdown while this caller was waiting for it.
    ///
    /// Its row stays `PENDING`, so a later executor recovers it. Nothing was lost; this caller
    /// simply stopped being the one waiting.
    #[error("the workflow {workflow_id} was interrupted by shutdown and left PENDING")]
    Interrupted {
        /// The workflow that was interrupted.
        workflow_id: String,
    },

    /// The workflow finished by returning an error.
    #[error("the workflow {workflow_id} failed: {message}")]
    WorkflowFailed {
        /// The workflow that failed.
        workflow_id: String,
        /// What it reported.
        message: String,
    },

    /// The workflow was cancelled.
    #[error("the workflow {workflow_id} was cancelled")]
    WorkflowCancelled {
        /// The workflow that was cancelled.
        workflow_id: String,
    },

    /// The workflow was recovered too many times and is parked.
    #[error("the workflow {workflow_id} exceeded {recovery_attempts} recovery attempts")]
    MaxRecoveryAttemptsExceeded {
        /// The workflow that was parked.
        workflow_id: String,
        /// How many attempts it took.
        recovery_attempts: i64,
    },

    /// The application's own code failed.
    ///
    /// This is how a workflow reports a failure of its own rather than one of ours, and it is what
    /// the `error` column records — the application's message, not a wrapper around it, because
    /// `transparent` forwards both `Display` and `source`.
    ///
    /// Built with [`Error::application`], never inferred. There is deliberately no blanket
    /// `impl<E: std::error::Error> From<E> for Error`: it collides with `core`'s reflexive
    /// `impl<T> From<T> for T` for as long as this type is itself a `std::error::Error`, which it
    /// must be so an application can hold one in its own error enum. `anyhow` buys the blanket
    /// conversion by giving up the trait; that trade is closed here.
    #[error(transparent)]
    Application(Box<dyn std::error::Error + Send + Sync>),

    /// The system database failed.
    #[error(transparent)]
    SystemDatabase(#[from] crate::sysdb::Error),
}

impl Error {
    /// Wraps an application's own error.
    ///
    /// The conversion `?` cannot do for itself, so it reads `.map_err(Error::application)?` at the
    /// boundary between an application's errors and ours. One line, and explicit about which side
    /// of the boundary a failure came from.
    pub fn application<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Application(Box::new(error))
    }
}
