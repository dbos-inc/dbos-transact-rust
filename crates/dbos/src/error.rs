//! The error channel for the execution engine.
//!
//! [`Error`] is generic over what the *application* failed with, and that one decision shapes the
//! rest. The engine wraps the application rather than the application making room for the engine:
//! a workflow returning `dbos::Result<Receipt, CheckoutError>` fails with either
//! [`Error::Application`] carrying a `CheckoutError`, or one of the engine's own variants. The
//! application's error type stays DBOS-agnostic — two derives, no variant of ours inside it, no
//! trait it has to know about.
//!
//! **The generic parameter buys back a blanket conversion**, which is what makes that work.
//! `impl<E> From<E> for Error<E>` is accepted where `impl<E: std::error::Error> From<E> for Error`
//! is not: the overlap with `core`'s reflexive `impl<T> From<T> for T` would need `E = Error<E>`,
//! an infinite type the occurs check rules out. So `?` lifts an application's own error into ours
//! with no impl written by anyone. `anyhow` gives up `std::error::Error` to get the same thing;
//! here it costs a type parameter instead.
//!
//! The price is paid inside this crate rather than by its users: the blanket impl is why
//! [`Error::SystemDatabase`] cannot also be a `#[from]` — that pair *does* overlap, at
//! `Error<sysdb::Error>` — so engine code names the variant, `.map_err(Error::SystemDatabase)?`.

use std::borrow::Cow;

/// The error type of a workflow or step with no failure of its own.
///
/// Named for what it makes the channel: `Error<EngineOnly>` carries the engine's own errors and
/// nothing else. Uninhabited, so the compiler knows [`Error::Application`] cannot be constructed,
/// which is what lets an engine error be carried into any workflow's channel by a total conversion
/// rather than by a panic waiting for an input that cannot arrive.
///
/// This is the default parameter, so bare `dbos::Error` means "a failure of DBOS's", and
/// `dbos::Result<T>` is the result of an engine operation. A `From<MyError>` bound failing against
/// `Error<EngineOnly>` is the compiler saying the workflow declared no application error type —
/// the fix is to return `dbos::Result<T, MyError>`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum EngineOnly {}

impl std::fmt::Display for EngineOnly {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for EngineOnly {}

/// What an application may fail with, inside [`Error::Application`].
///
/// Notably **not** `From<Error>`: an application error type knows nothing about this crate. The
/// bounds are only what the engine needs of it:
///
/// - `Serialize` + `DeserializeOwned` — a recorded failure has to survive a column, and decoding
///   needs a concrete type. This is the whole reason the error is a type parameter rather than a
///   trait object.
/// - `std::error::Error` — so a failure reads as a failure: `Display` for a log, `source` for a
///   caller walking the chain.
///
/// Implemented for anything meeting them, so an application writes no impl:
///
/// ```
/// #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
/// enum CheckoutError {
///     #[error("the card was declined")]
///     CardDeclined,
/// }
///
/// async fn checkout(cart: u32) -> dbos::Result<u32, CheckoutError> {
///     Err(CheckoutError::CardDeclined)?
/// }
/// ```
pub trait DurableError:
    serde::Serialize + serde::de::DeserializeOwned + std::error::Error + 'static
{
}

impl<E> DurableError for E where
    E: serde::Serialize + serde::de::DeserializeOwned + std::error::Error + 'static
{
}

/// The result of a durable function, or of an engine operation.
///
/// `Result<T>` is the engine's own — it cannot carry an application failure, because
/// [`EngineOnly`] has no values. `Result<T, CheckoutError>` is a workflow's, and note the second
/// parameter is the *application's* error rather than the whole error type: it expands to
/// `std::result::Result<T, Error<CheckoutError>>`.
pub type Result<T, E = EngineOnly> = std::result::Result<T, Error<E>>;

/// Everything a durable function can fail with.
///
/// `#[non_exhaustive]` because the twenty codes the other implementations share (§4.6) arrive with
/// the phases that raise them; matching callers need a wildcard arm from the start rather than a
/// breaking change later.
///
/// **Serializable, and that is load-bearing.** A failed workflow or step records the error it
/// failed with, and a replay has to give back *that error* rather than a description of it — the
/// same fidelity a successful result gets. Every payload is data this crate or the application
/// owns, so encoding costs a derive; the only fields that cannot survive a round trip are the
/// `serde_json::Error` sources, marked `#[serde(skip)]` and explained where they are declared.
#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum Error<E = EngineOnly> {
    /// A failure reported by the workflow or step body itself.
    ///
    /// The application's own error, held as itself rather than reduced to a description of one.
    /// The run that fails and the replay that reads the row back both produce this variant with an
    /// equal payload, which is the property the whole generic parameter exists for.
    #[error(transparent)]
    Application(E),

    /// An operation needing a running executor was called before [`launch`](crate::DBOS::launch).
    ///
    /// Names the operation, following Java's `ensureLaunched(caller)`: the useful half of this
    /// error is which call was too early, and a bare "not launched" makes the developer find it.
    #[error("cannot {operation} before DBOS is launched")]
    NotLaunched {
        /// The operation that was called too early.
        operation: Cow<'static, str>,
    },

    /// An operation that must happen before launch was called after it.
    ///
    /// Registration is the case that matters: the executor takes a snapshot of the registry at
    /// launch, so a workflow registered afterwards would be invisible to recovery and to dequeue.
    #[error("cannot {operation} after DBOS is launched")]
    AlreadyLaunched {
        /// The operation that was called too late.
        operation: Cow<'static, str>,
    },

    /// An operation that only makes sense inside a workflow was called outside one.
    ///
    /// [`set_event`](crate::set_event) is the first: it checkpoints its write under a step id,
    /// and outside a workflow there is no step-id sequence to record against — unlike a step,
    /// whose body can simply run plainly.
    #[error("{operation} must be called from within a workflow")]
    NotInWorkflow {
        /// The operation that needed a workflow around it.
        operation: Cow<'static, str>,
    },

    /// An operation that allocates a step id was called from inside a step.
    ///
    /// A step is a leaf: its checkpoint stands for everything the body did, so an id-allocating
    /// operation inside one would shift every later step onto the wrong replay slot. The same
    /// rule that makes a nested step a plain call makes this an error — there is no plain
    /// version of a durable write to degrade to.
    #[error("{operation} cannot be called from within a step")]
    InsideStep {
        /// The operation that was called inside a step.
        operation: Cow<'static, str>,
    },

    /// An instance method was called from inside a workflow another instance is running.
    ///
    /// An instance method takes its executor from the handle it was called on, and its step ids
    /// from the ambient context. Normally those are the same instance and the distinction never
    /// surfaces. When they are not, there is no safe way to pick: the ids come from this
    /// workflow's counter and the checkpoint would be written through the other instance's system
    /// database, so it would land where the workflow that allocated them cannot see it — the
    /// replay skip would never match, the read would run again on recovery, and this workflow's
    /// counter would have moved on regardless.
    ///
    /// The free functions cannot raise this: they take both halves from the one context.
    #[error(
        "{operation} was called on a different DBOS instance than the one running this workflow"
    )]
    WrongInstance {
        /// The operation that was called on the wrong instance.
        operation: Cow<'static, str>,
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
    #[error("could not serialize the workflow {what}: {message}")]
    Serialization {
        /// Which value: `argument`, `result`, `error`.
        what: Cow<'static, str>,
        /// What the serializer said.
        message: String,
        /// The live failure, when this process is the one that produced it.
        ///
        /// `serde_json::Error` is not itself deserializable, so this is one of the two payloads
        /// here that cannot come back out of a column. [`message`](Self::Serialization::message)
        /// carries what it said, which is what a reader of a recorded error actually needs.
        #[serde(skip)]
        #[source]
        source: Option<serde_json::Error>,
    },

    /// A value from the database could not be decoded.
    ///
    /// Usually a signature that changed under a workflow already in flight — the row holds what
    /// the old code wrote, and the new code cannot read it.
    #[error("could not deserialize the workflow {what}: {message}")]
    Deserialization {
        /// Which value: `argument`, `result`, `error`.
        what: Cow<'static, str>,
        /// What the deserializer said.
        message: String,
        /// The live failure, when this process is the one that produced it.
        #[serde(skip)]
        #[source]
        source: Option<serde_json::Error>,
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

    /// A workflow id resolved to no row at all.
    ///
    /// Unreachable through a handle this process minted — starting is what wrote the row — and
    /// reachable through anything that takes a caller's workflow id on faith.
    #[error("no workflow exists with id {workflow_id}")]
    WorkflowNotFound {
        /// The id that matched nothing.
        workflow_id: String,
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

    /// A workflow failed, and what it failed with was not one of ours to decode.
    ///
    /// The workflow-level twin of [`StepFailed`](Self::StepFailed), and raised for the same
    /// reason: the row was written by another SDK, whose serializer chose its own shape. The
    /// message is what survives. A workflow this SDK recorded comes back as the error itself.
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

    /// A recorded step failed, and what it failed with was not one of ours to decode.
    ///
    /// Raised on replay against a row another SDK wrote: its serializer chose its own shape, so
    /// the message is what survives. A step this SDK recorded replays as the error itself.
    #[error("the step {step} failed: {message}")]
    StepFailed {
        /// The step's name.
        step: String,
        /// What it reported when it ran.
        message: String,
    },

    /// The system database failed.
    ///
    /// Deliberately not a `#[from]`: that impl overlaps the blanket `From<E> for Error<E>` at
    /// `Error<sysdb::Error>`. Engine code names the variant instead.
    #[error(transparent)]
    SystemDatabase(crate::sysdb::Error),
}

/// The conversion that makes `?` work on an application's own error.
///
/// Accepted only because `Error` is generic — see the module documentation.
impl<E> From<E> for Error<E> {
    fn from(error: E) -> Self {
        Error::Application(error)
    }
}

impl<E> Error<E> {
    /// Re-targets this error at another application error type.
    ///
    /// One match over the engine's variants, kept in a single place so [`lift`](Self::lift) and
    /// any later conversion share the list rather than each carrying a copy of it.
    fn map_application<E2>(self, f: impl FnOnce(E) -> E2) -> Error<E2> {
        match self {
            Error::Application(error) => Error::Application(f(error)),
            Error::NotLaunched { operation } => Error::NotLaunched { operation },
            Error::AlreadyLaunched { operation } => Error::AlreadyLaunched { operation },
            Error::NotInWorkflow { operation } => Error::NotInWorkflow { operation },
            Error::InsideStep { operation } => Error::InsideStep { operation },
            Error::WrongInstance { operation } => Error::WrongInstance { operation },
            Error::Config(message) => Error::Config(message),
            Error::AlreadyRegistered { key } => Error::AlreadyRegistered { key },
            Error::Serialization {
                what,
                message,
                source,
            } => Error::Serialization {
                what,
                message,
                source,
            },
            Error::Deserialization {
                what,
                message,
                source,
            } => Error::Deserialization {
                what,
                message,
                source,
            },
            Error::NotRegistered { key } => Error::NotRegistered { key },
            Error::WorkflowNotFound { workflow_id } => Error::WorkflowNotFound { workflow_id },
            Error::Interrupted { workflow_id } => Error::Interrupted { workflow_id },
            Error::WorkflowFailed {
                workflow_id,
                message,
            } => Error::WorkflowFailed {
                workflow_id,
                message,
            },
            Error::WorkflowCancelled { workflow_id } => Error::WorkflowCancelled { workflow_id },
            Error::MaxRecoveryAttemptsExceeded {
                workflow_id,
                recovery_attempts,
            } => Error::MaxRecoveryAttemptsExceeded {
                workflow_id,
                recovery_attempts,
            },
            Error::StepFailed { step, message } => Error::StepFailed { step, message },
            Error::SystemDatabase(error) => Error::SystemDatabase(error),
        }
    }

    /// The control signal this failure is, if it is one.
    ///
    /// A control error is not the workflow's *outcome*: a cancelled workflow recorded as having
    /// failed would come back permanently failed, having lost that it was interrupted rather than
    /// wrong. So the recording layer asks this before writing anything terminal.
    ///
    /// A plain match on the outermost variant is enough. That is a consequence of the inversion:
    /// an application error type has nowhere to hide one of ours, so a control signal is either
    /// the outermost variant or it is not present.
    pub(crate) fn control(&self) -> Option<Error<EngineOnly>> {
        match self {
            Error::WorkflowCancelled { workflow_id } => Some(Error::WorkflowCancelled {
                workflow_id: workflow_id.clone(),
            }),
            Error::Interrupted { workflow_id } => Some(Error::Interrupted {
                workflow_id: workflow_id.clone(),
            }),
            // *Every* system-database failure, not just a cancellation. A database failure is a
            // failure of the engine's substrate; it is never a statement about what the workflow
            // computed, so recording it as that workflow's outcome asserts something false. A
            // transient blip while the engine checkpoints would otherwise permanently fail a
            // workflow that had not failed — and the row is the only copy of that fact.
            //
            // A deterministic one — `UnexpectedStep`, `Malformed` — retries rather than failing
            // fast. That is the deliberate trade: `MAX_RECOVERY_ATTEMPTS` bounds it and parks the
            // workflow, where recording ERROR would have discarded work a fixed deployment could
            // still have finished.
            Error::SystemDatabase(error) => Some(Error::SystemDatabase(error.clone())),
            _ => None,
        }
    }
}

impl Error<EngineOnly> {
    /// Carries an engine error into a durable function's own error channel.
    ///
    /// Total, and that is the point of [`EngineOnly`] being uninhabited: the `Application` arm
    /// holds a value of a type with no values, so the compiler discharges it rather than this
    /// needing a panic or a fallback for a case that cannot occur.
    ///
    /// Public because a workflow with its own error type sometimes holds an engine-channel
    /// result — [`WorkflowRef::start`](crate::WorkflowRef::start) is one, and
    /// [`DBOS::get_event`](crate::DBOS::get_event) called from inside a workflow another — and
    /// `?` cannot lift `Error<EngineOnly>` into `Error<E>` on its own: the blanket conversion
    /// would wrap the whole error as the application's. This is the conversion written out:
    ///
    /// ```ignore
    /// let child = checkout.start(order).await.map_err(Error::lift)?;
    /// ```
    ///
    /// The workflow-facing calls do not need it: [`step`](crate::step),
    /// [`set_event`](crate::set_event) and [`get_event`](crate::get_event) are all generic over
    /// the caller's channel, so `?` works on them directly.
    pub fn lift<E>(self) -> Error<E> {
        self.map_application(|impossible| match impossible {})
    }
}

/// How an erased workflow failed.
///
/// Two cases, and keeping them apart is the point: what the workflow itself returned, which is its
/// outcome and gets recorded, and a signal from the engine, which is not. A cancelled workflow
/// recorded as having *failed* would come back permanently failed, having lost that it was
/// interrupted rather than wrong.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The workflow's own failure — the whole [`Error`] envelope, encoded by the serializer that
    /// owns the row. This is what the error column records and what a replay decodes.
    Recorded(String),
    /// One of ours. Never recorded as the workflow's outcome.
    Control(Error<EngineOnly>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, PartialEq)]
    #[error("the card was declined after {attempts} attempts")]
    struct CardDeclined {
        attempts: u32,
    }

    /// The property the type parameter exists for: an application error is held as itself, so it
    /// comes back as itself.
    #[test]
    fn an_application_error_round_trips_whole() {
        let error: Error<CardDeclined> = CardDeclined { attempts: 3 }.into();
        let json = serde_json::to_string(&error).unwrap();
        let back: Error<CardDeclined> = serde_json::from_str(&json).unwrap();

        let Error::Application(back) = back else {
            panic!("expected an application error, got {back:?}")
        };
        assert_eq!(back, CardDeclined { attempts: 3 }, "the field survives");
        assert_eq!(error.to_string(), back.to_string());
    }

    /// `?` on the application's own error needs no impl from the application.
    #[test]
    fn the_blanket_conversion_lifts_an_application_error() {
        fn fallible() -> Result<(), CardDeclined> {
            Err(CardDeclined { attempts: 1 })?
        }
        assert!(matches!(
            fallible().unwrap_err(),
            Error::Application(CardDeclined { attempts: 1 })
        ));
    }

    #[test]
    fn an_engine_error_lifts_into_any_channel() {
        let engine: Error = Error::NotLaunched {
            operation: "run a workflow".into(),
        };
        let lifted: Error<CardDeclined> = engine.lift();
        assert_eq!(
            lifted.to_string(),
            "cannot run a workflow before DBOS is launched"
        );
    }

    #[test]
    fn only_control_errors_report_themselves_as_control() {
        let cancelled: Error<CardDeclined> = Error::WorkflowCancelled {
            workflow_id: "wf-1".to_owned(),
        };
        assert!(cancelled.control().is_some());

        let failed: Error<CardDeclined> = CardDeclined { attempts: 1 }.into();
        assert!(
            failed.control().is_none(),
            "an application failure is an outcome, not a signal"
        );
    }
}
