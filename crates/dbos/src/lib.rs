//! DBOS Transact for Rust — lightweight durable workflows on Postgres.
//!
//! This crate is the Rust port of [DBOS Transact](https://docs.dbos.dev/), joining the
//! Python, TypeScript, Go, and Java implementations. All five share one Postgres system
//! database schema, so a Rust application can run alongside applications written in any
//! of the others.
//!
//! # Status
//!
//! Under construction. The system database layer landed first and the execution engine is
//! being built on it; the lifecycle, registration, workflows, steps, events, recovery, queues
//! and the client are here, with the scheduler arriving next.
//!
//! # Cargo features
//!
//! - `engine` *(default)* — the durable execution engine: registry, contexts, workflows,
//!   steps, queues, scheduler, messaging, and the client. Turning it off leaves the system
//!   database and Conductor layers, which is the surface a future FFI host would consume.
//! - `macros` *(default)* — [`select_step!`], the durable race, which is a procedural macro and so
//!   costs `syn` and `quote` at build time. Implies `engine`, because the expansion is engine
//!   code; nothing it generates reaches the binary.

#![forbid(unsafe_code)]

// **What `$crate` would have been.** A `macro_rules!` refers to its own crate by `$crate` and so
// works inside it; a procedural macro has no such token and must write an absolute path, which
// this crate's own tests and doctests would otherwise fail to resolve. Renaming the dependency
// (`dbos_sdk = { package = "dbos" }`) breaks the expansion for the same reason, which is a real
// cost of the procedural form and the reason the path is `::dbos` and not something shorter.
extern crate self as dbos;

pub mod sysdb;

#[cfg(feature = "engine")]
mod checkpoint;
#[cfg(feature = "engine")]
mod client;
#[cfg(feature = "engine")]
mod config;
#[cfg(feature = "engine")]
mod connection;
#[cfg(feature = "engine")]
mod context;
#[cfg(feature = "engine")]
mod dequeue;
#[cfg(feature = "engine")]
mod error;
#[cfg(feature = "engine")]
mod event;
#[cfg(feature = "engine")]
mod handle;
#[cfg(feature = "engine")]
mod identity;
// The instance and its executor. Named for what it holds rather than for the crate, because
// `extern crate self as dbos` above puts the crate's own name in the root namespace and a module
// may not share it — which is a cost the procedural macro brings and `$crate` would not have.
#[cfg(feature = "engine")]
mod instance;
#[cfg(feature = "engine")]
mod management;
#[cfg(feature = "engine")]
mod queue;
#[cfg(feature = "engine")]
mod recovery;
#[cfg(feature = "engine")]
mod registry;
#[cfg(feature = "engine")]
mod select;
#[cfg(feature = "engine")]
mod serialization;
#[cfg(feature = "engine")]
mod sleep;
#[cfg(feature = "engine")]
mod step;
#[cfg(feature = "engine")]
mod wait;
#[cfg(feature = "engine")]
mod workflow;

// Flattened deliberately: the crate path is the branding, so these are `dbos::Config` and
// `dbos::Error` rather than `dbos::config::Config`. `DBOS` is the one type that spells the brand,
// because it *is* the brand — nobody writes `tokio::TOKIO`.
#[cfg(feature = "engine")]
pub use client::{Client, ClientConfig, EnqueueOptions, Forks, Message};
#[cfg(feature = "engine")]
pub use config::{Config, DATABASE_URL_ENV, Serializer};
#[cfg(feature = "engine")]
pub use context::Ctx;
#[cfg(feature = "engine")]
pub use error::{DurableError, EngineOnly, Error, Result};
#[cfg(feature = "engine")]
pub use event::{get_event, set_event};
#[cfg(feature = "engine")]
pub use handle::WorkflowHandle;
#[cfg(feature = "engine")]
pub use identity::{APP_ID_ENV, APP_VERSION_ENV, CLOUD_APP_NAME_ENV, CLOUD_ENV, EXECUTOR_ID_ENV};
#[cfg(feature = "engine")]
pub use instance::{DBOS, Executor};
#[cfg(feature = "engine")]
pub use management::{Children, ForkFrom, ForkOptions, ResumeOptions};
#[cfg(feature = "engine")]
pub use queue::{Queue, QueueChange, QueueConflict, QueueOptions};
#[cfg(feature = "engine")]
pub use registry::{WorkflowKey, WorkflowRef};
#[cfg(feature = "engine")]
pub use sleep::sleep;
#[cfg(feature = "engine")]
pub use step::{PendingStep, ShouldRetry, StepOptions, step, step_with};
#[cfg(feature = "engine")]
pub use sysdb::types::{Change, RateLimit, WorkflowDelay};
#[cfg(feature = "engine")]
pub use wait::{join_workflows, select_workflow};

/// Races these steps and runs the arm belonging to the one that finishes first.
///
/// The durable race, and the reason a plain `tokio::select!` over steps is a trap rather than a
/// shortcut. Since a step takes its id when it is *built*, a `select!` over steps allocates ids
/// deterministically and so looks fixed — and it is still not durable, because nothing records
/// which branch won and a replay is free to choose again, taking a path the first execution never
/// took. This records the winning position and a replay polls only that branch, which then replays
/// from its own checkpoint without running.
///
/// ```no_run
/// # async fn charge(cents: u32) -> dbos::Result<u32> { Ok(cents) }
/// # async fn timer() -> dbos::Result<()> { Ok(()) }
/// # async fn f(cents: u32) -> dbos::Result<String> {
/// use dbos::step;
/// dbos::select_step! {
///     charged = step("charge", || charge(cents)) => {
///         let cents = charged?;
///         format!("charged {cents}")
///     }
///     expired = step("expire", || timer()) => { expired?; "timed out".to_owned() }
/// }
/// # }
/// ```
///
/// # Arms, and the one place this is not `match`
///
/// An arm is `binding = step => expression`, and the comma between arms follows `match`'s rule
/// exactly: optional after a body that ends in a block, required otherwise.
///
/// # A branch is an expression, and exactly one step
///
/// **An expression, where [`select_workflow!`](macro@crate::select_workflow) needs a variable.**
/// That looks like the pair disagreeing and is not: a workflow handle is named twice, once for its
/// id and once to be consumed, so an expression there would be evaluated twice and
/// `child.start(n).await?` would start the workflow twice. A step is named once — this macro
/// builds it, owns it, polls it, drops it — so the hazard does not exist here and the form that
/// reads like `tokio::select!` is available.
///
/// **Exactly one step, not a block containing several.** The branches are built before any is
/// polled, which is what fixes their ids; an `async` block would defer the steps inside it to its
/// first poll and put their ids back on poll order. Work needing several steps in one branch is a
/// child workflow, which has a counter of its own.
///
/// # What it refuses, and why each refusal is the design
///
/// **Guards (`if cond`), `else` and `complete` arms.** All three change which branches exist
/// between runs, and the checkpoint records a *position* among them: a guard true on the first run
/// and false on the replay would make the recorded index name a branch that is no longer there.
///
/// **`biased`.** Not an option to offer, because fixed source order is the contract. Tokio
/// randomises for fairness, and fairness is exactly what a replay cannot reproduce.
///
/// **Fewer than two branches.** One branch is not a race: await the step, which is shorter and
/// durable on its own. There is no upper bound — a procedural macro names a slot per branch and
/// runs out of nothing — though a race wide enough to want one is usually a child workflow per
/// branch, which has a step counter of its own.
///
/// Each refusal is a `compile_error!` spanned on the offending tokens, which is better than
/// [`select_workflow!`](macro@crate::select_workflow) manages: it takes a slice and cannot see an
/// empty one until it runs.
///
/// # Inside and outside a workflow
///
/// Inside one, the winning position is recorded under
/// [`SELECT_STEP`](crate::sysdb::types::step_names::SELECT_STEP) and the branches checkpoint
/// themselves under the ids they were built with. Outside one — or inside another step — nothing
/// is recorded and the branches run plainly, which is the same fall-through an ordinary step takes
/// and what keeps a function built from steps ordinarily testable.
///
/// The value is a [`Result`], because the race itself can fail. Each arm binds its own branch's
/// outcome, so an arm decides for itself whether to `?` it, match it, or report it.
#[cfg(feature = "macros")]
pub use dbos_macros::select_step;

/// Machinery the macros expand into. **Not public API**, and not subject to its stability.
///
/// A macro expands in the *caller's* crate, so everything it names has to be reachable by an
/// absolute path — and the durable half of a race lives on `Placement` and the connection, both
/// `pub(crate)` in private modules. Re-exporting it here, hidden, is what lets `select_step!`
/// stay a shape over ordinary tested code rather than carrying the checkpoint logic in its
/// expansion.
///
/// Gated on `engine` rather than on `macros`, which is what actually reaches it: these are the
/// only paths to the race's core, and without them an engine-only build has a module of `pub`
/// items nothing can call.
#[cfg(feature = "engine")]
#[doc(hidden)]
pub mod __private {
    pub use crate::select::{Branches, Racing, Recording, check_select, record_select};
}
#[cfg(feature = "engine")]
pub use workflow::{DuplicationPolicy, Enqueue, RunOptions, StartOptions, Timeout};
