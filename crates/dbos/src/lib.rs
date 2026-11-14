//! DBOS Transact for Rust: lightweight durable workflows on Postgres.
//!
//! **Pre-production release.** DBOS Transact for Rust 0.6 is a pre-production release, and isn't
//! yet supported for production use. Its API may still change in breaking ways between releases.
//!
//! DBOS checkpoints each step of a workflow in a Postgres database. When a process stops, for
//! whatever reason, its workflows recover from those checkpoints and continue from the last
//! completed step. It is only a library: there is no separate service or orchestrator, and Postgres
//! is the only other thing it needs. Its system database has the same schema as the other
//! [DBOS Transact](https://docs.dbos.dev/) SDKs', so a Rust application can run alongside
//! applications written in their languages.
//!
//! # Getting started
//!
//! A workflow is an `async fn` that takes one input and returns a [`Result`], and a
//! [step](crate::step()) is a function whose result the workflow checkpoints. Set
//! `DBOS_SYSTEM_DATABASE_URL` to a Postgres connection string (the database is created if it
//! doesn't exist), then:
//!
//! ```no_run
//! use dbos::{Config, DBOS};
//!
//! async fn workflow(_: ()) -> dbos::Result<String> {
//!     dbos::step("step_one", step_one).await?;
//!     dbos::step("step_two", step_two).await?;
//!     Ok("success".to_owned())
//! }
//!
//! async fn step_one() -> dbos::Result<()> {
//!     println!("Step one completed");
//!     Ok(())
//! }
//!
//! async fn step_two() -> dbos::Result<()> {
//!     println!("Step two completed");
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> dbos::Result<()> {
//!     let mut config = Config::from_env("dbos-starter");
//!     config.app_version = Some("0.1.0".to_owned());
//!     let dbos = DBOS::new(config);
//!
//!     // Register workflows before launching, so that recovery can find them.
//!     let workflow = dbos.register_workflow("workflow", workflow)?;
//!     dbos.launch().await?;
//!
//!     let handle = workflow.start(()).await?;
//!     println!("Workflow result: {}", handle.result().await?);
//!
//!     dbos.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! The [programming guide](https://docs.dbos.dev/rust/programming-guide) is the place to start,
//! with tutorials alongside it. There are also runnable
//! [demo applications](https://github.com/dbos-inc/dbos-demo-apps/tree/main/rust).
//!
//! # Where to look
//!
//! Everything is at the crate root, apart from the system database layer in [`sysdb`], whose types
//! some calls take and return. Free functions such as [`send`](crate::send()) and
//! [`cancel_workflow`] are for workflow bodies, and fail with [`Error::NotInWorkflow`] elsewhere.
//! Outside a workflow, call the same operation on [`DBOS`] or on a [`Client`].
//!
//! | To... | Use |
//! |---|---|
//! | Configure and run an application | [`Config`], [`DBOS`] |
//! | Define and start workflows | [`DBOS::register_workflow`], [`WorkflowRef`], [`StartOptions`], [`WorkflowHandle`] |
//! | Checkpoint work | [`step`](crate::step()), [`step_with`], [`StepOptions`], [`PendingStep`] |
//! | Race or wait for durable calls | [`select_step!`], [`select_workflow`](fn@crate::select_workflow), [`join_workflows`](fn@crate::join_workflows) |
//! | Wait and communicate | [`sleep`](crate::sleep()), [`send`](crate::send()), [`recv`](crate::recv()), [`set_event`](crate::set_event()), [`get_event`](crate::get_event()) |
//! | Stream values from a workflow | [`write_stream`], [`close_stream`], [`read_stream`] |
//! | Limit concurrency and rate | [`DBOS::register_queue`], [`QueueOptions`], [`QueueRequest`], [`Debouncer`] |
//! | Run workflows on a schedule | [`Schedule`], [`ScheduleSpec`] |
//! | Manage workflows | [`DBOS::list_workflows`], [`DBOS::cancel`], [`DBOS::resume`], [`DBOS::fork`], [`DBOS::rewind`], [`DBOS::delete`]; from a workflow body, [`list_workflows`](crate::list_workflows()), [`cancel_workflow`], [`resume_workflow`], [`fork_workflow`], [`rewind_workflow`], [`delete_workflow`] |
//! | Change a workflow's code | [`patch`](crate::patch()), [`deprecate_patch`](crate::deprecate_patch()) |
//! | Choose how values are stored | [`SerializationFormat`], [`CustomSerializer`] |
//! | Reach an application from outside it | [`Client`] |
//! | Handle failures | [`Error`], [`DurableError`] |
//! | Use the system database directly | [`sysdb`] |
//!
//! # Determinism in workflows
//!
//! A workflow body must be deterministic. A recovery runs it again from the start and replays what
//! each durable call recorded, so it has to reach the same durable calls, in the same order, with
//! the same inputs. Anything non-deterministic belongs in a [step](crate::step()), whose result is
//! recorded, or has to be one of this crate's durable primitives. The
//! [workflow tutorial](https://docs.dbos.dev/rust/tutorials/workflow-tutorial#determinism) covers
//! the same ground with more examples.
//!
//! None of this applies **inside a step**: a step's body runs once per attempt and only its result
//! is recorded, so plain races, timeouts, sleeps and spawns are all fine there. A durable call
//! still has to be awaited where it was built, though, so one spawned from a step body fails. Nor
//! does any of this apply outside a workflow.
//!
//! ## Non-deterministic values
//!
//! The clock (`SystemTime::now`, `Instant::now`, `chrono::Utc::now`), random numbers (`rand`,
//! `Uuid::new_v4`), environment and file reads, network calls and any other I/O all go in a step,
//! so the value is recorded and replayed:
//!
//! ```no_run
//! # fn rand_bool() -> bool { true }
//! # async fn charge() -> dbos::Result<String> { Ok(String::new()) }
//! # async fn refund() -> dbos::Result<String> { Ok(String::new()) }
//! async fn workflow(_: ()) -> dbos::Result<String> {
//!     // Not `if rand_bool()`: a recovery could draw the other value and take the other branch.
//!     let choice = dbos::step("choose", || async { Ok(rand_bool()) }).await?;
//!     if choice {
//!         dbos::step("charge", charge).await
//!     } else {
//!         dbos::step("refund", refund).await
//!     }
//! }
//! ```
//!
//! Branching on such a value outside a step is the commonest way a recovery diverges. When a
//! recovery reaches a different step than the one recorded at that position, the call fails with
//! [`Error::SystemDatabase`] holding [`sysdb::Error::UnexpectedStep`]. That isn't recorded as the
//! workflow's outcome: the workflow stays `PENDING`, and recovery tries it again until it is
//! parked.
//!
//! **Most divergence reports nothing**, so don't rely on that error. A recovery compares step names
//! and not inputs, and only at positions that have a record. If `charge` above were chosen outside
//! a step and the process stopped before its result was recorded, a recovery that chose differently
//! would run `refund` as well, with no error, and both would have happened.
//!
//! ## Concurrency that makes a choice nothing records
//!
//! The following are **not supported inside a workflow body**, on any runtime. What matters is the
//! behaviour rather than the crate it comes from, so each form is listed under the runtime that
//! provides it.
//!
//! - **Races over durable calls.** The winner isn't recorded, so a recovery can take another
//!   branch. Use [`select_step!`] instead.
//!   - tokio: `tokio::select!`
//!   - `futures`: `futures::select!`, `select_biased!`, `futures::future::select`, `try_select`,
//!     `select_all` and `select_ok`, and `futures::stream::select` and `select_all`
//!   - smol and `futures-lite`: `future::or`, `future::race`, `stream::or` and `stream::race`, and
//!     `FutureExt::or` and `FutureExt::race` (`smol::future` and `smol::stream` re-export
//!     `futures_lite::future` and `futures_lite::stream`)
//! - **Timeouts around durable calls.** A timeout is a race against a timer. Use [`select_step!`]
//!   with a durable [`sleep`](crate::sleep()), or the call's own timeout option where it has one.
//!   - tokio: `tokio::time::timeout` and `timeout_at`
//!   - smol and async-global-executor: an `async_io::Timer` (also `smol::Timer`) raced with `or` or
//!     `race`, and timeout crates built on it
//! - **Non-durable sleeps.** `tokio::time::sleep`, `sleep_until` or `interval`, or a
//!   `smol::Timer::after` or `async_io::Timer::at`, used as a delay isn't recorded, so it runs
//!   again in full on every recovery. Use [`sleep`](crate::sleep()).
//! - **Completion-order streams over durable calls.** The order concurrent calls finish in comes
//!   from their latency, which isn't recorded. On a recovery they come out in an order unrelated to
//!   the first run, so steps a loop runs for each completion can be handed each other's recorded
//!   results, and nothing reports an error. There is no durable replacement yet: run such a fan-out
//!   as child workflows on a queue, or sequentially.
//!   - `futures::stream::FuturesUnordered`, `StreamExt::buffer_unordered` and
//!     `StreamExt::for_each_concurrent`
//!   - any other combinator that yields results in completion order, including `futures-lite`'s
//! - **Spawning a durable call.** The running workflow is found through a task-local, which a
//!   spawned task doesn't inherit on any runtime. What happens then depends on the call and on
//!   where it was built, and the commonest cases are silent:
//!   - Built inside the spawned task, a step or a [`sleep`](crate::sleep()) runs as a plain call
//!     and records nothing, and so do [`WorkflowHandle::result`] and the methods on [`DBOS`].
//!   - Built inside the spawned task, [`WorkflowRef::start`] and [`WorkflowRef::run`] start a new
//!     top-level workflow with no parent, so each recovery that repeats the spawn starts another.
//!   - Built inside the spawned task, [`send`](crate::send()), [`recv`](crate::recv()),
//!     [`set_event`](crate::set_event()), [`get_event`](crate::get_event()) and the management
//!     functions fail with [`Error::NotInWorkflow`].
//!   - Built in the workflow and awaited in the spawned task, any of them fails with
//!     [`Error::StepBuiltElsewhere`].
//!
//!   The ways to spawn include:
//!   - tokio: `tokio::spawn`, `tokio::task::spawn_blocking`, `spawn_local`, `LocalSet` and
//!     `JoinSet`
//!   - smol: `smol::spawn`, `smol::unblock` and `Executor::spawn`
//!   - async-global-executor: `async_global_executor::spawn`, `spawn_local` and `spawn_blocking`
//!   - `std::thread::spawn`
//!
//! These are supported in a workflow body:
//!
//! - `tokio::join!`, `futures::join!`, `futures::future::join_all` and
//!   `futures_lite::future::zip`, on every runtime, when each element is one durable call. Each
//!   waits for all of its elements and returns results in input order, and each call took its step
//!   ID when it was built, which is what [`PendingStep`] explains.
//!   - **An `async` block isn't one durable call.** The calls in it are built when it is polled, so
//!     several such blocks take IDs in the order they happen to run, which a recovery doesn't
//!     reproduce. Work needing several calls in one branch is a child workflow or a single step.
//!   - **Some calls don't belong in a join.** [`patch`](crate::patch()) and
//!     [`deprecate_patch`](crate::deprecate_patch()) claim their positions when awaited, and
//!     [`Debouncer::debounce`] can claim more when awaited: await each of those on its own.
//! - `tokio::try_join!`, `futures::try_join!`, `futures::future::try_join_all` and
//!   `futures_lite::future::try_zip`, under the same rules, **only when the error goes straight out
//!   of the workflow**. These stop at the first failure and drop the rest, which makes them a race
//!   on the error path: nothing records which error came back, so code that branches on it can
//!   take a different path on a recovery. To handle a failure, use an all-wait and inspect the
//!   results.
//! - [`select_step!`], [`select_workflow`](fn@crate::select_workflow) and
//!   [`join_workflows`](fn@crate::join_workflows), and the
//!   [`select_workflow!`](macro@crate::select_workflow) and
//!   [`join_workflows!`](macro@crate::join_workflows) macros.
//! - [`sleep`](crate::sleep()), for durable delays.
//!
//! # Changing a workflow's code
//!
//! A recovered workflow replays the step history it recorded, position by position, so code that
//! moves, adds or removes steps can no longer replay executions the old code started.
//! [`patch`](crate::patch()) is the way to make such a change while those executions are in
//! flight: new executions take the new path and record that they did, and old ones take the old
//! path. Once no old execution can run again, [`deprecate_patch`](crate::deprecate_patch())
//! replaces it and the old code goes:
//!
//! ```no_run
//! # async fn step_one() -> dbos::Result<u32> { Ok(1) }
//! # async fn step_three() -> dbos::Result<u32> { Ok(3) }
//! # async fn step_two() -> dbos::Result<u32> { Ok(2) }
//! // Deployed first.
//! async fn v1(_: ()) -> dbos::Result<u32> {
//!     Ok(step_one().await? + step_two().await?)
//! }
//!
//! // Deployed while runs of `v1` may still be recovered: they take the old path.
//! async fn v2(_: ()) -> dbos::Result<u32> {
//!     let first = if dbos::patch("use-step-three").await? {
//!         step_three().await?
//!     } else {
//!         step_one().await?
//!     };
//!     Ok(first + step_two().await?)
//! }
//!
//! // Deployed once no run of `v1` can run again.
//! async fn v3(_: ()) -> dbos::Result<u32> {
//!     dbos::deprecate_patch("use-step-three").await?;
//!     Ok(step_three().await? + step_two().await?)
//! }
//! ```
//!
//! Old and new code share executions only if they run under one application version, so the
//! deployments that patch keep [`Config::app_version`] the same. The other way to change a
//! workflow's code is a new version per deployment, with the old version's executors kept running
//! until its executions finish; that needs no patch. See
//! [`patch`](crate::patch()#application-version).
//!
//! # Cargo features
//!
//! | Feature | Default | What it adds |
//! |---|---|---|
//! | `engine` | yes | The durable execution engine: workflows, steps, queues, messaging and the client. Without it, what remains is the system database layer. |
//! | `macros` | yes | [`select_step!`], the durable race. A procedural macro, so it adds `syn` and `quote` to the build, though neither ends up in the binary. Implies `engine`. |
//! | `runtime-tokio` | yes | Run on tokio. |
//! | `runtime-smol` | no | Run on smol. |
//! | `runtime-async-global-executor` | no | Run on async-global-executor. |
//!
//! The crate needs at least one runtime feature, for spawning its background tasks and setting its
//! timers. Several may be enabled: tokio is used when the caller is in a tokio runtime, and
//! otherwise async-global-executor, then smol.

#![forbid(unsafe_code)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
// What docs.rs builds with (see `[package.metadata.docs.rs]`): each feature-gated item shows a
// badge naming its feature. Not `engine`, which nearly every item needs and which is on by default.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg(hide(feature, values("engine")))))]
// Spawning and timers go through `rt`, which picks the runtime; `clippy.toml` names what that
// rules out everywhere else. Denied here rather than left at clippy's default so the rule holds
// for this crate's own code, and not for the integration tests under `tests/` that run on tokio
// only; see the `[lints.clippy]` table in `Cargo.toml`.
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

// A procedural macro has no `$crate`, so `select_step!`'s expansion names `::dbos` absolutely;
// this is what lets that path resolve inside this crate's own tests and doctests.
extern crate self as dbos;

mod rt;
pub mod sysdb;

#[cfg(feature = "engine")]
mod checkpoint;
#[cfg(feature = "engine")]
mod client;
#[cfg(feature = "engine")]
mod config;
#[cfg(feature = "engine")]
mod context;
#[cfg(feature = "engine")]
mod cron;
#[cfg(feature = "engine")]
mod db_connection;
#[cfg(feature = "engine")]
mod debouncer;
#[cfg(feature = "engine")]
mod dequeue;
#[cfg(feature = "engine")]
mod enqueue;
#[cfg(feature = "engine")]
mod error;
#[cfg(feature = "engine")]
mod event;
#[cfg(feature = "engine")]
mod handle;
#[cfg(feature = "engine")]
mod identity;
#[cfg(feature = "engine")]
mod instance;
#[cfg(feature = "engine")]
mod management;
#[cfg(feature = "engine")]
mod message;
#[cfg(feature = "engine")]
mod patch;
#[cfg(feature = "engine")]
mod queue;
#[cfg(feature = "engine")]
mod recovery;
#[cfg(feature = "engine")]
mod registry;
#[cfg(feature = "engine")]
mod schedule;
#[cfg(feature = "engine")]
mod scheduler;
#[cfg(feature = "engine")]
mod select;
#[cfg(feature = "engine")]
mod serialization;
#[cfg(feature = "engine")]
mod sleep;
#[cfg(feature = "engine")]
mod step;
#[cfg(feature = "engine")]
mod stream;
#[cfg(feature = "engine")]
mod timeout_sweep;
#[cfg(feature = "engine")]
mod version;
#[cfg(feature = "engine")]
mod wait;
#[cfg(feature = "engine")]
mod workflow;

// Flattened deliberately: the crate path is the branding, so these are `dbos::Config` and
// `dbos::Error` rather than `dbos::config::Config`. `DBOS` is the one type that spells the brand,
// because it *is* the brand — nobody writes `tokio::TOKIO`.
#[cfg(feature = "engine")]
pub use checkpoint::PendingStep;
#[cfg(feature = "engine")]
pub use client::{Client, ClientConfig};
#[cfg(feature = "engine")]
pub use config::{Config, SYSTEM_DATABASE_URL_ENV};
#[cfg(feature = "engine")]
pub use context::{StepStatus, cancellation_token, step_id, step_status, workflow_id};
#[cfg(feature = "engine")]
pub use debouncer::{DebounceOptions, Debouncer, DebouncerClient};
#[cfg(feature = "engine")]
pub use enqueue::EnqueueOptions;
#[cfg(feature = "engine")]
pub use error::{DurableError, EngineOnly, Error, Result};
#[cfg(feature = "engine")]
pub use event::{SetEventOptions, get_event, set_event, set_event_with};
#[cfg(feature = "engine")]
pub use handle::WorkflowHandle;
#[cfg(feature = "engine")]
pub use identity::{APP_ID_ENV, APP_VERSION_ENV, CLOUD_APP_NAME_ENV, CLOUD_ENV, EXECUTOR_ID_ENV};
#[cfg(feature = "engine")]
pub use instance::{DBOS, Executor, ShutdownOptions};
#[cfg(feature = "engine")]
pub use management::{
    Children, Fork, ForkOptions, ListWorkflowStepsOptions, ResumeOptions, RewindOptions,
    cancel_workflow, cancel_workflows, delete_workflow, delete_workflows, fork_workflow,
    fork_workflow_with, fork_workflows, list_workflow_steps, list_workflow_steps_with,
    list_workflows, merge_workflow_attributes, resume_workflow, resume_workflow_with,
    resume_workflows, retrieve_workflow, rewind_workflow, rewind_workflow_with, set_workflow_delay,
    update_workflow_attributes,
};
#[cfg(feature = "engine")]
pub use message::{
    Forks, SendMessage, SendOptions, recv, send, send_bulk, send_bulk_with, send_with,
};
#[cfg(feature = "engine")]
pub use patch::{deprecate_patch, patch};
#[cfg(feature = "engine")]
pub use queue::{Queue, QueueChange, QueueConflict, QueueOptions};
#[cfg(feature = "engine")]
pub use registry::{WorkflowOptions, WorkflowRef};
#[cfg(feature = "engine")]
pub use schedule::{Schedule, ScheduleChange, ScheduleSpec, ScheduledWorkflowInput};
#[cfg(feature = "engine")]
pub use serialization::{
    CustomSerializer, Payload, PayloadSlot, PortableWorkflowArgs, SerializationFormat, Serializer,
};
#[cfg(feature = "engine")]
pub use sleep::sleep;
#[cfg(feature = "engine")]
pub use step::{ShouldRetry, StepOptions, step, step_with};
#[cfg(feature = "engine")]
pub use stream::{
    DEFAULT_STREAM_POLLING_INTERVAL, MIN_STREAM_POLLING_INTERVAL, ReadStreamOptions, StreamReader,
    WriteStreamOptions, close_stream, read_stream, read_stream_from, read_stream_value,
    write_stream, write_stream_with,
};
#[cfg(feature = "engine")]
pub use sysdb::types::{
    Change, IdleTransactionTimeout, PollingConcurrency, QualifiedWorkflowName, RateLimit,
    ScheduleFilter, ScheduleStatus, WorkflowDelay, WorkflowIdReusePolicy,
};
// Re-exported because [`cancellation_token`] hands one back: naming what it returns should not
// oblige an application to declare `tokio-util` itself. It is the same foreign type either way, so
// this makes `tokio-util` a public dependency rather than sparing anyone one — a major bump of it
// is a breaking change here, and an application on a different major sees a type mismatch the
// moment it passes a token across this boundary. The alternative, a newtype, would have to
// reimplement the half of `CancellationToken` a body actually waits on, to hide a coupling that
// watching a token is anyway.
#[cfg(feature = "engine")]
pub use tokio_util::sync::CancellationToken;
#[cfg(feature = "engine")]
pub use wait::{join_workflows, select_workflow};

/// Races these durable calls and runs the arm belonging to the one that finishes first.
///
/// The durable race, and the reason a plain race over steps (`tokio::select!`, `smol::future::race`
/// and the like) is a trap rather than a shortcut. Since a step takes its id when it is *built*, a
/// plain race over steps allocates ids deterministically and so looks fixed — and it is still not
/// durable, because nothing records which branch won and a replay is free to choose again, taking a
/// path the first execution never took. This records the winning position, and a replay polls only
/// that branch, which then replays from its own checkpoint without running. See
/// [determinism in workflows](crate#determinism-in-workflows) for the forms a plain race takes.
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
/// # This crate may not be renamed
///
/// The expansion names `::dbos` — this crate, by that name — so a dependency renamed with
/// `dbos_sdk = { package = "dbos" }` puts only `dbos_sdk` in the extern prelude and the expansion
/// resolves to nothing. Resolving the downstream name instead means `proc_macro_crate`, which
/// reads the dependent's manifest and so brings a TOML parser into the build graph — against the
/// point of the `macros` feature. `tokio` makes the same trade for `#[tokio::main]`. If a rename
/// ever has to be supported, the cheap way is `tokio`'s: an optional `crate = path` at the head of
/// the macro, which costs no dependency.
///
/// # Arms, and the one place this is not `match`
///
/// An arm is `binding = call => expression`, and the comma between arms follows `match`'s rule
/// exactly: optional after a body that ends in a block, required otherwise.
///
/// # A branch is any pending call that observes
///
/// A branch is any [`PendingStep`]: a [`step`](step()), a handle's
/// [`result`](WorkflowHandle::result), a wait over workflows
/// ([`select_workflow`](fn@select_workflow) or [`join_workflows`](fn@join_workflows)), a
/// [`get_event`], a [`set_event`], a [`sleep`](sleep()), or a management call that hands back no
/// handle — free, like [`cancel_workflow`] or [`list_workflows`], or on [`DBOS`]. Each checkpoints itself under the id it was built with and
/// replays from its own row when it is the recorded winner, so a race between a step and the await
/// of a child is as durable as one between two steps.
///
/// **Where *every* branch is a workflow's outcome, reach for
/// [`select_workflow!`](macro@crate::select_workflow) instead.** Both are durable; the difference
/// is what they cost. One wait settles the whole set — one query per poll interval however many
/// handles it is given — where N awaits raced here are N pollers asking separately. This macro is
/// for a race whose branches are *not* all of one kind: a step against an await, a sleep against
/// an event.
///
/// **A [`start`](WorkflowRef::start) and a [`run`](WorkflowRef::run) are refused, by type.** They
/// hand back a [`PendingStart`] and a [`PendingWorkflow`] rather than a `PendingStep`, and both
/// *create* a workflow: only
/// the winner is polled on a replay, so whether a child exists at all would follow the timing of
/// another branch, and a run additionally holds two ids and would leave a started, recorded child
/// whose outcome the parent never learns. Start outside the race, and race the handle's
/// [`result`](WorkflowHandle::result) — which observes rather than creates, and is the branch that
/// was meant. A fork, a resume and a rewind hand back a [`PendingStart`] too: each sets a workflow
/// running, so whether it runs would follow the race. The type stops only those. A losing branch
/// that committed has still done its work, so a cancel, a delete, a delay brought forward, or a
/// bulk fork or resume (which stay steps) are no better branches, though the type allows them.
///
/// **What losing means.** A losing step is dropped mid-body and records nothing, so a replay never
/// runs it; its [`cancellation_token`] fires on the way out, as it does for a timeout, so work it
/// handed to a blocking thread learns to stop. A losing [`sleep`](sleep()) is the exception that
/// does leave a row: it checkpoints the instant it will wake at *before* waiting on it, so what
/// stays behind records an abandoned wait rather than an outcome. A losing *await* is a dropped
/// wait on a child that keeps going, durably, with nobody watching it — losing the race does not
/// cancel it. Cancel from the winning arm if abandoning the loser is the intent.
///
/// **A control signal winning is not a decision.** A branch that resolves to a cancellation, an
/// interruption or a database failure has recorded nothing, as a step ending that way never does,
/// and the race records nothing either: the error is returned, the workflow stays pending, and a
/// recovery races every branch afresh. Recording that branch as the winner would pin every
/// recovery to one that never ran its body. An application error is the branch's own recorded
/// outcome, and winning with one is recorded and replayed like any other win.
///
/// **Every branch fails the same way, and is made to say so before the race.** The branches are
/// tied to one error type when they are pushed, which is at the build — so `map_err` on an arm's
/// body is too late, and the conversion belongs on the call. A call in the engine's channel joins
/// with [`lift`](PendingStep::lift), which the compiler writes itself because
/// [`EngineOnly`] is uninhabited; a call with an application error type of its
/// own joins with [`map_error`](PendingStep::map_error), which the caller writes because only the
/// caller knows what one failure means in terms of the other.
///
/// # A branch is an expression, and exactly one call
///
/// **An expression, where [`select_workflow!`](macro@crate::select_workflow) needs a variable.**
/// That looks like the pair disagreeing and is not: a workflow handle is named twice there, once
/// for its id and once to be consumed, so an expression would be evaluated twice. A branch here is
/// named once — this macro builds it, owns it, polls it, drops it — so the hazard does not exist
/// and the form that reads like `tokio::select!` is available.
///
/// **Exactly one call, not a block containing several.** The branches are built before any is
/// polled, which is what fixes their ids; an `async` block would defer the calls inside it to its
/// first poll and put their ids back on poll order. Work needing several steps in one branch is a
/// child workflow, which has a counter of its own — and the handle's `result` can be the branch.
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
/// **Fewer than two branches.** One branch is not a race: await the call, which is shorter and
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
#[cfg(feature = "engine")]
pub use workflow::{
    DuplicationPolicy, PendingRun, PendingStart, PendingWorkflow, QueueRequest, RunOptions,
    StartOptions, Timeout,
};

/// The async runtime seam, for this workspace's tests and **not public API**.
///
/// A test binary is another crate, and a test that is to run on more than one runtime has to
/// spawn and sleep the way the library does rather than through tokio. Not gated on `engine`,
/// because the test harness uses it and the harness builds without the engine.
#[doc(hidden)]
pub mod __rt {
    pub use crate::rt::{
        AbortHandle, Elapsed, JoinError, JoinHandle, fallback, sleep, sleep_until, spawn, timeout,
        timeout_at,
    };
}

/// What [`select_step!`](crate::select_step) expands into, and **not public API**.
///
/// A procedural macro has no `$crate`, so its expansion has to name an absolute path that the
/// calling crate can resolve — which means everything the expansion calls must be `pub`. This
/// module is where that surface lives, so that being `pub` for the macro's sake is not the same as
/// being part of the crate's API: nothing here is documented, nothing here is stable, and calling
/// any of it by hand is writing an expansion by hand.
///
/// The durable race itself is documented on [`select_step!`](crate::select_step); the reasoning
/// behind what it records is in `select.rs`.
///
/// It also holds `set_workflow_timeout_sweep_interval`, which this crate's tests use to make the
/// workflow-timeout sweep run more often. It is here for the same reason: `pub` because a test
/// binary is another crate, and not API.
///
/// Gated on `engine` rather than on `macros`, which is what actually reaches it: these are the
/// only paths into the race's core, and without them an engine-only build has a module of `pub`
/// items nothing can call.
#[cfg(feature = "engine")]
#[doc(hidden)]
pub mod __private {
    pub use crate::select::{
        Branches, Racing, Recording, check_select, control_error, record_select,
    };
    // Not for a macro: the one knob tests need that applications shouldn't have.
    pub use crate::timeout_sweep::set_interval as set_workflow_timeout_sweep_interval;
}
