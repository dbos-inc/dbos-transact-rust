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
//! being built on it; what is public here is the lifecycle — [`Config`], [`DBOS`], and
//! [`Executor`] — with registration, workflows and steps arriving next.
//!
//! # Cargo features
//!
//! - `engine` *(default)* — the durable execution engine: registry, contexts, workflows,
//!   steps, queues, scheduler, messaging, and the client. Turning it off leaves the system
//!   database and Conductor layers, which is the surface a future FFI host would consume.

#![forbid(unsafe_code)]

pub mod sysdb;

#[cfg(feature = "engine")]
mod config;
#[cfg(feature = "engine")]
mod context;
#[cfg(feature = "engine")]
mod dbos;
#[cfg(feature = "engine")]
mod error;
#[cfg(feature = "engine")]
mod registry;

// Flattened deliberately: the crate path is the branding, so these are `dbos::Config` and
// `dbos::Error` rather than `dbos::config::Config`. `DBOS` is the one type that spells the brand,
// because it *is* the brand — nobody writes `tokio::TOKIO`.
#[cfg(feature = "engine")]
pub use config::{APP_VERSION_ENV, Config, DATABASE_URL_ENV, Serializer};
#[cfg(feature = "engine")]
pub use context::Ctx;
#[cfg(feature = "engine")]
pub use dbos::{DBOS, Executor};
#[cfg(feature = "engine")]
pub use error::{Error, Result};
#[cfg(feature = "engine")]
pub use registry::{WorkflowFn, WorkflowKey, WorkflowRef};
// Nameable but hidden: the arity markers are inferred at every call site, and are exported only
// so that `WorkflowFn` can be spelled in a bound at all.
#[cfg(feature = "engine")]
#[doc(hidden)]
pub use registry::{NoArgs, OneArg};
