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

#![forbid(unsafe_code)]

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
mod dbos;
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
pub use dbos::{DBOS, Executor};
#[cfg(feature = "engine")]
pub use error::{DurableError, EngineOnly, Error, Result};
#[cfg(feature = "engine")]
pub use event::{get_event, set_event};
#[cfg(feature = "engine")]
pub use handle::WorkflowHandle;
#[cfg(feature = "engine")]
pub use identity::{APP_ID_ENV, APP_VERSION_ENV, CLOUD_APP_NAME_ENV, CLOUD_ENV, EXECUTOR_ID_ENV};
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
#[cfg(feature = "engine")]
pub use workflow::{DuplicationPolicy, Enqueue, RunOptions, StartOptions, Timeout};
