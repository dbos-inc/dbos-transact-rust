//! DBOS Transact for Rust — lightweight durable workflows on Postgres.
//!
//! This crate is the Rust port of [DBOS Transact](https://docs.dbos.dev/), joining the
//! Python, TypeScript, Go, and Java implementations. All five share one Postgres system
//! database schema, so a Rust application can run alongside applications written in any
//! of the others.
//!
//! # Status
//!
//! Under construction, and not yet usable. There is no public API here yet; the system
//! database layer lands first.
//!
//! # Cargo features
//!
//! - `engine` *(default)* — the durable execution engine: registry, contexts, workflows,
//!   steps, queues, scheduler, messaging, and the client. Turning it off leaves the system
//!   database and Conductor layers, which is the surface a future FFI host would consume.

#![forbid(unsafe_code)]
