//! The DBOS system database.
//!
//! This layer owns the schema, its migrations, and every query against it. It is
//! deliberately free of any dependency on the execution engine — no registry, no
//! contexts, no workflow types — so that it compiles on its own with
//! `--no-default-features`. CI enforces that, and it is what keeps a second backend
//! (SQLite) a second implementation rather than a rewrite.

pub mod migrations;
