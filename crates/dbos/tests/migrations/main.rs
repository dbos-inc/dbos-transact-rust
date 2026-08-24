//! Migrations against real databases: the corpus itself, and the runner that applies it.
//!
//! **One binary, two modules, because a binary is the unit of parallelism.** `cargo test` runs
//! test binaries one after another and only parallelises *within* one, so two files meant two
//! thread pools used in sequence — each draining to a tail where most threads sat idle waiting
//! for the last few tests. Merged, twenty-one tests share one pool and short tests backfill
//! behind long ones. It also halves the containers these tests need, which the harness docs ask
//! for directly: "prefer few, larger test files over many small ones — each additional one is
//! another container."
//!
//! Worth knowing before touching the split: these tests parallelise nearly linearly. Measured on
//! twelve cores, `runner`'s twelve tests take ~38s together and blow an 800s timeout at
//! `--test-threads=1`. The CockroachDB leg is slow here — 26 to 38 times slower than PostgreSQL —
//! because every test applies a full corpus and CockroachDB prices each migration as an online
//! schema change, not because the server serialises them. So thread count is the constraint, and
//! anything that widens the pool helps.
//!
//! The two modules stay separate because they test different things, and the distinction is
//! worth keeping in the failure output:
//!
//! - `sql` — whether the statements are *valid*: a `%s` bound to the wrong value, a variant
//!   selected for the wrong dialect, a statement that parses everywhere and executes on one
//!   backend.
//! - `runner` — whether the *bookkeeping* is right: a fresh database lands on the right version,
//!   a second call does nothing, an interrupted run resumes, a database another implementation
//!   took further is left alone.
//!
//! Both run on the raw lane rather than the pool, because the migrations are the thing under
//! test — a pre-migrated database would answer the question before it was asked.

mod runner;
mod sql;
