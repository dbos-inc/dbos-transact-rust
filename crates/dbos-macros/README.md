# dbos-macros

Procedural macros for [DBOS Transact](https://crates.io/crates/dbos). This crate is an
implementation detail: depend on `dbos`, which re-exports everything here behind its default-on
`macros` feature, and never on `dbos-macros` directly.
