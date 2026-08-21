# DBOS Rust Starter

The Workflows tab of the DBOS starter app: a three-step workflow that checkpoints each step to
Postgres, a progress display, and a crash button. Launch a workflow, crash the app, restart it —
execution resumes at the step after the last one that finished, with no application code involved.

## Run it

With a Postgres listening on `localhost:5432`, no configuration is needed:

```bash
cargo run -p dbos-rust-starter
```

The app connects to a `dbos_rust_starter` database, creating it if it does not exist. The default
URL carries no username or password, so the standard libpq variables apply — set `PGUSER` and
`PGPASSWORD` if your server wants them. Point it elsewhere entirely with:

```bash
export DBOS_DATABASE_URL=postgres://user:password@host:5432/dbname
```

Open <http://localhost:8080>, launch a workflow, and crash the app mid-run. Restart it with
`cargo run -p dbos-rust-starter` again — `launch()` recovers the workflow, the steps that already
finished do not run again, and the progress display picks up where it left off.

## The application version

The app pins its application version to `0.1.0` (override with `DBOS__APPVERSION`). DBOS defaults
the version to a hash of the running executable, and recovery only resumes workflows stamped with
its own version — the right rule in production, where a changed binary may contain a changed
workflow. For this demo it would be a trap: rebuild between the crash and the restart and the old
build's `PENDING` workflows wait for a binary that no longer exists, which looks exactly like
broken recovery. Pinning makes every `cargo run` the same version.
