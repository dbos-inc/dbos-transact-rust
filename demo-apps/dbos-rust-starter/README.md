# DBOS Rust Starter

The Workflows and Queues tabs of the DBOS starter app.

**Workflows** is a three-step workflow that checkpoints each step to Postgres, a progress display,
and a crash button. Launch a workflow, crash the app, restart it — execution resumes at the step
after the last one that finished, with no application code involved.

**Queues** is a fan-out under a concurrency limit. Enqueue five workflows against a queue that
allows three at a time, and watch three run while two wait.

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

Then open the Queues tab and press "Enqueue 5 workflows". Three go `PENDING` and two stay
`ENQUEUED`, because `demo-queue` is registered with `worker_concurrency: 3`. The part worth
pressing is **Apply**: change the number and the running app honours it within a poll — no
restart, no redeploy. A queue's configuration is a row that every worker re-reads on every pass,
so the new limit reaches the whole fleet rather than just the process you clicked in.

Nothing is dequeued by the process that enqueued it just because it asked. Each workflow is
recorded `ENQUEUED` and claimed by whichever executor next polls the queue, which here happens to
be the same one — run a second copy of the app against the same database and they will share the
backlog.

The server listens on loopback only, since the crash button exits the process. Ctrl-C is the
other way out and takes the tidy path: it stops serving and calls `shutdown()`, which leaves any
workflow still running `PENDING` for the next launch to recover — the same end as the crash
button, reached deliberately.

## The application version

The app pins its application version to `0.1.0` (override with `DBOS__APPVERSION`). DBOS defaults
the version to a hash of the running executable, and recovery only resumes workflows stamped with
its own version — the right rule in production, where a changed binary may contain a changed
workflow. For this demo it would be a trap: rebuild between the crash and the restart and the old
build's `PENDING` workflows wait for a binary that no longer exists, which looks exactly like
broken recovery. Pinning makes every `cargo run` the same version.
