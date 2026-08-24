<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Essentially, it helps you write long-lived, reliable code that can survive crashes, restarts, and failures without losing state or duplicating work.

As your workflows run, DBOS checkpoints each step they take in a Postgres database.
When a process stops (fails, intentionally suspends, or a machine dies), your program can recover from those checkpoints to restore its exact state and continue from where it left off, as if nothing happened.

In practice, this makes it easier to build reliable systems for use cases like AI agents, data synchronization, payments, or anything that takes minutes, days, or weeks to complete.
Rather than bolting on ad-hoc retry logic and database checkpoints, DBOS workflows give you one consistent model for ensuring your programs can recover from any failure from exactly where they left off.

This library contains all you need to add durable workflows to your program: there's no separate service or orchestrator or any external dependencies except Postgres.
Because it's just a library, you can incrementally add it to your projects.
And because it's built on Postgres, it natively supports all the tooling you're familiar with (backups, GUIs, CLI tools) and works with any Postgres provider.

## Try the starter app

The [starter app](./demo-apps/dbos-rust-starter) is durable execution in one page: a three-step
workflow that checkpoints each step to Postgres, a live progress display, and a crash button.
Launch a workflow, crash the app mid-run, restart it — execution resumes at the step after the
last one that finished.

With a Postgres listening on `localhost:5432`:

```bash
cargo run -p dbos-rust-starter
```

Then open <http://localhost:8080>. The app creates its `dbos_rust_starter` database if it does not
exist, and the standard libpq variables (`PGUSER`, `PGPASSWORD`) apply; point it at another server
with `DBOS_DATABASE_URL`. See the [starter's README](./demo-apps/dbos-rust-starter/README.md) for
details.

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./DEVELOPING.md).






