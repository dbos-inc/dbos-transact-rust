<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/rust/programming-guide) &nbsp;&nbsp;•&nbsp;&nbsp; [API reference](https://docs.rs/dbos) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

> [!WARNING]
> **Pre-production release.** DBOS Transact for Rust 0.6 is a pre-production release, and isn't
> yet supported for production use. Its API may still change in breaking ways between releases.

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

## Writing workflows

A workflow body must be deterministic: a recovery runs it again and replays what each durable call
recorded, so it has to reach the same calls in the same order. Non-deterministic values belong in
steps, and plain races, timeouts, spawns and completion-order streams over durable calls aren't
supported in a workflow body. The crate docs list [what isn't supported and what to use
instead](https://docs.rs/dbos/latest/dbos/#determinism-in-workflows), on every runtime.

## Try the demo apps

The [demo apps](https://github.com/dbos-inc/dbos-demo-apps/tree/main/rust) are in their own repository:

- The [starter](https://github.com/dbos-inc/dbos-demo-apps/tree/main/rust/dbos-starter) is durable execution in one page: launch a workflow, crash the app mid-run, restart it, and execution resumes at the step after the last one that finished.
- The [widget store](https://github.com/dbos-inc/dbos-demo-apps/tree/main/rust/widget-store) is an online storefront that survives being killed mid-checkout.
- [Queue patterns](https://github.com/dbos-inc/dbos-demo-apps/tree/main/rust/queue-patterns) shows fair queueing, rate limits and debouncing.

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./DEVELOPING.md).






