# DBOS Rust Widget Store

An online storefront that is resilient to any failure. Buy a widget and watch the order go out;
press the crash button at any point — including halfway through a dispatch — and start the app
again. The order resumes from the last step that finished, and the inventory count is still right.

No application code takes part in that recovery. `launch()` picks up whatever the previous run
abandoned, and each step replays from its checkpoint instead of running a second time.

This is the Rust port of the widget store that already exists in Python, TypeScript, Go and Java.
Same application schema, same HTTP surface, same frontend.

## Running it

You need a PostgreSQL server. With Docker:

```bash
docker run -d --name widget-store-pg -e POSTGRES_PASSWORD=dbos -p 5432:5432 postgres:17
export DBOS_DATABASE_URL="postgres://postgres:dbos@localhost:5432/dbos_rust_widget_store"
```

Then:

```bash
cargo run -p dbos-rust-widget-store
```

The app serves at http://localhost:8080. It creates the database, the DBOS system tables, and its
own `products` and `orders` tables on first run — there is nothing to migrate by hand.

Without `DBOS_DATABASE_URL` it falls back to `postgres://localhost:5432/dbos_rust_widget_store`,
letting the standard libpq variables (`PGUSER`, `PGPASSWORD`) fill in the rest.

## What to try

1. **Buy a widget.** Inventory drops, the order goes to PAID, and the progress bar walks it out
   over ten seconds.
2. **Crash it mid-dispatch.** Press the crash button while the bar is moving. Restart the app and
   watch the same order carry on from where it stopped, then reach DISPATCHED.
3. **Decline a payment.** The widget goes back on the shelf and the order is CANCELLED — the
   compensation the reservation was taken out against.
4. **Double-click Buy.** One order, not two: the browser's idempotency key is the workflow id, so
   the second request joins the checkout already running.

## What is worth reading

[`src/workflows.rs`](src/workflows.rs) is the demo. Two workflows:

- **`checkout`** reserves a widget, then *stops* and waits to be told whether the card was charged.
  That wait outlives the process, because the deadline is a row rather than a timer in memory.
  Either answer leaves the books balanced: a payment that fails puts the widget back, and so does a
  payment that never comes.
- **`dispatch_order`** is a child workflow, started and not awaited, so the buyer is not kept
  waiting for ten seconds of delivery. Being a child rather than a spawned task is what makes it
  survive a restart.

[`src/store.rs`](src/store.rs) holds the plain SQL, and [`src/main.rs`](src/main.rs) the HTTP
surface.

## A note on transactional steps

Rust does not have transactional steps yet. Every database write here runs inside an ordinary
`dbos::step`, so the step's checkpoint and the row it writes are **two separate commits** rather
than one. A crash in the window between them replays the step, so each write is either safe to
repeat or is one the workflow accounts for — see the note on `update_order_progress` in
`src/store.rs`. The other four widget stores use a transactional step for these, and this is the
one place the ports differ.
