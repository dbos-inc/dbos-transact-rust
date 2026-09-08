//! The writer's half of the wakeup path: what this process wrote, told to everyone else.
//!
//! The notifications channel is fed by migration 1's trigger, so a `send` needs nothing from this
//! module. Events and streams have no trigger — migrations 43 and 44 remove them — and **this is
//! what feeds their channels instead.** A trigger fires inside the writing transaction, so its
//! `NOTIFY` takes the async-notify queue lock before the commit and serialises every notifying
//! commit in the database against every other. Pushing from the application moves that lock off
//! the write path, and lets a batch of writes cost one notifying transaction instead of one each.
//!
//! **Nothing here is load-bearing**, exactly as in [`listener`](super::listener): every wait
//! re-queries on its own interval, so with this module deleted the same values are delivered, just
//! later. What it buys is that a reader in another process hears about a value in milliseconds
//! rather than waiting out an interval — and a reader in *this* process hears with no round trip at
//! all, since [`signal`](Notifier::signal) wakes the local registry directly.
//!
//! All four references do this, and agree on the shape: a per-channel set of payloads, a loop that
//! flushes it on a ~10ms cadence, one `pg_notify` statement per channel per flush, and a failed
//! batch dropped rather than retried.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::PgPool;

use crate::sysdb::notify::{Registry, key_for};

/// How long a payload waits for company before it is pushed.
///
/// **The whole point is that it is not zero.** A push per write would be one notifying transaction
/// per write, which is exactly the cost a database trigger has; ten milliseconds
/// of latency turns a burst of writes into one statement, and bounds the rate of notifying commits
/// however fast the application writes. Ten is Go's `DefaultNotificationCoalesceInterval`,
/// Python's `notification_coalesce_sec`, TypeScript's `DEFAULT_NOTIFICATION_COALESCE_MS` and
/// Java's flush period.
///
/// The default rather than the value:
/// [`Settings::notification_coalesce`](super::Settings::notification_coalesce) overrides it, as the
/// same setting does in all four.
pub(crate) const COALESCE_INTERVAL: Duration = Duration::from_millis(10);

/// One statement per channel per flush, however many payloads the batch holds.
///
/// `unnest` is what makes that possible: the SQL text is the same for one payload and a thousand,
/// so the server can reuse the plan, and one round trip takes the async-notify queue lock once.
/// This is the one statement all four references share character for character.
const PUSH_BATCH: &str = "SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p";

/// What this process has written and not yet told anyone else about.
///
/// One set per channel, so a batch that cannot be sent takes only its own channel down with it —
/// and a set rather than a list, because repeated writes to one key between flushes are one thing
/// to look at, not one each. Python, TypeScript, Go and Java all collapse duplicates the same way.
type Pending = HashMap<&'static str, HashSet<String>>;

/// The outbound half of the wakeup path.
pub(crate) struct Notifier {
    pool: PgPool,
    registry: std::sync::Arc<Registry>,
    /// The coalescing window, from [`Settings`](super::Settings) or [`COALESCE_INTERVAL`].
    interval: Duration,
    /// Whether payloads are queued for other processes at all.
    ///
    /// Off until [`enable`](Self::enable), and never on where there is nothing to push to:
    /// CockroachDB has no `LISTEN`/`NOTIFY`, and a handle configured without it has no channels.
    /// The local wake is not gated on this — it costs no database and is right on every backend.
    ///
    /// **Off must mean "do not queue", not merely "do not send".** Nothing drains a queue no
    /// flush loop is reading, so accumulating there would be an unbounded leak on exactly the
    /// backend that can do nothing with it.
    pushing: AtomicBool,
    pending: Mutex<Pending>,
    /// Wakes the flush loop: for the payload that opens a batch, and for a stop.
    ///
    /// One signal for both because the loop's answer to either is to look at what it has. What
    /// distinguishes them is [`stopping`](Self::stopping), which it checks on waking.
    woken: tokio::sync::Notify,
    /// Whether the flush loop should make its last flush and return.
    stopping: AtomicBool,
}

impl Notifier {
    pub(crate) fn new(
        pool: PgPool,
        registry: std::sync::Arc<Registry>,
        interval: Option<Duration>,
    ) -> Self {
        Self {
            pool,
            registry,
            interval: interval.unwrap_or(COALESCE_INTERVAL),
            pushing: AtomicBool::new(false),
            pending: Mutex::default(),
            woken: tokio::sync::Notify::new(),
            stopping: AtomicBool::new(false),
        }
    }

    /// Starts queueing payloads for the other processes listening on the channels.
    ///
    /// Separate from spawning [`run`](Self::run) only because the two have different owners — the
    /// caller does both at once, and one without the other is either a leak or a silence.
    pub(crate) fn enable(&self) {
        self.pushing.store(true, Ordering::Relaxed);
    }

    /// Whether payloads are being queued for other processes.
    pub(crate) fn is_pushing(&self) -> bool {
        self.pushing.load(Ordering::Relaxed)
    }

    /// Reports a row this process has just written.
    ///
    /// **Call it after the transaction commits**, never before: a waiter woken by this looks at the
    /// database, and one that looks before the row is visible finds nothing and goes back to sleep
    /// until its own interval comes round — which is the stall the wakeup existed to prevent. Go
    /// and Java both say the same thing on their equivalents.
    ///
    /// Waiters here are woken immediately and with no round trip, whether or not anything is being
    /// pushed. That is Go's behaviour and not Python's or TypeScript's, whose local waiters hear
    /// their own process's writes back off the wire; it costs nothing, and it is what makes a
    /// same-process write visible promptly on CockroachDB, where there is no wire.
    pub(crate) fn signal(&self, channel: &'static str, workflow_id: &str, key: &str) {
        // The wire form, exactly as migration 1's trigger builds it and as every other SDK sends
        // it: `id::key`, with neither half escaped. See [`crate::sysdb::notify`] on why nothing
        // splits it.
        let payload = format!("{workflow_id}::{key}");
        let Some(registry_key) = key_for(channel, &payload) else {
            // Unreachable: the callers pass channel constants. A channel nobody listens on has no
            // waiter to wake and nobody to push to, so there is nothing to do but leave.
            tracing::warn!(channel, "signalled on an unexpected channel");
            return;
        };
        self.registry.wake(&registry_key);

        if !self.is_pushing() {
            return;
        }
        let first = {
            let mut pending = self.pending.lock().expect("notifier lock");
            let batch = pending.entry(channel).or_default();
            batch.insert(payload);
            pending.values().map(HashSet::len).sum::<usize>() == 1
        };
        // **Only the payload that opens a batch wakes the loop**, and that is what makes the
        // window a window: the rest ride along on the flush it already scheduled, rather than each
        // cutting it short.
        if first {
            self.woken.notify_one();
        }
    }

    /// Flushes what has been signalled, until [`stop`](Self::stop), then once more.
    ///
    /// **Idle costs nothing.** The references all tick — Go a 10ms `Ticker`, Python a `sleep` loop,
    /// Java a fixed-delay schedule — which wakes a hundred times a second in a process that may
    /// never write an event at all. Waiting for the first payload and *then* opening the window
    /// coalesces exactly the same writes, since the window is measured from the first signal
    /// either way.
    pub(crate) async fn run(self: std::sync::Arc<Self>) {
        while !self.stopping.load(Ordering::Relaxed) {
            // Idle until a payload opens a batch, or the handle closes. One `Notify` carries both
            // because a single wait cannot take two sources without `tokio::select!`, and this
            // crate's tokio is `["time", "sync"]` — no `macros`. So the wake says only "look", and
            // the flag below says which it was.
            self.woken.notified().await;
            if self.stopping.load(Ordering::Relaxed) {
                break;
            }
            // The coalescing window: everything signalled during it goes out with the payload that
            // opened it. A `timeout` around the same wait rather than a plain `sleep` only so that
            // a stop can cut it short — with a long window configured, a close would otherwise sit
            // here waiting it out. Nothing else ends it early: `signal` notifies for the payload
            // that *opens* a batch, and this one is already open.
            let _ = tokio::time::timeout(self.interval, self.woken.notified()).await;
            self.flush().await;
        }
        // Whatever the window was still holding, so a value written just before a shutdown wakes
        // readers elsewhere rather than leaving them to their interval. Every reference makes the
        // same last flush.
        self.flush().await;
        tracing::debug!("the notifier stopped");
    }

    /// Asks [`run`](Self::run) to make its final flush and return.
    ///
    /// **The final flush is a database write**, so a caller closing a handle has to stop the
    /// notifier *before* closing the pool, not after — the opposite order from the listener, which
    /// is ended by that close.
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        self.woken.notify_one();
    }

    /// Emits one notifying transaction per channel for everything queued since the last flush.
    async fn flush(&self) {
        let batches: Vec<(&'static str, Vec<String>)> = {
            let mut pending = self.pending.lock().expect("notifier lock");
            pending
                .drain()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(channel, batch)| (channel, batch.into_iter().collect()))
                .collect()
        };

        for (channel, payloads) in batches {
            let sent = sqlx::query(PUSH_BATCH)
                .bind(channel)
                .bind(&payloads)
                .execute(&self.pool)
                .await;
            if let Err(e) = sent {
                // **Dropped, never requeued**, which all four references are explicit about: a
                // payload the database will not take — one over `pg_notify`'s 8000-byte limit, say
                // — would otherwise be retried forever and stall every later batch behind it. What
                // is lost is an interval of latency for whoever was waiting, not the value.
                //
                // Not retried through [`with_retry`](crate::sysdb::retry::with_retry) either, for
                // the same reason: it has no attempt limit, so a channel that cannot be pushed
                // would hold the loop rather than the queue. Python, TypeScript and Java do not
                // retry; Go does, bounded.
                tracing::warn!(
                    channel,
                    count = payloads.len(),
                    error = %e,
                    "could not push notifications; readers fall back to re-querying",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::sysdb::notify::{EVENTS_CHANNEL, STREAMS_CHANNEL, event_key};

    /// Never connected to: these are about what is queued, and a lazy pool opens nothing.
    fn notifier() -> Arc<Notifier> {
        Arc::new(Notifier::new(
            PgPool::connect_lazy("postgres://unused").expect("lazy pool"),
            Arc::default(),
            None,
        ))
    }

    fn drained(notifier: &Notifier) -> Vec<(&'static str, Vec<String>)> {
        let mut pending = notifier.pending.lock().expect("notifier lock");
        let mut batches: Vec<_> = pending
            .drain()
            .map(|(channel, batch)| {
                let mut payloads: Vec<String> = batch.into_iter().collect();
                payloads.sort();
                (channel, payloads)
            })
            .collect();
        batches.sort();
        batches
    }

    /// Repeated writes to one key cost one wakeup, not one each.
    ///
    /// This is the coalescing the interval exists for, and the half of it that does not need a
    /// clock: a producer writing a stream in a tight loop wakes its readers once per flush, and
    /// each of them then reads everything that landed.
    #[tokio::test]
    async fn a_key_written_repeatedly_is_pushed_once() {
        let notifier = notifier();
        notifier.enable();

        for _ in 0..5 {
            notifier.signal(STREAMS_CHANNEL, "wf", "progress");
        }

        assert_eq!(
            drained(&notifier),
            vec![(STREAMS_CHANNEL, vec!["wf::progress".to_owned()])],
        );
    }

    /// Each channel batches on its own, so one unsendable payload costs only its own channel.
    #[tokio::test]
    async fn the_two_channels_batch_separately() {
        let notifier = notifier();
        notifier.enable();

        notifier.signal(EVENTS_CHANNEL, "wf", "ready");
        notifier.signal(STREAMS_CHANNEL, "wf", "progress");
        notifier.signal(EVENTS_CHANNEL, "wf", "result");

        assert_eq!(
            drained(&notifier),
            vec![
                (STREAMS_CHANNEL, vec!["wf::progress".to_owned()]),
                (
                    EVENTS_CHANNEL,
                    vec!["wf::ready".to_owned(), "wf::result".to_owned()],
                ),
            ],
        );
    }

    /// With nothing to push to, nothing accumulates — but the local wake still happens.
    ///
    /// The queue matters because nothing drains it while the flush loop is not running: a handle
    /// on CockroachDB that queued anyway would grow one entry per write for the life of the
    /// process. The wake matters because it is free and correct there: it is the *only* thing that
    /// can shorten a local waiter's interval on a backend with no `LISTEN`/`NOTIFY` at all.
    #[tokio::test]
    async fn without_the_push_a_signal_still_wakes_a_local_waiter() {
        let notifier = notifier();
        let mut subscription = notifier.registry.subscribe(event_key("wf", "ready"));

        notifier.signal(EVENTS_CHANNEL, "wf", "ready");

        assert!(drained(&notifier).is_empty(), "nothing drains this queue");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), subscription.notified())
                .await
                .is_ok(),
            "the waiter was not woken",
        );
    }

    /// The payload that goes on the wire is the key a waiter elsewhere is holding.
    ///
    /// Both are built here from one pair, which is what keeps them agreeing: a listener in another
    /// process prepends its channel's prefix to what arrives, and has to land on the string a
    /// waiter there registered. This is the round trip that test in `notify` asserts, closed from
    /// the writing end.
    #[tokio::test]
    async fn the_pushed_payload_is_the_key_a_waiter_holds() {
        let notifier = notifier();
        notifier.enable();

        notifier.signal(EVENTS_CHANNEL, "wf", "ready");

        let [(_, ref payloads)] = drained(&notifier)[..] else {
            panic!("expected one channel's batch");
        };
        assert_eq!(
            key_for(EVENTS_CHANNEL, &payloads[0]).as_deref(),
            Some(event_key("wf", "ready").as_str()),
        );
    }
}
