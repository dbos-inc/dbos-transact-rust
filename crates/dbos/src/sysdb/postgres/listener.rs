//! The LISTEN/NOTIFY half of the wakeup path: one connection, feeding the registry.
//!
//! **Nothing here is load-bearing.** Every wait in this crate re-queries on its own interval and is
//! correct with this module deleted — which is not a hypothetical, because CockroachDB has no
//! `LISTEN`/`NOTIFY` at all and is one of the two backends CI runs. What this buys is latency: a
//! `recv` that would have waited out its interval returns as soon as the writer's notification
//! lands.
//!
//! It buys one other thing, and that is what makes it worth its complexity: with a listener
//! delivering, the interval no longer has to be short, so the same waits go from a query a second
//! to a query a minute. See [`SHORT_INTERVAL`] and [`LONG_INTERVAL`].
//!
//! **The listener never parses a payload.** It prepends its channel's prefix and looks the whole
//! string up; see [`key_for`] for why splitting is not merely unnecessary but wrong.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;

use crate::sysdb::notify::{
    EVENTS_CHANNEL, NOTIFICATIONS_CHANNEL, Registry, STREAMS_CHANNEL, key_for,
};

/// What every wait uses with no listener delivering, and what a stream read always uses.
///
/// **It is a delivery latency, not a fallback**: with nothing pushing, this is how long a `recv`
/// takes to see a message another process has already committed. One second is Java's constant, and
/// Python's whenever no listener is running. TypeScript's 10s is not a counterexample — it is a
/// fallback beneath a listener that does the delivering, and taking that number without the
/// listener would be a tenfold regression against every other implementation's no-push behaviour.
///
/// **A stream read gets this whatever the listener is doing.** A stream reader waits on two things:
/// a value arriving, which is pushed, and the producer *terminating*, which nothing pushes in any
/// implementation. A minute's interval there would mean a minute to notice a finished workflow.
/// Python, Go and Java all draw the line in the same place.
const SHORT_INTERVAL: Duration = Duration::from_secs(1);

/// What `recv` and `get_event` use once a listener is proven to deliver.
///
/// A safety net rather than a delivery path: it catches only what the listener dropped — a
/// notification lost on the wire, or one sent while this process was reconnecting. Sixty seconds,
/// matching Python's `_notification_fallback_polling_interval`, Go's
/// `_NOTIFICATION_FALLBACK_RECHECK_INTERVAL` and Java's `NOTIFICATION_FALLBACK_INTERVAL`.
const LONG_INTERVAL: Duration = Duration::from_secs(60);

/// The payload the self-test sends itself.
///
/// On the notifications channel, so it needs no channel of its own — and it names no waiter, since
/// a registry key derived from it is `m::<this>` and nothing registers that. A stray one costs a
/// lookup that misses.
const SELF_TEST_PAYLOAD: &str = "dbos_listen_selftest";

/// How long the self-test waits for its own notification before giving up on the subscription.
const SELF_TEST_TIMEOUT: Duration = Duration::from_secs(3);

/// How long to wait before reconnecting after the listener errors.
///
/// A second, which is what Python, TypeScript and Java all sleep between attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// One connection, three channels, feeding one registry.
///
/// Also the answer to how long a wait may sleep, because that is a fact about the listener: it is
/// short with nothing pushing and long once something is.
pub(crate) struct Listener {
    pool: PgPool,
    registry: Arc<Registry>,
    /// Whether this listener is delivering, which is the one thing about it the waits read.
    ///
    /// **Set only once the listener has proved itself**, never merely because `LISTEN` returned
    /// without error; see [`run`](Self::run). Cleared when it stops, so a handle whose listener has
    /// died goes back to looking for itself every second rather than every minute.
    delivering: AtomicBool,
}

impl Listener {
    pub(crate) fn new(pool: PgPool, registry: Arc<Registry>) -> Self {
        Self {
            pool,
            registry,
            delivering: AtomicBool::new(false),
        }
    }

    /// Whether this listener is currently delivering.
    pub(crate) fn is_delivering(&self) -> bool {
        self.delivering.load(Ordering::Relaxed)
    }

    /// How long a wait may sleep before looking again, which is what the bit above is *for*.
    pub(crate) fn poll_interval(&self) -> Duration {
        if self.is_delivering() {
            LONG_INTERVAL
        } else {
            SHORT_INTERVAL
        }
    }

    fn set_delivering(&self, delivering: bool) {
        self.delivering.store(delivering, Ordering::Relaxed);
    }

    /// Listens until the pool closes, reconnecting through anything else.
    ///
    /// **The reconnect is the interesting part, not the receiving.** `NOTIFY` is not queued for
    /// absent listeners, so everything sent while this process had no connection is simply gone.
    /// Waking every waiter after each connect turns that hole into one extra look each — which is
    /// all a wakeup ever was — instead of a stall until each caller's own interval comes round, and
    /// with the long interval that stall can outlast the caller's timeout entirely. Go and Java
    /// both do this; Python and TypeScript do not, and rely on the fallback catching it.
    ///
    /// Waking on the *first* connect too, not only on reconnects: before it there was no listener
    /// either, so the same hole is there.
    pub(crate) async fn run(self: Arc<Self>) {
        loop {
            let listener = match self.connect().await {
                Ok(listener) => listener,
                // The pool closing is the shutdown path and the only way out of this loop.
                Err(e) if is_closed(&e) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "could not start the notification listener");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                }
            };
            if let Err(e) = self.receive(listener).await {
                if is_closed(&e) {
                    break;
                }
                tracing::warn!(error = %e, "the notification listener failed");
                self.set_delivering(false);
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
        }
        self.set_delivering(false);
        tracing::debug!("the notification listener stopped");
    }

    /// Subscribes, then proves the subscription actually delivers.
    ///
    /// **`LISTEN` returning without error does not mean a notification will ever arrive.** Through
    /// a transaction-mode pooler — PgBouncer with `pool_mode=transaction`, and anything like it —
    /// the statement succeeds and the subscription is dropped the moment the backend is handed to
    /// someone else. So this sends itself one notification and waits for it.
    ///
    /// TypeScript does the same test and only warns, which it can afford because its interval is a
    /// constant either way. Here the test is what makes the interval switch safe: believing a dead
    /// subscription would take every `recv` and `get_event` from a second to a minute, silently,
    /// which is worse than having no listener at all. So a failed self-test leaves the short
    /// interval in place and the listener running anyway — a notification that does arrive is still
    /// worth having.
    async fn connect(&self) -> Result<PgListener, sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener
            .listen_all([NOTIFICATIONS_CHANNEL, EVENTS_CHANNEL, STREAMS_CHANNEL])
            .await?;

        let delivers = self.self_test(&mut listener).await?;
        if !delivers {
            tracing::warn!(
                "LISTEN/NOTIFY self-test failed: no notification arrived within {SELF_TEST_TIMEOUT:?}. \
                 A transaction-mode connection pooler (PgBouncer with pool_mode=transaction, or \
                 similar) accepts LISTEN and then silently drops the subscription. Waits will keep \
                 re-querying every {SHORT_INTERVAL:?} rather than relying on notifications."
            );
        }
        self.set_delivering(delivers);

        // Everything sent while this process had no connection is gone; see the method doc.
        self.registry.wake_all();
        tracing::debug!(
            delivering = delivers,
            "the notification listener is connected"
        );
        Ok(listener)
    }

    /// Sends one notification to itself and reports whether it came back.
    ///
    /// Errors only on a failure that is the connection's, not the subscription's — a self-test that
    /// simply does not arrive is `Ok(false)`, because that is the answer it exists to give.
    async fn self_test(&self, listener: &mut PgListener) -> Result<bool, sqlx::Error> {
        // From the pool rather than the listening connection: a backend does not receive its own
        // `NOTIFY` any differently, but sending from elsewhere is what a real writer does, and it
        // is the path a pooler breaks.
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(NOTIFICATIONS_CHANNEL)
            .bind(SELF_TEST_PAYLOAD)
            .execute(&self.pool)
            .await?;

        // Any notification arriving on a subscribed channel proves the subscription delivers,
        // whether it is this test's or real traffic that got here first — so there is nothing to
        // wait past. Traffic that is not the self-test is passed on rather than swallowed.
        let Ok(received) = tokio::time::timeout(SELF_TEST_TIMEOUT, listener.try_recv()).await
        else {
            return Ok(false);
        };
        match received? {
            Some(notification) => {
                if notification.payload() != SELF_TEST_PAYLOAD {
                    self.deliver(notification.channel(), notification.payload());
                }
                Ok(true)
            }
            // Reconnected mid-test, so the subscription this was testing is gone. Reported as a
            // failure rather than retried here; the caller's loop reconnects and tests again.
            None => Ok(false),
        }
    }

    /// Receives until the connection is lost or the pool closes.
    async fn receive(&self, mut listener: PgListener) -> Result<(), sqlx::Error> {
        loop {
            match listener.try_recv().await? {
                Some(notification) => {
                    self.deliver(notification.channel(), notification.payload());
                }
                // `try_recv` reports a lost connection as `None`, having already reconnected and
                // re-subscribed. So this is the gap, and the wake is what covers it.
                None => {
                    tracing::debug!("the notification listener reconnected");
                    self.registry.wake_all();
                }
            }
        }
    }

    /// Wakes whoever is waiting on what this notification names.
    ///
    /// A notification for nobody is the ordinary case rather than a problem: one connection sees
    /// every process's traffic, and almost none of it is this one's callers'. Stream notifications
    /// are all of them today — nothing in this crate waits on a stream, because the loop that would
    /// is the engine's and does not exist yet.
    fn deliver(&self, channel: &str, payload: &str) {
        match key_for(channel, payload) {
            Some(key) => self.registry.wake(&key),
            None => tracing::warn!(channel, "notification on an unexpected channel"),
        }
    }
}

/// Whether an error means the pool has closed, which is this crate's shutdown.
///
/// `PgListener` surfaces it directly while waiting, and as a failure to acquire when reconnecting.
fn is_closed(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::PoolClosed)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Async only because a lazy pool still starts its reaper, which needs a runtime.
    #[tokio::test]
    async fn the_interval_lengthens_only_once_a_listener_delivers() {
        // Never connected to: this is about the bit, and a lazy pool opens nothing.
        let listener = Listener::new(
            PgPool::connect_lazy("postgres://unused").expect("lazy pool"),
            Arc::default(),
        );
        assert_eq!(
            listener.poll_interval(),
            SHORT_INTERVAL,
            "with nothing delivering, the re-query is the delivery",
        );

        listener.set_delivering(true);
        assert_eq!(listener.poll_interval(), LONG_INTERVAL);

        // A listener that stops takes the long interval with it, rather than leaving waits a
        // minute apart with nothing pushing.
        listener.set_delivering(false);
        assert_eq!(listener.poll_interval(), SHORT_INTERVAL);
    }

    /// The short interval is the one every reference uses with no push, and the long one is the
    /// fallback the three that switch agree on.
    #[test]
    fn the_intervals_are_the_cross_sdk_constants() {
        assert_eq!(SHORT_INTERVAL, Duration::from_secs(1));
        assert_eq!(LONG_INTERVAL, Duration::from_secs(60));
    }
}
