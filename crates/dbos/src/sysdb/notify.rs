//! Who is waiting for what, so a write can wake them.
//!
//! `recv`, `get_event` and `read_stream_value` all wait for a row another process may not have
//! written yet. Each waits by looping — look at the database, wait a bounded interval, look again —
//! and **that loop is what delivers.** Every reference works this way: a notification is only ever
//! a hint to look again, never the value, and nobody acts on its payload. On CockroachDB, which has
//! no `LISTEN`/`NOTIFY` at all, the loop is the whole mechanism in all four SDKs.
//!
//! So this registry has exactly one job: let something that already knows a row was written cut a
//! waiter's interval short. With nothing pushing, it is simply never woken and every waiter runs at
//! its own interval, which is correct — just slower. Nothing in here polls, and nothing in here
//! touches the database.
//!
//! **Subscribe before looking.** A waiter that looks first and subscribes second misses anything
//! written in between, which is why [`Registry::subscribe`] is synchronous and infallible: there is
//! no await between deciding to wait and being able to be woken.
//!
//! **Keys are flat and prefixed**, following Java's `SignalKey` — `m::`, `e::`, `s::` over one map.
//! The prefix is load-bearing rather than decorative: an event and a stream on the same workflow
//! and key are different things to wait for, and without it a `set_event` would wake a stream
//! reader.
//!
//! **Nothing here ever splits a key, and nothing should.** A wire payload is `id::key`, and both
//! halves are caller-supplied strings that may themselves contain `::` — so splitting is ambiguous
//! and a parser has to guess. Java takes the same position and is the reference to follow: its
//! listener does `raiseSignal("m::" + payload)` and its map does an exact-key lookup, so a key
//! built by a waiter and one built from a notification are string-identical without anyone parsing.
//! TypeScript and Python do not parse either — Python keeps the `(dest_uuid, topic)` pair beside
//! the payload precisely so it never has to. Go alone parses, in its polling fallback
//! (`SplitN(payload, "::", 2)`), which is a thing this crate has no equivalent of: the poller was
//! the only component that would ever have needed the halves back, and there is no poller.
//!
//! So the listener builds the key by **concatenating** the prefix onto the payload exactly as it
//! arrived, and never splits it.
//!
//! **Prefixing does not make keys injective, and that is fine.** The pairs `("a", "b::c")` and
//! `("a::b", "c")` both render `m::a::b::c`, so a wake for one wakes the other. A wakeup is only
//! ever a hint: the woken waiter looks at the database, finds nothing for itself, and waits again
//! — one wasted query, no wrong answer. Escaping the separator would fix a non-problem and,
//! worse, would disagree with the `id || '::' || key` that migration 1's trigger and every other
//! SDK put on the wire.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::NULL_TOPIC;

/// The channel a message notification arrives on, published by migration 1's trigger.
pub(crate) const NOTIFICATIONS_CHANNEL: &str = "dbos_notifications_channel";
/// The channel an event notification arrives on. No trigger feeds it (migration 44 removes the
/// one there was); the writer pushes instead.
pub(crate) const EVENTS_CHANNEL: &str = "dbos_workflow_events_channel";
/// The channel a stream notification arrives on. Fed by the writer, like events: migration 43
/// removes its trigger.
pub(crate) const STREAMS_CHANNEL: &str = "dbos_streams_channel";

/// The prefixes, as constants rather than literals in three `format!`s.
///
/// A waiter's key and a notification's key are built by different code and have to come out
/// string-identical; sharing the prefix is what stops them drifting apart silently. Java's
/// `SignalKey` centralises the same three.
const MESSAGE_PREFIX: &str = "m";
const EVENT_PREFIX: &str = "e";
const STREAM_PREFIX: &str = "s";

/// The key a `recv` waits on.
///
/// The topic is resolved to [`NULL_TOPIC`] here rather than by the caller, because it has to match
/// what a notification carries, and the stored row never holds `NULL` — nothing equals `NULL`, so a
/// receiver selecting on `topic = $1` would never find its own message.
pub(crate) fn message_key(destination_id: &str, topic: Option<&str>) -> String {
    format!(
        "{MESSAGE_PREFIX}::{destination_id}::{}",
        topic.unwrap_or(NULL_TOPIC)
    )
}

/// The key a `get_event` waits on.
pub(crate) fn event_key(workflow_id: &str, key: &str) -> String {
    format!("{EVENT_PREFIX}::{workflow_id}::{key}")
}

/// The key a `read_stream_value`'s loop waits on.
///
/// **The one key here with no caller**, and deliberately so: `read_stream_value` reads a single
/// offset and returns, so the subscription belongs to the loop above it — the engine's
/// `read_stream`, which does not exist yet. That loop stays there rather than moving behind the
/// trait, so this waits for an engine rather than for a change of mind. The listener already
/// derives this key from the streams channel and wakes nobody with it, which is correct until then.
#[allow(dead_code)]
pub(crate) fn stream_key(workflow_id: &str, key: &str) -> String {
    format!("{STREAM_PREFIX}::{workflow_id}::{key}")
}

/// The key a notification names, or `None` for a channel this does not listen on.
///
/// **The concatenation the module header is about.** The payload is a wire `id::key` whose halves
/// may each contain `::`, so it is prepended to whole and never split — which is what makes this
/// agree with the three functions above for every input, including the ones a parser would get
/// wrong. Java does exactly this (`SignalKey.signalFor(channel, payload)`); Go is the only
/// implementation that splits, and only in a polling fallback this crate has no equivalent of.
pub(crate) fn key_for(channel: &str, payload: &str) -> Option<String> {
    let prefix = match channel {
        NOTIFICATIONS_CHANNEL => MESSAGE_PREFIX,
        EVENTS_CHANNEL => EVENT_PREFIX,
        STREAMS_CHANNEL => STREAM_PREFIX,
        _ => return None,
    };
    Some(format!("{prefix}::{payload}"))
}

/// Who is waiting for what.
///
/// One flat map rather than one per kind of wait: the keys are already disjoint by prefix, and a
/// wake only ever needs to match one string.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    waiters: Mutex<HashMap<String, tokio::sync::broadcast::Sender<()>>>,
}

impl Registry {
    /// Registers interest in `key`, returning the handle that is woken.
    ///
    /// Synchronous and infallible on purpose: a caller subscribes, *then* looks at the database,
    /// and anything landing in between wakes it. An `async` registration would put an await in
    /// exactly the gap the ordering exists to close.
    ///
    /// Several callers may wait on one key — two readers of one stream, or two waiters on one event
    /// — and each is woken. They share a single registration.
    pub(crate) fn subscribe(self: &Arc<Self>, key: String) -> Subscription {
        let mut waiters = self.waiters.lock().expect("registry lock");
        let sender = waiters.entry(key.clone()).or_insert_with(|| {
            // One slot. The wakeup carries no payload, so a second before the first is read says
            // nothing new, and a receiver further behind than that is told it lagged — which it
            // treats as a wakeup, because "look again" is all a wakeup ever meant.
            tokio::sync::broadcast::Sender::new(1)
        });
        let receiver = sender.subscribe();
        Subscription {
            key,
            registry: Arc::clone(self),
            receiver,
        }
    }

    /// Registers *sole* interest in `key`, or `None` if something is already waiting on it.
    ///
    /// For `recv`, where two waiters are a bug rather than a pattern: one message can only go to
    /// one of them, so the loser waits out its timeout and reports that nothing arrived — which is
    /// indistinguishable, to the workflow that wrote it, from nothing having been sent. Python and
    /// Go both reject the second receiver for this reason; TypeScript and Java allow it and let the
    /// consuming `UPDATE` decide.
    ///
    /// **This is a per-process guard and cannot be more than that.** Two receivers in different
    /// processes — the recovery double-run case — never meet here, and are arbitrated at the
    /// database by the `consumed = FALSE` predicate. Rejecting here is worth having anyway: it
    /// turns the case a single process *can* see into an error the caller can act on.
    ///
    /// A key built from two different pairs (see the non-injectivity note above) is rejected as a
    /// duplicate although it is not one. Python and Go concatenate the same way and share the
    /// property exactly; it errs towards refusing a legitimate receiver rather than admitting two,
    /// which is the direction to err in.
    pub(crate) fn subscribe_exclusive(self: &Arc<Self>, key: String) -> Option<Subscription> {
        let mut waiters = self.waiters.lock().expect("registry lock");
        if waiters.contains_key(&key) {
            return None;
        }
        let sender = tokio::sync::broadcast::Sender::new(1);
        let receiver = sender.subscribe();
        waiters.insert(key.clone(), sender);
        Some(Subscription {
            key,
            registry: Arc::clone(self),
            receiver,
        })
    }

    /// Wakes everything waiting on `key`.
    ///
    /// A wake for nobody is ordinary rather than an error: a listener sees every process's
    /// notifications, and almost none of them are this one's callers'.
    pub(crate) fn wake(&self, key: &str) {
        let waiters = self.waiters.lock().expect("registry lock");
        if let Some(sender) = waiters.get(key) {
            // Fails only with no receivers, which the drop guard makes momentary rather than
            // lasting.
            let _ = sender.send(());
            tracing::trace!(key, "woke a waiter");
        }
    }

    /// Wakes everything registered, whatever it is waiting for.
    ///
    /// For the one case where a wakeup source knows it has *missed* wakeups but not which: a
    /// listener whose connection dropped and came back saw nothing in between, and the database
    /// does not replay it. Waking everybody turns that gap into one extra look per waiter — which
    /// is what a wakeup always was — instead of a stall until each caller's own interval comes
    /// round.
    pub(crate) fn wake_all(&self) {
        let waiters = self.waiters.lock().expect("registry lock");
        for sender in waiters.values() {
            let _ = sender.send(());
        }
        if !waiters.is_empty() {
            tracing::debug!(
                waiters = waiters.len(),
                "woke every waiter after a gap in notifications"
            );
        }
    }

    /// How many keys are registered. Tests only; nothing branches on this.
    #[cfg(test)]
    fn registered(&self) -> usize {
        self.waiters.lock().expect("registry lock").len()
    }
}

/// A registration, held for as long as a caller is waiting.
///
/// Dropping it deregisters, so the registry never carries state for a caller that has gone and
/// there is no unregister call to forget.
#[derive(Debug)]
pub(crate) struct Subscription {
    key: String,
    registry: Arc<Registry>,
    receiver: tokio::sync::broadcast::Receiver<()>,
}

impl Subscription {
    /// Waits until something is written on this key.
    ///
    /// **No timeout of its own**, deliberately: the caller owns the deadline and re-checks the
    /// database on every return, because a wakeup is a hint that something changed and never the
    /// change itself.
    pub(crate) async fn notified(&mut self) {
        // `Lagged` means wakeups were dropped, which is itself a wakeup. `Closed` cannot happen
        // while this subscription holds a receiver, since the drop guard only removes a key whose
        // last receiver is going away.
        let _ = self.receiver.recv().await;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut waiters = self.registry.waiters.lock().expect("registry lock");
        // `receiver` is still counted: struct fields drop after this body, so the last subscription
        // on a key sees exactly one receiver.
        if waiters
            .get(&self.key)
            .is_some_and(|sender| sender.receiver_count() <= 1)
        {
            waiters.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The prefix is what keeps one workflow's event from waking its stream reader.
    #[test]
    fn a_key_is_prefixed_by_what_is_being_waited_for() {
        assert_eq!(event_key("wf-1", "k"), "e::wf-1::k");
        assert_eq!(stream_key("wf-1", "k"), "s::wf-1::k");
        assert_ne!(event_key("wf-1", "k"), stream_key("wf-1", "k"));
    }

    /// An absent topic is the sentinel every SDK writes, not an empty string and not a missing
    /// half.
    #[test]
    fn a_message_key_resolves_an_absent_topic_to_the_sentinel() {
        assert_eq!(message_key("wf-1", Some("orders")), "m::wf-1::orders");
        assert_eq!(message_key("wf-1", None), format!("m::wf-1::{NULL_TOPIC}"));
    }

    /// A waiter's key and a notification's key agree without anyone parsing.
    ///
    /// The wire payload is `id::key`, and both halves may contain `::` themselves, so splitting one
    /// is guesswork. Nothing needs to: a listener prepends its channel's prefix to the payload
    /// exactly as it arrived, and gets the string the waiter registered. This is the test that
    /// fails if someone later reaches for a split — and it uses ids and keys that would defeat one.
    #[test]
    fn a_listener_reaches_a_waiter_by_concatenation_never_by_parsing() {
        // What migration 1's trigger puts on the wire, and what every SDK's writers publish.
        let wire = |id: &str, key: &str| format!("{id}::{key}");

        for (id, key) in [
            ("wf-1", "progress"),
            ("wf::with::colons", "k"),
            ("wf-1", "key::with::colons"),
            ("::leading", "trailing::"),
        ] {
            let payload = wire(id, key);
            assert_eq!(event_key(id, key), format!("e::{payload}"));
            assert_eq!(stream_key(id, key), format!("s::{payload}"));
            assert_eq!(
                message_key(id, Some(key)),
                format!("m::{payload}"),
                "a listener must reach the waiter by prefixing the payload verbatim"
            );

            // The round trip, which is the property the whole rule exists for: what a waiter
            // registered and what the listener derives from the wire are the same string. These
            // ids and keys are chosen to defeat a parser, so anything that splits fails here.
            assert_eq!(
                key_for(NOTIFICATIONS_CHANNEL, &payload).as_deref(),
                Some(message_key(id, Some(key)).as_str())
            );
            assert_eq!(
                key_for(EVENTS_CHANNEL, &payload).as_deref(),
                Some(event_key(id, key).as_str())
            );
            assert_eq!(
                key_for(STREAMS_CHANNEL, &payload).as_deref(),
                Some(stream_key(id, key).as_str())
            );
        }

        // The no-topic case goes through the same rule, the sentinel standing in for the topic.
        let sentinel_payload = wire("wf-1", NULL_TOPIC);
        assert_eq!(message_key("wf-1", None), format!("m::{sentinel_payload}"));
        assert_eq!(
            key_for(NOTIFICATIONS_CHANNEL, &sentinel_payload).as_deref(),
            Some(message_key("wf-1", None).as_str()),
            "a message with no topic is reached the same way as any other",
        );
    }

    /// A channel this does not listen on is `None` rather than a guess.
    ///
    /// Nothing else publishes on a connection this listens on today, so this is about what happens
    /// when something does: a notification with no home is dropped, not turned into a key that
    /// might collide with a real one.
    #[test]
    fn an_unknown_channel_names_no_key() {
        assert_eq!(key_for("some_other_channel", "wf-1::k"), None);
        assert_eq!(key_for("", "wf-1::k"), None);
    }

    /// Two different pairs can render one key, and the registry is allowed to conflate them.
    ///
    /// A wakeup is a hint, so the cost is a wasted look rather than a wrong answer. Escaping the
    /// separator would fix nothing and would disagree with the wire format every other SDK writes.
    #[tokio::test]
    async fn colliding_keys_only_cost_a_spurious_wakeup() {
        let registry = Arc::new(Registry::default());
        assert_eq!(
            message_key("a", Some("b::c")),
            message_key("a::b", Some("c"))
        );

        let mut waiter = registry.subscribe(message_key("a", Some("b::c")));
        // Woken by the other pair's write. The waiter re-checks the database and finds nothing for
        // itself, which is what a hint-only wakeup licenses.
        registry.wake(&message_key("a::b", Some("c")));
        tokio::time::timeout(Duration::from_secs(5), waiter.notified())
            .await
            .expect("a colliding key should still wake, harmlessly");
    }

    /// One receiver per (workflow, topic), and the registration is free again once it goes.
    #[tokio::test]
    async fn a_second_exclusive_waiter_is_refused_until_the_first_is_gone() {
        let registry = Arc::new(Registry::default());
        let key = message_key("wf-1", Some("orders"));

        let first = registry
            .subscribe_exclusive(key.clone())
            .expect("nothing was waiting");
        assert!(
            registry.subscribe_exclusive(key.clone()).is_none(),
            "a second receiver on one topic must be refused, not queued behind the first",
        );

        drop(first);
        assert_eq!(registry.registered(), 0);
        assert!(
            registry.subscribe_exclusive(key).is_some(),
            "the topic is free again once its receiver has gone",
        );
    }

    /// Exclusivity is per key, so it does not reach across topics or across kinds of wait.
    #[tokio::test]
    async fn exclusivity_covers_one_key_and_no_more() {
        let registry = Arc::new(Registry::default());
        let _orders = registry
            .subscribe_exclusive(message_key("wf-1", Some("orders")))
            .expect("nothing was waiting");

        let _refunds = registry
            .subscribe_exclusive(message_key("wf-1", Some("refunds")))
            .expect("another topic on the same workflow is a different wait");
        let _elsewhere = registry
            .subscribe_exclusive(message_key("wf-2", Some("orders")))
            .expect("another workflow on the same topic is a different wait");
        // And the prefix keeps a receiver from blocking an event waiter that names the same two
        // strings, which is what the prefix is for.
        let _event = registry.subscribe(event_key("wf-1", "orders"));

        assert_eq!(registry.registered(), 4, "four distinct waits, four keys");
    }

    /// A shared waiter and an exclusive one refuse each other, in both orders.
    ///
    /// Nothing in the crate does this — the prefixes keep `recv` and the shared waits on disjoint
    /// keys — but the map is one map, so the behaviour is worth pinning rather than discovering.
    #[tokio::test]
    async fn an_exclusive_waiter_and_a_shared_one_do_not_share_a_key() {
        let registry = Arc::new(Registry::default());
        let key = message_key("wf-1", None);

        let shared = registry.subscribe(key.clone());
        assert!(registry.subscribe_exclusive(key.clone()).is_none());
        drop(shared);

        let _exclusive = registry
            .subscribe_exclusive(key.clone())
            .expect("free again");
        // The other order is *not* symmetric: `subscribe` does not check for an exclusive holder,
        // so it joins rather than refusing. Which is why what keeps `recv` alone on its key is the
        // prefix, not this map — pinned here so the asymmetry is a decision rather than a surprise.
        let _joined = registry.subscribe(key);
        assert_eq!(registry.registered(), 1);
    }

    #[tokio::test]
    async fn a_subscriber_is_woken_and_deregisters_when_dropped() {
        let registry = Arc::new(Registry::default());
        let mut subscription = registry.subscribe(event_key("wf-1", "progress"));
        assert_eq!(registry.registered(), 1);

        registry.wake(&event_key("wf-1", "progress"));
        subscription.notified().await;

        drop(subscription);
        assert_eq!(
            registry.registered(),
            0,
            "the last subscriber takes its registration with it"
        );
    }

    /// One registration serves every waiter on a key, and survives losing one of them.
    #[tokio::test]
    async fn every_waiter_on_one_key_is_woken() {
        let registry = Arc::new(Registry::default());
        let key = stream_key("wf-1", "log");
        let mut first = registry.subscribe(key.clone());
        let mut second = registry.subscribe(key.clone());
        assert_eq!(registry.registered(), 1);

        registry.wake(&key);
        first.notified().await;
        second.notified().await;

        // This assertion is the load-bearing one, not the wake below it: `notified` returns on a
        // closed channel as readily as on a wakeup, so a registration wrongly dropped here would
        // still let the wait below return immediately. Verified by mutation — removing the
        // last-waiter guard fails on this line.
        drop(first);
        assert_eq!(registry.registered(), 1, "the other waiter is still here");
        registry.wake(&key);
        second.notified().await;
    }

    /// A waiter parked before the write is woken by it, which is the ordering the whole design
    /// turns on.
    #[tokio::test]
    async fn a_parked_waiter_is_woken_by_a_later_write() {
        let registry = Arc::new(Registry::default());
        let mut subscription = registry.subscribe(event_key("wf-1", "k"));
        let waiting = tokio::spawn(async move { subscription.notified().await });
        // Long enough for the task to be parked rather than not yet scheduled.
        tokio::time::sleep(Duration::from_millis(50)).await;

        registry.wake(&event_key("wf-1", "k"));
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("a parked waiter was never woken")
            .unwrap();
    }

    /// A wakeup that lands before the wait is not lost: the slot holds it.
    ///
    /// This is what makes subscribe-then-look safe. A waiter that subscribes, looks, finds nothing,
    /// and only then waits must still see a write that landed during the look.
    #[tokio::test]
    async fn a_wakeup_between_subscribing_and_waiting_is_not_missed() {
        let registry = Arc::new(Registry::default());
        let mut subscription = registry.subscribe(event_key("wf-1", "k"));

        // The write lands here — after the subscription, before the wait.
        registry.wake(&event_key("wf-1", "k"));

        tokio::time::timeout(Duration::from_secs(5), subscription.notified())
            .await
            .expect("a wakeup during the look was dropped");
    }

    #[tokio::test]
    async fn a_gap_wakes_every_waiter_whatever_it_waits_for() {
        let registry = Arc::new(Registry::default());
        let mut on_event = registry.subscribe(event_key("wf-1", "progress"));
        let mut on_stream = registry.subscribe(stream_key("wf-2", "log"));
        let mut on_message = registry.subscribe(message_key("wf-3", None));

        registry.wake_all();

        on_event.notified().await;
        on_stream.notified().await;
        on_message.notified().await;
    }

    #[test]
    fn a_wake_for_nobody_is_not_an_error() {
        let registry = Arc::new(Registry::default());
        // A listener sees every process's notifications; almost none are this one's.
        registry.wake(&event_key("someone-else", "key"));
        registry.wake_all();
        assert_eq!(registry.registered(), 0);
    }
}
