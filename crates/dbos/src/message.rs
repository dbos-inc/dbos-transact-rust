//! Workflow messages: a payload sent to one workflow, for that workflow to receive once.
//!
//! The counterpart of [`event`](crate::event) and the mirror image of its asymmetry. An event is
//! written by one workflow and readable by anyone, so its *reader* has three surfaces and its
//! writer has one. A message is written by anyone and readable only by the workflow it was
//! addressed to, so it is the *sender* that has three surfaces and the receiver that has one:
//!
//! | | inside a workflow | outside |
//! |---|---|---|
//! | [`send`] | free function | [`DBOS::send`], [`Client::send`](crate::Client::send) |
//! | [`recv`] | free function | — |
//!
//! **[`recv`] has no form outside a workflow, and that is structural rather than an omission.** A
//! receive consumes: the message is marked consumed in the same transaction that records the step
//! that took it, so exactly one receiver gets it and a replay gets it again rather than taking
//! another. A caller with no workflow has no step to record against, so it could only consume and
//! then lose the message if anything went wrong afterwards. All four references reach the same
//! conclusion, and `sysdb` states it at [`SystemDatabase::recv`](crate::sysdb::SystemDatabase::recv):
//! its caller is not optional, where [`get_event`](crate::get_event)'s is. Code outside a workflow
//! that wants to observe messages without taking them has
//! [`get_all_notifications`](crate::sysdb::SystemDatabase::get_all_notifications).
//!
//! **The two calls differ on standing inside a step, and the references differ with them.** A
//! [`send`] there is plain and uncheckpointed, as it is in Python, TypeScript and Java; a [`recv`]
//! there is [`Error::InsideStep`], as it is in all four. The asymmetry is the point: a send that
//! runs twice delivers twice, while a receive that runs twice *loses* a message, and only the
//! second is unrecoverable. Go refuses both.
//!
//! **The free [`send`] is not just symmetry with [`set_event`](crate::set_event).** The registry
//! lives on the instance, so a registered closure that captures a [`DBOS`] is stored inside the
//! very `Arc` it holds a strong reference to — a cycle that keeps the instance, its executor and
//! its connection pool alive for the life of the process. Making a workflow capture a handle in
//! order to send a message would have made that the documented way to write one.
//!
//! **Every send in the crate is here, and they meet at one place.** `Connection::send` is the
//! shared path — it encodes the payloads, names the format, and hands `sysdb` a batch — so the
//! three public surfaces differ only in what they can say before they reach it: which ambient
//! context to read, whether the send is checkpointed, whether a handle's executor has to be
//! reconciled with it, and whether an idempotency key or a fork fan-out is on offer. That is
//! [`event`](crate::event)'s shape too, where `Connection::get_event` is the one read under three
//! callers. A batch is the primitive and a single send is a caller with one message, which is what
//! `sysdb` says and how it derives the step name.
//!
//! All of these are thin. `sysdb` owns the transactional insert, the fork fan-out, the replay
//! skip, the consuming read, the concurrent-receive guard and the cross-SDK step names
//! (`DBOS.send`, `DBOS.recv`). What the engine adds is the step ids from the ambient context, the
//! payload encoding, and the guards on where each call may stand.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::DBOS;
use crate::connection::Connection;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::Message as EncodedMessage;

/// Sends a message to a workflow, for it to [`recv`] when it is ready.
///
/// The sender for a workflow body, and the counterpart of [`recv`]: it takes the executor and the
/// step-id sequence from the ambient context, so a workflow that sends needs no [`DBOS`] handle and
/// its registered closure captures nothing. That is what keeps the registry free of strong
/// references back to the instance holding it — see the module documentation.
///
/// [`Message::new`] addresses the default topic; a receiver selects on the topic, so a message sent
/// under one nobody is receiving on waits in the database rather than being delivered to a
/// different [`recv`].
///
/// The message waits until the destination reads it, so sending to a workflow that has not reached
/// its receive — or is not running at all — is normal rather than an error. Sending to a workflow
/// that *does not exist* is [`Error::SystemDatabase`] carrying the system database's
/// non-existent-workflow error: the foreign key catches it, so a message is never left addressed to
/// nothing.
///
/// The send is checkpointed under a step id, so a replay does not send twice — `sysdb` finds the
/// recorded step and inserts nothing, in the same transaction that would have written. **That is a
/// workflow's whole idempotency**, and it is why a workflow body rarely sets
/// [`Message::idempotency_key`]: the step already makes the send exactly-once. The field is
/// available here all the same, as it is in Python, and it is what a send from inside a *step* has
/// instead.
///
/// The error is the *workflow's* channel, like [`step`](crate::step)'s and
/// [`set_event`](crate::set_event)'s, so `?` needs no conversion.
///
/// **From inside a step it sends plainly, with no checkpoint** — the leaf rule
/// [`get_event`](crate::get_event) follows, the enclosing step's own checkpoint standing for
/// everything its body did. The cost is real and is the references' accepted position rather than
/// ours: a step that retries sends again, and this form has no idempotency key to stop it. Python,
/// TypeScript and Java all do exactly this; Go alone refuses the call. A send that must happen once
/// across retries wants a [`Message::idempotency_key`], or wants hoisting out of the step, where
/// the step id makes it exactly-once.
///
/// Outside a workflow there is no context to take an executor from, so this is
/// [`Error::NotInWorkflow`]. That is where [`DBOS::send`] is the call.
pub async fn send<T, E>(message: Message<'_, T>) -> Result<(), E>
where
    T: Serialize,
    E: DurableError,
{
    send_with(message, SendOptions::default()).await
}

/// [`send`], with [`SendOptions`] rather than the defaults.
///
/// The same call in every respect but the options — see [`send`] for what it does, where it may
/// stand, and what a step does to it.
pub async fn send_with<T, E>(message: Message<'_, T>, options: SendOptions) -> Result<(), E>
where
    T: Serialize,
    E: DurableError,
{
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "send".into(),
        });
    };
    // Absent inside a step, which is what makes that send plain: no id is allocated, so nothing
    // shifts the replay slots of the steps around it.
    let caller = (!ctx.in_step()).then_some(&ctx);
    send_one(ctx.executor().connection(), caller, message, options).await
}

/// Takes the oldest message sent to this workflow, waiting up to `timeout` for one to arrive.
///
/// The receiver, and the one surface in this module with no form outside a workflow: the workflow
/// receiving *is* the workflow calling, so there is no destination to name — a receive is always
/// for the caller's own mailbox. See the module documentation for why no client can have this.
///
/// `topic` of `None` is the default topic, and a receive on one topic never takes a message sent on
/// another.
///
/// `Ok(None)` means nothing was waiting when the deadline passed — absence is a value, not an error,
/// and `Duration::ZERO` makes this a poll: look once, do not wait. Go is the outlier here and raises
/// a timeout instead; the other three agree with this.
///
/// The receive is checkpointed as two steps (the read and its deadline), so a replay returns the
/// message the first run took, including a timeout's `None`, instead of consuming a second one. The
/// error is the *workflow's* channel, like [`step`](crate::step)'s, so `?` needs no conversion.
///
/// **Two concurrent receives on one topic in one workflow is an error**, surfaced as
/// [`Error::SystemDatabase`] carrying `sysdb`'s `ConcurrentRecv`. One message can go to only one of
/// them, so the other could only wait out its timeout and report nothing — which the sender cannot
/// distinguish from never having sent. Python and Go reject it too; TypeScript and Java allow it.
///
/// # Why not from inside a step
///
/// [`Error::InsideStep`], where [`get_event`](crate::get_event) reads plainly from inside a step
/// under the leaf rule — the enclosing step's own checkpoint standing for everything its body did.
/// A receive cannot take that rule, because it is not only a read: it *consumes*. The step's
/// checkpoint records that the body ran, not which message it swallowed, so a retried attempt would
/// consume a second message and the first would be gone with nothing recording it. `sysdb` says the
/// same by construction — its `recv` caller is a required parameter, so there is no uncheckpointed
/// form of this call to degrade to.
///
/// **[`send`] is the other way, and deliberately.** `sysdb` takes `None` for its caller there, so an
/// uncheckpointed send is expressible where an uncheckpointed receive is not — and a send that runs
/// twice delivers twice, where a receive that runs twice *loses* a message. Python, TypeScript and
/// Java all send plainly from inside a step; only Go refuses, and following Go here would have made
/// a workflow that ports cleanly between the other three stop working on Rust.
///
/// All four references block a receive inside a step, so this refusal is unanimous rather than a
/// reading: Python raises from `is_workflow()`, TypeScript raises
/// `DBOSInvalidWorkflowTransitionError` naming `step`, Go raises *"cannot call Recv within a step"*,
/// and Java raises *"DBOS.recv() must not be called from within a step."*
pub async fn recv<T, E>(topic: Option<&str>, timeout: Duration) -> Result<Option<T>, E>
where
    T: DeserializeOwned,
    E: DurableError,
{
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "recv".into(),
        });
    };
    if ctx.in_step() {
        return Err(Error::InsideStep {
            operation: "recv".into(),
        });
    }

    // Field order is the contract, as it is for `get_event`'s caller: the read's id first, the
    // deadline's second, matching what every SDK records and what a replay looks up.
    let step_id = ctx.next_step_id();
    let timeout_step_id = ctx.next_step_id();
    let found = ctx
        .executor()
        .sysdb()
        .recv(ctx.workflow_id(), step_id, timeout_step_id, topic, timeout)
        .await
        .map_err(Error::SystemDatabase)?;
    match found {
        None => Ok(None),
        Some(found) => decode(Some(&found.value), "message").map(Some),
    }
}

impl DBOS {
    /// Sends a message to a workflow, for it to [`recv`] when it is ready.
    ///
    /// The sender for code outside a workflow that still has an instance — an HTTP handler in the
    /// application's own process approving an order the workflow is waiting on. **Inside a workflow,
    /// reach for the free [`send`] instead**: it needs no handle, so the closure a workflow is
    /// registered as captures nothing, and a captured [`DBOS`] is a cycle with the registry that
    /// holds the closure.
    ///
    /// Called from outside a workflow there is no step to record, so the send is a plain insert and
    /// the only idempotency available is not sending twice. Called from inside one anyway, it
    /// behaves as the free function does — checkpointed in a workflow body, plain inside a step —
    /// except that it reports in the engine's own error channel, so a workflow with its own error
    /// type carries the result over with [`Error::lift`].
    ///
    /// With one exception it cannot share: this takes its executor from `self` and its step ids from
    /// the ambient context, so a handle to some *other* instance would split the two. That is
    /// [`Error::WrongInstance`] rather than a silent write into the wrong database.
    pub async fn send<T: Serialize>(&self, message: Message<'_, T>) -> Result<()> {
        self.send_with(message, SendOptions::default()).await
    }

    /// [`send`](Self::send), with [`SendOptions`] rather than the defaults.
    pub async fn send_with<T: Serialize>(
        &self,
        message: Message<'_, T>,
        options: SendOptions,
    ) -> Result<()> {
        let executor = self.executor("send")?;
        // Inside a step this is already `None`, so nothing is checkpointed and there is nothing to
        // disagree about — that send is plain whichever instance serves it, exactly as
        // `DBOS::get_event`'s read is.
        let ctx = Ctx::current().filter(|ctx| !ctx.in_step());
        // Exactly where the two halves would be combined: a step id is about to be taken from the
        // ambient context and recorded against `self`'s executor.
        if ctx
            .as_ref()
            .is_some_and(|ctx| !Arc::ptr_eq(ctx.executor(), &executor))
        {
            return Err(Error::WrongInstance {
                operation: "send".into(),
            });
        }
        send_one(executor.connection(), ctx.as_ref(), message, options).await
    }
}

/// One message, from a surface that sends exactly one.
///
/// The free [`send`] and [`DBOS::send`] differ only in where the connection comes from and which
/// guards ran before they got here; everything after that is this. `caller` present is the
/// checkpointed send and absent is the plain one, which is exactly what `sysdb`'s optional caller
/// means. Taking the [`Ctx`] rather than a pre-built caller is what keeps the step id from being
/// allocated on a path that then refuses: both callers have finished their guards by the time they
/// reach this.
async fn send_one<T, E>(
    connection: &Connection,
    caller: Option<&Ctx>,
    message: Message<'_, T>,
    options: SendOptions,
) -> Result<(), E>
where
    T: Serialize,
    E: DurableError,
{
    connection
        .send(
            &[message],
            caller.map(|ctx| (ctx.workflow_id(), ctx.next_step_id())),
            options,
        )
        .await
}

impl Connection {
    /// The send itself, shared by every surface that has one.
    ///
    /// `sysdb` owns the transaction, the fork fan-out and the replay skip, so what is left here is
    /// encoding the payloads and naming the format they were encoded in. On the connection because
    /// that is where the serializer lives and a send is otherwise one insert: the free [`send`]
    /// reaches it through the ambient context's connection, [`DBOS::send`] through its executor's,
    /// and [`Client::send_all`](crate::Client::send_all) through the only one it has. The same
    /// shape [`get_event`](crate::get_event)'s three surfaces share, and named as they are.
    ///
    /// The batch is the primitive and a single send is a caller with one message — `sysdb` says so,
    /// and derives the step name from the count. What each surface adds is the *caller*: which
    /// ambient context to read, whether the send is checkpointed, and whether a handle's executor
    /// has to be reconciled with it.
    ///
    /// Encoded up front so that nothing is sent when one payload cannot be: the whole batch is one
    /// transaction, and failing halfway through the encoding would be the prefix a batch exists to
    /// avoid.
    ///
    /// Generic over the caller's error channel for the same reason [`encode`] is: the failure is an
    /// engine variant either way, and `E` only says which channel it travels in.
    pub(crate) async fn send<T, E>(
        &self,
        messages: &[Message<'_, T>],
        caller: Option<(&str, i32)>,
        options: SendOptions,
    ) -> Result<(), E>
    where
        T: Serialize,
        E: DurableError,
    {
        let encoded = messages
            .iter()
            .map(|message| encode(message.message, "message"))
            .collect::<Result<Vec<_>, E>>()?;
        let messages: Vec<EncodedMessage<'_>> = messages
            .iter()
            .zip(&encoded)
            .map(|(message, encoded)| EncodedMessage {
                destination_id: message.destination_id,
                topic: message.topic,
                message: encoded,
                idempotency_key: message.idempotency_key,
            })
            .collect();
        self.sysdb()
            .send_messages(
                &messages,
                Some(self.serializer().name()),
                caller,
                options.forks == Forks::Include,
            )
            .await
            .map_err(Error::SystemDatabase)
    }
}

impl crate::Client {
    /// Sends a message to a workflow, for it to [`recv`] when it is ready.
    ///
    /// [`recv`]: crate::sysdb::SystemDatabase::recv
    ///
    /// The message waits in the database until the destination reads it, so sending to a workflow
    /// that has not reached its receive — or is not running at all — is normal rather than an
    /// error. Sending to a workflow that *does not exist* is
    /// [`Error::SystemDatabase`](crate::Error::SystemDatabase) carrying the system database's
    /// non-existent-workflow error: the foreign key catches it, so a message is never left
    /// addressed to nothing.
    ///
    /// **A client's send is not a step**, and that is the difference from the send a workflow body
    /// will make. A workflow's send is checkpointed, so a replay does not send twice; a client has
    /// no replay and no step sequence, and [`Message::idempotency_key`] is the mechanism it has
    /// instead — the key becomes the row's identity, so a retried request delivers one message.
    pub async fn send<T: Serialize>(&self, message: Message<'_, T>) -> Result<()> {
        self.send_with(message, SendOptions::default()).await
    }

    /// [`send`](Self::send), with [`SendOptions`] rather than the defaults.
    pub async fn send_with<T: Serialize>(
        &self,
        message: Message<'_, T>,
        options: SendOptions,
    ) -> Result<()> {
        self.send_all(std::slice::from_ref(&message), options).await
    }

    /// Sends many messages in one transaction.
    ///
    /// **All or none**, which is the reason to prefer this over a loop of [`send`](Self::send):
    /// the batch is one insert, so a failure halfway through delivers nothing rather than a prefix.
    /// Python and Java expose the same as `send_bulk`; TypeScript and Go have no equivalent, and a
    /// caller there writes the loop and lives with the prefix.
    ///
    /// One payload type for the whole batch, which is what typing it costs. A batch of genuinely
    /// different shapes is a batch of `serde_json::Value`, or two calls.
    pub async fn send_all<T: Serialize>(
        &self,
        messages: &[Message<'_, T>],
        options: SendOptions,
    ) -> Result<()> {
        // No caller: a client is never inside a workflow, so there is no step to record the batch
        // against and nothing to replay it for.
        self.connection().send(messages, None, options).await
    }
}

/// A message for a workflow, and how to address it.
///
/// [`Message::new`] plus functional update, like everything else that takes options here:
///
/// ```no_run
/// # use dbos::Message;
/// let message = Message {
///     topic: Some("approvals"),
///     ..Message::new("order-42", &"approved")
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a, T> {
    /// The workflow it is for.
    pub destination_id: &'a str,
    /// The payload, encoded by this client's serializer on the way in.
    pub message: &'a T,
    /// The topic it is filed under, or `None` for the default one.
    ///
    /// A receiver selects on the topic, so a message sent under one nobody is receiving on waits
    /// forever rather than being delivered to a different receive.
    pub topic: Option<&'a str>,
    /// A key that makes re-sending this message a no-op.
    ///
    /// It becomes the row's identity, so a second send under the same key is discarded by the
    /// database rather than delivered twice.
    ///
    /// **Per message rather than per call, and that is why it lives here and not on
    /// [`SendOptions`].** The key *is* the row's primary key, so a batch whose messages shared one
    /// would collide with itself and deliver a single message instead of all of them. Python's and
    /// Java's `SendMessage` carry it in exactly this position for the same reason.
    ///
    /// **Chiefly for a sender with no step to protect it** — a [`Client`](crate::Client), or a
    /// [`send`] from inside a step, both of which may run twice with nothing recording that they
    /// did. A workflow body rarely needs one: its send is a checkpointed step, which already makes
    /// it exactly-once. It is offered on every surface all the same, as Python and Java offer
    /// theirs.
    pub idempotency_key: Option<&'a str>,
}

impl<'a, T> Message<'a, T> {
    /// A message for `destination_id`, on the default topic.
    pub fn new(destination_id: &'a str, message: &'a T) -> Self {
        Self {
            destination_id,
            message,
            topic: None,
            idempotency_key: None,
        }
    }
}

/// What a send may ask for, beyond the messages themselves.
///
/// **A struct so that the next option is not a breaking change.** A send has accumulated options in
/// every implementation — the fork fan-out here, and a portable serializer, a caller-supplied
/// transaction and a serialization strategy in the references — and each one arriving as another
/// positional parameter would break every call site. This is where they land instead. TypeScript
/// and Java call theirs `SendOptions` too; Go takes variadic `SendOption` functions to the same end.
///
/// Built by functional update from [`Default`], as [`Config`](crate::Config) and
/// [`ForkOptions`](crate::ForkOptions) are:
///
/// ```no_run
/// # use dbos::{Forks, SendOptions};
/// let options = SendOptions { forks: Forks::Include, ..Default::default() };
/// ```
///
/// **Deliberately not `#[non_exhaustive]`.** That attribute forbids struct-literal construction
/// from other crates entirely, functional update included, which is exactly the form above. Adding
/// a field stays source-compatible for every caller who wrote `..Default::default()`, so the
/// convention is documented rather than enforced — the same choice [`Config`](crate::Config) makes
/// and for the same reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendOptions {
    /// Whether the messages also reach the workflows forked from their destinations.
    ///
    /// Defaults to [`Forks::Skip`]: the destination named, and nothing else.
    pub forks: Forks,
}

/// Whether a message also reaches the workflows forked from its destination.
///
/// A fork is a new workflow replaying an old one's steps, so a message the original was waiting for
/// is one the fork will wait for too — and it was consumed by the original. Including the forks is
/// how a send reaches both.
///
/// The fork set is resolved inside the sending transaction, so a fork created while the send is in
/// flight cannot make it stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Forks {
    /// The destination named, and nothing else.
    #[default]
    Skip,
    /// The destination, and everything recursively forked from it.
    Include,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::EngineOnly;

    #[test]
    fn a_message_is_untopicked_and_sent_once_per_call() {
        let message = Message::new("workflow-1", &"payload");
        assert_eq!(message.destination_id, "workflow-1");
        assert_eq!(message.topic, None);
        assert_eq!(
            message.idempotency_key, None,
            "each send is a distinct message unless a key says otherwise"
        );
        assert_eq!(Forks::default(), Forks::Skip);
    }

    /// Neither free function has anything to stand on outside a workflow, and both say so rather
    /// than reaching for a database. No instance is built here because none is needed: the guard
    /// runs before anything is read.
    #[tokio::test]
    async fn the_free_calls_refuse_outside_a_workflow() {
        let err = send::<_, EngineOnly>(Message::new("wf", &"hello"))
            .await
            .expect_err("a send outside a workflow should be refused");
        assert!(
            matches!(&err, Error::NotInWorkflow { operation } if operation == "send"),
            "{err}"
        );

        let err = recv::<String, EngineOnly>(None, Duration::ZERO)
            .await
            .expect_err("a receive outside a workflow should be refused");
        assert!(
            matches!(&err, Error::NotInWorkflow { operation } if operation == "recv"),
            "{err}"
        );
    }
}
