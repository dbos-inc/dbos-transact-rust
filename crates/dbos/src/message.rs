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
//! | [`send_bulk`] | free function | [`DBOS::send_bulk`], [`Client::send_bulk`](crate::Client::send_bulk) |
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
//! **Every send in the crate is here, and they meet at one place per surface.**
//! `Connection::send_message` and `Connection::send_messages` are the shared paths — each encodes
//! the payloads, names the format, and hands `sysdb` the matching call — so the surfaces above
//! them differ only in what they can say before they reach one: which ambient context to read,
//! whether the send is checkpointed, and whether a handle's executor has to be reconciled with it.
//! That is [`event`](crate::event)'s shape too, where `Connection::get_event` is the one read under
//! three callers. The single send and the batch stay separate all the way down, because which one
//! is called is what chooses the recorded step name — `sysdb` takes it from the method rather than
//! deriving it from a count.
//!
//! **Required as arguments, optional in a struct.** All three single sends read
//! `send(destination_id, message)`, with a topic, an idempotency key and the fork fan-out in
//! [`SendOptions`] on the `_with` form — the split [`step`](crate::step())/[`step_with`](crate::step_with)
//! and `fork`/[`fork_with`](crate::DBOS::fork_with) already make. The batch is the one exception and
//! has to be: the [`send_bulk`] family takes [`Message`] values carrying their own topic and key,
//! because those vary per message, beside a batch-wide [`SendBulkOptions`]. Python and Java split it
//! in the same place.
//!
//! **The batch is on all three surfaces, not just the client.** Python and Java are the only
//! references with a batch send and both expose it on their runtime as well as their client, and
//! both checkpoint it from inside a workflow — one step for the whole batch. TypeScript and Go have
//! no batch send to compare.
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
use crate::checkpoint::{Built, PendingStep, StepPlacement};
use crate::connection::Connection;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::Message as EncodedMessage;
use crate::sysdb::types::step_names;

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
/// [`SendOptions::idempotency_key`]: the step already makes the send exactly-once. The key is
/// available here all the same, through [`send_with`], as it is in Python, and it is what a send
/// from inside a *step* has instead.
///
/// **The step id is taken at the call, not at the first poll** — see [`PendingStep`] — so a send
/// built beside a step and driven with it by `tokio::join!` takes the same slot on every execution.
/// The payload is encoded first, ahead of the id, so a message that cannot be encoded is a send
/// that never happened and never moved the counter.
///
/// The error is the *workflow's* channel, like [`step`](crate::step())'s and
/// [`set_event`](crate::set_event)'s, so `?` needs no conversion.
///
/// **From inside a step it sends plainly, with no checkpoint** — the leaf rule
/// [`get_event`](crate::get_event) follows, the enclosing step's own checkpoint standing for
/// everything its body did. The cost is real and is the references' accepted position rather than
/// ours: a step that retries sends again, and this form has no idempotency key to stop it. Python,
/// TypeScript and Java all do exactly this; Go alone refuses the call. A send that must happen once
/// across retries wants [`send_with`] and a [`SendOptions::idempotency_key`], or wants hoisting out
/// of the step, where the step id makes it exactly-once.
///
/// Outside a workflow there is no context to take an executor from, so this is
/// [`Error::NotInWorkflow`]. That is where [`DBOS::send`] is the call.
pub fn send<'a, T, E>(destination_id: &'a str, message: &T) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    send_with(destination_id, message, SendOptions::default())
}

/// [`send`], with [`SendOptions`] rather than the defaults — a topic, an idempotency key, or the
/// fork fan-out.
///
/// The same call in every respect but the options: see [`send`] for what it does, where it may
/// stand, and what a step does to it.
pub fn send_with<'a, T, E>(
    destination_id: &'a str,
    message: &T,
    options: SendOptions<'a>,
) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    // Encoded before the id is taken, so a payload that cannot be encoded is a send that never
    // happened and never moved the counter. Inside a step the placement records nothing, which is
    // what makes that send plain: no id is allocated, so nothing shifts the replay slots of the
    // steps around it.
    let built = StepPlacement::ambient_connection("send")
        .and_then(|conn| Ok((encode_one(destination_id, message, options)?, conn)))
        .and_then(|(encoded, conn)| place_send(encoded, conn, "send"));
    pending_send(built, options.forks)
}

/// Sends many messages in one transaction, for their destinations to [`recv`] when they are ready.
///
/// **All or none**, which is the reason to prefer this over a loop of [`send`]: the batch is one
/// insert, so a failure halfway through delivers nothing rather than a prefix.
///
/// The batch is checkpointed as **one** step, so a replay sends none of it again. The step is
/// recorded as `DBOS.sendBulk` however long the batch is, a batch of exactly one included: the
/// name says which API surface was reached for, not how many messages it carried. A workflow that
/// swaps a [`send`] for a [`send_bulk`] between runs therefore flips names and is caught as a
/// determinism error, while one whose message *count* merely changes is not, that being no change
/// of operation.
///
/// Where a single [`send`] takes its destination and payload as arguments, a batch takes
/// [`Message`] values: the topic and the idempotency key vary per message, so they travel with the
/// message rather than in the options. Only [`SendBulkOptions`] is left, for what is uniform across
/// the call.
///
/// Python and Java expose the same as `send_bulk` on their runtime as well as their client;
/// TypeScript and Go have no batch send at all, and a caller there writes the loop and lives with
/// the prefix.
///
/// # Why `bulk` and not `_all`
///
/// The bulk *management* verbs here are [`cancel_all`](DBOS::cancel_all),
/// [`resume_all`](DBOS::resume_all), [`fork_all`](DBOS::fork_all) and
/// [`delete_all`](DBOS::delete_all), so `send_all` would have been the local rhyme. It is not the
/// name because those four render a *pluralised noun* — Python's `cancel_workflows`, TypeScript's
/// `cancelWorkflows`, Go's `CancelWorkflows` — into a form that reads on a type where the noun is
/// implicit. A batch send is not that: Python and Java both chose the distinct word `bulk`, and
/// `sysdb` already records this call under the cross-SDK step name `DBOS.sendBulk`. A method named
/// for one word that writes another is a seam for nothing.
pub fn send_bulk<'a, T, E>(messages: &'a [Message<'a, T>]) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    send_bulk_with(messages, SendBulkOptions::default())
}

/// [`send_bulk`], with [`SendBulkOptions`] rather than the defaults.
pub fn send_bulk_with<'a, T, E>(
    messages: &'a [Message<'a, T>],
    options: SendBulkOptions,
) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    let built = StepPlacement::ambient_connection("send_bulk")
        .and_then(|conn| Ok((encode_all(messages)?, conn)))
        .and_then(|(encoded, conn)| place_send(encoded, conn, "send_bulk"));
    pending_send_bulk(built, options.forks)
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
/// message the first run took, including a timeout's `None`, instead of consuming a second one.
/// **Both ids are taken at the call, not at the first poll** — see [`PendingStep`] — so a receive
/// built beside another durable call and driven with it takes the same slots on every execution.
/// The error is the *workflow's* channel, like [`step`](crate::step())'s, so `?` needs no
/// conversion.
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
pub fn recv<'a, T, E>(topic: Option<&'a str>, timeout: Duration) -> PendingStep<'a, Option<T>, E>
where
    T: DeserializeOwned + 'a,
    E: DurableError + 'a,
{
    PendingStep::placed(
        step_names::RECV,
        place_recv(),
        move |(executor, timeout_step_id), placement| async move {
            // `recv` is refused anywhere that records nothing, so the placement is always
            // `Recorded` here — `sysdb::recv` takes a required caller for the same reason, there
            // being no uncheckpointed form of a call that consumes.
            let Some((workflow_id, step_id)) = placement.step() else {
                unreachable!("recv is refused outside a workflow and inside a step")
            };
            let found = executor
                .sysdb()
                .recv(workflow_id, step_id, timeout_step_id, topic, timeout)
                .await
                .map_err(Error::SystemDatabase)?;
            match found {
                None => Ok(None),
                Some(found) => decode(Some(&found.value), "message").map(Some),
            }
        },
    )
}

/// Everything [`recv`] settles before it has run anything: the two refusals, and then both ids.
///
/// **Two ids, and only one of them fits in a [`StepPlacement`].** Field order is the contract, as
/// it is for [`get_event`](crate::get_event)'s caller: the read's id first, the deadline's second,
/// matching what every SDK records and what a replay looks up. So the deadline's id comes from the
/// counter immediately behind the one the placement took.
fn place_recv() -> Built<(Arc<crate::Executor>, i32)> {
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
    // The refusals above settled that this stands at a step boundary of its own workflow, served
    // by its own executor, so there is no second connection to disagree with and the placement
    // cannot fail. It takes the context those refusals read rather than reading it again.
    let executor = Arc::clone(ctx.executor());
    let placement = StepPlacement::at(ctx);
    // The deadline's id from the same counter the placement drew the read's from, so the pair is
    // one decision rather than two that could disagree.
    let timeout_step_id = placement
        .next_step_id()
        .expect("recv is refused anywhere that records nothing");
    Ok(((executor, timeout_step_id), placement))
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
    pub fn send<'a, T: Serialize>(
        &self,
        destination_id: &'a str,
        message: &T,
    ) -> PendingStep<'a, ()> {
        self.send_with(destination_id, message, SendOptions::default())
    }

    /// [`send`](Self::send), with [`SendOptions`] rather than the defaults.
    pub fn send_with<'a, T: Serialize>(
        &self,
        destination_id: &'a str,
        message: &T,
        options: SendOptions<'a>,
    ) -> PendingStep<'a, ()> {
        let built = encode_one(destination_id, message, options)
            .and_then(|encoded| {
                Ok((
                    encoded,
                    StepPlacement::taken(self.executor("send"), "send")?,
                ))
            })
            .map(|(encoded, (executor, placement))| {
                ((Arc::clone(executor.connection()), encoded), placement)
            });
        pending_send(built, options.forks)
    }

    /// Sends many messages in one transaction, for their destinations to [`recv`] when ready.
    ///
    /// The instance's [`send_bulk`], and it stands to that as [`send`](Self::send) does to the free
    /// one: the caller for code outside a workflow that still has an instance. See [`send_bulk`] for
    /// the batch's guarantees and how it is checkpointed.
    pub fn send_bulk<'a, T: Serialize>(
        &self,
        messages: &'a [Message<'a, T>],
    ) -> PendingStep<'a, ()> {
        self.send_bulk_with(messages, SendBulkOptions::default())
    }

    /// [`send_bulk`](Self::send_bulk), with [`SendBulkOptions`] rather than the defaults.
    pub fn send_bulk_with<'a, T: Serialize>(
        &self,
        messages: &'a [Message<'a, T>],
        options: SendBulkOptions,
    ) -> PendingStep<'a, ()> {
        let built = encode_all(messages)
            .and_then(|encoded| {
                Ok((
                    encoded,
                    StepPlacement::taken(self.executor("send_bulk"), "send_bulk")?,
                ))
            })
            .map(|(encoded, (executor, placement))| {
                ((Arc::clone(executor.connection()), encoded), placement)
            });
        pending_send_bulk(built, options.forks)
    }
}

/// Where a send stands, with the payloads it has already encoded kept beside it.
///
/// **The order is the contract**: the payloads are encoded by the caller *before* this is reached,
/// so a send that cannot be encoded is one that never happened and never moved the workflow's step
/// counter. A send refused here — an unlaunched instance, another instance's handle — moves it no
/// further.
fn place_send<C>(
    encoded: C,
    conn: Arc<Connection>,
    operation: &'static str,
) -> Built<(Arc<Connection>, C)> {
    let placement = StepPlacement::of(&conn, operation)?;
    Ok(((conn, encoded), placement))
}

/// One message with its payload already encoded, which is what a send carries from its call into
/// its run.
///
/// [`Message`] cannot be that: it holds a *reference* to an unencoded payload, so keeping one
/// would make the run generic over the caller's `T` and demand `T: Sync` of every sender for no
/// reason — the run has no use for the value, only for the string it encoded to. The three
/// addressing fields are `&str`, so they travel as they are.
struct Encoded<'a> {
    destination_id: &'a str,
    topic: Option<&'a str>,
    idempotency_key: Option<&'a str>,
    message: String,
}

impl<'a> Encoded<'a> {
    /// What `sysdb` is handed, borrowing the encoded payload rather than copying it.
    fn as_message(&self) -> EncodedMessage<'_> {
        EncodedMessage {
            destination_id: self.destination_id,
            topic: self.topic,
            message: &self.message,
            idempotency_key: self.idempotency_key,
        }
    }
}

/// One message, encoded.
///
/// Encoded before the step id is taken, for the reason [`place_send`] gives.
fn encode_one<'a, T: Serialize>(
    destination_id: &'a str,
    message: &T,
    options: SendOptions<'a>,
) -> Result<Encoded<'a>> {
    Ok(Encoded {
        destination_id,
        topic: options.topic,
        idempotency_key: options.idempotency_key,
        message: encode(message, "message")?,
    })
}

/// Every payload in a batch, encoded, or nothing.
///
/// Encoded up front so that nothing is sent when one payload cannot be: the whole batch is one
/// transaction, and failing halfway through the encoding would be the prefix a batch exists to
/// avoid. Ahead of the step id for the reason [`place_send`] gives.
fn encode_all<'a, T: Serialize>(messages: &'a [Message<'a, T>]) -> Result<Vec<Encoded<'a>>> {
    messages
        .iter()
        .map(|message| {
            Ok(Encoded {
                destination_id: message.destination_id,
                topic: message.topic,
                idempotency_key: message.idempotency_key,
                message: encode(message.message, "message")?,
            })
        })
        .collect()
}

/// A single send as a [`PendingStep`], for the two surfaces that take a step id.
///
/// The free [`send`] and [`DBOS::send`] differ only in how they reach a connection and in which
/// error channel they answer in, which is the whole of what `built` carries; a
/// [`Client`](crate::Client)'s stays a plain `async fn`, because it takes no id and is legitimately
/// driven from anywhere.
fn pending_send<'a, E: DurableError + 'a>(
    built: Built<(Arc<Connection>, Encoded<'a>)>,
    forks: Forks,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::SEND,
        built,
        move |(conn, encoded), placement| async move {
            conn.send_encoded(&encoded.as_message(), placement.step(), forks)
                .await
                .map_err(Error::lift)
        },
    )
}

/// A batch send as a [`PendingStep`] — see [`pending_send`], of which this is the plural.
fn pending_send_bulk<'a, E: DurableError + 'a>(
    built: Built<(Arc<Connection>, Vec<Encoded<'a>>)>,
    forks: Forks,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::SEND_BULK,
        built,
        move |(conn, encoded), placement| async move {
            let messages: Vec<EncodedMessage<'_>> =
                encoded.iter().map(Encoded::as_message).collect();
            conn.send_all_encoded(&messages, placement.step(), forks)
                .await
                .map_err(Error::lift)
        },
    )
}

impl Connection {
    /// One message, written to `sysdb`'s single-send method.
    ///
    /// **Takes a payload that is already encoded, because every caller encodes before it gets
    /// here.** The two checkpointed surfaces have to: they encode at the call, ahead of the step
    /// id, so that an unencodable payload moves no counter. A [`Client`](crate::Client) has no
    /// counter to move, but it encodes at the same point anyway, so there is one path to the write
    /// rather than a generic wrapper for the one caller that could have skipped it.
    ///
    /// A method apiece rather than one taking a slice, mirroring the trait: which one is called is
    /// what chooses the recorded step name, so a batch of one records `DBOS.sendBulk` and this
    /// records `DBOS.send`. On the connection because that is where the serializer lives — the
    /// free [`send`] reaches it through the ambient context's connection, [`DBOS::send`] through
    /// its executor's, and [`Client::send`](crate::Client::send) through the only one it has, the
    /// same shape [`get_event`](crate::get_event)'s three surfaces share.
    ///
    /// Generic over the caller's error channel for the same reason [`encode`] is: the failure is an
    /// engine variant either way, and `E` only says which channel it travels in.
    pub(crate) async fn send_encoded<E>(
        &self,
        message: &EncodedMessage<'_>,
        caller: Option<(&str, i32)>,
        forks: Forks,
    ) -> Result<(), E>
    where
        E: DurableError,
    {
        self.sysdb()
            .send_message(
                message,
                Some(self.serializer().name()),
                caller,
                forks == Forks::Include,
            )
            .await
            .map_err(Error::SystemDatabase)
    }

    /// The batch write, the plural of [`send_encoded`](Self::send_encoded) and encoded for the
    /// same reasons.
    ///
    /// Its callers encode the whole batch up front, so nothing is sent when one payload cannot be:
    /// the batch is one transaction, and failing halfway through the encoding would be the prefix
    /// a batch exists to avoid.
    pub(crate) async fn send_all_encoded<E>(
        &self,
        messages: &[EncodedMessage<'_>],
        caller: Option<(&str, i32)>,
        forks: Forks,
    ) -> Result<(), E>
    where
        E: DurableError,
    {
        self.sysdb()
            .send_messages(
                messages,
                Some(self.serializer().name()),
                caller,
                forks == Forks::Include,
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
    /// error. Sending to a workflow that *does not exist* is [`Error::SystemDatabase`] carrying
    /// the system database's non-existent-workflow error: the foreign key catches it, so a
    /// message is never left addressed to nothing.
    ///
    /// **A client's send is not a step**, and that is the difference from a workflow's. A
    /// workflow's send is checkpointed, so a replay does not send twice; a client has no replay and
    /// no step sequence, and [`SendOptions::idempotency_key`] is the mechanism it has instead — the
    /// key becomes the row's identity, so a retried request delivers one message.
    pub async fn send<T: Serialize>(&self, destination_id: &str, message: &T) -> Result<()> {
        self.send_with(destination_id, message, SendOptions::default())
            .await
    }

    /// [`send`](Self::send), with [`SendOptions`] rather than the defaults.
    pub async fn send_with<T: Serialize>(
        &self,
        destination_id: &str,
        message: &T,
        options: SendOptions<'_>,
    ) -> Result<()> {
        // Encoded here rather than inside the write, so a client reaches the payload the same way
        // the two checkpointed surfaces do. They have to encode at the call, ahead of the step id;
        // a client has no id to be ahead of, but one encoding path for all three is worth more
        // than the one it saved.
        let encoded = encode_one(destination_id, message, options)?;
        // No caller: a client is never inside a workflow, so there is no step to record the send
        // against and nothing to replay it for.
        self.connection()
            .send_encoded(&encoded.as_message(), None, options.forks)
            .await
    }

    /// Sends many messages in one transaction.
    ///
    /// **All or none**, which is the reason to prefer this over a loop of [`send`](Self::send): the
    /// batch is one insert, so a failure halfway through delivers nothing rather than a prefix.
    /// Python and Java expose the same as `send_bulk`; TypeScript and Go have no equivalent, and a
    /// caller there writes the loop and lives with the prefix.
    ///
    /// **The per-message options move onto [`Message`] here, and only the batch-wide ones stay in
    /// an options struct.** A topic and an idempotency key belong to one message — the key
    /// especially, since it becomes that row's identity and a batch sharing one would collide with
    /// itself — while the fork fan-out is a property of the call. Python and Java split it exactly
    /// here too: a list of `SendMessage`, carrying destination, payload, topic and key, beside a
    /// batch-wide `send_to_forks`.
    ///
    /// One payload type for the whole batch, which is what typing it costs. A batch of genuinely
    /// different shapes is a batch of `serde_json::Value`, or two calls.
    pub async fn send_bulk<T: Serialize>(&self, messages: &[Message<'_, T>]) -> Result<()> {
        self.send_bulk_with(messages, SendBulkOptions::default())
            .await
    }

    /// [`send_bulk`](Self::send_bulk), with [`SendBulkOptions`] rather than the defaults.
    pub async fn send_bulk_with<T: Serialize>(
        &self,
        messages: &[Message<'_, T>],
        options: SendBulkOptions,
    ) -> Result<()> {
        // Encoded up front, so nothing is sent when one payload cannot be: the batch is one
        // transaction, and failing halfway through the encoding would be the prefix a batch exists
        // to avoid.
        let encoded = encode_all(messages)?;
        let messages: Vec<EncodedMessage<'_>> = encoded.iter().map(Encoded::as_message).collect();
        // No caller: a client is never inside a workflow, so there is no step to record the batch
        // against and nothing to replay it for.
        self.connection()
            .send_all_encoded(&messages, None, options.forks)
            .await
    }
}

/// One message of a batch: a payload, the workflow it is for, and how to address it.
///
/// **The element type of the [`send_bulk`] family, and only that.** A single send names its
/// destination
/// and payload as arguments and everything else through [`SendOptions`] — required as parameters,
/// optional in the bag, which is the shape [`step_with`](crate::step_with),
/// [`run_with`](crate::WorkflowRef::run_with) and [`fork_with`](crate::DBOS::fork_with) all take. A
/// *batch* cannot: its topic and its idempotency key vary per message, so they travel with the
/// message. Python's and Java's `SendMessage` carry the same four fields for the same reason.
///
/// [`Message::new`] plus functional update, like everything else that takes options here:
///
/// ```no_run
/// # use dbos::Message;
/// let messages = [Message {
///     topic: Some("approvals"),
///     ..Message::new("order-42", &"approved")
/// }];
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
    /// **Per message rather than per batch, and that is why it lives here and not on
    /// [`SendBulkOptions`].** The key *is* the row's primary key, so a batch whose messages shared
    /// one would collide with itself and deliver a single message instead of all of them. Python's
    /// and Java's `SendMessage` carry it in exactly this position for the same reason.
    ///
    /// A single send says the same thing through [`SendOptions::idempotency_key`].
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

/// What a send may ask for, beyond the destination and the payload.
///
/// **Everything optional, and nothing required.** A destination and a payload are what a send
/// cannot do without, so they are arguments; a topic, an idempotency key and the fork fan-out are
/// choices, so they are here. That is the split [`step`](crate::step())/[`step_with`](crate::step_with),
/// `run`/[`run_with`](crate::WorkflowRef::run_with) and `fork`/[`fork_with`](crate::DBOS::fork_with)
/// already make, and it is why the plain [`send`] is two arguments long.
///
/// **A struct so that the next option is not a breaking change.** A send has accumulated options in
/// every implementation — a serialization override in TypeScript and Go, a caller-supplied
/// transaction in Go — and each one arriving as another positional parameter would break every call
/// site. This is where they land instead. TypeScript and Java call theirs `SendOptions` too; Go
/// takes variadic `SendOption` functions to the same end.
///
/// Built by functional update from [`Default`], as [`Config`](crate::Config) and
/// [`ForkOptions`](crate::ForkOptions) are:
///
/// ```no_run
/// # use dbos::SendOptions;
/// let options = SendOptions { topic: Some("approvals"), ..Default::default() };
/// ```
///
/// **Deliberately not `#[non_exhaustive]`.** That attribute forbids struct-literal construction
/// from other crates entirely, functional update included, which is exactly the form above. Adding
/// a field stays source-compatible for every caller who wrote `..Default::default()`, so the
/// convention is documented rather than enforced — the same choice [`Config`](crate::Config) makes
/// and for the same reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendOptions<'a> {
    /// The topic to file the message under, or `None` for the default one.
    ///
    /// A receiver selects on the topic, so a message sent under one nobody is receiving on waits in
    /// the database rather than being delivered to a different [`recv`].
    pub topic: Option<&'a str>,
    /// A key that makes re-sending this message a no-op.
    ///
    /// It becomes the row's identity, so a second send under the same key is discarded by the
    /// database rather than delivered twice.
    ///
    /// **Chiefly for a sender with no step to protect it** — a [`Client`](crate::Client), or a
    /// [`send`] from inside a step, both of which may run twice with nothing recording that they
    /// did. A workflow body rarely needs one: its send is a checkpointed step, which already makes
    /// it exactly-once. It is offered on every surface all the same, as Python and Java offer
    /// theirs.
    pub idempotency_key: Option<&'a str>,
    /// Whether the message also reaches the workflows forked from its destination.
    ///
    /// Defaults to [`Forks::Skip`]: the destination named, and nothing else.
    pub forks: Forks,
}

/// What a batch send may ask for, beyond the messages themselves.
///
/// **Separate from [`SendOptions`] because a batch's options are the ones that are *uniform*.** A
/// topic and an idempotency key belong to a single message and travel on [`Message`]; the fork
/// fan-out is a property of the call and belongs here. Folding the two together would let a caller
/// give one idempotency key to a whole batch, and since the key becomes each row's identity that
/// batch would collide with itself and deliver one message instead of all of them.
///
/// Python and Java draw the line in the same place: a list of `SendMessage` beside a batch-wide
/// `send_to_forks`.
///
/// Built by functional update from [`Default`], as [`SendOptions`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendBulkOptions {
    /// Whether each message also reaches the workflows forked from its destination.
    ///
    /// Defaults to [`Forks::Skip`]: the destinations named, and nothing else.
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
        let err = send::<_, EngineOnly>("wf", &"hello")
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
