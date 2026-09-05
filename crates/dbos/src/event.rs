//! Workflow events: a key/value a workflow publishes and anyone may read.
//!
//! Four surfaces, split by where the caller stands. [`set_event`] and [`get_event`] are free
//! functions like [`step`](crate::step), callable only from inside a workflow: they take the
//! executor and the step-id sequence from the ambient context, so a workflow body needs no
//! handle to anything. [`DBOS::get_event`] is the instance method for the other reader — an HTTP
//! handler polling for progress, outside any workflow, where there is nothing ambient to take an
//! executor from — and [`Client::get_event`](crate::Client::get_event) is that same reader from
//! outside the application altogether, where there is not even an instance.
//!
//! **The free reader is not just symmetry.** The registry lives on the instance, so a registered
//! closure that captures a [`DBOS`] is stored inside the very `Arc` it holds a strong reference
//! to: a cycle, which keeps the instance, its executor and its connection pool alive for the life
//! of the process. Making a workflow capture a handle in order to read an event would have made
//! that the documented way to write one.
//!
//! All three are thin: `sysdb` owns the transactional write, the replay skip, the blocking read,
//! and the cross-SDK step names (`DBOS.setEvent`, `DBOS.getEvent`). What the engine adds is the
//! step ids from the ambient context, the payload encoding, and the guards on where each call may
//! stand.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::DBOS;
use crate::checkpoint::{Pending, Placement};
use crate::connection::Connection;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::instance::Executor;
use crate::serialization::{decode, encode};
use crate::sysdb::types::{GetEventCaller, step_names};

/// Publishes a key/value on the current workflow, for anyone to read.
///
/// Setting the same key again replaces the value, which is what makes it a progress channel: the
/// starter app publishes the number of completed steps under one key after each step, and a
/// reader polling [`DBOS::get_event`] sees the latest.
///
/// The write is checkpointed under a step id, so a replay does not publish again — `sysdb` sees
/// the recorded step and skips, in the same transaction that would have written. The error is the
/// *workflow's* channel, like [`step`](crate::step)'s, so `?` needs no conversion.
///
/// Only from inside a workflow, and not from inside a step. Outside a workflow there is no step
/// sequence to checkpoint against, and unlike a step there is no plain version of a durable write
/// to degrade to; inside a step, allocating an id would shift every later step onto the wrong
/// replay slot.
pub fn set_event<'a, T, E>(key: &'a str, value: &T) -> Pending<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    // The checks and the encoding come before the id is taken, so a refused or unencodable write
    // moves nothing: a workflow fixed to pass it replays onto the slot it would have recorded.
    // **The id itself is taken here, at the call** — see [`Pending`].
    let built = match Ctx::current() {
        None => Err(Error::NotInWorkflow {
            operation: "set_event".into(),
        }),
        Some(ctx) if ctx.in_step() => Err(Error::InsideStep {
            operation: "set_event".into(),
        }),
        Some(ctx) => encode(value, "event value").map(|encoded| {
            let placement = Placement::Recorded {
                workflow_id: ctx.workflow_id().to_owned(),
                step_id: ctx.next_step_id(),
            };
            ((Arc::clone(ctx.executor()), encoded), placement)
        }),
    };
    Pending::placed(
        step_names::SET_EVENT,
        built,
        move |(executor, encoded), placement| async move {
            let (workflow_id, step_id) = placement
                .step()
                .expect("a set_event that was built is a recorded one");
            executor
                .sysdb()
                .set_event(
                    workflow_id,
                    step_id,
                    key,
                    &encoded,
                    Some(executor.serializer().name()),
                )
                .await
                .map_err(Error::SystemDatabase)?;
            Ok(())
        },
    )
}

/// Reads a key a workflow published, waiting up to `timeout` for it to appear.
///
/// The reader for a workflow body, and the counterpart of [`set_event`]: it takes the executor
/// from the ambient context, so a workflow that reads an event needs no [`DBOS`] handle and its
/// registered closure captures nothing. That is what keeps the registry free of strong references
/// back to the instance holding it — see the module documentation.
///
/// `Ok(None)` means the key was not there when the deadline passed — absence is a value, not an
/// error, and `Duration::ZERO` makes this a poll: look once, do not wait.
///
/// The read is checkpointed as two steps (the read and its deadline), so a replay returns what the
/// first run saw, including a timeout's `None`, instead of waiting again. **Both ids are taken at
/// the call, not at the first poll** — see [`Pending`] — so a read built beside a step and driven
/// with it takes the same slots on every execution. The error is the
/// *workflow's* channel, like [`step`](crate::step)'s and [`set_event`]'s, so `?` needs no
/// conversion. From inside a *step* it reads plainly with no checkpoint, the step's own checkpoint
/// standing for everything its body did — the same leaf rule as a nested step.
///
/// Outside a workflow there is no context to read, so this is [`Error::NotInWorkflow`]. That is
/// where [`DBOS::get_event`] is the call.
pub fn get_event<'a, T, E>(
    workflow_id: &'a str,
    key: &'a str,
    timeout: Duration,
) -> Pending<'a, Option<T>, E>
where
    T: DeserializeOwned + 'a,
    E: DurableError + 'a,
{
    let ctx = Ctx::current();
    let executor = ctx
        .as_ref()
        .map(|ctx| Arc::clone(ctx.executor()))
        .ok_or(Error::NotInWorkflow {
            operation: "get_event".into(),
        });
    get_event_at(executor, ctx.as_ref(), workflow_id, key, timeout)
}

impl DBOS {
    /// Reads a key a workflow published, waiting up to `timeout` for it to appear.
    ///
    /// The reader for code outside a workflow — the natural caller, an HTTP handler asking how far
    /// a workflow has got — where there is nothing ambient to take an executor from. **Inside a
    /// workflow, reach for the free [`get_event`] instead**: it needs no handle, so the closure a
    /// workflow is registered as captures nothing, and a captured [`DBOS`] is a cycle with the
    /// registry that holds the closure.
    ///
    /// `Ok(None)` means the key was not there when the deadline passed — absence is a value, not
    /// an error, and `Duration::ZERO` makes this a poll: look once, do not wait.
    ///
    /// Called from inside a workflow anyway, it behaves as the free function does — the read is
    /// checkpointed as two steps, and a read from inside a step is plain — except that it reports
    /// in the engine's own error channel, so a workflow with its own error type carries the result
    /// over with [`Error::lift`].
    ///
    /// With one exception it cannot share: this takes its executor from `self` and its step ids
    /// from the ambient context, so a handle to some *other* instance would split the two. That is
    /// [`Error::WrongInstance`] rather than a silent write into the wrong database.
    pub fn get_event<'a, T>(
        &'a self,
        workflow_id: &'a str,
        key: &'a str,
        timeout: Duration,
    ) -> Pending<'a, Option<T>>
    where
        T: DeserializeOwned + 'a,
    {
        let ctx = Ctx::current();
        get_event_at(
            self.executor("get_event"),
            ctx.as_ref(),
            workflow_id,
            key,
            timeout,
        )
    }
}

impl crate::Client {
    /// Reads a key a workflow published, waiting up to `timeout` for it to appear.
    ///
    /// **The reader a client is built for.** An application publishes progress under a key with
    /// [`set_event`] and anything outside it — an HTTP handler answering "how far along is my
    /// order?", a test waiting for a workflow to reach a known point — reads it here. Nothing
    /// about it is checkpointed, because a client has no workflow to checkpoint against and no
    /// replay to protect: the read is exactly one wait on the database.
    ///
    /// `Ok(None)` means the key was not there when the deadline passed — absence is a value, not
    /// an error, and `Duration::ZERO` makes this a poll: look once, do not wait.
    ///
    /// The wait is woken by a notification rather than polled, when the client was connected with
    /// [`ClientConfig::use_listen_notify`](crate::ClientConfig::use_listen_notify) left on.
    pub async fn get_event<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        // No caller, and there is no case where there could be one: a client is not a workflow, so
        // unlike `DBOS::get_event` there is no ambient context to reconcile with this handle's
        // executor and no `WrongInstance` to refuse.
        self.connection()
            .get_event(workflow_id, key, timeout, None)
            .await
    }
}

/// The read as a [`Pending`], both of its step ids taken here at the call.
///
/// That is what lets a read be built beside a step and driven together — `tokio::join!` over the
/// two, or a [`select_step!`](crate::select_step) branch — and take the same slots on a replay.
///
/// A read is checkpointed as two steps — the read and its deadline — so a recorded placement
/// takes a second id straight after its own, from the same counter: `ctx` is the context the
/// placement was decided against. Id order is the contract: the read's first, the deadline's
/// second, matching what every SDK records and what a replay looks up.
fn get_event_at<'a, T, E>(
    executor: Result<Arc<Executor>>,
    ctx: Option<&Ctx>,
    workflow_id: &'a str,
    key: &'a str,
    timeout: Duration,
) -> Pending<'a, Option<T>, E>
where
    T: DeserializeOwned + 'a,
    E: 'a,
{
    let built = Placement::taken(executor, "get_event").map(|(executor, placement)| {
        let timeout_step_id = placement.step_id().and(ctx).map(|ctx| ctx.next_step_id());
        ((executor, timeout_step_id), placement)
    });
    Pending::placed(
        step_names::GET_EVENT,
        built,
        move |(executor, timeout_step_id), placement| async move {
            let caller = placement.step().zip(timeout_step_id).map(
                |((workflow_id, step_id), timeout_step_id)| GetEventCaller {
                    workflow_id,
                    step_id,
                    timeout_step_id,
                },
            );
            executor
                .connection()
                .get_event(workflow_id, key, timeout, caller)
                .await
        },
    )
}

impl Connection {
    /// The read itself, shared by every surface that has one.
    ///
    /// `sysdb` owns the blocking wait and the replay skip, so what is left here is decoding what it
    /// found. Generic over the caller's error channel for the same reason [`decode`] is: the
    /// failure is an engine variant either way, and `E` only says which channel it travels in.
    ///
    /// On the connection because a read is all it is: the free [`get_event`] reaches it through the
    /// ambient context's, [`DBOS::get_event`] through its executor's, and
    /// [`Client::get_event`](crate::Client::get_event) through the only one it has. Named as they
    /// are, which is what this crate's other shared internals do — `Connection::register_queue`,
    /// `queue`, `list_queues`, `update_queue`, `delete_queue` all carry their surface's name. What
    /// each surface adds is the *caller*: which ambient context to read, whether the read is
    /// checkpointed, and whether a handle's executor has to be reconciled with it.
    pub(crate) async fn get_event<T: DeserializeOwned, E>(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
        caller: Option<GetEventCaller<'_>>,
    ) -> Result<Option<T>, E> {
        match self
            .sysdb()
            .get_event(workflow_id, key, timeout, caller)
            .await
            .map_err(Error::SystemDatabase)?
        {
            None => Ok(None),
            Some(found) => decode(Some(&found.value), "event value").map(Some),
        }
    }
}
