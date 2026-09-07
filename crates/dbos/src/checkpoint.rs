//! Where a durable call stands, and what that means for its replay.
//!
//! Several calls in this crate are *steps the caller never wrote*: awaiting a workflow's result,
//! waiting on a set of handles, reading an event. Each is a single durable act that a replay must
//! not perform twice, so each takes a step id from the ambient workflow and records its answer
//! under it — which is exactly what makes it a step, and what puts it here beside the ones
//! [`step`](crate::step) builds. What none of them is is a `step` *call*: there is no user body,
//! no retry policy and no timeout, so none of them goes through `step_with`.
//!
//! **Being a step and being a [`PendingStep`] are not yet the same thing.** Today only `step` and
//! `step_with` build one, taking their id through [`StepPlacement::here`] at the call. Every
//! library step above still allocates inside its own `async fn` — through
//! [`StepPlacement::of`] for the awaits and waits, and straight from the counter for `sleep`,
//! the events, the messages, a child `start` and the management surface — so its id lands
//! wherever it is first *polled*. That is the difference this module exists to close, one
//! producer at a time; until it is closed, only steps may be driven concurrently, and the
//! [`step`](crate::step) docs say so.
//!
//! What they share is not the recording but the **decision of whether to record at all**, and that
//! decision is subtle enough to be worth having in one place:
//!
//! - Outside a workflow there is no step counter, so nothing is checkpointed and the call is
//!   plain. That is the operator's case, and it is not an error.
//! - Inside a *step* nothing is checkpointed either, by the leaf rule the whole crate follows:
//!   the step's own checkpoint stands for everything its body did, and allocating an id inside one
//!   would shift every later step onto the wrong replay slot.
//! - Inside a workflow at a step boundary the call is a step of that workflow.
//! - Reached through a handle whose connection is not the ambient workflow's, the answer depends
//!   on *whose* connection it is — a [`Client`](crate::Client)'s degrades to the plain call, a
//!   second application instance's is [`Error::WrongInstance`].
//!
//! Those four cases and the argument for each were written once, for
//! [`WorkflowHandle::result`](crate::WorkflowHandle::result), and are now shared by every caller
//! that has the same question. The references keep the same logic in one place for the same
//! reason: Python's `call_function_as_step`, TypeScript's `runInternalStep` and Java's
//! `runDbosFunctionAsStep` are each one wrapper that every library step goes through.
//!
//! **What is *not* shared is the write**, and deliberately. A child await records the awaited id
//! alongside the outcome and reads it back through
//! [`check_child_result`](crate::sysdb::SystemDatabase::check_child_result); the management surface
//! commits its checkpoint inside the same transaction as the operation it records; a wait over
//! handles records a plain value. Each knows what its own row means. This module owns only the
//! placement and the generic value-shaped write, which is what the callers with nothing special to
//! say reach for.
//!
//! It also owns [`PendingStep`] itself, the value such a call hands back once it has taken its step
//! id and before it has run. That belongs here rather than beside any one producer because the id
//! and the rule it implies — *polled where it was built* — are the same whichever call took it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use crate::connection::{Connection, Owner};
use crate::context::{Ctx, StepMarker};
use crate::error::Error;
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp};

/// A durable call that has taken its step id and has not run.
///
/// **The id is taken where the call is written, not where it is first polled.** That is what makes
/// a set of durable calls driven together deterministic: `tokio::join!` builds every branch before
/// polling any, so the ids follow source order — which is the order a replay builds them in again —
/// however the bodies then interleave. An id allocated at the first poll would instead depend on
/// which future reached the counter first, and a replay does not reproduce that.
///
/// Awaiting one runs it. This is a [`Future`], so `step(..).await?` reads exactly as it did when
/// [`step`](crate::step) was an `async fn`, and not one call site had to change.
///
/// **Built and dropped, it has still spent the id**, which is why this is `#[must_use]`. That is
/// deterministic — the same construction sequence burns the same ids on the replay — but it is no
/// longer the no-op it was when the id was taken at the first poll.
///
/// **Polled where it was built.** The id is a claim on one position in one workflow, so a call
/// carried into another workflow, or built outside one and polled inside, is refused as
/// [`Error::StepBuiltElsewhere`] rather than run under an id nothing there can honour. Each
/// producer makes that check inside its own run, which is why nothing here knows how.
///
/// **`Unpin`, which is contract rather than accident**: the run is already boxed, so a combinator
/// holding one of these as a branch can do so by `Pin::new(&mut _)` rather than pinning it a
/// second time.
#[must_use = "a durable call that is not awaited has spent its step id without running; await it, \
              or hand it to a combinator"]
pub struct PendingStep<'a, T, E = crate::EngineOnly> {
    /// What the call is called, which is the name its checkpoint is checked against on replay.
    name: Arc<str>,
    /// The id this call claimed when it was built, or `None` where it claimed none.
    step_id: Option<i32>,
    /// The run, built by the constructor and driven by whatever polls this.
    ///
    /// An `async fn` body does not begin until it is polled, so the future is built where the id
    /// is taken and this field is the whole of what runs. The two above it are identity, not
    /// state: nothing reads them to decide what happens, and nothing mutates them.
    running: Pin<Box<dyn Future<Output = crate::Result<T, E>> + Send + 'a>>,
}

impl<'a, T, E> PendingStep<'a, T, E> {
    pub(crate) fn new(
        name: Arc<str>,
        step_id: Option<i32>,
        running: impl Future<Output = crate::Result<T, E>> + Send + 'a,
    ) -> Self {
        Self {
            name,
            step_id,
            running: Box::pin(running),
        }
    }

    /// What this call is called — the name its checkpoint is checked against on replay.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The id this call claimed when it was built, or `None` if it claimed none.
    ///
    /// `None` is not a failure. Outside a workflow there is no counter to draw from, and a step
    /// built inside another step is a plain call by the leaf rule.
    ///
    /// **Readable here because the run cannot be asked.** Once the call is a future the id is
    /// sealed inside it, and the callers that need to *name* one are all outside it — a `Debug`
    /// that says something, and a combinator reporting which branch a stale checkpoint meant.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.step_id
    }
}

impl<T, E> Future for PendingStep<'_, T, E> {
    type Output = crate::Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // `get_mut` rather than a projection: every field is `Unpin`, the run because it is
        // already a `Pin<Box<_>>`, so there is nothing here for pinning to protect.
        self.get_mut().running.as_mut().poll(cx)
    }
}

impl<T, E> std::fmt::Debug for PendingStep<'_, T, E> {
    /// The identity, which is all there is to say: the run is an opaque future.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingStep")
            .field("name", &self.name)
            .field("step_id", &self.step_id)
            .finish_non_exhaustive()
    }
}

/// Where a durable call stands: which of the workflow's step ids it occupies, if any.
///
/// **One type for both kinds of step.** A *user step* is what [`step`](crate::step) and
/// [`step_with`](crate::step_with) build: a body the caller wrote, a retry policy, a timeout. A
/// *library step* is one this crate writes on the caller's behalf — awaiting a workflow's result,
/// waiting on a set of handles, reading or setting an event, a checkpointed management call. They
/// differ in what runs and in nothing that matters here: each occupies one step id, records its
/// answer under it, and must not be performed twice by a replay. So both ask this question, and
/// take their answer from [`here`](Self::here) or [`of`](Self::of).
///
/// Decides two independent things: whether the call is checkpointed, and whether there is a
/// surrounding workflow that a *cancelled awaited workflow* would otherwise be confused with.
pub(crate) enum StepPlacement {
    /// Not inside a workflow. Nothing is recorded, and there is no other workflow here for an
    /// awaited one to be confused with.
    Outside,
    /// Inside a step's body, so a plain call by the leaf rule: the step's own checkpoint stands
    /// for everything its body did, and allocating an id inside one would shift every later step
    /// onto the wrong replay slot.
    ///
    /// Carries *which* body, so a call built here and polled anywhere else is refused rather than
    /// run unrecorded where a checkpoint was expected. The marker is the only thing that can tell
    /// those two places apart, since they share a workflow id; `workflow_id` is carried for the
    /// refusal's message and never for the comparison, which a process-unique marker settles on
    /// its own.
    InsideStep {
        workflow_id: String,
        marker: StepMarker,
    },
    /// Reached through a [`Client`](crate::Client)'s connection. A client has no step counter to
    /// agree with this workflow's, and no execution of its own that a recorded call could belong
    /// to, so the call degrades to the undurable version of itself — which is what
    /// `Client::enqueue` documents and what `Client::get_event` already does for the read.
    ///
    /// A handle from *another instance* is not this case: that is [`Error::WrongInstance`],
    /// because two instances each have a counter and the caller meant one of them.
    ///
    /// **A variant of its own rather than sharing an uncheckpointed one with
    /// [`InsideStep`](Self::InsideStep)**, because the two want opposite treatment. A client's
    /// call is legitimately driven from anywhere and nothing pins it; an in-step call has to be
    /// held to the body it was built in. Conflating them is how the second went unchecked.
    ClientConnection,
    /// Inside a workflow at a step boundary: this call is a step of that workflow.
    ///
    /// Holds the [`Ctx`] rather than a copy of its workflow id, because a user step needs it to
    /// run the body under, and every reader of the id can take it from here. Only reachable when
    /// the serving connection *is* this workflow's, which is what makes that sound.
    Recorded { ctx: Ctx, step_id: i32 },
}

impl StepPlacement {
    /// Where a call reached through `conn` stands, allocating the step id if it is to be recorded.
    ///
    /// **Allocating is the point, and it happens here rather than at the write**: the position of
    /// this call in the workflow has to be the same on the replay as it was on the run, so the id
    /// is taken before anything that could fail and before the check it gates.
    ///
    /// `operation` names the call for [`Error::WrongInstance`], which is the one error this can
    /// return.
    pub(crate) fn of(conn: &Arc<Connection>, operation: &'static str) -> Result<Self, Error> {
        let Some(ctx) = Ctx::current() else {
            return Ok(Self::Outside);
        };
        // First, because inside a step nothing is checkpointed whoever the connection belongs to,
        // and there is then nothing for the halves below to disagree about. `DBOS::get_event`
        // orders its own two checks the same way.
        if let Some(marker) = ctx.step_marker() {
            return Ok(Self::InsideStep {
                workflow_id: ctx.workflow_id().to_owned(),
                marker,
            });
        }
        // Where the two halves would be combined: a step id is about to come from this workflow's
        // counter while the write goes through the caller's own connection. What that means
        // depends on whose connection it is, and it is the one question [`Owner`] exists for.
        //
        // The comparison is of *databases* rather than of executors, because the database is what
        // the two halves would disagree about, and it is the thing a connection from a
        // [`Client`](crate::Client) has in common with one from a running executor.
        if !Arc::ptr_eq(ctx.executor().connection(), conn) {
            return match conn.owner() {
                // **A client's is a plain call, not a refusal.** There is no second counter here
                // to have meant instead — a client has none — so the operation degrades to the
                // undurable version of itself, which is what `Client::enqueue` documents about a
                // client used from inside a workflow body and what `Client::get_event` already
                // does for the read.
                Owner::Client => Ok(Self::ClientConnection),
                // **Another instance's is the mistake the variant was raised for.** Both
                // instances have a step counter, the caller meant one of them, and the record
                // would land where the workflow that allocated the id cannot see it. Refused
                // rather than quietly downgraded, because a second instance in a process is
                // nearly always a wiring error and this is the only place it shows.
                Owner::Application => Err(Error::WrongInstance {
                    operation: operation.into(),
                }),
            };
        }
        let step_id = ctx.next_step_id();
        Ok(Self::Recorded { ctx, step_id })
    }

    /// Where a call served by the ambient workflow's own executor stands.
    ///
    /// [`of`](Self::of) with no second connection to disagree with, which is every *user* step:
    /// [`step`](crate::step) is always served by the workflow it is written in, so
    /// [`ClientConnection`](Self::ClientConnection) is unreachable and
    /// [`Error::WrongInstance`] cannot arise. That is the whole of why this cannot fail where
    /// `of` can.
    ///
    /// Allocating is the point, and it happens here rather than at the poll: the position of this
    /// call has to be the same on the replay as it was on the run, and building is what fixes it.
    pub(crate) fn here() -> Self {
        let Some(ctx) = Ctx::current() else {
            return Self::Outside;
        };
        if let Some(marker) = ctx.step_marker() {
            return Self::InsideStep {
                workflow_id: ctx.workflow_id().to_owned(),
                marker,
            };
        }
        let step_id = ctx.next_step_id();
        Self::Recorded { ctx, step_id }
    }

    /// The id this call claimed, or `None` where it claimed none.
    pub(crate) fn step_id(&self) -> Option<i32> {
        match self {
            Self::Recorded { step_id, .. } => Some(*step_id),
            Self::Outside | Self::InsideStep { .. } | Self::ClientConnection => None,
        }
    }

    /// How to describe this place in [`Error::StepBuiltElsewhere`].
    pub(crate) fn whereabouts(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Recorded { ctx, .. } => format!("in workflow {}", ctx.workflow_id()).into(),
            Self::InsideStep { workflow_id, .. } => {
                format!("inside a step of workflow {workflow_id}").into()
            }
            Self::ClientConnection => "on a client's connection".into(),
            Self::Outside => "outside a workflow".into(),
        }
    }

    /// Whether there is a surrounding workflow — true wherever a cancelled *awaited* workflow has
    /// to be distinguished from this caller being cancelled.
    pub(crate) fn inside_a_workflow(&self) -> bool {
        !matches!(self, Self::Outside)
    }

    /// The workflow and step id this call is recorded under, if it is recorded at all.
    pub(crate) fn step(&self) -> Option<(&str, i32)> {
        match self {
            Self::Recorded { ctx, step_id } => Some((ctx.workflow_id(), *step_id)),
            _ => None,
        }
    }

    /// Reads back what this call recorded, if this workflow has run this far before.
    ///
    /// `check_step` compares the recorded name, so a call whose position now holds some other
    /// step is already [`Error::UnexpectedStep`](crate::sysdb::Error::UnexpectedStep) before this
    /// returns — which is the determinism check, not an incidental one.
    ///
    /// **Named for the layer below rather than for this type.** `check`/[`record`](Self::record)
    /// is the pair `sysdb` already uses — `check_step`/`record_step`,
    /// `check_child_result`/`record_child_result` — and this is the thin wrapper over the first of
    /// them. It also keeps the word `Recorded` meaning one thing: it is a
    /// [`StepPlacement`] variant, describing where the caller stands, and a method of the same name
    /// describing a step row would be the same word for two unrelated things.
    pub(crate) async fn check(
        &self,
        conn: &Connection,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let Some((workflow_id, step_id)) = self.step() else {
            return Ok(None);
        };
        conn.sysdb()
            .check_step(workflow_id, step_id, step_name)
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Records what this call answered, so [`check`](Self::check) finds it.
    ///
    /// `started_at` is when the operation began rather than when it finished being written down,
    /// so a workflow's timeline shows the wait. It is paired with a completion stamped here, and
    /// the pair is what makes the write idempotent for this caller and a conflict for a rival —
    /// see [`StepTiming::completed_at`].
    ///
    /// A no-op where nothing is checkpointed, so a caller need not branch: the placement already
    /// decided, and repeating the decision at every call site is how the two halves drift apart.
    ///
    /// Pairs with [`check`](Self::check), as `record_step` pairs with `check_step`.
    pub(crate) async fn record(
        &self,
        conn: &Connection,
        step_name: &str,
        outcome: Outcome<'_>,
        started_at: Timestamp,
    ) -> Result<(), Error> {
        let Some((workflow_id, step_id)) = self.step() else {
            return Ok(());
        };
        conn.sysdb()
            .record_step(
                workflow_id,
                step_id,
                step_name,
                outcome,
                Some(conn.serializer().name()),
                Some(StepTiming {
                    started_at,
                    completed_at: Timestamp::now(),
                }),
            )
            .await
            .map_err(Error::SystemDatabase)
    }
}
