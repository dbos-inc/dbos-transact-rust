//! Where a durable call stands, and what that means for its replay.
//!
//! Several calls in this crate are *steps the caller never wrote*: awaiting a workflow's result,
//! waiting on a set of handles, reading an event. Each is a single durable act that a replay must
//! not perform twice, so each takes a step id from the ambient workflow and records its answer
//! under it — which is exactly what makes it a step, and what puts it here beside the ones
//! [`step`](crate::step) builds. What none of them is is a `step` *call*: there is no user body,
//! no retry policy and no timeout, so none of them goes through `step_with`.
//!
//! **Being a step and being a [`PendingStep`] are the same thing.** `step` and `step_with` take
//! their id through [`StepPlacement::here`] at the call; a child's
//! [`start`](crate::WorkflowRef::start) and the await of a handle
//! ([`WorkflowHandle::result`](crate::WorkflowHandle::result)) take theirs through
//! [`StepPlacement::of`] — which is why [`run`](crate::WorkflowRef::run), being the two of them in
//! sequence, claims two ids where it is written rather than wherever it happens to be driven; and
//! `sleep`, the events, the messages, the waits and every checkpointed management call take theirs
//! through [`StepPlacement::of`] at the call and hand it to [`PendingStep::placed`]. Nothing
//! allocates inside its own `async fn`, so any of these may be built first and driven
//! concurrently, with steps and with each other.
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
//! Those four cases and the argument for each are written once and shared by every caller that
//! has the same question. The references keep the same logic in one place for the same reason:
//! Python's `call_function_as_step`, TypeScript's `runInternalStep` and Java's
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
//!
//! # Racing, and what a workflow body may not use
//!
//! `tokio::join!` over durable calls is ordinary code; `tokio::select!` and `tokio::time::timeout`
//! are not, because a race *decides* something and nothing records what it decided. [`PendingStep`]
//! sets out the rule and what to reach for instead.
//!
//! Two calls are kept out of a durable race by their own types:
//! [`PendingStart`](crate::PendingStart) and [`PendingRun`](crate::PendingRun) *create* a workflow,
//! and a race polls in source order and stops at the first branch that is ready — so whether the
//! child exists at all would follow the timing of some other branch. Start outside the race; race
//! what observes the result.
//! [`select_step!`](crate::select_step) pushes each branch onto a set that takes a [`PendingStep`],
//! so a start handed to one is a type error rather than a paragraph ignored. What no type stops is
//! `tokio::select!`, which takes any future — there the prose is what stands between a body and
//! the mistake.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use crate::connection::{Connection, Owner};
use crate::context::Ctx;
use crate::error::{DurableError, Error};
use crate::instance::Executor;
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp};

/// What a durable call works out at the call, before anything of it has run.
///
/// The placement it took — which is its step id, or the reason there is none — and beside it
/// whatever its run will need and could only get here: an executor, a second step id, an encoded
/// payload. `Err` is the refusal the build produced, carried into the call rather than raised, so
/// that `dbos.cancel(id).await?` keeps its single `?`.
///
/// Named because [`PendingStep::placed`] and every producer that calls it spell this type, and a
/// three-deep `Result<(_, _), _>` at each of them says less than the word does.
pub(crate) type Built<C> = Result<(C, StepPlacement), Error>;

/// A durable call that has taken its step id and has not run.
///
/// **The id is taken where the call is written, not where it is first polled.** That is what makes
/// a set of durable calls driven together deterministic: `tokio::join!` builds every branch before
/// polling any, so the ids follow source order — which is the order a replay builds them in again —
/// however the bodies then interleave. An id allocated at the first poll would instead depend on
/// which future reached the counter first, and a replay does not reproduce that.
///
/// Awaiting one runs it: this is a [`Future`], so `step(..).await?` reads as an `async fn` call
/// would.
///
/// **Built and dropped, it has still spent the id**, which is why this is `#[must_use]`. That is
/// deterministic — the same construction sequence burns the same ids on the replay — but it is
/// not a no-op.
///
/// **Polled where it was built, and asked again on every poll.** The id is a claim on one position
/// in one workflow, so a call carried into another workflow, or built outside one and polled
/// inside, is refused as [`Error::StepBuiltElsewhere`] rather than run under an id nothing there
/// can honour. Asking once would not be enough: the run makes that check as its first act and is
/// then past it, and this is `Send` and `Unpin`, so a call polled once where it belongs and then
/// moved would go on running under the context it captured. Being polled is the only moment
/// anything can tell where the call now stands.
///
/// **A workflow body may `join!` these; it may not `select!` over them, and may not wrap one in
/// `tokio::time::timeout`.** The ids are not the problem — those were taken where each call was
/// written, and a replay rebuilds the same ones in the same order, which is the whole point of
/// this type. The problem is that a race **decides** something and nothing records what it
/// decided, so a replay that races again may see the other branch answer first and take a path the
/// first execution never took. A `timeout` is that same race against a clock, and the clock is not
/// replayed either. An all-wait decides nothing, which is why `join!` needs no help.
///
/// Inside a workflow body, reach for these instead: race durable calls with
/// [`select_step!`](crate::select_step), which records which branch won and replays only that one;
/// race workflows with [`select_workflow!`](macro@crate::select_workflow), which records its
/// winner and is the cheaper call where every branch is a workflow's outcome; bound a call with
/// the deadline it already takes — [`get_event`](crate::get_event) and [`recv`](crate::recv) take
/// one, and a whole workflow's is [`StartOptions::timeout`](crate::StartOptions::timeout); and put
/// any other race **inside a step**, whose checkpoint stands for however its body reached the
/// answer. That last is the general rule and the reason the others are narrow: the restriction is
/// on racing *in a workflow body*, not on racing. In a step body, or outside a workflow, the whole
/// of tokio is available.
///
/// **`Unpin`, which is contract rather than accident**: the run is already boxed, so a combinator
/// holding one of these as a branch can do so by `Pin::new(&mut _)` rather than pinning it a
/// second time.
#[must_use = "a durable call that is not awaited never runs, and if it claimed a step id that id \
              is spent; await it, or hand it to a combinator"]
pub struct PendingStep<'a, T, E = crate::EngineOnly> {
    /// What the call is called, which is the name its checkpoint is checked against on replay.
    name: Arc<str>,
    /// Where this call was built, and the id it claimed there — `None` for one that never got
    /// as far as being placed.
    ///
    /// **Kept beside the run rather than only inside it**, because the run can only be asked once:
    /// it checks its placement as its first act and is then past that check for the rest of its
    /// life. A value that is `Send` and `Unpin` can be polled once and then moved, so the check
    /// has to be made by whatever is doing the polling — which is [`poll`](Future::poll), on every
    /// poll. Cheap to hold: a [`Ctx`] is a couple of `Arc`s.
    ///
    /// `None` is for a call that claims no position **anywhere**, which is not the same as
    /// claiming none *here*: a build that failed before it reached the counter
    /// ([`placed`](Self::placed)) never stood in any workflow, so there is nothing for the poll to
    /// hold it to and the run has the better answer to give. The three placements that take no id
    /// are still `Some`, because each of them says *where* — and being carried out of that place
    /// is exactly what they refuse.
    placement: Option<StepPlacement>,
    /// The run, built by the constructor and driven by whatever polls this.
    ///
    /// An `async fn` body does not begin until it is polled, so the future is built where the id
    /// is taken and this field is the whole of what runs.
    running: Pin<Box<dyn Future<Output = crate::Result<T, E>> + Send + 'a>>,
}

impl<'a, T, E> PendingStep<'a, T, E> {
    pub(crate) fn new(
        name: Arc<str>,
        placement: StepPlacement,
        running: impl Future<Output = crate::Result<T, E>> + Send + 'a,
    ) -> Self {
        Self {
            name,
            placement: Some(placement),
            running: Box::pin(running),
        }
    }

    /// A durable call whose placement was decided at the call, running whatever `built` carried.
    ///
    /// **The one constructor every non-step durable call goes through**, so the shape they share
    /// is written once. `built` is what the call worked out before it had run anything: the
    /// placement it took, with whatever the run will need beside it — an executor, a second step
    /// id, an encoded payload — or the error that stopped it getting that far. Polled, it reports
    /// that error and only then hands the placement to `run`.
    ///
    /// **Nothing here allocates.** The id was spent by whoever built `built`, in the caller's own
    /// sequential order, which is the whole point of the exercise: a set of these built and then
    /// driven together takes the same slots on a replay however the futures interleave.
    ///
    /// **It does not check [`check_here`](StepPlacement::check_here) either**, and that is not an
    /// omission. What this returns is a [`PendingStep`], whose [`poll`](Future::poll) asks on
    /// every poll — which is stricter than a run asking once and is the whole reason the check
    /// lives there. A producer that wraps one of these in something else, rather than handing it
    /// back, is the one that has to ask for itself.
    ///
    /// `name` is the step name the call records under, and what a refusal names: the cross-SDK
    /// constant for the library's own calls, and the workflow's own name where the call is a
    /// child start — which is why this takes anything that becomes an [`Arc<str>`] rather than a
    /// `&'static str`.
    pub(crate) fn placed<C, F, Fut>(name: impl Into<Arc<str>>, built: Built<C>, run: F) -> Self
    where
        C: Send + 'a,
        F: FnOnce(C, StepPlacement) -> Fut + Send + 'a,
        Fut: Future<Output = crate::Result<T, E>> + Send + 'a,
        T: 'a,
        E: 'a,
    {
        // Read off the placement rather than passed in, because the placement is where the id
        // already is and two copies of one number are two chances to disagree.
        let placement = built.as_ref().ok().map(|(_, placement)| placement.clone());
        Self {
            name: name.into(),
            placement,
            running: Box::pin(async move {
                // The build's own error, in the caller's channel. Reported at the poll rather
                // than at the call so that `dbos.cancel(id).await?` reads as it always did,
                // with one `?` at the end rather than one at each half.
                let (carried, placement) = built.map_err(Error::lift)?;
                run(carried, placement).await
            }),
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
        self.placement.as_ref().and_then(StepPlacement::step_id)
    }
}

impl<'a, T, E> PendingStep<'a, T, E> {
    /// Retargets this call's error channel through a conversion of the caller's.
    ///
    /// **For a race whose branches fail differently.** The branches of a
    /// [`select_step!`](crate::select_step) must agree on how they fail *before* any of them is
    /// awaited — [`Branches::push`](crate::__private::Branches::push) is what ties them together,
    /// and it runs at the build — so `map_err` on an arm's body is too late. This converts the
    /// call while it is still a value, which is early enough:
    ///
    /// ```ignore
    /// dbos::select_step! {
    ///     charged = billing.result().map_error(Mine::from) => charged?,
    ///     expired = dbos::sleep(deadline) => expired?,
    /// }
    /// ```
    ///
    /// [`lift`](Self::lift) is this with the conversion the compiler can write itself, and is what
    /// to reach for where the channel being left is the engine's. This is the other case: two
    /// application error types, where only the caller knows what one means in terms of the other.
    ///
    /// **The engine's own variants are carried across unchanged** — the conversion sees the
    /// application's error alone, which is the whole of `Error::map_application`'s job, so a
    /// cancellation stays a cancellation and a race still reads it as a control signal rather than
    /// as an outcome.
    ///
    /// **What is recorded is the error the call actually made.** This step writes its own row
    /// before it returns, in its own channel, so the conversion changes what the *caller* sees and
    /// not what the database holds — and a replay reads the original back and converts it again.
    /// A conversion that is a pure function of its input therefore replays identically, which is
    /// what `Fn + Copy` asks for and what a caller should keep to.
    ///
    /// The name and the placement are carried across unchanged, so this is the same call reported
    /// differently: it claims no new id, and the per-poll check still holds it to the workflow it
    /// was built in.
    #[must_use = "a durable call that is not awaited never runs, and if it claimed a step id that \
                  id is spent; await it, or hand it to a combinator"]
    pub fn map_error<F>(self, convert: impl Fn(E) -> F + Copy + Send + 'a) -> PendingStep<'a, T, F>
    where
        T: 'a,
        E: 'a,
        F: 'a,
    {
        let Self {
            name,
            placement,
            running,
        } = self;
        PendingStep {
            name,
            placement,
            running: Box::pin(async move {
                running
                    .await
                    .map_err(|failed| failed.map_application(convert))
            }),
        }
    }
}

impl<'a, T> PendingStep<'a, T, crate::EngineOnly> {
    /// Retargets an engine-channel call into the caller's own error channel.
    ///
    /// **For a race that mixes channels.** The branches of a
    /// [`select_step!`](crate::select_step) must agree on how they fail before any of them is
    /// awaited, so `map_err(Error::lift)` in an arm is too late: it converts the branch's
    /// *output*, where what has to change is its declared type. This converts the call itself,
    /// while it is still a value, which is early enough. The management surface is where it comes
    /// up — those calls answer in the engine's channel and a workflow body rarely does.
    ///
    /// Only from the engine's channel, which is not so much a restriction as the whole reason it
    /// is sound: [`EngineOnly`](crate::EngineOnly) is uninhabited, so there is no application
    /// error to translate and nothing can be lost. Two *different* application error types have no
    /// such conversion, and a race across them is refused — rightly, since it would have no honest
    /// answer for what it returns.
    ///
    /// The name and the placement are carried across unchanged, so this is the same call reported
    /// differently: it claims no new id, and the per-poll check still holds it to the workflow it
    /// was built in.
    #[must_use = "a durable call that is not awaited never runs, and if it claimed a step id that \
                  id is spent; await it, or hand it to a combinator"]
    pub fn lift<F>(self) -> PendingStep<'a, T, F>
    where
        T: 'a,
        F: 'a,
    {
        self.map_error(|impossible| match impossible {})
    }
}

impl<T, E> Future for PendingStep<'_, T, E> {
    type Output = crate::Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // `get_mut` rather than a projection: every field is `Unpin`, the run because it is
        // already a `Pin<Box<_>>`, so there is nothing here for pinning to protect.
        let this = self.get_mut();
        // **Every poll, not only the first.** The run checks its placement as its first act, but
        // it is past that check forever after, and this value is `Send` and `Unpin` — so a call
        // polled once where it belongs and then moved would go on running under the context it
        // captured, recording under a workflow that is no longer the one around it. Being polled
        // is the only moment anything can tell where this call now stands, so the question is
        // asked here and asked again each time.
        //
        // Affordable at that rate because it borrows: `with_current` rather than `current`, so
        // the check is a thread-local read and two pointer comparisons rather than a `Ctx` clone's
        // three atomic increments on refcounts every concurrent step of this workflow shares. The
        // first poll asks twice, once here and once inside the run, which is the price of the run
        // needing the answer rather than merely needing it to be yes.
        //
        // A call with no placement at all is one whose build failed before it could take an id.
        // It claims no position anywhere, so there is nothing here to hold it to, and the run
        // reports the build's own error — which says more than a refusal would.
        if let Some(placement) = &this.placement
            && let Err(refused) = Ctx::with_current(|here| placement.check_here(&this.name, here))
        {
            return Poll::Ready(Err(refused));
        }
        this.running.as_mut().poll(cx)
    }
}

impl<T, E> std::fmt::Debug for PendingStep<'_, T, E> {
    /// The identity, which is all there is to say: the run is an opaque future.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingStep")
            .field("name", &self.name)
            .field("step_id", &self.step_id())
            .finish_non_exhaustive()
    }
}

/// Whether a call is durable where it is being polled, and what to record if it is.
///
/// What [`StepPlacement::check_here`] answers for a call it accepts. Two states rather than an
/// `Option`, because the caller has to handle both and a name is what stops one being forgotten —
/// an unnamed `None` here would quietly mean *run this undurably*. A refusal is not among them:
/// that is the `Err` half.
pub(crate) enum StepDurability<'a> {
    /// Durable: record it, under this workflow and this id.
    ///
    /// Borrowed from the placement, which outlives the call that is asking, so nothing is cloned
    /// to answer a question about where it stands.
    Recorded { ctx: &'a Ctx, step_id: i32 },
    /// Undurable, and rightly so: it claimed no id, and this is a place that expects none of it.
    Plain,
}

/// Rebuilds the error a recorded step failed with.
///
/// The same error, not a description of it: an application failure comes back as its own variant
/// with its own fields, and an engine failure as the variant it was. The only payloads that do not
/// survive are the `serde_json::Error` sources, which arrive absent rather than different.
///
/// Falls back to a plain message when the column does not hold one of ours, which is what a row
/// written by another SDK looks like — its serializer chose its own shape, and the `serialization`
/// column says so. A readable message beats a decode failure standing in for somebody else's error.
pub(crate) fn revive<E: DurableError>(recorded: &str, step: &str) -> Error<E> {
    serde_json::from_str(recorded).unwrap_or_else(|_| Error::StepFailed {
        step: step.to_owned(),
        message: recorded.to_owned(),
    })
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
///
/// `Clone` because a [`PendingStep`] holds one and hands another to its run: the run needs the
/// answer, and the poll needs to ask again. A [`Ctx`] is a couple of `Arc`s, so a copy is cheap
/// and both halves see one placement, never two that could disagree.
#[derive(Clone)]
pub(crate) enum StepPlacement {
    /// Not inside a workflow. Nothing is recorded, and there is no other workflow here for an
    /// awaited one to be confused with.
    Outside,
    /// Inside a step's body, so a plain call by the leaf rule: the step's own checkpoint stands
    /// for everything its body did, and allocating an id inside one would shift every later step
    /// onto the wrong replay slot.
    ///
    /// Carries the body's own [`Ctx`], so a call built here and polled anywhere else is refused
    /// rather than run unrecorded where a checkpoint was expected. Its
    /// [`step_marker`](Ctx::step_marker) is the only thing that can tell the two places apart,
    /// since they share a workflow id, and being process-unique it settles the comparison on its
    /// own. The id the refusal's message needs comes off the same `Ctx`, which is why there is no
    /// copy of it here — the same reason [`Recorded`](Self::Recorded) holds one.
    ///
    /// **Only ever built where that marker is present.** [`here`](Self::here) and [`of`](Self::of)
    /// are the only constructors, and each reaches this arm only for a `Ctx` inside a step body.
    InsideStep { ctx: Ctx },
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
    /// held to the body it was built in.
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
        // Before the connection, because inside a step nothing is checkpointed whoever the
        // connection belongs to, and there is then nothing for the halves below to disagree
        // about. `DBOS::get_event` orders its own two checks the same way.
        if ctx.step_marker().is_some() {
            return Ok(Self::InsideStep { ctx });
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

    /// Where a call served by `executor` stands, with that executor kept beside the placement.
    ///
    /// [`of`](Self::of) with the executor threaded through, which is the pair every
    /// [`PendingStep::placed`] caller needs: the placement says whether to record and under which
    /// id, and the executor is what the run talks to the database through.
    ///
    /// **`executor` is a `Result` so that a refusal upstream of the placement is carried rather
    /// than raised.** [`DBOS::executor`](crate::DBOS) answers [`Error::NotLaunched`] for an
    /// instance that was never launched, and a caller that wrote `dbos.cancel(id).await?` should
    /// meet that at the `?` it already has rather than at a second one the conversion would
    /// otherwise force on it. Threading it through here also fixes the order: the launch is
    /// checked before any id is taken, so a call to an unlaunched instance moves no counter.
    pub(crate) fn taken(
        executor: Result<Arc<Executor>, Error>,
        operation: &'static str,
    ) -> Result<(Arc<Executor>, Self), Error> {
        let executor = executor?;
        let placement = Self::of(executor.connection(), operation)?;
        Ok((executor, placement))
    }

    /// The ambient workflow's connection, or [`Error::NotInWorkflow`] naming the call that
    /// wanted it.
    ///
    /// **What the *free* forms of the library calls stand on.** `send`, `send_bulk`,
    /// `select_workflow` and `join_workflows` have no handle to take an executor from, so being
    /// inside a workflow is the whole of what makes them callable — which is why each has a
    /// [`DBOS`](crate::DBOS) form for everyone else, and why
    /// [`get_event`](crate::get_event)'s free form follows the same rule.
    ///
    /// Here rather than in each of those modules because it is one function: the question of how
    /// a call reaches the executor that will serve it belongs beside [`taken`](Self::taken), which
    /// asks the other half of it.
    ///
    /// `operation` names the caller for the error, which is the only thing that differs between
    /// them.
    pub(crate) fn ambient_connection(operation: &'static str) -> Result<Arc<Connection>, Error> {
        Ctx::current()
            .map(|ctx| Arc::clone(ctx.executor().connection()))
            .ok_or(Error::NotInWorkflow {
                operation: operation.into(),
            })
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
        Ctx::current().map_or(Self::Outside, Self::at)
    }

    /// [`here`](Self::here) for a caller that is already holding the context.
    ///
    /// The same decision, minus the read of the ambient context that `here` makes for itself.
    /// Several library calls refuse from that context before they may place — a call outside a
    /// workflow, or inside a step, or one whose payload will not encode — and the refusals have to
    /// happen before the id is claimed, so those callers hold a [`Ctx`] by the time they get here.
    /// Handing it over means one read rather than two, and means the context that refused and the
    /// context that placed are the same value rather than two reads that agreed.
    ///
    /// `here` is this composed with that read, which is why `Outside` is the only answer it adds.
    pub(crate) fn at(ctx: Ctx) -> Self {
        if ctx.step_marker().is_some() {
            return Self::InsideStep { ctx };
        }
        let step_id = ctx.next_step_id();
        Self::Recorded { ctx, step_id }
    }

    /// One more id from the same counter this placement drew from, or `None` where it drew none.
    ///
    /// **For the calls that are two steps rather than one.** A read waits, so it records the read
    /// and its deadline under consecutive ids — `get_event` and `recv` both — and only the first
    /// of them fits in a placement. Taking the second from here rather than from the ambient
    /// context is what makes the pair one decision: a placement that recorded nothing has no
    /// second id either, so the two can never disagree about whether the call is checkpointed.
    pub(crate) fn next_step_id(&self) -> Option<i32> {
        match self {
            Self::Recorded { ctx, .. } => Some(ctx.next_step_id()),
            Self::Outside | Self::InsideStep { .. } | Self::ClientConnection => None,
        }
    }

    /// The id this call claimed, or `None` where it claimed none.
    pub(crate) fn step_id(&self) -> Option<i32> {
        match self {
            Self::Recorded { step_id, .. } => Some(*step_id),
            Self::Outside | Self::InsideStep { .. } | Self::ClientConnection => None,
        }
    }

    /// How to describe this place in [`Error::StepBuiltElsewhere`].
    ///
    /// Three of the four arms are a [`Ctx`] or the absence of one, so they go through
    /// [`describe`](Self::describe) — the same function the *polled* side of that error uses, so
    /// the two halves of one message cannot end up in different dialects.
    pub(crate) fn whereabouts(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Recorded { ctx, .. } | Self::InsideStep { ctx } => Self::describe(Some(ctx)),
            Self::ClientConnection => "on a client's connection".into(),
            Self::Outside => Self::describe(None),
        }
    }

    /// How to describe the place a call is *standing*, given the context in scope there.
    ///
    /// The counterpart to [`whereabouts`](Self::whereabouts) and deliberately the same three
    /// phrasings: a refusal names where the call was built and where it was polled, and a reader
    /// comparing them should be comparing places rather than wordings. Reads the context rather
    /// than building a placement from it, because placing costs a step id and describing must not.
    pub(crate) fn describe(ctx: Option<&Ctx>) -> std::borrow::Cow<'static, str> {
        match ctx {
            Some(ctx) if ctx.step_marker().is_some() => {
                format!("inside a step of workflow {}", ctx.workflow_id()).into()
            }
            Some(ctx) => format!("in workflow {}", ctx.workflow_id()).into(),
            None => "outside a workflow".into(),
        }
    }

    /// Whether a call built here may be polled where `ambient` is the context in scope, and what
    /// polling it there means.
    ///
    /// **The rule the step id implies, and it lives on the placement because every producer needs
    /// it.** An id is a claim on one position in one workflow, so a call carried somewhere that
    /// cannot honour it is refused rather than run: the alternatives are recording it under the
    /// wrong workflow's id, or running it unrecorded where the surrounding workflow expects a
    /// checkpoint and every replay would run it again. [`step`](crate::step) asks this at the
    /// run, and [`PendingStep`]'s poll asks it for every placed call.
    ///
    /// `step` names the call for [`Error::StepBuiltElsewhere`].
    pub(crate) fn check_here<E>(
        &self,
        step: &str,
        ambient: Option<&Ctx>,
    ) -> crate::Result<StepDurability<'_>, E> {
        match (self, ambient) {
            // The ordinary durable case: built at a step boundary of this workflow, polled at one.
            // No step body may be in scope on either side, or this is a call claimed in the
            // workflow proper and carried *into* a step body, where its checkpoint would sit
            // beneath a step whose own row already covers whatever that body did.
            //
            // By execution identity rather than by workflow id: an id is a position in one
            // counter, and a second instance serving the same workflow, or a second execution of
            // it, has a counter of its own that this id means nothing in. Matching strings would
            // let the run write through the executor it captured while the workflow around it
            // belongs to the other.
            (Self::Recorded { ctx, step_id }, Some(here))
                if here.is_same_execution(ctx) && here.step_marker().is_none() =>
            {
                Ok(StepDurability::Recorded {
                    ctx,
                    step_id: *step_id,
                })
            }
            // Took no id, and is polled in the same step body it was built in. Compared by marker
            // alone: it is process-unique, so equal markers are the same body and therefore the
            // same workflow. The `is_some` is what keeps that true — two absent markers are not a
            // match, they are the workflow proper twice — and it holds by construction.
            (Self::InsideStep { ctx }, Some(here))
                if ctx.step_marker().is_some() && here.step_marker() == ctx.step_marker() =>
            {
                Ok(StepDurability::Plain)
            }
            (Self::Outside, None) => Ok(StepDurability::Plain),
            // **Pinned to nothing, on purpose.** A client's call has no counter anywhere to
            // disagree with and no execution of its own to belong to, so it is the undurable
            // version of itself wherever it is driven — which is the whole reason this is a
            // variant of its own rather than an uncheckpointed one shared with `InsideStep`.
            (Self::ClientConnection, _) => Ok(StepDurability::Plain),
            // Everything else is a claim nobody here can honour.
            //
            // Both places are described the same way, except when both are step bodies: two
            // different bodies of one workflow describe identically, and a refusal reading "built
            // inside a step of workflow w but polled inside a step of workflow w" names one place
            // twice and explains nothing. The marker is what told them apart, so the message says
            // so.
            _ => Err(Error::StepBuiltElsewhere {
                step: step.to_owned(),
                built: self.whereabouts(),
                polled: match ambient {
                    Some(here)
                        if here.step_marker().is_some()
                            && matches!(self, Self::InsideStep { .. }) =>
                    {
                        format!("inside a different step of workflow {}", here.workflow_id()).into()
                    }
                    here => Self::describe(here),
                },
            }),
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

    /// The executor serving this call, where the placement knows one.
    ///
    /// **`Some` wherever there is a workflow**, including inside a step body, where the call is
    /// plain but still has an instance behind it. `None` is [`Outside`](Self::Outside) and
    /// [`ClientConnection`](Self::ClientConnection): the first has no instance in scope, and the
    /// second was reached through a connection this placement never took an executor from.
    ///
    /// **For callers that would otherwise read the ambient context twice** — once for the
    /// executor and once through [`here`](Self::here) for the placement. Taking both from one
    /// value is a clone cheaper, and it is what makes "an executor and an id, or neither" hold by
    /// construction rather than because two reads of the same task-local agreed.
    pub(crate) fn executor(&self) -> Option<&Arc<Executor>> {
        match self {
            Self::Recorded { ctx, .. } | Self::InsideStep { ctx } => Some(ctx.executor()),
            Self::Outside | Self::ClientConnection => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineOnly;
    use crate::context::StepStatus;
    use tokio_util::sync::CancellationToken;

    /// A launched instance and three contexts of one workflow: the workflow proper, and one inside
    /// each of two different step bodies.
    ///
    /// A real `Ctx` needs a real `Executor`, which needs a database — but nothing below writes a
    /// row or reads one. [`StepPlacement::check_here`] is a decision about two contexts, so these
    /// are the whole of its input, and the placements are built by hand rather than by `here`,
    /// which would spend a step id per case.
    ///
    /// **Two step bodies rather than one**, because the marker's *value* is what tells them apart
    /// and only a second body can show that it does. Everything else the rule asks is answered by
    /// the marker merely being there.
    async fn contexts() -> (Ctx, Ctx, Ctx, crate::DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..crate::Config::new("placement-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        let proper = Ctx::new(dbos.executor("test").expect("launched"), "wf", None);
        // The only way to hold one: the marker is bound by the scope, so the body reads it back
        // out. What a step body's own calls see.
        let body = || {
            proper.in_step_scope(CancellationToken::new(), StepStatus::first(0), async {
                Ctx::current().expect("inside the scope")
            })
        };
        let in_step = body().await;
        let sibling = body().await;
        assert_ne!(
            in_step.step_marker(),
            sibling.step_marker(),
            "each entry into a step body is a body of its own"
        );
        (proper, in_step, sibling, dbos, db)
    }

    fn durability(outcome: &crate::Result<StepDurability<'_>, EngineOnly>) -> &'static str {
        match outcome {
            Ok(StepDurability::Recorded { .. }) => "recorded",
            Ok(StepDurability::Plain) => "plain",
            Err(Error::StepBuiltElsewhere { .. }) => "refused",
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    /// Every combination of where a call was built and where it is polled, in one place.
    ///
    /// The table is the point: the rule is a decision over two contexts and reading it as a table
    /// is how a missing arm shows. Three accept and the rest refuse, which is the sentence the
    /// error's own documentation makes.
    #[tokio::test]
    async fn a_call_is_durable_only_where_its_id_means_something() {
        let (proper, in_step, sibling, dbos, _db) = contexts().await;
        let other = Ctx::new(dbos.executor("test").expect("launched"), "wf-other", None);
        // The same workflow *id* under a second context, which is what a recovery re-run is. Its
        // step counter starts from zero again, so an id from the first execution means nothing in
        // it — and comparing ids rather than execution identity is exactly what would miss that.
        let rerun = Ctx::new(dbos.executor("test").expect("launched"), "wf", None);
        let recorded = StepPlacement::Recorded {
            ctx: proper.clone(),
            step_id: 0,
        };
        let inside = StepPlacement::InsideStep {
            ctx: in_step.clone(),
        };

        for (built, polled, expected, why) in [
            (
                &recorded,
                Some(&proper),
                "recorded",
                "built and polled at a step boundary",
            ),
            (
                &recorded,
                Some(&in_step),
                "refused",
                "carried into a step body",
            ),
            (&recorded, None, "refused", "carried out of the workflow"),
            (
                &recorded,
                Some(&other),
                "refused",
                "carried into another workflow",
            ),
            (
                &recorded,
                Some(&rerun),
                "refused",
                "carried into a second execution of the same workflow id",
            ),
            (&inside, Some(&in_step), "plain", "the body it was built in"),
            (
                &inside,
                Some(&proper),
                "refused",
                "escaped to the workflow proper",
            ),
            (&inside, None, "refused", "escaped the workflow"),
            // The one row the marker's *value* decides. Every other case here turns on the marker
            // being present or absent, so a rule comparing only presence would pass them all.
            (
                &inside,
                Some(&sibling),
                "refused",
                "carried into another step's body",
            ),
            (
                &StepPlacement::Outside,
                None,
                "plain",
                "no workflow either side",
            ),
            (
                &StepPlacement::Outside,
                Some(&proper),
                "refused",
                "built before the workflow",
            ),
            (
                &StepPlacement::Outside,
                Some(&in_step),
                "refused",
                "built before the step",
            ),
            // Pinned to nothing: a client has no counter anywhere to disagree with.
            (
                &StepPlacement::ClientConnection,
                None,
                "plain",
                "a client's, outside",
            ),
            (
                &StepPlacement::ClientConnection,
                Some(&proper),
                "plain",
                "a client's, in a workflow",
            ),
            (
                &StepPlacement::ClientConnection,
                Some(&in_step),
                "plain",
                "a client's, in a step",
            ),
        ] {
            assert_eq!(
                durability(&built.check_here::<EngineOnly>("call", polled)),
                expected,
                "{why}: built {}, polled {}",
                built.whereabouts(),
                StepPlacement::describe(polled)
            );
        }

        dbos.shutdown().await;
    }

    /// A refusal names two places, and it has to name them the same way.
    ///
    /// Both halves come from one describer, so this is what says they still do.
    #[tokio::test]
    async fn a_refusal_names_where_it_was_built_and_where_it_is_polled() {
        let (proper, in_step, sibling, dbos, _db) = contexts().await;
        let built_inside = StepPlacement::InsideStep {
            ctx: in_step.clone(),
        };

        let Err(Error::<EngineOnly>::StepBuiltElsewhere {
            step,
            built,
            polled,
        }) = built_inside.check_here("call", Some(&proper))
        else {
            panic!("a call that left its step body is refused");
        };
        assert_eq!(step, "call");
        assert_eq!(built, "inside a step of workflow wf");
        assert_eq!(polled, "in workflow wf");
        // The same context, described from the placement and from the poll site. One function
        // answers both, and this is what says it still does.
        assert_eq!(
            built_inside.whereabouts(),
            StepPlacement::describe(Some(&in_step)),
            "a place should read the same whichever side of the refusal names it",
        );

        // Two step bodies of one workflow describe identically, so the refusal that tells them
        // apart has to say which one it means. Without this the message reads "built inside a step
        // of workflow wf but polled inside a step of workflow wf", which names one place twice.
        let Err(Error::<EngineOnly>::StepBuiltElsewhere { built, polled, .. }) =
            built_inside.check_here("call", Some(&sibling))
        else {
            panic!("a call carried into another step's body is refused");
        };
        assert_eq!(built, "inside a step of workflow wf");
        assert_eq!(polled, "inside a different step of workflow wf");

        dbos.shutdown().await;
    }
}
