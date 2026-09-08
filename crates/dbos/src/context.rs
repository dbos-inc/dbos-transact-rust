//! The ambient workflow context.
//!
//! A workflow function is an ordinary `async fn` taking its own arguments and nothing else. What
//! makes it durable — which workflow it is, and which step comes next — travels in a
//! [`tokio::task_local!`], set for the duration of the body and unset outside it. Python's
//! `ContextVar`, TypeScript's `AsyncLocalStorage` and Java's thread-locals are the same mechanism;
//! Go is the outlier that threads a parameter, and its own starter application is the argument
//! against copying it.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

use tokio_util::sync::CancellationToken;

use crate::instance::Executor;
use crate::sysdb::types::Timestamp;

tokio::task_local! {
    /// Set while a workflow body runs, and read by everything the body calls.
    static CURRENT: Ctx;
}

/// What a running workflow knows about itself.
///
/// Cheap to clone: two `Arc`s. Cloning it does not make a second workflow — the clone shares the
/// same step counter, which is the point, because a step allocated through either must not reuse
/// an id allocated through the other.
///
/// **Internal.** Everything a workflow may ask about itself is a free function —
/// [`workflow_id`](crate::workflow_id), [`step_id`](crate::step_id) and
/// [`cancellation`](crate::cancellation) — so user code never names this type or reaches through
/// it. That keeps the ambient context an implementation detail: the questions are stable API, the
/// thing that answers them is not.
#[derive(Clone)]
pub(crate) struct Ctx {
    executor: Arc<Executor>,
    workflow: Arc<WorkflowState>,
    /// Which step body this context is inside, or `None` in the workflow proper.
    ///
    /// A step is a leaf: the checkpoint it writes stands for everything the body did, so a step
    /// inside a step is a plain call. Without this the inner call would allocate a step id of its
    /// own and every step after it would replay against the wrong slot — a correctness trap rather
    /// than a policy question, and Go #420 draws the same line.
    ///
    /// **Here rather than on [`WorkflowState`], because the question is about one call stack and
    /// not about the workflow.** [`in_step_scope`](Self::in_step_scope) binds it on the context it
    /// rebinds the task-local with, so it is in scope only while that body is being polled: two
    /// steps in flight cannot see each other's, and one that finishes cannot answer for another
    /// still inside its own. A *count* of live steps would belong on the shared state instead,
    /// since refusing concurrency outright is a question about the workflow.
    step: Option<StepScope>,
}

/// What a context inside a step body knows about that body.
///
/// **One `Option` for both, because a context has both or neither.** The marker and the status are
/// bound together by [`in_step_scope`](Ctx::in_step_scope) and go out of scope together, and that
/// invariant is worth more as a type than as a convention two fields keep by agreement: nothing can
/// leave a context holding a status it is no longer inside, or a marker with nothing to report.
///
/// They stay *distinct* inside it, because they answer different questions on different clocks —
/// see [`StepMarker`] for the one that is per attempt, and [`StepStatus`] for what the body may
/// read about itself.
#[derive(Clone, Debug)]
pub(crate) struct StepScope {
    /// Which body, for the leaf rule. Opaque, and never compared across runs.
    marker: StepMarker,
    /// What [`step_status`](crate::step_status) reports.
    ///
    /// **The id inside it is per step, where the marker is per attempt.** A retry enters a new
    /// scope with a fresh marker and the *same* id: the attempts are different bodies, and they are
    /// attempts at one step, competing to record under one row. `current_attempt` is the field that
    /// moves between them.
    status: StepStatus,
    /// Fires when the body running under this scope should stop.
    ///
    /// **Per attempt, like the marker beside it.** A retried step gets a fresh token, and shared
    /// state is exactly what must not carry one — a token cancelled by attempt one would arrive
    /// already-cancelled at attempt two.
    ///
    /// **Not an `Option`, because a step body always has one.** The engine mints a token for every
    /// attempt whether or not anything watches it, so "in a step" and "has a token" are the same
    /// condition, and one `Option` on the `Ctx` says it once.
    ///
    /// **This is the receiving end, and only that.** The engine raises a cancellation on the token
    /// it holds itself; what lands here is a clone, handed to the body so it can watch — see
    /// [`cancellation_token`](crate::cancellation_token). Nothing a body does with this cancels
    /// anything.
    cancellation: CancellationToken,
}

/// What a step body can learn about the attempt it is running as.
///
/// Read with [`step_status`](crate::step_status). The equivalent is `DBOS.step_status` in Python
/// and `DBOS.stepStatus` in TypeScript; Go and Java expose nothing like it.
///
/// TypeScript's carries a fourth field, `timeoutSignal`, which here is
/// [`cancellation_token`](crate::cancellation_token) — a free function rather than a field, because
/// it is useful to a body that has no interest in which attempt it is.
///
/// **A read-only snapshot, and read-only by construction.** The fields are behind accessors rather
/// than public, so there is no way to write one. That is not a guard against a body changing its
/// own retry policy — it could not anyway, since this is `Copy` and the engine rebuilds its own
/// copy from its own counters on every attempt — but against the *appearance* of one: a settable
/// `max_attempts` that silently changed nothing would be worse than no field at all. A step decides
/// how many attempts it gets through [`StepOptions`](crate::StepOptions), before it runs.
#[cfg(test)]
impl StepStatus {
    /// A first attempt at `step_id`, for tests that only care which step they are inside.
    pub(crate) fn first(step_id: i32) -> Self {
        Self {
            step_id,
            current_attempt: 1,
            max_attempts: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepStatus {
    /// The step's ordinal position in its workflow, counting from zero — the same number
    /// [`step_id`](crate::step_id) reports, and half a checkpoint row's key.
    pub(crate) step_id: i32,
    /// Which attempt is running, counting from **one**.
    ///
    /// **One-based, matching TypeScript, whose `attemptNum` reaches the body as 1 on the first
    /// try.** Python is the outlier and documents its own as zero-indexed, which makes a plain step
    /// "attempt 0 of 1"; this crate already spells the same number one-based in a step's tracing
    /// span, and a body that logs "attempt 2 of 3" should not have to add one to say so.
    pub(crate) current_attempt: u32,
    /// How many attempts the policy allows in total, so a body can tell it is on its last.
    ///
    /// **Always a number, where Python and TypeScript both report nothing for a step that does not
    /// retry.** They have to: their step config leaves the count unset. Here every step runs the
    /// same loop with `max_attempts` defaulting to 1, so a plain step is honestly attempt 1 of 1 —
    /// and "does this step retry?" is `max_attempts() > 1` rather than a second `Option` to unwrap
    /// inside one.
    pub(crate) max_attempts: u32,
}

impl StepStatus {
    /// The step's ordinal position in its workflow, counting from zero.
    pub fn step_id(&self) -> i32 {
        self.step_id
    }

    /// Which attempt is running, counting from one.
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    /// How many attempts the policy allows in total.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

/// Which step body a context is inside.
///
/// **A newtype rather than a bare integer**, because the only other integer in reach is a step id
/// and the two mean nothing alike: a step id is an ordinal position, restarts from zero on every
/// replay, and is half the primary key of a checkpoint row, where a marker is opaque, never
/// persisted, and never compared across runs. Distinct types are what keep a later edit from
/// passing one where the other belongs.
///
/// **One per attempt, not one per step.** [`Ctx::in_step_scope`] is entered again for every retry,
/// and each entry is a body of its own — work the first attempt handed out is not inside the
/// second.
///
/// Named `marker` rather than `body` because in [`step`](crate::step) a step's *body* is already
/// its closure, and the field would shadow it wherever both are in scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StepMarker(u64);

/// Hands out a fresh [`StepMarker`] for each step body.
///
/// Process-wide rather than per-workflow, because a marker is only ever compared for equality and
/// a value distinct across the process is distinct within any one workflow. `Relaxed` is enough:
/// the counter orders nothing, it only has to stop handing out the same value twice.
static NEXT_STEP_MARKER: AtomicU64 = AtomicU64::new(0);

/// The parts of a workflow that outlive any one call within it.
struct WorkflowState {
    workflow_id: String,
    /// When this workflow must stop, if it was given a budget.
    ///
    /// **The instant the database holds, not the timeout its caller offered.** A recovered
    /// workflow reads the deadline stored on its row, so a workflow given an hour that crashed
    /// after fifty minutes has ten left — and a child that inherits this inherits what is left
    /// rather than a fresh hour.
    ///
    /// Here rather than passed down to the one place that watches it, because a **child workflow**
    /// has to read it at launch: an inherited deadline is how a parent's budget reaches work the
    /// parent is not itself running, and the child's `select!` fires on the same instant with no
    /// signal passing between them.
    deadline: Option<Timestamp>,
    /// The next step id to hand out, so the first step in a workflow is step 0.
    ///
    /// **Zero-based, matching Go, TypeScript and Java** — Go initializes to `-1` and
    /// pre-increments (`stepID: -1, // Steps are 0-indexed`), while TypeScript and Java start at 0
    /// and post-increment. Python is the outlier at one-based, and that is an upstream
    /// inconsistency rather than a choice open to this port: the step id addresses a fork point
    /// and labels a row in a step listing, so it is a number Conductor renders and a user passes
    /// to `fork`. Three of four is what a fourth implementation has to match.
    ///
    /// TODO(dbos-team): UPSTREAM item 19.
    next_step_id: AtomicI32,
}

impl WorkflowState {
    fn next_step_id(&self) -> i32 {
        self.next_step_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Ctx {
    /// A context for a workflow about to run.
    pub(crate) fn new(
        executor: Arc<Executor>,
        workflow_id: impl Into<String>,
        deadline: Option<Timestamp>,
    ) -> Self {
        Self {
            executor,
            workflow: Arc::new(WorkflowState {
                workflow_id: workflow_id.into(),
                deadline,
                next_step_id: AtomicI32::new(0),
            }),
            step: None,
        }
    }

    /// The context of the workflow this code is running inside, or `None` outside one.
    ///
    /// The sole accessor. Crate-internal: user code asks its questions through the free functions
    /// that wrap this, which is what keeps the context itself off the public surface.
    ///
    /// `None` is not an error. A step called outside a workflow runs plainly and undurably, which
    /// is Python's behaviour and is what makes a `#[dbos::step]` function ordinarily testable.
    pub(crate) fn current() -> Option<Ctx> {
        CURRENT.try_with(Ctx::clone).ok()
    }

    /// [`current`](Self::current) without the clone: runs `f` on the ambient context, or on
    /// `None`.
    ///
    /// **For callers that only want to look.** `current` hands back an owned `Ctx`, which costs
    /// three atomic increments and three decrements — the executor, the workflow state, and the
    /// attempt's cancellation token are each behind an `Arc`. Those refcounts are shared by every
    /// concurrent step of the workflow, so they are exactly the words under contention when steps
    /// run together, and paying for them to answer a question that borrows is waste. A step's
    /// `poll` asks where it stands on every poll, which is what makes that waste worth a second
    /// accessor.
    pub(crate) fn with_current<R>(f: impl FnOnce(Option<&Ctx>) -> R) -> R {
        // The `Option` is what lets one `FnOnce` serve both arms: `try_with` runs the closure
        // exactly when it returns `Ok`, so precisely one of these two takes finds a value.
        let mut f = Some(f);
        match CURRENT.try_with(|ctx| f.take().expect("the closure runs once")(Some(ctx))) {
            Ok(answer) => answer,
            Err(_) => f.take().expect("the closure runs once")(None),
        }
    }

    /// The id of the workflow this context belongs to.
    pub(crate) fn workflow_id(&self) -> &str {
        &self.workflow.workflow_id
    }

    /// The id of the step body this context is inside, or `None` in the workflow proper.
    pub(crate) fn step_id(&self) -> Option<i32> {
        self.step.as_ref().map(|step| step.status.step_id)
    }

    /// What the body this context is inside may read about its own attempt, or `None` outside one.
    pub(crate) fn step_status(&self) -> Option<StepStatus> {
        self.step.as_ref().map(|step| step.status)
    }

    /// Whether this and `other` are the same *execution* of the same workflow.
    ///
    /// **Identity, not equality of ids.** A workflow id names a row; this asks whether the two
    /// contexts share one [`WorkflowState`], and therefore one step counter. Two things a matching
    /// id would wave through do not share one: a second `DBOS` in the process serving the same id,
    /// which is what [`Error::WrongInstance`](crate::Error::WrongInstance) exists to catch, and a
    /// second *execution* of one id — a recovery re-run — whose counter restarts from zero. A
    /// step id means nothing across either, so anything holding one has to ask this rather than
    /// compare strings.
    ///
    /// Cheap: one pointer comparison. Clones of a `Ctx` share the state, so the ordinary case —
    /// a step built and polled inside one workflow body — answers true without touching memory
    /// the caller did not already have.
    pub(crate) fn is_same_execution(&self, other: &Ctx) -> bool {
        Arc::ptr_eq(&self.workflow, &other.workflow)
    }

    /// When this workflow must stop, if it has a deadline at all.
    pub(crate) fn deadline(&self) -> Option<Timestamp> {
        self.workflow.deadline
    }

    /// Runs `future` with `ctx` ambient.
    ///
    /// The context is restored on every poll and removed on every yield, so it is present for the
    /// whole body including across `.await`. It does **not** cross a [`tokio::spawn`]: a spawned
    /// task is a new task with its own empty task-local map. That is the correct default — the
    /// spawned work is not part of the durable workflow, and silently adopting the parent's step
    /// counter would let two tasks allocate the same step id — but it is surprising enough that
    /// `the_context_does_not_cross_a_spawn` pins it as behaviour rather than leaving it to be
    /// discovered.
    pub(crate) async fn scope<F: Future>(ctx: Ctx, future: F) -> F::Output {
        CURRENT.scope(ctx, future).await
    }

    /// The next step id in this workflow, zero-based and never reused.
    pub(crate) fn next_step_id(&self) -> i32 {
        self.workflow.next_step_id()
    }

    /// The executor running this workflow.
    pub(crate) fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    /// Whether this call is inside a step body.
    ///
    /// **Per call stack, not per workflow**: it reads the [`StepMarker`] that
    /// [`in_step_scope`](Self::in_step_scope) binds on the context it rebinds for the body, so a
    /// sibling step running concurrently has no bearing on the answer, and neither has one that
    /// has just finished.
    pub(crate) fn in_step(&self) -> bool {
        self.step.is_some()
    }

    /// Which step body this context is inside, if any.
    ///
    /// [`in_step`](Self::in_step) asks whether there is one; this asks *which*, and the difference
    /// is what lets a durable call built inside a step body be refused when it is polled somewhere
    /// else. Both places have the same workflow id, so comparing workflow identity cannot tell them
    /// apart. See [`StepMarker`].
    pub(crate) fn step_marker(&self) -> Option<StepMarker> {
        self.step.as_ref().map(|step| step.marker)
    }

    /// Runs `body` under a context that is [`in_step`](Self::in_step).
    ///
    /// **Nothing to unset afterwards**, which is what moving the answer off the shared state
    /// bought: it lives on the `Ctx` bound for this body alone, so it goes out of scope with the
    /// body however the body ends — an early return, an error, a panic — and no other call stack
    /// ever saw it. The drop guard this used to need existed only because the flag was shared, and
    /// a guard could not have fixed that: restoring rather than clearing still hands one step's
    /// answer to another.
    pub(crate) async fn in_step_scope<F: Future>(
        &self,
        cancellation: CancellationToken,
        status: StepStatus,
        body: F,
    ) -> F::Output {
        // Rebinding rather than mutating: the body must see this attempt's token, its own marker
        // and its own id, and the `Ctx` the workflow body holds must acquire none of them.
        let scoped = Ctx {
            executor: Arc::clone(&self.executor),
            workflow: Arc::clone(&self.workflow),
            step: Some(StepScope {
                marker: StepMarker(NEXT_STEP_MARKER.fetch_add(1, Ordering::Relaxed)),
                status,
                cancellation,
            }),
        };
        CURRENT.scope(scoped, body).await
    }

    /// Fires when the running step should stop.
    ///
    /// Observe it from work the runtime cannot stop by dropping this future — a `spawn_blocking`
    /// thread, or a client that holds its own cancel handle. It fires whenever the attempt ends
    /// **without completing**: a timeout, a cancelled workflow, or anything else that drops the
    /// step's future. An attempt that reaches an outcome does *not* fire it, because the body has
    /// had its chance to clean up and work it deliberately left running is not the engine's to
    /// stop. **Ordinary `async` code needs nothing**: a step that times out has its future dropped,
    /// which stops it at its next suspension point and runs its destructors on the way out, so a
    /// connection is returned and a guard released without the body containing a line about it.
    /// That is what TypeScript's `stepStatus.timeoutSignal` is for, and Rust gets the common case
    /// for free where TypeScript has to abandon the attempt and discard its eventual settlement.
    ///
    /// Returns a token that is never cancelled when there is no step running, so a body that is
    /// also called outside a workflow needs no second path.
    ///
    pub(crate) fn cancellation(&self) -> CancellationToken {
        // `unwrap_or_default` means exactly one thing now: there is no step here, and a token
        // that never fires is the honest answer.
        self.step
            .as_ref()
            .map(|step| step.cancellation.clone())
            .unwrap_or_default()
    }
}

/// The id of the workflow this code is running inside, or `None` outside one.
///
/// A workflow that wants to log its own id, publish it, or hand it to something that will later
/// address the workflow by it — the id is an idempotency key, so this is how a workflow tells a
/// caller what to send to — has no parameter to read it from: a DBOS workflow is an ordinary
/// `async fn` taking its own arguments and nothing else.
///
/// `None` is not an error, and code that runs both inside and outside a workflow is the reason it
/// is an `Option` rather than a panic: a helper called from a workflow and from a plain handler
/// gets an answer in both places.
///
/// Answers from inside a step as well as from the workflow body — a step is part of its workflow,
/// and asking which workflow it belongs to is not the same question as
/// [`step_id`](crate::step_id).
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// // The payment id a caller pays against *is* this workflow's id.
/// let id = dbos::workflow_id().expect("inside a workflow");
/// dbos::set_event("payment_id", &id).await?;
/// # Ok(())
/// # }
/// ```
///
/// The equivalent is `DBOS.workflow_id` in Python, `DBOS.workflowID` in TypeScript,
/// `dbos.GetWorkflowID(ctx)` in Go and `DBOS.workflowId()` in Java.
pub fn workflow_id() -> Option<String> {
    Ctx::with_current(|ctx| ctx.map(|ctx| ctx.workflow_id().to_owned()))
}

/// The id of the step this code is running inside, or `None` when it is not inside one.
///
/// The ordinal position of the step within its workflow, counting from zero — the number that
/// addresses a checkpoint row, labels a step in a listing, and names a fork point. It restarts
/// from zero on every replay, which is what makes it an address rather than a serial number: the
/// same step in the same workflow has the same id on every execution.
///
/// **`None` in the workflow body itself**, not just outside a workflow. A step id belongs to a
/// step, and between two steps a workflow is inside neither; Python and TypeScript answer `None`
/// and `undefined` in exactly the same place. So this is `Some` only while a step body is
/// executing, which includes the retries of a step — every attempt of one step reports that
/// step's id.
///
/// ```no_run
/// async fn charge() -> dbos::Result<()> {
///     // Inside a step body, so this is the id of the step being run.
///     tracing::info!(step = dbos::step_id(), "charging");
///     Ok(())
/// }
/// ```
///
/// The equivalent is `DBOS.step_id` in Python, `DBOS.stepID` in TypeScript,
/// `dbos.GetStepID(ctx)` in Go and `DBOS.stepId()` in Java.
pub fn step_id() -> Option<i32> {
    Ctx::with_current(|ctx| ctx.and_then(Ctx::step_id))
}

/// What the step this code is running inside knows about its own attempt, or `None` outside one.
///
/// [`step_id`](crate::step_id) is the common case and stays its own function; this is the rest of
/// what a body may ask — chiefly **which attempt it is**, so a step can behave differently on its
/// last one: log the failure loudly, fall back to a cheaper path, or stop paying for a cache it is
/// about to give up on.
///
/// `None` in the workflow body proper and outside a workflow, exactly as
/// [`step_id`](crate::step_id) is — a status belongs to a step, and between two steps a workflow is
/// inside neither.
///
/// ```no_run
/// # #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
/// # #[error("upstream is down")]
/// # struct Upstream;
/// async fn charge() -> dbos::Result<(), Upstream> {
///     let status = dbos::step_status().expect("inside a step");
///     if status.current_attempt() == status.max_attempts() {
///         tracing::warn!(step = status.step_id(), "last attempt; the step is about to fail");
///     }
///     Err(Upstream)?
/// }
/// ```
///
/// The equivalent is `DBOS.step_status` in Python and `DBOS.stepStatus` in TypeScript. See
/// [`StepStatus`] for where the three fields differ from theirs.
pub fn step_status() -> Option<StepStatus> {
    Ctx::with_current(|ctx| ctx.and_then(Ctx::step_status))
}

/// A token that fires when the step this code is running inside is abandoned.
///
/// **This receives a cancellation; it does not raise one.** The engine holds the token and fires
/// it — on a step's timeout, on a workflow cancelled elsewhere, and on any other path that drops
/// an attempt. What this hands back is a clone to watch, and nothing a body does with it cancels
/// anything.
///
/// Watch it from work the runtime cannot stop by dropping the step's future — a
/// [`spawn_blocking`](tokio::task::spawn_blocking) thread, or a client holding its own cancel
/// handle. **Ordinary `async` code needs nothing:** a step that times out has its future dropped,
/// which stops it at its next suspension point and runs its destructors on the way out, so a
/// connection is returned and a guard released without the body containing a line about it. That
/// is the half TypeScript cannot do — it abandons a timed-out attempt and discards whatever the
/// abandoned promise eventually settles to — and this covers the remainder Rust cannot reach by
/// dropping.
///
/// It fires whenever an attempt ends **without completing**. An attempt that reaches an outcome
/// does *not* fire it: the body has had its chance to clean up, and work it deliberately left
/// running is not the engine's to stop.
///
/// Returns a token that is never cancelled when there is no step running, so a body that is also
/// called outside a workflow needs no second path — which is why this is a [`CancellationToken`]
/// rather than an `Option` of one, unlike [`workflow_id`] and [`step_id`].
///
/// ```no_run
/// async fn hashes() -> dbos::Result<u64> {
///     let token = dbos::cancellation_token();
///     // A blocking thread: dropping this step's future cannot reach it, so it watches instead.
///     let hashed = tokio::task::spawn_blocking(move || {
///         let mut total = 0;
///         while !token.is_cancelled() {
///             total += 1;
///         }
///         total
///     });
///     Ok(hashed.await.expect("the hashing thread panicked"))
/// }
/// ```
///
/// TypeScript's `stepStatus.timeoutSignal` is the equivalent, and the only one: Python, Go and
/// Java expose nothing a step body can watch.
pub fn cancellation_token() -> CancellationToken {
    Ctx::with_current(|ctx| ctx.map(Ctx::cancellation).unwrap_or_default())
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("workflow_id", &self.workflow.workflow_id)
            .field(
                "steps_taken",
                &self.workflow.next_step_id.load(Ordering::Relaxed),
            )
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The step counter is the half of the context that needs no executor, so it is tested on
    /// `WorkflowState` directly. The scoping tests below need a real `Ctx`, so they launch against
    /// a leased database rather than faking an executor into existence.
    fn state() -> WorkflowState {
        WorkflowState {
            workflow_id: "wf-1".to_owned(),
            deadline: None,
            next_step_id: AtomicI32::new(0),
        }
    }

    #[test]
    fn step_ids_are_zero_based() {
        let state = state();
        assert_eq!(
            state.next_step_id(),
            0,
            "Go, TypeScript and Java all number the first step 0"
        );
        assert_eq!(state.next_step_id(), 1);
        assert_eq!(state.next_step_id(), 2);
    }

    #[test]
    fn step_ids_are_never_reused_across_threads() {
        // Two `Ctx` clones share one counter, so ids must be unique even when steps are allocated
        // from different threads. Sequential *execution* is the v1 contract; a counter that could
        // hand out a duplicate would be a replay bug rather than a policy question.
        let state = Arc::new(state());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    (0..100).map(|_| state.next_step_id()).collect::<Vec<_>>()
                })
            })
            .collect();
        let mut ids: Vec<i32> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..800).collect::<Vec<_>>(),
            "800 allocations, 800 distinct ids"
        );
    }

    #[tokio::test]
    async fn there_is_no_context_outside_a_workflow() {
        assert!(Ctx::current().is_none());
    }

    /// A real `Ctx` needs a real `Executor`, which needs a database. These live here rather than
    /// in `tests/` because `Ctx::new` and `Ctx::scope` are crate-internal — entering a context is
    /// something the engine does, never something a caller does.
    async fn ctx(
        workflow_id: &str,
        deadline: Option<Timestamp>,
    ) -> (Ctx, crate::DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..crate::Config::new("ctx-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        let ctx = Ctx::new(
            dbos.executor("test").expect("launched"),
            workflow_id,
            deadline,
        );
        (ctx, dbos, db)
    }

    #[tokio::test]
    async fn a_context_is_ambient_within_its_scope_and_gone_outside_it() {
        let (ctx, dbos, _db) = ctx("wf-42", None).await;

        assert!(Ctx::current().is_none(), "nothing before");
        Ctx::scope(ctx, async {
            assert_eq!(Ctx::current().expect("inside").workflow_id(), "wf-42");
            tokio::task::yield_now().await;
            assert_eq!(
                Ctx::current().expect("still inside").workflow_id(),
                "wf-42",
                "the context survives an await, which is the whole point of a task-local"
            );
        })
        .await;
        assert!(Ctx::current().is_none(), "nothing after");

        dbos.shutdown().await;
    }

    /// The deadline reaches everything the workflow calls, including from inside a step.
    ///
    /// It lives on the shared `WorkflowState` rather than on the `Ctx`, and the difference shows up
    /// exactly here: `in_step_scope` builds a *new* `Ctx` to carry the attempt's cancellation
    /// token, so anything held on the `Ctx` itself would be silently dropped at every step
    /// boundary. A child workflow launched after a step has run must still inherit the budget its
    /// parent has left.
    #[tokio::test]
    async fn the_deadline_travels_with_the_context_and_survives_a_step_scope() {
        let deadline = Timestamp::from_epoch_ms(1_700_000_000_000);
        let (ctx, dbos, _db) = ctx("wf-deadline", Some(deadline)).await;

        let inner = ctx.clone();
        Ctx::scope(ctx, async move {
            assert_eq!(
                Ctx::current().expect("inside").deadline(),
                Some(deadline),
                "the body reads the deadline its row carries"
            );
            inner
                .in_step_scope(CancellationToken::new(), StepStatus::first(0), async {
                    assert_eq!(
                        Ctx::current().expect("inside a step").deadline(),
                        Some(deadline),
                        "a step's rebinding shares the workflow state, so the deadline comes with it"
                    );
                })
                .await;
        })
        .await;

        dbos.shutdown().await;
    }

    /// A sibling that has finished does not answer for a step still inside its own body.
    ///
    /// One half of what a workflow-wide flag got wrong, and the half that corrupts a step that is
    /// still *running*: whichever scope ended first cleared the answer for the other, so a body
    /// still inside itself read as no longer in a step, and the next call it made allocated a step
    /// id instead of being the plain call a nested one has to be.
    #[tokio::test]
    async fn a_sibling_that_finishes_does_not_end_this_step() {
        let (ctx, dbos, _db) = ctx("wf-siblings", None).await;
        let (inside, wait_for_inside) = tokio::sync::oneshot::channel();
        let (done, wait_for_done) = tokio::sync::oneshot::channel();

        let long = ctx.in_step_scope(CancellationToken::new(), StepStatus::first(0), async move {
            inside.send(()).expect("the sibling is waiting on this");
            wait_for_done.await.expect("the sibling ran to completion");
            Ctx::current().expect("inside a step").in_step()
        });
        let short = async {
            wait_for_inside
                .await
                .expect("the long step reached its body");
            // A whole step scope opens and closes while the other one is suspended inside its own.
            ctx.in_step_scope(
                CancellationToken::new(),
                StepStatus::first(0),
                std::future::ready(()),
            )
            .await;
            done.send(()).expect("the long step is waiting on this");
        };

        let (still_inside, ()) = tokio::join!(long, short);
        assert!(
            still_inside,
            "a sibling finishing must not tell a step it has left its own body"
        );

        dbos.shutdown().await;
    }

    /// A step in flight does not make the workflow body around it look nested.
    ///
    /// The other half of the same bug: with the answer shared, anything the workflow proper did
    /// while a step was running took the plain path meant for a nested call, and so was never
    /// checkpointed at all. `a_step_built_while_a_sibling_runs_is_still_checkpointed` in
    /// [`step`](crate::step) is what that costs a caller.
    #[tokio::test]
    async fn a_running_step_does_not_make_the_workflow_proper_look_nested() {
        let (ctx, dbos, _db) = ctx("wf-proper", None).await;
        let (inside, wait_for_inside) = tokio::sync::oneshot::channel();
        let (looked, wait_for_look) = tokio::sync::oneshot::channel();

        let stepping = ctx.clone();
        Ctx::scope(ctx, async move {
            let held = stepping.in_step_scope(
                CancellationToken::new(),
                StepStatus::first(0),
                async move {
                    inside
                        .send(())
                        .expect("the workflow body is waiting on this");
                    wait_for_look.await.expect("the workflow body looked");
                },
            );
            let proper = async {
                wait_for_inside.await.expect("the step reached its body");
                let seen = Ctx::current().expect("inside the workflow").in_step();
                looked.send(()).expect("the step is waiting on this");
                seen
            };

            let ((), seen) = tokio::join!(held, proper);
            assert!(
                !seen,
                "the workflow body is not inside the step it is waiting on"
            );
        })
        .await;

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn the_context_does_not_cross_a_spawn() {
        // Documented rather than discovered: a spawned task is a new task with an empty
        // task-local map. That is the correct default — the spawned work is not part of the
        // durable workflow — and adopting the parent's counter would let two tasks allocate the
        // same step id.
        let (ctx, dbos, _db) = ctx("wf-42", None).await;

        let spawned_saw = Ctx::scope(ctx, async {
            tokio::spawn(async { Ctx::current().map(|c| c.workflow_id().to_owned()) })
                .await
                .expect("the spawned task panicked")
        })
        .await;
        assert_eq!(spawned_saw, None);

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_clone_shares_one_step_counter() {
        let (ctx, dbos, _db) = ctx("wf-42", None).await;
        let clone = ctx.clone();

        assert_eq!(ctx.next_step_id(), 0);
        assert_eq!(
            clone.next_step_id(),
            1,
            "a clone is the same workflow, not a second one"
        );
        assert_eq!(ctx.next_step_id(), 2);

        dbos.shutdown().await;
    }
}
