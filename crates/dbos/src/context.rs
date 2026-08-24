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
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use tokio_util::sync::CancellationToken;

use crate::dbos::Executor;

tokio::task_local! {
    /// Set while a workflow body runs, and read by everything the body calls.
    static CURRENT: Ctx;
}

/// What a running workflow knows about itself.
///
/// Cheap to clone: two `Arc`s. Cloning it does not make a second workflow — the clone shares the
/// same step counter, which is the point, because a step allocated through either must not reuse
/// an id allocated through the other.
#[derive(Clone)]
pub struct Ctx {
    executor: Arc<Executor>,
    workflow: Arc<WorkflowState>,
    /// Fires when the step running under this context should stop.
    ///
    /// **On the `Ctx` rather than on [`WorkflowState`], because it belongs to one attempt.** A
    /// retried step gets a fresh token per attempt, and the shared state is exactly what must not
    /// carry it — a token cancelled by attempt one would arrive already-cancelled at attempt two.
    /// [`in_step_scope`](Self::in_step_scope) rebinds the task-local with a `Ctx` holding the
    /// attempt's token, which works because cloning a `Ctx` shares the workflow state that has to
    /// be shared and copies only this.
    ///
    /// `None` outside a step, and outside a step there is nothing to cancel.
    step_cancellation: Option<CancellationToken>,
}

/// The parts of a workflow that outlive any one call within it.
struct WorkflowState {
    workflow_id: String,
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
    /// Whether a step is on the stack right now.
    ///
    /// A step is a leaf: the checkpoint it writes stands for everything the body did, so a step
    /// inside a step is a plain call. Without this the inner call would allocate a step id of its
    /// own and every step after it would replay against the wrong slot — a correctness trap rather
    /// than a policy question, and Go #420 draws the same line.
    ///
    /// **Here, and so per-workflow, which is only right while steps run one at a time.** The
    /// question is really per-call-stack, so two steps in flight at once share an answer meant for
    /// one — silently, and in both directions; [`step`](crate::step) documents what that costs a
    /// caller and why sequential is the contract for now. Supporting concurrency starts by moving
    /// this out of here: the flag belongs to the [`Ctx`] that [`Ctx::in_step_scope`] wraps the body
    /// with, rather than to the state every clone shares, and then `WorkflowState` holds only what
    /// genuinely belongs to the whole workflow — the id and the step counter. A count of live steps
    /// would stay here, since refusing concurrency is a question about the workflow.
    in_step: AtomicBool,
}

impl WorkflowState {
    fn next_step_id(&self) -> i32 {
        self.next_step_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Ctx {
    /// A context for a workflow about to run.
    pub(crate) fn new(executor: Arc<Executor>, workflow_id: impl Into<String>) -> Self {
        Self {
            executor,
            workflow: Arc::new(WorkflowState {
                workflow_id: workflow_id.into(),
                next_step_id: AtomicI32::new(0),
                in_step: AtomicBool::new(false),
            }),
            step_cancellation: None,
        }
    }

    /// The context of the workflow this code is running inside, or `None` outside one.
    ///
    /// The sole accessor, and public because user code legitimately asks: a workflow that wants to
    /// log its own id, or a helper that behaves differently when it is being replayed, has no
    /// parameter to read it from by design.
    ///
    /// `None` is not an error. A step called outside a workflow runs plainly and undurably, which
    /// is Python's behaviour and is what makes a `#[dbos::step]` function ordinarily testable.
    pub fn current() -> Option<Ctx> {
        CURRENT.try_with(Ctx::clone).ok()
    }

    /// The id of the workflow this context belongs to.
    pub fn workflow_id(&self) -> &str {
        &self.workflow.workflow_id
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

    /// Whether a step is already running in this workflow.
    pub(crate) fn in_step(&self) -> bool {
        self.workflow.in_step.load(Ordering::Relaxed)
    }

    /// Runs `body` with [`in_step`](Self::in_step) set, clearing it afterwards.
    ///
    /// A guard rather than a plain pair of writes, so the flag is cleared even when the body
    /// returns early or panics — a step that failed must not leave the workflow believing it is
    /// still inside one.
    ///
    /// **Clearing, not restoring**, and along this call stack the two are the same thing: [`step`]
    /// only reaches here when no step was on the stack, so the flag was false on the way in. They
    /// come apart only between *concurrent* steps, where one finishing clears the flag for another
    /// still running — which is the gap [`in_step`](Self::in_step) describes, and is not something
    /// restoring here would fix, because the flag is shared rather than per-stack in the first
    /// place.
    ///
    /// [`step`]: crate::step
    pub(crate) async fn in_step_scope<F: Future>(
        &self,
        cancellation: Option<CancellationToken>,
        body: F,
    ) -> F::Output {
        let _guard = InStep(Arc::clone(&self.workflow));
        self.workflow.in_step.store(true, Ordering::Relaxed);
        // Rebinding rather than mutating: the body must see this attempt's token, and the `Ctx`
        // the workflow body holds must not acquire one that outlives the step.
        let scoped = Ctx {
            executor: Arc::clone(&self.executor),
            workflow: Arc::clone(&self.workflow),
            step_cancellation: cancellation,
        };
        CURRENT.scope(scoped, body).await
    }

    /// Fires when the running step should stop.
    ///
    /// Observe it from work the runtime cannot stop by dropping this future — a `spawn_blocking`
    /// thread, or a client that holds its own cancel handle. **Ordinary `async` code needs
    /// nothing**: a step that times out has its future dropped, which stops it at its next
    /// suspension point and runs its destructors on the way out, so a connection is returned and a
    /// guard released without the body containing a line about it. That is what TypeScript's
    /// `stepStatus.timeoutSignal` is for, and Rust gets the common case for free where TypeScript
    /// has to abandon the attempt and discard its eventual settlement.
    ///
    /// Returns a token that is never cancelled when there is no step running, so a body that is
    /// also called outside a workflow needs no second path.
    pub fn cancellation(&self) -> CancellationToken {
        self.step_cancellation.clone().unwrap_or_default()
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("workflow_id", &self.workflow.workflow_id)
            .field(
                "steps_taken",
                &self.workflow.next_step_id.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// Clears the in-step flag however the step ends.
struct InStep(Arc<WorkflowState>);

impl Drop for InStep {
    fn drop(&mut self) {
        self.0.in_step.store(false, Ordering::Relaxed);
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
            next_step_id: AtomicI32::new(0),
            in_step: AtomicBool::new(false),
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
    async fn ctx(workflow_id: &str) -> (Ctx, crate::DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            ..crate::Config::new("ctx-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        let ctx = Ctx::new(dbos.executor("test").expect("launched"), workflow_id);
        (ctx, dbos, db)
    }

    #[tokio::test]
    async fn a_context_is_ambient_within_its_scope_and_gone_outside_it() {
        let (ctx, dbos, _db) = ctx("wf-42").await;

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

    #[tokio::test]
    async fn the_context_does_not_cross_a_spawn() {
        // Documented rather than discovered: a spawned task is a new task with an empty
        // task-local map. That is the correct default — the spawned work is not part of the
        // durable workflow — and adopting the parent's counter would let two tasks allocate the
        // same step id.
        let (ctx, dbos, _db) = ctx("wf-42").await;

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
        let (ctx, dbos, _db) = ctx("wf-42").await;
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
