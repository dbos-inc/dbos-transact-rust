//! Running a workflow durably.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::AbortHandle;
use tracing::Instrument;

use crate::context::Ctx;
use crate::dbos::Executor;
use crate::error::{DurableError, Error, Failure, Result};
use crate::handle::WorkflowHandle;
use crate::registry::{WorkflowKey, WorkflowRef};
use crate::serialization::encode;
use crate::sysdb::types::{
    AwaitedOutcome, NewWorkflow, Outcome, OutcomeWrite, Submission, Timestamp,
};

/// Attempts before a workflow is parked as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
///
/// The default every implementation shares. It only counts recoveries and dequeues; starting a
/// workflow fresh never approaches it.
pub(crate) const MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// The workflows this executor started and has not seen finish.
///
/// Abort handles to reach them with, and a count of how many are still alive to wait on. Nothing
/// here holds a `JoinHandle`: a caller awaiting its own workflow holds that, and a caller that
/// dropped its future is no longer waiting though the workflow is still running — which is exactly
/// the case shutdown has to reach. The count stands in for joining, and is what lets shutdown mean
/// "quiet" rather than "told to stop".
#[derive(Default)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    /// Woken as each task's future is dropped, so [`abort_all`](Tasks::abort_all) can wait for the
    /// last one out without polling for it.
    ended: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    running: Vec<AbortHandle>,
    /// How many spawned futures exist and have not been dropped.
    live: usize,
    /// Set by [`abort_all`](Tasks::abort_all). A task registered after the sweep is cancelled on
    /// arrival rather than added to a list nothing will read again.
    closed: bool,
}

impl Tasks {
    /// Records a running workflow, dropping the handles of any that have since finished.
    ///
    /// Pruning on insert rather than on completion keeps this to one lock and no bookkeeping ids.
    /// The cost is that a finished workflow's handle lingers until the next one starts, which is a
    /// pointer, and the benefit is that a workflow whose caller dropped its future is still
    /// reachable by shutdown — which is the case that matters.
    ///
    /// A task arriving after the sweep is aborted here instead. That is the race between spawning
    /// and registering: a workflow started as shutdown ran would otherwise never be cancelled and
    /// would outlive the shutdown meant to stop it.
    fn insert(&self, handle: AbortHandle) {
        let mut state = self.lock();
        if state.closed {
            drop(state);
            handle.abort();
            return;
        }
        state.running.retain(|handle| !handle.is_finished());
        state.running.push(handle);
    }

    /// Counts a task about to be spawned.
    fn arrived(&self) {
        self.lock().live += 1;
    }

    /// Un-counts one whose future has been dropped, waking a shutdown waiting for the last.
    ///
    /// Runs from a `Drop`, including one unwinding out of a panicking workflow, so it takes care
    /// not to panic itself: the count cannot legitimately go below zero — one guard per task,
    /// dropped once — and saturating rather than wrapping keeps a bug here from becoming an abort.
    fn departed(&self) {
        {
            let mut state = self.lock();
            state.live = state.live.saturating_sub(1);
        }
        self.ended.notify_waiters();
    }

    /// Cancels every workflow still running and waits for them to stop, reporting how many were
    /// cancelled.
    ///
    /// Their rows stay `PENDING`, which is the point: a cancelled workflow is one a later executor
    /// recovers, so an abrupt shutdown loses no work. Writing a terminal status here would be the
    /// bug — it would mark as finished something that never finished.
    ///
    /// **The wait is the half that makes this mean anything.** `abort` is not synchronous; it
    /// schedules cancellation at the task's next yield point, so without waiting, shutdown returns
    /// while the bodies it cancelled may still be running. The wait is unbounded, matching what
    /// `close` already does with its listener: a task that ignores cancellation should be a hang
    /// that gets fixed rather than a warning logged forever.
    pub(crate) async fn abort_all(&self) -> usize {
        let cancelled = {
            let mut state = self.lock();
            state.closed = true;
            let live: Vec<AbortHandle> = state
                .running
                .drain(..)
                .filter(|handle| !handle.is_finished())
                .collect();
            for handle in &live {
                handle.abort();
            }
            live.len()
        };

        loop {
            // Registered before the count is read, so a task ending in between is not missed:
            // `notify_waiters` wakes only those already waiting.
            let ended = self.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            // Bound to a `let` so the guard is released before the await, not held across it.
            let live = self.lock().live;
            if live == 0 {
                break;
            }
            ended.await;
        }
        cancelled
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Keeps a spawned task counted for as long as its future exists.
///
/// Built *before* the spawn and moved into the future, which is the whole trick: it is then
/// dropped however the task ends — completed, panicked, or cancelled before its first poll. A
/// guard created inside the body would miss that last case entirely and leave
/// [`abort_all`](Tasks::abort_all) waiting forever on a task that never started.
struct TaskGuard(Arc<Executor>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.tasks().departed();
    }
}

/// Spawns `future` on the executor's runtime, counted and reachable by shutdown.
///
/// Two things every spawn here needs and both are easy to get subtly wrong alone: the abort handle
/// is what lets shutdown *reach* the task, and the guard is what lets it *wait* for it. Doing them
/// in one place is also what orders them correctly — the guard exists before the spawn, so a
/// shutdown racing this one either aborts the task through `insert` or waits for it through the
/// count, and never misses it through both.
pub(crate) fn spawn_tracked<F>(
    executor: &Arc<Executor>,
    future: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    executor.tasks().arrived();
    let guard = TaskGuard(Arc::clone(executor));
    let task = executor.runtime().clone().spawn(async move {
        let _guard = guard;
        future.await
    });
    executor.tasks().insert(task.abort_handle());
    task
}

/// Logs a panic escaping a workflow body, from inside the unwind.
///
/// A panic is deliberately not an outcome — nothing is recorded and the row stays `PENDING`, the
/// same as a crash — but it must not be *silent*. Tokio parks the panic in the `JoinHandle`, and a
/// caller that dropped its future never joins it, so without this the only evidence would be the
/// panic hook's stderr line: no workflow id, nothing about the durable consequence.
///
/// A drop guard because the unwind is the only place the case can be observed: an async block's
/// live locals are dropped as the panic unwinds out of `poll`, with `std::thread::panicking()`
/// true, and the workflow span still entered.
///
/// `panicking()` is the whole of the discrimination, which is why there is nothing to defuse. The
/// guard is dropped on every path — completion, panic, and the abort that shutdown issues — and
/// only the middle one is inside an unwind.
struct PanicLog<'a> {
    workflow_id: &'a str,
}

impl Drop for PanicLog<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(
                workflow_id = self.workflow_id,
                "the workflow body panicked: no outcome is recorded, and the row stays PENDING \
                 for a later executor to recover"
            );
        }
    }
}

/// What a caller may say about a start, beyond the input.
///
/// A struct rather than a builder, matching how `sysdb` spells optional arguments; the common case
/// pays nothing because [`run`](WorkflowRef::run) and [`start`](WorkflowRef::start) keep their
/// no-option forms. Deliberately **not** `#[non_exhaustive]` — that would forbid the
/// `..Default::default()` form this type is built around. Adding a field stays non-breaking for
/// every caller who wrote it.
#[derive(Debug, Clone, Default)]
pub struct StartOptions<'a> {
    /// The workflow's id, in place of a generated one.
    ///
    /// A caller-supplied id is an idempotency key: starting the same id twice joins the workflow
    /// already running — or already finished — rather than failing the second caller.
    pub workflow_id: Option<&'a str>,
    /// How long the whole workflow may take, and whether it takes a parent's budget when it names
    /// none of its own. Defaults to [`Timeout::Inherit`].
    ///
    /// **Durable, and unlike a step timeout it cancels rather than fails.** The budget becomes a
    /// wall-clock deadline stored on the row, so it survives a crash: a workflow recovered with
    /// two minutes left has two minutes, not the whole budget again. On expiry the row goes
    /// `CANCELLED` and callers awaiting it get [`Error::WorkflowCancelled`], which is what all four
    /// references do.
    ///
    /// A step's [`timeout`](crate::StepOptions::timeout) bounds one attempt and is recorded as a
    /// step failure; this bounds everything and is not the workflow's *outcome* at all — a
    /// cancelled workflow was interrupted, not wrong.
    pub timeout: Timeout,
}

/// How long a workflow may take, and — started from inside another one — what it does about the
/// deadline its parent is under.
///
/// **Three states rather than an `Option<Duration>`, because a child has three things to say and
/// only one of them is silence.** Saying nothing and saying "no limit" are different instructions
/// once there is a parent budget to inherit, and an `Option` collapses them: `None` would have to
/// mean both, and inheritance is the more useful default, so an unbounded child would have been
/// inexpressible.
///
/// **All four references carry these three states**, which is why they are an enum here rather
/// than a convention:
///
/// - **Java** is the same shape under the same first name: a sealed `Timeout` permitting
///   `Timeout.Inherit`, `Timeout.None` and `Timeout.Explicit` (`workflow/Timeout.java`), resolved
///   at `DBOSContext.resolveTimeoutAndDeadline` — where the `None` case clears the propagated
///   deadline *and* the timeout, exactly as [`None`](Self::None) does here. This is the one place
///   Java **is** the model, variant names included; decision 6's "Java is not a model" is about
///   its user-facing `withDeadline` and the precedence that follows from it, which Rust lacks.
/// - **TypeScript** spells the three as `number | null | undefined` (`context.ts:31`) and branches
///   on the middle one under the comment *"Detach child deadline if a null timeout is configured"*
///   (`dbos.ts:1969`, and again at `enqueue_workflow.ts:92`).
/// - **Python** reaches them through `SetWorkflowTimeout(None)`, whose `__enter__` clears the
///   propagated deadline as well as the timeout (`_context.py:568`).
/// - **Go** gets all three for free, because its deadline rides on a `context` a caller may
///   decline to pass on.
///
/// **The variant names are Java's**, so the two SDKs spell the same three states the same way and
/// a reader crossing between them has nothing to translate. `Timeout::None` does sit next to the
/// `Option`s this crate is written in, which is worth a moment's care at a call site and worth
/// less than the parity.
///
/// Outside a workflow there is nothing to inherit, so [`Inherit`](Self::Inherit) and
/// [`None`](Self::None) mean the same thing there: no deadline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Timeout {
    /// Say nothing about the budget: a child takes its parent's deadline, a root runs unbounded.
    ///
    /// The default, and the reason the default is this rather than [`None`](Self::None):
    /// a workflow factored out of a parent's body should stay inside the budget that body was
    /// under, without its call site having to say so.
    #[default]
    Inherit,
    /// Run for as long as it likes, **even under a parent that has a deadline**.
    ///
    /// The state an `Option<Duration>` cannot express, and the reason this is an enum: `Inherit`
    /// is a caller who said nothing, and this is a caller who decided. `Timeout.None` in Java,
    /// a `null` timeout in TypeScript, `SetWorkflowTimeout(None)` in Python.
    None,
    /// A budget, counted from now.
    ///
    /// **Replaces an inherited deadline rather than being bounded by it**, even when it is the
    /// longer of the two — so a child given more time than its parent has left outlives its
    /// parent. That is Python's and TypeScript's rule and their shared comment (*"If a timeout is
    /// explicitly specified, use it over any propagated deadline"*), and it is defensible for the
    /// reason it reads: an explicit timeout on one specific child is a statement about that child.
    /// Go takes the earlier of the two instead, but not by design — `context.WithTimeout` composes
    /// as a minimum and nobody wrote a precedence rule.
    Explicit(Duration),
}

impl From<Duration> for Timeout {
    fn from(timeout: Duration) -> Self {
        Self::Explicit(timeout)
    }
}

impl Timeout {
    /// The budget to record on the row, which only [`Explicit`](Self::Explicit) has.
    ///
    /// Neither of the others is a budget: `Inherit` takes an *instant* from its parent and
    /// `None` takes nothing, and a row's `workflow_timeout_ms` is what a **queue** recomputes
    /// a deadline from on dequeue. Writing one for either would hand that path a budget the caller
    /// never asked for.
    fn budget(self) -> Option<Duration> {
        match self {
            Self::Explicit(timeout) => Some(timeout),
            Self::Inherit | Self::None => Option::None,
        }
    }
}

impl<P, R, E> WorkflowRef<P, R, E>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    E: DurableError,
{
    /// Runs this workflow durably and waits for its result.
    ///
    /// [`start`](Self::start) followed by [`result`](crate::WorkflowHandle::result) — one
    /// mechanism, packaged as the common case. The workflow is recorded before its body starts,
    /// so a process that dies mid-run leaves a `PENDING` row a later executor recovers. Awaiting
    /// is a convenience over the durable run rather than the thing that makes it durable: drop
    /// this future and the workflow carries on.
    pub async fn run(&self, input: P) -> Result<R, E> {
        self.run_with(input, StartOptions::default()).await
    }

    /// [`run`](Self::run), with something to say about how it starts.
    pub async fn run_with(&self, input: P, options: StartOptions<'_>) -> Result<R, E> {
        self.start_with(input, options)
            .await
            .map_err(Error::lift)?
            .result()
            .await
    }

    /// The running workflow this call is a child of, or `None` when it is a root.
    ///
    /// Two refusals, both of them four-of-four, and both about the step counter:
    ///
    /// - **Inside a step is an error, not a plain call.** A step is a leaf whose checkpoint stands
    ///   for everything its body did, so an id-allocating call inside one would shift every later
    ///   step onto the wrong replay slot. A nested *step* degrades to a plain call because there is
    ///   a plain version of it; there is no undurable version of starting a child.
    /// - **A `WorkflowRef` from another instance is [`Error::WrongInstance`].** The step id would
    ///   come from this workflow's counter and the launch record would be written through the other
    ///   instance's system database, landing where the workflow that allocated it cannot see it.
    ///   `get_event` refuses the same combination for the same reason.
    ///
    /// Allocating the step id here is what makes the launch position stable across a replay, and it
    /// happens before anything can fail on the way to using it.
    fn parent(&self) -> Result<Option<Parent>> {
        let Some(ctx) = Ctx::current() else {
            return Ok(None);
        };
        if ctx.in_step() {
            return Err(Error::InsideStep {
                operation: "starting a workflow".into(),
            });
        }
        if !Arc::ptr_eq(ctx.executor(), &self.dbos().executor("start a workflow")?) {
            return Err(Error::WrongInstance {
                operation: "starting a workflow".into(),
            });
        }
        Ok(Some(Parent {
            workflow_id: ctx.workflow_id().to_owned(),
            step_id: ctx.next_step_id(),
            deadline: ctx.deadline(),
        }))
    }

    /// Starts this workflow durably and returns a handle to it, without waiting.
    pub async fn start(&self, input: P) -> Result<WorkflowHandle<R, E>> {
        self.start_with(input, StartOptions::default()).await
    }

    /// [`start`](Self::start), with something to say about how.
    ///
    /// Returns as soon as the workflow is recorded and spawned. If the id is already owned —
    /// another process is running it, or a previous run finished it — the handle joins the
    /// existing run rather than this being an error: the id is an idempotency key, and honouring
    /// it is the promise (decision 13). The error is the engine's own channel, because a start
    /// fails only in the engine's terms; the *workflow's* failures come out of the handle.
    pub async fn start_with(
        &self,
        input: P,
        options: StartOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        let executor = self.dbos().executor("start a workflow")?;
        // **The ambient context is what makes this a child.** Every reference overloads the same
        // call rather than adding a `start_child`, so factoring a workflow body out into its own
        // workflow does not change how its call sites are written — and a workflow started from
        // outside one is unaffected by everything below.
        let parent = self.parent()?;
        let input = Some(encode(&input, "argument")?);

        // The launch is recorded against the parent before anything is created, so a replay of
        // this position finds the child it already started instead of starting a second one.
        if let Some(parent) = &parent
            && let Some(child) = parent.recorded_launch(&executor, &self.key().name).await?
        {
            tracing::debug!(
                parent_workflow_id = parent.workflow_id,
                step_id = parent.step_id,
                workflow_id = child,
                "the child workflow was already started; the handle joins it"
            );
            return Ok(WorkflowHandle::polling(executor, child));
        }

        let workflow_id = match (options.workflow_id, &parent) {
            // An application-assigned id wins over the derivation, in every reference.
            (Some(id), _) => id.to_owned(),
            // **`{parent}-{step_id}`, and it must be derived rather than random**: a recovered
            // parent re-derives the same id, so the launch is idempotent even when the crash
            // landed between creating the child and recording it. Rust's step ids are zero-based
            // (Go, TypeScript and Java; Python is the one-based outlier — see UPSTREAM item 19),
            // so a first child is `parent-0` here and in three of the four.
            (None, Some(parent)) => format!("{}-{}", parent.workflow_id, parent.step_id),
            (None, None) => uuid::Uuid::new_v4().to_string(),
        };

        // **A directly started workflow gets its deadline now, and the row carries it.** Python
        // does the same (`_get_timeout_deadline`: *"Otherwise, compute the deadline immediately"*)
        // and so does Go. Persisting it rather than recomputing on recovery is the whole point of a
        // durable timeout: a workflow given an hour that crashes after fifty minutes has ten left,
        // not another hour, and a crash loop cannot extend the budget indefinitely.
        //
        // A *queued* workflow is assigned its deadline on dequeue instead, because the wait in the
        // queue is not part of the budget. That path arrives with queues; nothing here enqueues.
        let deadline = match (options.timeout, &parent) {
            // **An explicit timeout replaces an inherited deadline**, which is Python's and
            // TypeScript's rule and their shared comment: *"If a timeout is explicitly specified,
            // use it over any propagated deadline"*. So a child given longer than its parent has
            // left outlives its parent — an explicit timeout on a specific child is a statement
            // about that child, and the alternative would silently ignore what the caller asked
            // for. Go differs by taking the earlier of the two, but not by design: its deadline
            // rides on a `context`, and `context.WithTimeout` composes as a minimum. Java lets an
            // explicitly *set* deadline win, and is not a model there — its user-facing `deadline`
            // is Java's alone and Rust does not have one.
            (Timeout::Explicit(timeout), _) => Timestamp::now().checked_add(timeout),
            // **A deliberate refusal to inherit**, which is why the option is an enum: this is a
            // caller who decided, and `Inherit` below is a caller who said nothing. Java's
            // `Timeout.None` clears the propagated deadline in the same words.
            (Timeout::None, _) => None,
            // **Inherited as an instant, not as a budget**, which is what makes it a deadline the
            // parent and the child genuinely share: both `select!`s fire at the same moment, in
            // different tasks and possibly in different processes, with no signal passing between
            // them. A propagated deadline is the cancellation cascade, and needs no other one.
            (Timeout::Inherit, Some(parent)) => parent.deadline,
            (Timeout::Inherit, None) => None,
        };

        let started_at = Timestamp::now();
        let initialized = executor
            .sysdb()
            .init_workflow(
                &NewWorkflow {
                    name: Some(&self.key().name),
                    class_name: self.key().class_name.as_deref(),
                    config_name: self.key().config_name.as_deref(),
                    input: input.as_deref(),
                    serialization: Some(executor.serializer().name()),
                    executor_id: Some(executor.executor_id()),
                    application_name: Some(executor.app_name()),
                    application_version: Some(executor.application_version()),
                    // Only a budget is written: an inherited deadline is an *instant* and has no
                    // budget behind it, and `Timeout::None` has neither. The column is what a
                    // queue recomputes a deadline from on dequeue, so filling it in for either
                    // would hand that path a budget nobody asked for.
                    timeout: options.timeout.budget(),
                    deadline,
                    parent_workflow_id: parent.as_ref().map(|parent| parent.workflow_id.as_str()),
                    ..NewWorkflow::new(&workflow_id)
                },
                Some(MAX_RECOVERY_ATTEMPTS),
                Submission::Fresh,
            )
            .await
            .map_err(Error::SystemDatabase)?;

        // **After the child exists, not before**, and the order is what makes a crash between the
        // two harmless: a parent that dies here leaves a child row and no launch record, and the
        // replay re-derives the same id, finds the row owned, and joins it. The reverse order
        // would leave a launch record pointing at a workflow that was never created.
        if let Some(parent) = &parent {
            parent
                .record_launch(&executor, &workflow_id, &self.key().name, started_at)
                .await?;
        }

        // Someone else owns this row — the id was supplied and a previous run has it, or another
        // executor claimed it first. Joining rather than erroring is what makes a retried request
        // idempotent, and it is where Python waits too.
        if !initialized.should_execute {
            tracing::debug!(
                workflow_id,
                "the workflow is already owned; the handle joins the existing run"
            );
            return Ok(WorkflowHandle::polling(executor, workflow_id));
        }

        // The deadline the *database* holds, not the one this caller offered: `init_workflow`
        // turns a budget into an instant against its own clock, and an existing row keeps the
        // deadline it already had rather than taking a new one from a joining caller.
        let task = spawn_execution(
            &executor,
            self.key().clone(),
            workflow_id.clone(),
            input,
            initialized.deadline,
        );
        Ok(WorkflowHandle::local(executor, workflow_id, task))
    }
}

/// The workflow a child is being started from: its id, and the step id the launch occupies.
///
/// Built once per `start_with` call, because building it *allocates a step id* — the parent's
/// counter moves whether or not the launch ends up creating anything, which is what keeps a replay
/// aligned with the run it is replaying.
struct Parent {
    workflow_id: String,
    step_id: i32,
    /// The parent's own deadline, for the child to inherit when it asks for no budget of its own.
    deadline: Option<Timestamp>,
}

impl Parent {
    /// Reads back a launch recorded at this position, if this parent has run this far before.
    ///
    /// `check_step` compares the recorded name, so a mismatch here is already
    /// [`Error::UnexpectedStep`] before this sees it. What is left to check is the child id: a row
    /// under the right name carrying none was written by a plain step, which means the parent's
    /// code changed — `step("charge")` became a child workflow named `charge` — and starting a
    /// child now would give this position two meanings across two runs.
    ///
    /// **Stricter than the references here.** Python falls through to a fresh launch when the
    /// recorded row has no child id, and Go's `CheckChildWorkflow` returns nothing for it. Both end
    /// up loud rather than wrong — the write conflicts a moment later — but only after a child row
    /// has been created and orphaned, which is a worse thing to leave behind than an error.
    async fn recorded_launch(
        &self,
        executor: &Executor,
        step_name: &str,
    ) -> Result<Option<String>> {
        let Some(recorded) = executor
            .sysdb()
            .check_step(&self.workflow_id, self.step_id, step_name)
            .await
            .map_err(Error::SystemDatabase)?
        else {
            return Ok(None);
        };
        recorded.child_workflow_id.map(Some).ok_or_else(|| {
            Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                workflow_id: self.workflow_id.clone(),
                step_id: self.step_id,
                expected: format!("a child workflow launch of {step_name}"),
                recorded: format!("a plain step named {step_name}"),
            })
        })
    }

    /// Records the launch, so the replay above finds it.
    async fn record_launch(
        &self,
        executor: &Executor,
        child_workflow_id: &str,
        step_name: &str,
        started_at: Timestamp,
    ) -> Result<()> {
        executor
            .sysdb()
            .record_child_workflow(
                &self.workflow_id,
                child_workflow_id,
                self.step_id,
                step_name,
                Some(started_at),
            )
            .await
            .map_err(Error::SystemDatabase)
    }
}

/// Spawns the workflow body on the executor's runtime and registers it for shutdown to reach.
///
/// Spawned, not awaited in place: the workflow's life is the executor's, not the caller's, so
/// dropping any future that observes the returned handle must not stop the run. Both submitters go
/// through here — a caller's `run`, which awaits the handle, and recovery, which does not.
///
/// The span travels with the task, so everything the workflow logs — the engine's own events and
/// the application's — carries the workflow id without threading it anywhere.
pub(crate) fn spawn_execution(
    executor: &Arc<Executor>,
    key: WorkflowKey,
    workflow_id: String,
    input: Option<String>,
    deadline: Option<Timestamp>,
) -> tokio::task::JoinHandle<std::result::Result<Option<String>, Failure>> {
    let span = tracing::info_span!("workflow", workflow_id = %workflow_id, name = %key);
    spawn_tracked(
        executor,
        {
            let executor = Arc::clone(executor);
            async move {
                let ctx = Ctx::new(Arc::clone(&executor), &workflow_id, deadline);
                let _panic_log = PanicLog {
                    workflow_id: &workflow_id,
                };
                execute(&executor, &key, &workflow_id, input, ctx).await
            }
        }
        .instrument(span),
    )
}

/// Runs `body` until it finishes or its deadline passes, cancelling it durably if the deadline wins.
///
/// **This is the engine's only durable-cancellation path, and shutdown deliberately does not go
/// through it.** Go #426 had to fix exactly that confusion: its shutdown cancelled a context, the
/// context fired the durable cancel hook, and workflows that should have stayed `PENDING` for
/// recovery were written `CANCELLED` — inverting what shutdown means. Here the two use different
/// mechanisms rather than a shared hook. A deadline is a branch of this `select!` that writes
/// `CANCELLED` before returning; shutdown aborts the task outright, so no code of ours runs, no
/// row is written, and the workflow is recovered. There is no cause to inspect because there is no
/// single hook both reach, which is why this carries no cancellation-cause enum.
///
/// The deadline is an *instant* rather than a budget, and that is what makes it survive a crash: a
/// workflow recovered with two minutes left waits two minutes, not the whole timeout again. A
/// deadline already in the past yields a zero wait and cancels at once, which is the correct
/// reading of a workflow recovered after its expiry.
async fn run_until_deadline<F>(
    executor: &Executor,
    workflow_id: &str,
    deadline: Option<Timestamp>,
    body: F,
) -> Ended
where
    F: Future<Output = std::result::Result<Option<String>, Failure>>,
{
    let Some(deadline) = deadline else {
        return Ended::Body(body.await);
    };
    let remaining = deadline
        .duration_since(Timestamp::now())
        .unwrap_or(Duration::ZERO);

    // Biased so a body that finished in the same instant keeps its outcome: it did complete, and
    // recording a completed workflow as cancelled would discard work that was actually done.
    tokio::select! {
        biased;
        outcome = body => Ended::Body(outcome),
        () = tokio::time::sleep(remaining) => {
            Ended::Terminal(cancel_at_deadline(executor, workflow_id).await)
        }
    }
}

/// How an execution ended, and so whether its caller has anything left to record.
enum Ended {
    /// The body ran to a conclusion. That conclusion is this execution's outcome and is written.
    Body(std::result::Result<Option<String>, Failure>),
    /// The deadline fired and the row is already terminal — cancelled here, or finished by another
    /// execution while this one was working. What it holds is the answer; nothing more is written.
    Terminal(std::result::Result<Option<String>, Failure>),
}

/// Cancels a workflow whose deadline has passed, and settles on what the row ends up holding.
///
/// **`cancel_workflows` reports which ids actually moved, and that is the whole reason to read
/// it.** `cancel_batch` leaves a workflow that has already finished alone, so an empty result here
/// means a rival execution recorded an outcome first — recovery after this process was presumed
/// dead, or another SDK sharing the system database. That outcome is the workflow's, and this
/// caller must be told the same thing every other caller reads, not a cancellation that the row
/// does not record and that `handle.status()` would immediately contradict.
async fn cancel_at_deadline(
    executor: &Executor,
    workflow_id: &str,
) -> std::result::Result<Option<String>, Failure> {
    let cancelled = || {
        Err(Failure::Control(Error::WorkflowCancelled {
            workflow_id: workflow_id.to_owned(),
        }))
    };
    tracing::info!(
        workflow_id,
        "the workflow exceeded its deadline and is cancelled"
    );
    // Durable, unlike every other stop this engine performs. Failing to write it is not fatal —
    // the row stays PENDING and is recovered, where the expired deadline is read again and cancels
    // immediately — so this logs rather than propagating, and still reports the cancellation it
    // was unable to record.
    match executor
        .sysdb()
        .cancel_workflows(&[workflow_id], false)
        .await
    {
        Ok(moved) if moved.iter().any(|id| id == workflow_id) => cancelled(),
        Ok(_) => {
            tracing::warn!(
                workflow_id,
                "the deadline fired on a workflow another execution had already finished; its \
                 recorded outcome stands"
            );
            adopt(executor, workflow_id).await
        }
        Err(error) => {
            tracing::error!(
                workflow_id,
                %error,
                "could not record the deadline cancellation; the row stays PENDING for recovery"
            );
            cancelled()
        }
    }
}

/// Runs the body with a context ambient and records what it did.
async fn execute(
    executor: &Executor,
    key: &WorkflowKey,
    workflow_id: &str,
    input: Option<String>,
    ctx: Ctx,
) -> std::result::Result<Option<String>, Failure> {
    let workflow = executor
        .workflows()
        .get(key)
        .ok_or_else(|| {
            Failure::Control(Error::NotRegistered {
                key: key.to_string(),
            })
        })?
        .clone();

    // From the context rather than from a parameter beside it: the deadline is part of what this
    // workflow *is* for the whole of its run — a child launched halfway through reads the same
    // value this `select!` is watching — so one copy travels with the context and nothing can hand
    // the two halves different instants.
    let deadline = ctx.deadline();
    let outcome = match run_until_deadline(
        executor,
        workflow_id,
        deadline,
        Ctx::scope(ctx, workflow(input)),
    )
    .await
    {
        Ended::Body(outcome) => outcome,
        // The row is terminal already — this execution has no outcome of its own to write, and a
        // write behind the one that is there would be refused anyway.
        Ended::Terminal(settled) => return settled,
    };

    let write = match &outcome {
        Ok(output) => {
            executor
                .sysdb()
                .record_workflow_outcome(workflow_id, Outcome::Output(output.as_deref()))
                .await
        }
        // A control signal is not the workflow's outcome, so this execution writes nothing
        // terminal. Where that leaves the row depends on the signal — PENDING for a later executor
        // after a shutdown or a failed write, already CANCELLED when the signal is a cancellation
        // raised elsewhere and observed by a preemptible step — so the line reports what this
        // execution did and does not claim a status it has not read. Warned rather than
        // debug-logged, because the caller may have dropped its future: this can be the only
        // evidence.
        Err(Failure::Control(control)) => {
            tracing::warn!(
                error = %control,
                "a control signal ended this execution: it records no outcome of its own"
            );
            return outcome;
        }
        Err(Failure::Recorded(encoded)) => {
            executor
                .sysdb()
                .record_workflow_outcome(workflow_id, Outcome::Error(encoded))
                .await
        }
    }
    .map_err(|error| {
        tracing::warn!(
            error = %error,
            "could not record the workflow's outcome: the row stays PENDING"
        );
        Failure::Control(Error::SystemDatabase(error))
    })?;

    match write {
        OutcomeWrite::Recorded => {
            match &outcome {
                Ok(_) => tracing::debug!("the workflow completed; its output is recorded"),
                Err(_) => tracing::debug!("the workflow failed; its error is recorded"),
            }
            outcome
        }
        // Another run finished this workflow while this one was working. It is superseded, so what
        // it computed is not the answer — the recorded outcome is, and every caller must agree on
        // which one that is.
        OutcomeWrite::AlreadyFinished => {
            tracing::warn!(
                workflow_id,
                "another execution recorded this workflow's outcome first"
            );
            adopt(executor, workflow_id).await
        }
    }
}

/// Reads back the outcome of a workflow this caller does not own.
pub(crate) async fn adopt(
    executor: &Executor,
    workflow_id: &str,
) -> std::result::Result<Option<String>, Failure> {
    let outcome = executor
        .sysdb()
        .await_workflow_result(workflow_id, executor.outcome_poll_interval())
        .await
        .map_err(|e| Failure::Control(Error::SystemDatabase(e)))?;
    match outcome {
        AwaitedOutcome::Succeeded { output, .. } => Ok(output),
        // Handed back encoded, for the caller to decode into its own error type — the adopting
        // caller knows what that is and this function does not.
        AwaitedOutcome::Failed { error, .. } => Err(Failure::Recorded(error)),
        AwaitedOutcome::Cancelled => Err(Failure::Control(Error::WorkflowCancelled {
            workflow_id: workflow_id.to_owned(),
        })),
        AwaitedOutcome::Parked { recovery_attempts } => {
            Err(Failure::Control(Error::MaxRecoveryAttemptsExceeded {
                workflow_id: workflow_id.to_owned(),
                recovery_attempts,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    /// A subscriber that keeps every event's level and message, and nothing else.
    ///
    /// Hand-rolled because the crate has no `tracing-subscriber` dependency, and one test does not
    /// justify one: the trait is eight methods, six of which do nothing here.
    struct Capture(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        use std::fmt::Write;
                        let _ = write!(self.0, "{value:?}");
                    }
                }
            }
            let mut message = Message(format!("{} ", event.metadata().level()));
            event.record(&mut message);
            self.0.lock().unwrap().push(message.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// The premise the guard is built on, pinned: an async block's live locals are dropped during
    /// the panic's unwind out of `poll`, so the guard fires while `std::thread::panicking()` is
    /// still true. Were a runtime change to defer that drop past the catch, the error line would
    /// vanish silently — this is the test that notices.
    ///
    /// `#[tokio::test]` runs a current-thread runtime, which is what lets a thread-local
    /// subscriber observe a spawned task.
    #[tokio::test]
    async fn a_panic_in_a_spawned_task_is_logged_from_the_unwind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(Capture(Arc::clone(&events)));

        let task = tokio::spawn(async {
            let _log = PanicLog {
                workflow_id: "wf-boom",
            };
            // Held across an await point, as the real guard is.
            tokio::task::yield_now().await;
            panic!("a bug, not an outcome");
        });
        let joined = task.await.expect_err("the task must panic");
        assert!(joined.is_panic());

        let events = events.lock().unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.starts_with("ERROR") && e.contains("panicked")),
            "the unwind must produce the error event: {events:?}"
        );
    }

    /// The half that makes shutdown mean "stopped" rather than "told to stop": the sweep does not
    /// finish while a task it cancelled is still on its way out.
    #[tokio::test]
    async fn abort_all_waits_until_every_task_has_departed() {
        let tasks = Arc::new(Tasks::default());
        tasks.arrived();

        let mut sweep = {
            let tasks = Arc::clone(&tasks);
            tokio::spawn(async move { tasks.abort_all().await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut sweep)
                .await
                .is_err(),
            "the sweep returned while a task was still live"
        );

        tasks.departed();
        tokio::time::timeout(Duration::from_secs(10), sweep)
            .await
            .expect("the sweep never noticed the last task leaving")
            .expect("the sweep panicked");
    }

    /// The race between spawning a workflow and registering it: one that arrives after the sweep
    /// has run is cancelled here, rather than outliving the shutdown meant to stop it.
    #[tokio::test]
    async fn a_task_arriving_after_the_sweep_is_cancelled() {
        let tasks = Tasks::default();
        tasks.abort_all().await;

        let task = tokio::spawn(std::future::pending::<()>());
        tasks.insert(task.abort_handle());

        let joined = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the late task was never cancelled");
        assert!(joined.expect_err("it must be cancelled").is_cancelled());
    }

    /// The defused path: a task that completes normally logs nothing.
    #[tokio::test]
    async fn a_completed_task_logs_no_panic() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(Capture(Arc::clone(&events)));

        tokio::spawn(async {
            let _panic_log = PanicLog {
                workflow_id: "wf-fine",
            };
            tokio::task::yield_now().await;
        })
        .await
        .expect("the task must complete");

        let events = events.lock().unwrap();
        assert!(events.is_empty(), "{events:?}");
    }
}
