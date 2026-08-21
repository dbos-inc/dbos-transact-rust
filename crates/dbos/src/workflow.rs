//! Running a workflow durably.

use std::future::Future;
use std::sync::{Arc, Mutex};

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
use crate::sysdb::types::{AwaitedOutcome, NewWorkflow, Outcome, OutcomeWrite, Submission};

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
/// true, and the workflow span still entered. Completing normally defuses it with `mem::forget`;
/// an abort also drops it, but outside a panic, so shutdown stays quiet.
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
        let workflow_id = match options.workflow_id {
            Some(id) => id.to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let input = Some(encode(&input, "argument")?);

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
                    ..NewWorkflow::new(&workflow_id)
                },
                Some(MAX_RECOVERY_ATTEMPTS),
                Submission::Fresh,
            )
            .await
            .map_err(Error::SystemDatabase)?;

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

        // No slot: a caller starting a workflow is its own backpressure, and blocking
        // `start` behind recovery's cap would make an unrelated backlog look like a hang.
        let task = spawn_execution(
            &executor,
            self.key().clone(),
            workflow_id.clone(),
            input,
            None,
        );
        Ok(WorkflowHandle::local(executor, workflow_id, task))
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
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
) -> tokio::task::JoinHandle<std::result::Result<Option<String>, Failure>> {
    let span = tracing::info_span!("workflow", workflow_id = %workflow_id, name = %key);
    spawn_tracked(
        executor,
        {
            let executor = Arc::clone(executor);
            async move {
                // Held for the whole run, so a bounded submitter's cap counts workflows that are
                // *running* rather than workflows it has managed to spawn.
                let _slot = slot;
                let ctx = Ctx::new(Arc::clone(&executor), &workflow_id);
                let panic_log = PanicLog {
                    workflow_id: &workflow_id,
                };
                let outcome = execute(&executor, &key, &workflow_id, input, ctx).await;
                std::mem::forget(panic_log);
                outcome
            }
        }
        .instrument(span),
    )
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

    let outcome = Ctx::scope(ctx, workflow(input)).await;

    let write = match &outcome {
        Ok(output) => {
            executor
                .sysdb()
                .record_workflow_outcome(workflow_id, Outcome::Output(output.as_deref()))
                .await
        }
        // A control signal is not the workflow's outcome, so nothing terminal is written and the
        // row stays where it was for a later executor to pick up. Warned rather than debug-logged,
        // because the caller may have dropped its future — this line can be the only evidence.
        Err(Failure::Control(control)) => {
            tracing::warn!(
                error = %control,
                "a control signal ended this execution: nothing is recorded, and the row stays \
                 PENDING"
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
            let panic_log = PanicLog {
                workflow_id: "wf-fine",
            };
            tokio::task::yield_now().await;
            std::mem::forget(panic_log);
        })
        .await
        .expect("the task must complete");

        let events = events.lock().unwrap();
        assert!(events.is_empty(), "{events:?}");
    }
}
