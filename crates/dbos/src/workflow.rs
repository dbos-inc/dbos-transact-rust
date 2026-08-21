//! Running a workflow durably.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::AbortHandle;
use tracing::Instrument;

use crate::context::Ctx;
use crate::dbos::Executor;
use crate::error::{DurableError, Error, Failure, Result};
use crate::registry::{WorkflowKey, WorkflowRef};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{AwaitedOutcome, NewWorkflow, Outcome, OutcomeWrite, Submission};

/// How often an adopting caller asks whether the run that won has finished.
///
/// Workflow completion has no wakeup in any implementation — no channel carries it and no trigger
/// publishes it — so the only way to learn a status changed is to look. One second is the interval
/// every reference polls at.
const OUTCOME_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Attempts before a workflow is parked as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
///
/// The default every implementation shares. It only counts recoveries and dequeues; starting a
/// workflow fresh never approaches it.
const MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// The workflows this executor started and has not seen finish.
///
/// Only abort handles, deliberately: this is what [`shutdown`](Executor::shutdown) uses to cancel
/// in-flight work, and nothing here ever awaits a task. A caller awaiting its own workflow holds
/// the `JoinHandle`; a caller that dropped its future is no longer waiting but the workflow is
/// still running, and shutdown still has to reach it.
#[derive(Default)]
pub(crate) struct Tasks {
    running: Mutex<Vec<AbortHandle>>,
}

impl Tasks {
    /// Records a running workflow, dropping the handles of any that have since finished.
    ///
    /// Pruning on insert rather than on completion keeps this to one lock and no bookkeeping ids.
    /// The cost is that a finished workflow's handle lingers until the next one starts, which is a
    /// pointer, and the benefit is that a workflow whose caller dropped its future is still
    /// reachable by shutdown — which is the case that matters.
    fn insert(&self, handle: AbortHandle) {
        let mut running = self.lock();
        running.retain(|handle| !handle.is_finished());
        running.push(handle);
    }

    /// Cancels every workflow still running, reporting how many were cancelled.
    ///
    /// Their rows stay `PENDING`, which is the point: a cancelled workflow is one a later executor
    /// recovers, so an abrupt shutdown loses no work. Writing a terminal status here would be the
    /// bug — it would mark as finished something that never finished.
    pub(crate) fn abort_all(&self) -> usize {
        let mut running = self.lock();
        let live: Vec<AbortHandle> = running.drain(..).filter(|h| !h.is_finished()).collect();
        for handle in &live {
            handle.abort();
        }
        live.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<AbortHandle>> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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

impl<P, R, E> WorkflowRef<P, R, E>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    E: DurableError,
{
    /// Runs this workflow durably and waits for its result.
    ///
    /// The workflow is recorded before its body starts, so a process that dies mid-run leaves a
    /// `PENDING` row a later executor recovers. Awaiting is a convenience over the durable run
    /// rather than the thing that makes it durable: drop this future and the workflow carries on.
    pub async fn run(&self, input: P) -> Result<R, E> {
        let executor = self
            .dbos()
            .executor("run a workflow")
            .map_err(Error::lift)?;
        let workflow_id = uuid::Uuid::new_v4().to_string();
        let input = Some(encode(&input, "argument")?);

        match run_durably(executor, self.key().clone(), workflow_id.clone(), input).await {
            Ok(output) => decode(output.as_deref(), "result"),
            // The workflow's own failure, decoded back into the caller's error type — the same
            // fidelity the result gets, and the reason the error is a type parameter. Execution
            // and replay read the same bytes, so they return the same error.
            //
            // Degrades to the message when the column does not hold one of ours, which is what a
            // row written by another SDK looks like: its serializer chose its own shape, and no
            // amount of type information here will reconstruct a type that was never Rust's.
            Err(Failure::Recorded(encoded)) => Err(decode::<_, E>(Some(&encoded), "error")
                .unwrap_or_else(|_| Error::WorkflowFailed {
                    workflow_id,
                    message: encoded,
                })),
            Err(Failure::Control(control)) => Err(control.lift()),
        }
    }
}

/// Records the workflow, runs it if this caller owns it, and returns its encoded outcome.
///
/// Typeless from here down, which is what lets recovery reuse it: recovery has a row, not types.
async fn run_durably(
    executor: Arc<Executor>,
    key: WorkflowKey,
    workflow_id: String,
    input: Option<String>,
) -> std::result::Result<Option<String>, Failure> {
    let initialized = executor
        .sysdb()
        .init_workflow(
            &NewWorkflow {
                name: Some(&key.name),
                class_name: key.class_name.as_deref(),
                config_name: key.config_name.as_deref(),
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
        .map_err(|e| Failure::Control(Error::SystemDatabase(e)))?;

    // Someone else owns this row — the id was supplied and a previous run has it, or another
    // executor claimed it first. Adopting rather than erroring is what makes a retried request
    // idempotent, and it is where Python waits too.
    if !initialized.should_execute {
        tracing::debug!(
            workflow_id,
            "the workflow is already owned; adopting its outcome"
        );
        return adopt(&executor, &workflow_id).await;
    }

    // Spawned, not awaited in place: the workflow's life is the executor's, not the caller's, so
    // dropping the returned future must not stop the run and shutdown must be able to reach it.
    //
    // The span travels with the task, so everything the workflow logs — the engine's own events
    // and the application's — carries the workflow id without threading it anywhere.
    let span = tracing::info_span!("workflow", workflow_id = %workflow_id, name = %key);
    let task = {
        let executor = Arc::clone(&executor);
        let workflow_id = workflow_id.clone();
        executor.runtime().clone().spawn(
            async move {
                let ctx = Ctx::new(Arc::clone(&executor), &workflow_id);
                let panic_log = PanicLog {
                    workflow_id: &workflow_id,
                };
                let outcome = execute(&executor, &key, &workflow_id, input, ctx).await;
                std::mem::forget(panic_log);
                outcome
            }
            .instrument(span),
        )
    };
    executor.tasks().insert(task.abort_handle());

    match task.await {
        Ok(outcome) => outcome,
        // Only shutdown aborts a workflow task, and it leaves the row `PENDING` on purpose.
        Err(join) if join.is_cancelled() => {
            Err(Failure::Control(Error::Interrupted { workflow_id }))
        }
        Err(join) => std::panic::resume_unwind(join.into_panic()),
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
async fn adopt(
    executor: &Executor,
    workflow_id: &str,
) -> std::result::Result<Option<String>, Failure> {
    let outcome = executor
        .sysdb()
        .await_workflow_result(workflow_id, OUTCOME_POLL_INTERVAL)
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
