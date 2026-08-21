//! Running a workflow durably.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::AbortHandle;

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
    let task = {
        let executor = Arc::clone(&executor);
        let workflow_id = workflow_id.clone();
        executor.runtime().clone().spawn(async move {
            let ctx = Ctx::new(Arc::clone(&executor), &workflow_id);
            execute(&executor, &key, &workflow_id, input, ctx).await
        })
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
        // row stays where it was for a later executor to pick up.
        Err(Failure::Control(_)) => return outcome,
        Err(Failure::Recorded(encoded)) => {
            executor
                .sysdb()
                .record_workflow_outcome(workflow_id, Outcome::Error(encoded))
                .await
        }
    }
    .map_err(|e| Failure::Control(Error::SystemDatabase(e)))?;

    match write {
        OutcomeWrite::Recorded => outcome,
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
