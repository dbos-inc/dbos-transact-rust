//! Recovery on launch: resuming what a previous process abandoned.
//!
//! The list of abandoned workflows is taken **before `launch` returns** — by
//! [`Executor::start`](crate::dbos::Executor), not here — and that ordering is load-bearing: a
//! workflow the application starts the instant launch returns is `PENDING` too, indistinguishable
//! from one a dead process left behind, so a list taken any later would claim it and run it a
//! second time alongside the caller still running it. Only the *execution* of the list is
//! backgrounded, which is what this module does.

use std::sync::Arc;

use tracing::Instrument;

use crate::dbos::Executor;
use crate::error::Error;
use crate::registry::WorkflowKey;
use crate::sysdb;
use crate::sysdb::types::{NewWorkflow, Submission};
use crate::workflow::{MAX_RECOVERY_ATTEMPTS, spawn_execution, spawn_tracked};

/// Runs the recovery list in the background, one submission at a time.
///
/// The driver is registered with the executor's task set, so shutdown aborts a recovery still in
/// flight the same way it aborts the workflows themselves — everything it managed to submit stays
/// `PENDING` for the next launch, which is recovery's own contract applied to itself.
///
/// Per-workflow failures are logged, never propagated: recovery is a sweep, and one bad row must
/// not strand every workflow behind it.
///
/// # This should become a re-enqueue
///
/// Executing here, in the process that found the row, is **Java's shape and only Java's**.
/// Python, TypeScript and Go re-enqueue every recovered workflow instead — non-queued ones onto
/// the internal queue — through one `reenqueue_for_recovery` / `ReenqueueForRecovery` call, and
/// let the queue dispatch them. `sdk-parity.md` divergence 12 records the split and calls Java the
/// odd one out.
///
/// Three things that buys, and none of them is available to this module today:
///
/// - **Idempotence.** The queue's atomic `ENQUEUED` → `PENDING` dequeue admits exactly one runner,
///   so a duplicate recovery request costs nothing. Here, two executors that both believe they own
///   a row are held apart by `init_workflow`'s guard instead, which is a narrower promise.
/// - **The fleet shares the work.** A re-enqueued workflow can be picked up by any executor on the
///   version; one recovered here runs here, so an executor that comes back to a large backlog
///   works through all of it alone.
/// - **Concurrency stops being this module's problem.** The sweep below spawns every pending
///   workflow at once, unbounded, and a process that died holding a large backlog will contend for
///   its own connection pool on the next launch. A queue owns that question — and note the other
///   three do not answer it either: their internal queue carries no default limit, so the bound is
///   a knob on the queue rather than something recovery decides.
///
/// This is not the shape to keep, but it is the only one available until the engine has queues:
/// `sysdb` has `INTERNAL_QUEUE`, `start_queued_workflows` and `clear_queue_assignment`, and no
/// `reenqueue_for_recovery` at all. When queues land, this module should lose its executor and its
/// `spawn_execution` call and become that one write.
pub(crate) fn spawn(executor: Arc<Executor>, pending: Vec<String>) {
    if pending.is_empty() {
        tracing::debug!("no workflows to recover");
        return;
    }
    tracing::info!(
        workflows = pending.len(),
        "recovering workflows a previous run left PENDING"
    );
    spawn_tracked(
        &executor,
        {
            let executor = Arc::clone(&executor);
            async move {
                for workflow_id in pending {
                    if let Err(error) = recover_one(&executor, &workflow_id).await {
                        tracing::warn!(
                            workflow_id,
                            error = %error,
                            "could not recover the workflow; it stays PENDING for a later launch"
                        );
                    }
                }
            }
        }
        .instrument(tracing::info_span!("recovery")),
    );
}

/// Resubmits one abandoned workflow, if this executor still can and should.
///
/// The registry is consulted *before* the row is claimed: an `init_workflow` that succeeded and
/// then found no registration would have burned a recovery attempt and re-stamped the executor on
/// a workflow this process cannot run.
async fn recover_one(executor: &Arc<Executor>, workflow_id: &str) -> crate::Result<()> {
    let row = executor
        .sysdb()
        .get_workflow(workflow_id)
        .await
        .map_err(Error::SystemDatabase)?;
    let Some(row) = row else {
        tracing::debug!(workflow_id, "the row is gone; nothing to recover");
        return Ok(());
    };
    let Some(name) = row.name else {
        tracing::warn!(workflow_id, "the row names no workflow; skipped");
        return Ok(());
    };

    let key = WorkflowKey::from_row(name, row.class_name.as_deref(), row.config_name.as_deref());
    if !executor.workflows().contains_key(&key) {
        // Logged and skipped, never fatal: the code that knew this workflow was removed or
        // renamed, and the row waits for a launch that recognises it.
        tracing::warn!(
            workflow_id,
            workflow = %key,
            "no workflow is registered under the row's name; it stays PENDING"
        );
        return Ok(());
    }

    let initialized = match executor
        .sysdb()
        .init_workflow(
            &NewWorkflow {
                name: Some(&key.name),
                class_name: key.class_name.as_deref(),
                config_name: key.config_name.as_deref(),
                input: row.input.as_deref(),
                // The row's own, not this executor's default: a recovery must not rewrite how a
                // payload it did not encode is described.
                serialization: row.serialization.as_deref(),
                executor_id: Some(executor.executor_id()),
                application_name: Some(executor.app_name()),
                application_version: Some(executor.application_version()),
                ..NewWorkflow::new(workflow_id)
            },
            Some(MAX_RECOVERY_ATTEMPTS),
            Submission::Recovery,
        )
        .await
    {
        Ok(initialized) => initialized,
        // Parked, not failed to recover: the row is now MAX_RECOVERY_ATTEMPTS_EXCEEDED and no
        // later launch will pick it up, which deserves its own line rather than the generic one.
        Err(error @ sysdb::Error::MaxRecoveryAttemptsExceeded { .. }) => {
            tracing::warn!(workflow_id, error = %error, "the workflow is parked");
            return Ok(());
        }
        Err(error) => return Err(Error::SystemDatabase(error)),
    };

    if !initialized.should_execute {
        tracing::debug!(
            workflow_id,
            "another executor claimed the workflow, or it already finished"
        );
        return Ok(());
    }

    tracing::debug!(
        workflow_id,
        workflow = %key,
        attempt = initialized.recovery_attempts,
        "recovering the workflow"
    );
    // Detached: recovered workflows run concurrently, and the driver moves on. The handle is not
    // awaited by anyone, which is exactly the case the execution layer's own logging covers.
    spawn_execution(executor, key, workflow_id.to_owned(), row.input);
    Ok(())
}
