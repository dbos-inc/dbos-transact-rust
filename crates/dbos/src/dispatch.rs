//! Running a workflow from its row: the path recovery and the queue runner share.
//!
//! Three things submit a workflow to this process, and two of them start from a row that already
//! exists. A fresh [`start`](crate::DBOS::start) has the arguments in hand and writes the row
//! itself; recovery and a dequeue both find a row someone else wrote — a dead executor's, or one
//! this fleet enqueued — and have to reconstruct the call from it. That reconstruction is what
//! lives here: look the registration up by the row's own name, claim the row, and spawn.
//!
//! **The fetch is deliberately not part of it.** Recovery reads one row at a time because it
//! processes one id at a time; a dequeue reads every row it claimed in a single round trip, which
//! is the shape Python's `start_dequeued_workflows` chose and the reason Rust's
//! `start_queued_workflows` returns ids rather than rows. Taking the row as an argument lets each
//! caller keep its own read and share everything after it.

use std::sync::Arc;

use crate::dbos::Executor;
use crate::dequeue::Slot;
use crate::error::Error;
use crate::registry::WorkflowKey;
use crate::sysdb;
use crate::sysdb::types::{NewWorkflow, Submission, WorkflowRecord};
use crate::workflow::{MAX_RECOVERY_ATTEMPTS, spawn_execution};

/// Submits one workflow from the row that describes it, if this executor still can and should.
///
/// `submission` is the whole of the difference between the two callers. Both
/// [`Submission::Recovery`] and [`Submission::Dequeue`] claim a row another executor may hold and
/// both count against the recovery budget — that is what [`Submission::claims_ownership`] is —
/// so the cap below is not a parameter. [`Submission::Fresh`] does not belong here: a first
/// attempt starts from arguments, not from a row, and has never been through this path.
///
/// **The registry is consulted before the row is claimed.** An `init_workflow` that succeeded and
/// then found no registration would have burned an attempt and re-stamped the executor on a
/// workflow this process cannot run.
///
/// `slot` is the dequeue's place in its queue's local running tally, and is `None` for
/// recovery. It travels into the spawned execution so the tally is released when the workflow's
/// task ends — and is dropped here, releasing it immediately, on every path that does not spawn.
///
/// Returning `Ok(())` covers every reason not to run that is not a fault: no name, no
/// registration, a row already claimed, a row already finished, a parked row. The caller logs a
/// genuine `Err` and moves on to the next id — a sweep must not strand every workflow behind one
/// bad row.
pub(crate) async fn dispatch(
    executor: &Arc<Executor>,
    row: WorkflowRecord,
    submission: Submission,
    slot: Option<Slot>,
) -> crate::Result<()> {
    let workflow_id = row.workflow_id;
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
            "no workflow is registered under the row's name; it stays where it is"
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
                // The row's own, not this executor's default: a submission from a row must not
                // rewrite how a payload it did not encode is described.
                serialization: row.serialization.as_deref(),
                // The queue the row is already on. Omitting it does not mean "leave the queue
                // alone" — `init_workflow` reads it as `None` and warns that the workflow is
                // being submitted onto a different queue, which for every dequeued row is both
                // untrue and unavoidable. Passing the row's own value is what lets that warning
                // go on meaning a genuine requeue.
                queue_name: row.queue_name.as_deref(),
                executor_id: Some(executor.executor_id()),
                application_name: Some(executor.app_name()),
                application_version: Some(executor.application_version()),
                ..NewWorkflow::new(&workflow_id)
            },
            Some(MAX_RECOVERY_ATTEMPTS),
            submission,
        )
        .await
    {
        Ok(initialized) => initialized,
        // Parked, not failed to submit: the row is now MAX_RECOVERY_ATTEMPTS_EXCEEDED and no
        // later launch or dequeue will pick it up, which deserves its own line rather than the
        // generic one.
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
        submission = ?submission,
        "running the workflow"
    );
    // Detached: submitted workflows run concurrently and the caller moves on. The handle is not
    // awaited by anyone, which is exactly the case the execution layer's own logging covers.
    // The stored deadline, so a workflow gets what is *left* of its budget rather than the whole
    // of it again — and one submitted after its expiry cancels at once instead of running on
    // unbounded.
    spawn_execution(
        executor,
        key,
        workflow_id,
        row.input,
        initialized.deadline,
        slot,
    );
    Ok(())
}
