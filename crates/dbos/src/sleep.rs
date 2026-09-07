//! Durable sleep: a wait that survives the process waiting it out.

use std::sync::Arc;
use std::time::Duration;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::context::Ctx;
use crate::error::{DurableError, Error};
use crate::sysdb::types::{Timestamp, step_names};

/// Waits for `duration`, resuming at the **original** wake time after a crash.
///
/// The difference from [`tokio::time::sleep`] is what happens when the process dies. A plain sleep
/// restarts from the beginning on recovery, so a workflow that sleeps an hour and crashes after
/// fifty minutes sleeps another hour. This records the wake time as a checkpoint before waiting, so
/// the same workflow has ten minutes left — which is the only version of "sleep for an hour" that
/// composes with durable execution.
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// use std::time::Duration;
/// dbos::sleep(Duration::from_secs(3600)).await?;
/// # Ok(()) }
/// ```
///
/// **The step id is taken here, at the call, not at the first poll** — see [`PendingStep`]. A sleep
/// built beside a step and driven with it by `tokio::join!` therefore takes the same slot on every
/// execution, however the two futures interleave, and a sleep built and dropped has spent that slot
/// all the same.
///
/// **Outside a workflow this is a plain sleep**, the way a step outside a workflow is a plain call:
/// there is no step sequence to checkpoint against, and a function built from steps and sleeps
/// stays ordinarily callable and ordinarily testable. Inside a *step* the same applies, since a
/// step is a leaf.
///
/// The checkpoint's `completed_at` is stamped at the wake time rather than at the moment the row is
/// written, so an hour's sleep reads as an hour on a timeline instead of as an instant. All four
/// references now do this — Go was the holdout and joined in #442.
///
/// A zero or negative duration is not an error: it records the checkpoint and returns, so a
/// computed delay that has already elapsed behaves the same on the first run and on a replay.
pub fn sleep<E>(duration: Duration) -> PendingStep<'static, (), E>
where
    E: DurableError + 'static,
{
    // The whole of what happens at the call: read where we are, and take the id if this is a place
    // that checkpoints. Outside a workflow there is no executor to take either, which is the one
    // case that has nothing to carry.
    let built = match Ctx::current() {
        None => Ok((None, StepPlacement::Outside)),
        Some(ctx) => StepPlacement::taken(Ok(Arc::clone(ctx.executor())), "sleep")
            .map(|(executor, placement)| (Some(executor), placement)),
    };
    PendingStep::placed(
        step_names::SLEEP,
        built,
        move |executor, placement| async move {
            // Both halves or neither: an executor with no id behind it is the in-step and
            // outside-a-workflow cases, where the placement recorded nothing and there is
            // nothing to record now.
            let Some((executor, (workflow_id, step_id))) = executor.zip(placement.step()) else {
                tracing::debug!(
                    duration_ms = duration.as_millis(),
                    "the sleep is not checkpointed: it is outside a workflow, or inside a step"
                );
                tokio::time::sleep(duration).await;
                return Ok(());
            };

            let wake_at = executor
                .sysdb()
                .record_sleep(workflow_id, step_id, duration)
                .await
                .map_err(Error::SystemDatabase)?;

            // From the recorded wake time, not from now: on a replay `record_sleep` gives back the
            // instant the first run chose, and the remaining wait is whatever is left of it. A
            // replay that woke long ago waits not at all.
            let remaining = wake_at.duration_since(Timestamp::now()).unwrap_or_default();
            tracing::debug!(
                step_id,
                remaining_ms = remaining.as_millis(),
                "sleeping until the recorded wake time"
            );
            tokio::time::sleep(remaining).await;
            Ok(())
        },
    )
}
