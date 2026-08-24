//! Durable sleep: a wait that survives the process waiting it out.

use std::time::Duration;

use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::sysdb::types::Timestamp;

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
pub async fn sleep<E>(duration: Duration) -> Result<(), E>
where
    E: DurableError,
{
    let Some(ctx) = Ctx::current().filter(|ctx| !ctx.in_step()) else {
        tracing::debug!(
            duration_ms = duration.as_millis(),
            "the sleep is not checkpointed: it is outside a workflow, or inside a step"
        );
        tokio::time::sleep(duration).await;
        return Ok(());
    };

    let step_id = ctx.next_step_id();
    let wake_at = ctx
        .executor()
        .sysdb()
        .record_sleep(ctx.workflow_id(), step_id, duration)
        .await
        .map_err(Error::SystemDatabase)?;

    // From the recorded wake time, not from now: on a replay `record_sleep` gives back the instant
    // the first run chose, and the remaining wait is whatever is left of it. A replay that woke
    // long ago waits not at all.
    let remaining = wake_at.duration_since(Timestamp::now()).unwrap_or_default();
    tracing::debug!(
        step_id,
        remaining_ms = remaining.as_millis(),
        "sleeping until the recorded wake time"
    );
    tokio::time::sleep(remaining).await;
    Ok(())
}
