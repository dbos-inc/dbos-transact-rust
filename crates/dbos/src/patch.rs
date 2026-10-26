//! Patching: changing a workflow's code while executions of the old code are still in flight.
//!
//! A replay follows the step history its first execution recorded, position by position, so a
//! workflow's code cannot simply change under a run that is still going: the new code asks for a
//! step at a position where the history holds a different one. A patch is a recorded fork in the
//! code. An execution that reaches it for the first time records a marker there and takes the new
//! path; one whose history already holds something else at that position was started by the old
//! code, takes the old path, and replays as it was recorded.
//!
//! The marker is an ordinary step row, written through
//! [`record_step`](crate::sysdb::SystemDatabase::record_step) under the same ownership check as any
//! checkpoint, and read back through [`check_step`](crate::sysdb::SystemDatabase::check_step) —
//! whose refusal of a row recorded under another name is exactly the "old code was here" answer a
//! patch is asking for.
//!
//! # Why the position is peeked, and then claimed
//!
//! Every durable call fixes its step position where it is built, before anything runs, so the
//! order calls are written in is the order their positions follow. A patch does too: building one
//! looks at the next id without taking it ([`Ctx::peek_step_id`]). It cannot take it there,
//! because whether it occupies the position depends on what the history holds at it, and reading
//! that is a database round trip. So awaiting it reads the history at the peeked position, and
//! then takes the id only if it is still the next one ([`Ctx::claim_step_id`], one
//! compare-and-swap).
//!
//! The swap is what keeps the marker at the position the code says. If any other call of this
//! execution was built between this one being built and its read coming back — written after it
//! but awaited first, or built in another branch of the same body — that call has the position,
//! and the patch fails with [`Error::PatchNondeterminism`] rather than record its marker anywhere
//! else. If none was, the peek, the read and the take behave as one act at the moment the patch
//! was built, which is where sequential code puts it. A patch that decides *not* to occupy the
//! position checks the same thing, since "nothing else moved" is also what makes its answer the
//! one a replay will give.
//!
//! The marker is written only after the position is claimed, so it never lands at a position this
//! execution has handed to another call.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::checkpoint::StepPlacement;
use crate::context::Ctx;
use crate::error::{DurableError, Error};
use crate::sysdb::types::{Outcome, StepTiming, Timestamp, step_names};

/// Whether this execution should take the new code path at a patch point.
///
/// Wrap a change to a workflow's code in a patch when executions of the old code may still be
/// running, or may be recovered, once the new code is deployed:
///
/// ```no_run
/// # async fn charge_card() -> dbos::Result<u32> { Ok(1) }
/// # async fn charge_wallet() -> dbos::Result<u32> { Ok(1) }
/// # async fn body() -> dbos::Result<u32> {
/// let charged = if dbos::patch("pay-by-wallet").await? {
///     charge_wallet().await? // the new code
/// } else {
///     charge_card().await? // the code that was here before
/// };
/// # Ok(charged) }
/// ```
///
/// - **An execution reaching this point for the first time** — a new workflow, or an old one that
///   had not got this far — records a marker at the next step position, `DBOS.patch-<name>`, and
///   gets `true`: it runs the new code from here on.
/// - **An execution that recorded this marker before** gets `true` again, and replays the new code.
/// - **An execution whose history holds some other step at this position** was started by the old
///   code and got past this point with it. It gets `false` and takes the old path, which replays
///   what it recorded. Nothing is recorded, and the position is left for the old path's step.
///
/// Patches nest: a later change wraps the patched code in a second patch with a new name, tested
/// first, so each generation of executions finds its own marker or none.
///
/// # Retiring a patch
///
/// Once no execution of the old code can run again — every one has finished, and none will be
/// forked or recovered — replace the `if` with [`deprecate_patch`] and keep only the new code.
/// That honours the markers already recorded but writes none, so once every execution carrying a
/// marker has finished too, the `deprecate_patch` call can be deleted.
///
/// # Application version
///
/// A workflow is recovered and dequeued only by an executor running the application version that
/// started it, so new code reaches the old code's executions only if both run under one version.
/// Patching therefore goes with keeping [`Config::app_version`](crate::Config::app_version) the
/// same across the deployments that patch. Patches with different names need no versions of their
/// own: each marker is matched by its name at its position.
///
/// **The protection runs one way.** New code meeting the old code's history takes the old path,
/// but old code meeting a history new code has patched finds the marker where it expects one of its
/// own steps, and fails with an unexpected step. While old and new executors share the version
/// during a rolling deployment, an old executor can recover or dequeue an execution the new code
/// already patched, so finish rolling the new code out before relying on the patch, or expect
/// such an execution to fail if an old executor picks it up.
///
/// Giving each deployment its own version is the other way to change a workflow's code, and it
/// needs no patch: executors of the old version keep running until they have finished the old
/// executions, while new executions go to the new version. Nothing moves an execution from one
/// version to another on its own; an execution left on a version no executor runs any more waits
/// until one does, or until it is forked onto another version.
///
/// # Errors
///
/// - [`Error::NotInWorkflow`] outside a workflow, and [`Error::InsideStep`] inside a step body: a
///   patch is a position in the workflow's own history, and neither place has one to record at.
/// - [`Error::PatchNondeterminism`] when another durable call of the workflow was built between
///   this one being built and awaited — see that variant. Await a patch where it is written, from
///   sequential workflow code.
/// - [`Error::StepBuiltElsewhere`] when the call is polled anywhere but the workflow it was built
///   in — carried into a step body, onto a spawned task, or into another workflow — as for every
///   durable call.
/// - Whatever reading or writing the system database fails with, as at any step. A workflow
///   cancelled meanwhile arrives as [`Error::SystemDatabase`] holding the system database's
///   `WorkflowCancelled`, a control signal like every cancellation.
///
/// # Position
///
/// The position is the one reached when the call is built, as for every durable call. Dropping
/// the future after it has claimed the position but before its marker is written — inside a
/// `tokio::select!` or a `timeout`, say — leaves the position used with nothing recorded there, and
/// a replay records the marker at it: a patch, like any step, is not something to race or time
/// out.
pub fn patch<E: DurableError>(
    name: &str,
) -> impl Future<Output = crate::Result<bool, E>> + Send + use<E> {
    Patching::build("patch", name, Marker::Write)
}

/// Honours a patch's markers already recorded, without recording new ones.
///
/// What a [`patch`] becomes once no execution of the code from before it can run again. The `if`
/// goes and the new code stays:
///
/// ```no_run
/// # async fn charge_wallet() -> dbos::Result<u32> { Ok(1) }
/// # async fn body() -> dbos::Result<u32> {
/// dbos::deprecate_patch("pay-by-wallet").await?;
/// let charged = charge_wallet().await?;
/// # Ok(charged) }
/// ```
///
/// An execution that recorded this patch's marker at the next step position moves past it, so its
/// later steps replay at the positions they were recorded at. Any other execution records nothing
/// and moves past nothing. Always `true`, since every execution that can still reach this point
/// runs the new code; the value is there so the call reads the same as the `patch` it replaced.
///
/// Once every execution that recorded the marker has finished, delete this call too.
///
/// # Errors
///
/// The same as [`patch`]'s: [`Error::NotInWorkflow`], [`Error::InsideStep`],
/// [`Error::PatchNondeterminism`], [`Error::StepBuiltElsewhere`], and the system database's own. Its position is fixed where it
/// is built, as [`patch`]'s is.
pub fn deprecate_patch<E: DurableError>(
    name: &str,
) -> impl Future<Output = crate::Result<bool, E>> + Send + use<E> {
    Patching::build("deprecate_patch", name, Marker::Honour)
}

/// A [`patch`] or [`deprecate_patch`] call, held to the execution it was built in.
///
/// The position it peeked belongs to one execution's counter, and the marker it may write belongs
/// to that execution's history. So, as for every durable call, where it is polled is checked on
/// **every poll**, not only the first: a call carried into a step body, onto another task, or into
/// another workflow is refused with [`Error::StepBuiltElsewhere`] rather than claim a position or
/// write a marker for a workflow that is no longer the one around it. Inside a step body the claim
/// would also be lost on replay, where the step's recorded result is returned without running
/// the body, and every later position would shift.
struct Patching<E> {
    /// The execution the call was built in, or `None` when the build itself refused.
    built: Option<Ctx>,
    /// The marker's step name, which names the call in a refusal.
    label: String,
    running: Pin<Box<dyn Future<Output = crate::Result<bool, E>> + Send>>,
}

impl<E: DurableError> Patching<E> {
    fn build(operation: &'static str, name: &str, marker: Marker) -> Self {
        let point = PatchPoint::here(operation, name, marker);
        let built = point.as_ref().ok().map(|point| point.ctx.clone());
        Self {
            built,
            label: format!("{}{name}", step_names::PATCH_PREFIX),
            running: Box::pin(async move {
                match point {
                    Ok(point) => point.resolve().await,
                    Err(refused) => Err(refused),
                }
                .map_err(Error::lift)
            }),
        }
    }
}

impl<E> Future for Patching<E> {
    type Output = crate::Result<bool, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(built) = &this.built {
            let held = Ctx::with_current(|here| match here {
                Some(here) if here.is_same_execution(built) && here.step_marker().is_none() => None,
                here => Some(StepPlacement::describe(here)),
            });
            if let Some(polled) = held {
                return Poll::Ready(Err(Error::StepBuiltElsewhere {
                    step: this.label.clone(),
                    built: StepPlacement::describe(Some(built)),
                    polled,
                }));
            }
        }
        this.running.as_mut().poll(cx)
    }
}

/// What a patch point does about its marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    /// [`patch`]: record one where the history has nothing yet.
    Write,
    /// [`deprecate_patch`]: move past one already recorded, and never record one.
    Honour,
}

/// What the history holds at the position a patch point looked at.
enum Found {
    /// Nothing yet: this execution has not been this far.
    Nothing,
    /// This patch's marker: an execution that took the new path.
    ThisMarker,
    /// Some other step: an execution of the code from before the patch.
    OtherStep,
}

/// A patch point as built: the execution it is in, and the position it peeked.
struct PatchPoint {
    ctx: Ctx,
    name: String,
    marker: Marker,
    step_id: i32,
}

impl PatchPoint {
    /// What building [`patch`] or [`deprecate_patch`] does: refuse a place with no history to look
    /// at, and peek the next position without taking it.
    fn here(operation: &'static str, name: &str, marker: Marker) -> crate::Result<Self> {
        let Some(ctx) = Ctx::current() else {
            return Err(Error::NotInWorkflow {
                operation: operation.into(),
            });
        };
        if ctx.in_step() {
            return Err(Error::InsideStep {
                operation: operation.into(),
            });
        }
        let step_id = ctx.peek_step_id();
        Ok(Self {
            ctx,
            name: name.to_owned(),
            marker,
            step_id,
        })
    }

    /// What awaiting it does: read, claim, write.
    async fn resolve(self) -> crate::Result<bool> {
        let Self {
            ctx,
            name,
            marker,
            step_id,
        } = self;
        let name = name.as_str();
        let marker_name = format!("{}{name}", step_names::PATCH_PREFIX);
        let workflow_id = ctx.workflow_id();
        let sysdb = ctx.executor().sysdb();

        let found = match sysdb.check_step(workflow_id, step_id, &marker_name).await {
            Ok(Some(_)) => Found::ThisMarker,
            Ok(None) => Found::Nothing,
            // The determinism check's refusal is this call's answer: a row recorded under another
            // name is the old code's step.
            Err(crate::sysdb::Error::UnexpectedStep { .. }) => Found::OtherStep,
            Err(error) => return Err(Error::from_sysdb(error)),
        };

        // From here on, the read means something only if nothing took the position while it was out.
        let raced = || Error::PatchNondeterminism {
            workflow_id: workflow_id.to_owned(),
            patch: name.to_owned(),
            step_id,
        };
        let occupy = match (&found, marker) {
            (Found::ThisMarker, _) | (Found::Nothing, Marker::Write) => true,
            (Found::OtherStep, _) | (Found::Nothing, Marker::Honour) => false,
        };
        if occupy {
            if !ctx.claim_step_id(step_id) {
                return Err(raced());
            }
        } else if ctx.peek_step_id() != step_id {
            return Err(raced());
        }

        match (found, marker) {
            (Found::Nothing, Marker::Write) => {
                // Held in a variable, so a retry of the write repeats the same completion time and
                // is recognised as this caller's own — see `record_step`.
                let now = Timestamp::now();
                sysdb
                    .record_step(
                        workflow_id,
                        step_id,
                        &marker_name,
                        Outcome::Output(None),
                        None,
                        Some(StepTiming {
                            started_at: now,
                            completed_at: now,
                        }),
                        ctx.owner_xid(),
                    )
                    .await
                    .map_err(Error::from_sysdb)?;
                tracing::debug!(
                    workflow_id,
                    step_id,
                    patch = name,
                    "recorded a patch marker"
                );
                Ok(true)
            }
            (Found::ThisMarker, _) => {
                tracing::debug!(
                    workflow_id,
                    step_id,
                    patch = name,
                    "replaying a patch marker"
                );
                Ok(true)
            }
            (Found::OtherStep, Marker::Write) => {
                tracing::debug!(
                    workflow_id,
                    step_id,
                    patch = name,
                    "an execution from before the patch: taking the old path"
                );
                Ok(false)
            }
            (Found::OtherStep | Found::Nothing, Marker::Honour) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineOnly;

    /// Outside a workflow there is no history to look at or record in, so both calls refuse rather
    /// than answer.
    #[tokio::test]
    async fn outside_a_workflow_both_calls_are_refused() {
        let refused = patch::<EngineOnly>("v2").await;
        assert!(
            matches!(&refused, Err(Error::NotInWorkflow { operation }) if operation == "patch"),
            "{refused:?}"
        );
        let refused = deprecate_patch::<EngineOnly>("v2").await;
        assert!(
            matches!(
                &refused,
                Err(Error::NotInWorkflow { operation }) if operation == "deprecate_patch"
            ),
            "{refused:?}"
        );
    }
}
