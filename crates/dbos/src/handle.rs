//! The workflow handle: three methods over a workflow id, in two flavours.
//!
//! Every reference agrees on the surface — the id, the result, the status — and every reference
//! splits the implementation the same way (decision 10). A handle to a workflow running in *this*
//! process awaits the running task directly; a handle to one running elsewhere, or to one that
//! finished before this process started, has nothing local to await and polls the database. The
//! two are one public type, because a caller has no reason to care which it holds — and with a
//! caller-supplied id, which one it gets is decided by a race it cannot see.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;

use crate::context::Ctx;
use crate::dbos::Executor;
use crate::error::EngineOnly;
use crate::error::{DurableError, Error, Failure, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp, WorkflowStatus};
use crate::workflow::adopt;

/// A running — or finished — workflow, by id.
///
/// Returned by [`start`](crate::WorkflowRef::start), and consumed by
/// [`result`](Self::result): a result can be taken once, which the signature says rather than a
/// runtime error (Go returns *"workflow result channel is already closed"* at the same point).
/// Dropping a handle does not stop the workflow — it only stops watching.
pub struct WorkflowHandle<R, E = crate::EngineOnly> {
    executor: Arc<Executor>,
    workflow_id: String,
    provenance: Provenance,
    /// `fn() -> (R, E)`: the handle holds neither, it only names them — which keeps it `Send`,
    /// `Sync` and `Unpin` whatever the parameters are.
    types: PhantomData<fn() -> (R, E)>,
}

/// Where this handle's result will come from.
enum Provenance {
    /// The workflow runs in this process: the task's outcome is awaited directly.
    Local(JoinHandle<std::result::Result<Option<String>, Failure>>),
    /// The workflow runs elsewhere, or already finished: the database is the only witness.
    Polling,
}

impl<R, E> WorkflowHandle<R, E> {
    /// A handle over the task this process spawned.
    pub(crate) fn local(
        executor: Arc<Executor>,
        workflow_id: String,
        task: JoinHandle<std::result::Result<Option<String>, Failure>>,
    ) -> Self {
        Self {
            executor,
            workflow_id,
            provenance: Provenance::Local(task),
            types: PhantomData,
        }
    }

    /// A handle over a workflow some other execution owns.
    pub(crate) fn polling(executor: Arc<Executor>, workflow_id: String) -> Self {
        Self {
            executor,
            workflow_id,
            provenance: Provenance::Polling,
            types: PhantomData,
        }
    }

    /// The workflow's id.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// The workflow's status, as its row records it right now.
    pub async fn status(&self) -> Result<WorkflowStatus> {
        let row = self
            .executor
            .sysdb()
            .get_workflow(&self.workflow_id)
            .await
            .map_err(Error::SystemDatabase)?
            .ok_or_else(|| Error::WorkflowNotFound {
                workflow_id: self.workflow_id.clone(),
            })?;
        Ok(row.status)
    }
}

impl<R, E> WorkflowHandle<R, E>
where
    R: DeserializeOwned,
    E: DurableError,
{
    /// Waits for the workflow to finish and returns what it returned.
    ///
    /// The same answer whoever asks: a local handle awaits the task, a polling handle reads the
    /// row another execution writes, and both decode the same recorded bytes — the output, or the
    /// workflow's own error with the fidelity the type parameter exists for. A failure that is
    /// not ours to decode — a row another SDK wrote — degrades to
    /// [`WorkflowFailed`](Error::WorkflowFailed) carrying the message.
    ///
    /// **Awaited from inside the workflow that started it, this is a durable step.** The parent
    /// records what the child returned, so a replayed parent continues from a value it already has
    /// rather than waiting again on a workflow that may since have been forked or deleted — and the
    /// wait costs one row read instead of a poll to completion. All four implementations record it,
    /// under the same name, `DBOS.getResult`.
    pub async fn result(self) -> Result<R, E> {
        // Allocated before anything can fail, and before the check it gates: the position of this
        // await in the parent has to be the same on the replay as it was on the run.
        let checkpoint = match Awaiting::of(&self.executor) {
            Ok(checkpoint) => checkpoint,
            Err(wrong) => return Err(wrong.lift()),
        };
        if let Some(checkpoint) = &checkpoint
            && let Some(recorded) = checkpoint
                .recorded(&self.executor)
                .await
                .map_err(Error::lift)?
        {
            tracing::debug!(
                workflow_id = self.workflow_id,
                step_id = checkpoint.step_id,
                "the child's outcome was already recorded; the parent does not wait again"
            );
            return Self::interpret(recorded, self.workflow_id);
        }

        let started_at = Timestamp::now();
        let outcome = match self.provenance {
            Provenance::Local(task) => match task.await {
                Ok(outcome) => outcome,
                // Only shutdown aborts a workflow task, and it leaves the row `PENDING` on
                // purpose.
                Err(join) if join.is_cancelled() => Err(Failure::Control(Error::Interrupted {
                    workflow_id: self.workflow_id.clone(),
                })),
                Err(join) => std::panic::resume_unwind(join.into_panic()),
            },
            Provenance::Polling => adopt(&self.executor, &self.workflow_id).await,
        };

        // Recorded from the outcome as it arrived, before it is decoded: the child's bytes go into
        // the parent's row exactly as the child's own row holds them, so nothing is re-encoded and
        // nothing can drift between the two copies.
        if let Some(checkpoint) = &checkpoint {
            checkpoint
                .record(&self.executor, &self.workflow_id, &outcome, started_at)
                .await
                .map_err(Error::lift)?;
        }

        match outcome {
            Ok(output) => decode(output.as_deref(), "result"),
            // The workflow's own failure, decoded back into the caller's error type — the same
            // fidelity the result gets. Execution and replay read the same bytes, so they return
            // the same error.
            Err(Failure::Recorded(encoded)) => Err(decode::<_, E>(Some(&encoded), "error")
                .unwrap_or_else(|_| Error::WorkflowFailed {
                    workflow_id: self.workflow_id.clone(),
                    message: encoded,
                })),
            // **A cancelled child is not a cancelled parent.** `WorkflowCancelled` means the
            // workflow *asking* is being cancelled — the step-replay check refusing to run it —
            // so reporting it here would read as the wrong workflow having been stopped. Every
            // reference raises a separate awaited-cancelled error for exactly this, and it is
            // recorded like any other outcome: the child is over, and the parent has learned so.
            Err(Failure::Control(Error::WorkflowCancelled { workflow_id })) => {
                Err(Error::AwaitedWorkflowCancelled { workflow_id })
            }
            Err(Failure::Control(control)) => Err(control.lift()),
        }
    }

    /// Turns a recorded await back into what the parent returned the first time.
    fn interpret(recorded: StepRecord, workflow_id: String) -> Result<R, E> {
        match recorded.error {
            None => decode(recorded.output.as_deref(), "result"),
            Some(encoded) => Err(decode::<_, E>(Some(&encoded), "error").unwrap_or_else(|_| {
                Error::WorkflowFailed {
                    workflow_id,
                    message: encoded,
                }
            })),
        }
    }
}

/// The parent awaiting a child, and the step id its await occupies.
///
/// `None` when there is no parent to record against, which covers three cases that all mean the
/// same thing here: awaiting from outside any workflow, awaiting from inside a *step* (a step is a
/// leaf, and an id-allocating call inside one would shift every later step onto the wrong replay
/// slot — `get_event` degrades the same way), and awaiting a handle that is not a child at all.
/// The last of those is not distinguished on purpose: a workflow awaiting some *other* workflow it
/// did not start is still learning an outcome it should not have to learn twice, and Python and Go
/// both checkpoint it.
struct Awaiting {
    workflow_id: String,
    step_id: i32,
}

impl Awaiting {
    fn of(executor: &Arc<Executor>) -> std::result::Result<Option<Self>, Error> {
        let Some(ctx) = Ctx::current().filter(|ctx| !ctx.in_step()) else {
            return Ok(None);
        };
        // The step id would come from this workflow's counter while the write went through the
        // handle's own system database — the split `get_event` refuses for the same reason.
        if !Arc::ptr_eq(ctx.executor(), executor) {
            return Err(Error::WrongInstance {
                operation: "awaiting a workflow's result".into(),
            });
        }
        Ok(Some(Self {
            workflow_id: ctx.workflow_id().to_owned(),
            step_id: ctx.next_step_id(),
        }))
    }

    async fn recorded(
        &self,
        executor: &Executor,
    ) -> std::result::Result<Option<StepRecord>, Error> {
        executor
            .sysdb()
            .check_child_result(&self.workflow_id, self.step_id)
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Records a *decided* outcome, and only that.
    ///
    /// A success, a failure and a cancellation are all things the child is finished doing, and the
    /// parent may safely be replayed straight past them. The signals deliberately left unrecorded
    /// are the ones that are not the child's outcome at all:
    ///
    /// - **the parent being interrupted** — shutdown aborted the task, nothing is decided, and a
    ///   recovered parent must await again (Go says the same in its own words: *"nothing is
    ///   checkpointed, so a resume re-executes the await"*);
    /// - **the child being parked** at `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, which is not terminal —
    ///   it can be resumed, and a parent holding "parked" as the answer could never see that;
    /// - **a system-database failure**, which is a statement about the substrate rather than about
    ///   the child.
    async fn record(
        &self,
        executor: &Executor,
        child_workflow_id: &str,
        settled: &std::result::Result<Option<String>, Failure>,
        started_at: Timestamp,
    ) -> std::result::Result<(), Error> {
        let cancelled;
        let outcome = match settled {
            Ok(output) => Outcome::Output(output.as_deref()),
            Err(Failure::Recorded(encoded)) => Outcome::Error(encoded),
            // Cancellation is the one control-shaped signal that *is* a decided outcome, and it is
            // rewritten on the way in for the same reason it is rewritten on the way out: the row
            // has to say the awaited workflow was cancelled, not this one.
            Err(Failure::Control(Error::WorkflowCancelled { workflow_id })) => {
                cancelled = encode(
                    &Error::<EngineOnly>::AwaitedWorkflowCancelled {
                        workflow_id: workflow_id.clone(),
                    },
                    "error",
                )?;
                Outcome::Error(&cancelled)
            }
            Err(Failure::Control(_)) => return Ok(()),
        };
        executor
            .sysdb()
            .record_child_result(
                &self.workflow_id,
                self.step_id,
                child_workflow_id,
                outcome,
                Some(executor.serializer().name()),
                Some(StepTiming {
                    started_at,
                    completed_at: Timestamp::now(),
                }),
            )
            .await
            .map_err(Error::SystemDatabase)
    }
}

impl<R, E> std::fmt::Debug for WorkflowHandle<R, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandle")
            .field("workflow_id", &self.workflow_id)
            .field(
                "provenance",
                match &self.provenance {
                    Provenance::Local(_) => &"local",
                    Provenance::Polling => &"polling",
                },
            )
            .finish_non_exhaustive()
    }
}
