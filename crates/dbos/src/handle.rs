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

use crate::dbos::Executor;
use crate::error::{DurableError, Error, Failure, Result};
use crate::serialization::decode;
use crate::sysdb::types::WorkflowStatus;
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
    pub async fn result(self) -> Result<R, E> {
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

        match outcome {
            Ok(output) => decode(output.as_deref(), "result"),
            // The workflow's own failure, decoded back into the caller's error type — the same
            // fidelity the result gets. Execution and replay read the same bytes, so they return
            // the same error.
            Err(Failure::Recorded(encoded)) => Err(decode::<_, E>(Some(&encoded), "error")
                .unwrap_or_else(|_| Error::WorkflowFailed {
                    workflow_id: self.workflow_id,
                    message: encoded,
                })),
            Err(Failure::Control(control)) => Err(control.lift()),
        }
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
