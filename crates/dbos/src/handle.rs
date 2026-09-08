//! The workflow handle: three methods over a workflow id, in two flavours.
//!
//! Every reference agrees on the surface — the id, the result, the status — and every reference
//! splits the implementation the same way. A handle to a workflow running in *this*
//! process awaits the running task directly; a handle to one running elsewhere, or to one that
//! finished before this process started, has nothing local to await and polls the database. The
//! two are one public type, because a caller has no reason to care which it holds — and with a
//! caller-supplied id, which one it gets is decided by a race it cannot see.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::connection::Connection;
use crate::error::EngineOnly;
use crate::error::{DurableError, Error, Failure, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp, WorkflowStatus, step_names};

/// A running — or finished — workflow, by id.
///
/// Returned by [`start`](crate::WorkflowRef::start), and consumed by
/// [`result`](Self::result): a result can be taken once, which the signature says rather than a
/// runtime error (Go returns *"workflow result channel is already closed"* at the same point).
/// Dropping a handle does not stop the workflow — it only stops watching.
pub struct WorkflowHandle<R, E = crate::EngineOnly> {
    conn: Arc<Connection>,
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
    Polling {
        /// Whether the call that minted this handle had the row in front of it.
        ///
        /// `true` where this process inserted or read it, so a later absence is a deletion and
        /// waiting for the row is waiting for something that will never come back; `false` for an
        /// id taken on faith, which may name a workflow whose enqueue has not committed yet. It is
        /// the references' `fail_if_missing`, and it reaches
        /// [`await_workflow_result`](crate::sysdb::SystemDatabase::await_workflow_result) as
        /// exactly that.
        fail_if_missing: bool,
    },
}

impl<R, E> WorkflowHandle<R, E> {
    /// A handle over the task this process spawned.
    pub(crate) fn local(
        conn: Arc<Connection>,
        workflow_id: String,
        task: JoinHandle<std::result::Result<Option<String>, Failure>>,
    ) -> Self {
        Self {
            conn,
            workflow_id,
            provenance: Provenance::Local(task),
            types: PhantomData,
        }
    }

    /// A handle over a workflow some other execution owns.
    ///
    /// **Takes a connection rather than an executor**, which is what lets a
    /// [`Client`](crate::Client) hand one back: watching a workflow is reading its row, and reading
    /// a row needs no process that could run it. Java's client builds the same thing — a small
    /// handle class over the system database alone.
    ///
    /// `fail_if_missing` says whether this call saw the row: `true` when it inserted or read it,
    /// `false` for an id it was merely given. [`result`](Self::result) spells out what the two do
    /// differently when the row is not there.
    pub(crate) fn polling(
        conn: Arc<Connection>,
        workflow_id: String,
        fail_if_missing: bool,
    ) -> Self {
        Self {
            conn,
            workflow_id,
            provenance: Provenance::Polling { fail_if_missing },
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
            .conn
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
    /// rather than waiting again on a workflow that may since have been forked — and the wait costs
    /// one row read instead of a poll to completion. All four implementations record it, under the
    /// same name, `DBOS.getResult`.
    ///
    /// **That the row survives its child is a property of the recording, not a guarantee against
    /// deletion.** The value lives in the *parent's* `operation_outputs`, so a replay that has
    /// reached this step reads it back whether or not the child's own row is still there. What that
    /// buys is real and it is worth knowing, but it is not a licence: deleting a workflow something
    /// live still replays through is collecting what is not garbage, and what the parent does then
    /// is undefined — this call happens to finish, and a
    /// [`join_workflows`](crate::join_workflows()) over the same child does not.
    ///
    /// **A handle from a [`Client`](crate::Client) awaited there is a plain wait**, because a
    /// client has no step counter of its own to agree with the workflow's: nothing is recorded,
    /// and a replayed body waits again. A handle from another *instance* is
    /// [`Error::WrongInstance`] instead — there the two counters both exist, and the caller meant
    /// one of them.
    ///
    /// **An id that names no row is waited on when nothing has seen that row, and refused when
    /// something has.** A handle minted by a call that had the row in front of it — a start, an
    /// enqueue, a join onto a workflow another execution owns — reports
    /// [`Error::WorkflowNotFound`], because a row that was there and is gone was deleted and will
    /// not come back. A handle over an id taken on faith —
    /// [`Client::retrieve_workflow`](crate::Client::retrieve_workflow), or a parent replaying a
    /// launch it recorded rather than a row it read — polls for the row to appear, because such an
    /// id may legitimately name a workflow whose enqueue has not committed yet.
    ///
    /// [`status`](Self::status) is the other half, and answers an unknown id the same way whatever
    /// the handle: it reports the absence, because a single read has nothing to wait for.
    ///
    /// That split is the references' `fail_if_missing` drawn one line further out. Java draws it
    /// at a handle too — `WorkflowHandleDBPoll` carries the flag, set for a handle built from a row
    /// it just read — and stops after the already-finished start; the other three pass it only
    /// where a run parks on its own row and let every handle wait. What is left waiting here is the
    /// case none of them can avoid: an id whose row this process has never seen, where "deleted"
    /// and "not yet" are the same observation.
    ///
    /// **A wait that could go on forever is bounded by dropping it, outside a workflow.**
    /// `tokio::time::timeout` around this future, or dropping the future outright, ends the poll —
    /// so the hazard [`await_workflow_result`](crate::sysdb::SystemDatabase::await_workflow_result)
    /// describes costs a caller here what Go and TypeScript charge an argument for and Python and
    /// Java cannot offer at all. **Inside the workflow that started the child it is neither**, for
    /// the reason [`PendingStep`] gives every durable call: a `timeout` is a race nothing records,
    /// so a replay is free to decide it the other way and continue from a wait this execution
    /// abandoned. What is wrong there is the *unrecorded* decision and not the racing — bound the
    /// child where its bound belongs, with [`StartOptions::timeout`](crate::StartOptions::timeout)
    /// or the deadline it inherits, and race this wait with the macro built for the shape:
    /// [`select_workflow!`](macro@crate::select_workflow) against other waits, which checkpoints
    /// the winner and then awaits that handle alone on the run and on the replay both, or
    /// [`select_step!`](crate::select_step) against a call of another kind — a step, a sleep, an
    /// event — since this wait is a [`PendingStep`] and so may be one of its branches.
    ///
    /// **The id is claimed here, where the call is written, not where the wait is first polled.**
    /// So a `join!` over several handles' results is ordinary code — `join!` builds every branch
    /// before polling any, which is the order the ids were taken in and the order a replay takes
    /// them again — and the awaits are numbered by what the parent's body *says* rather than by
    /// which child answers first. A handle built in one workflow and awaited in another, or
    /// carried across a step-body boundary, is refused as [`Error::StepBuiltElsewhere`]: the id is
    /// a claim on one position in one execution, and nowhere else can honour it.
    pub fn result<'a>(self) -> PendingStep<'a, R, E>
    where
        R: 'a,
    {
        // Allocated before anything can fail, and before the check it gates: the position of this
        // await in the parent has to be the same on the replay as it was on the run. A refusal
        // here is carried into the future by `placed`, so `handle.result().await?` keeps its
        // single `?` and nothing was claimed on the way to it.
        let built = ChildResultPlacement::of(&self.conn).map(|awaiting| {
            let placement = awaiting.placement().clone();
            (awaiting, placement)
        });
        // The placement is handed back beside the `ChildResultPlacement` that carries it, and the
        // await wants the latter: it is what holds the id this call already claimed.
        PendingStep::placed(step_names::GET_RESULT, built, move |awaiting, _| {
            self.settle(awaiting)
        })
    }

    /// The await itself, once something polls it.
    ///
    /// Takes the [`ChildResultPlacement`] rather than building one, because the id it holds was
    /// claimed where the call was written. That is also what lets [`run_with`](crate::WorkflowRef::run_with)
    /// claim the await's id immediately behind the start's and hand it here once the child
    /// exists: where an await stands is decided by the ambient context and the connection, and
    /// the handle has no say in either.
    pub(crate) async fn settle(self, awaiting: ChildResultPlacement) -> Result<R, E> {
        if let Some(recorded) = awaiting
            .check(&self.conn, &self.workflow_id)
            .await
            .map_err(Error::lift)?
        {
            tracing::debug!(
                workflow_id = self.workflow_id,
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
            // Whether a missing row is "not yet" or "never again" is decided where the handle
            // was minted, because that is the only place that knows whether anything ever saw the
            // row.
            Provenance::Polling { fail_if_missing } => {
                self.conn.adopt(&self.workflow_id, fail_if_missing).await
            }
        };

        // Recorded from the outcome as it arrived, before it is decoded: the child's bytes go into
        // the parent's row exactly as the child's own row holds them, so the two copies cannot
        // disagree about what the child returned.
        //
        // The *label* beside them is this executor's serializer rather than the one that wrote
        // the bytes, which `adopt` does not carry back. Nothing reads it to choose a decoder while
        // there is one encoding, so it costs nothing yet; it becomes a real question when a second
        // serializer does, and it is that change's to answer.
        awaiting
            .record(&self.conn, &self.workflow_id, &outcome, started_at)
            .await
            .map_err(Error::lift)?;

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
            Err(Failure::Control(Error::WorkflowCancelled { workflow_id }))
                // Only where there is a caller for the two to be confused with each other.
                if awaiting.placement().inside_a_workflow() =>
            {
                Err(Error::AwaitedWorkflowCancelled { workflow_id })
            }
            Err(Failure::Control(control)) => Err(control.lift()),
        }
    }

    /// Turns a recorded await back into what the parent returned the first time.
    ///
    /// Which workflow the row belongs to was settled by [`ChildResultPlacement::check`] before
    /// this sees it, so what is left here is the outcome alone.
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

/// Where the caller awaiting this handle stands, which decides two independent things: whether the
/// outcome is checkpointed, and how a cancelled *awaited* workflow is reported.
///
/// **A [`StepPlacement`] and the two writes that are an await's own**, which is the whole of what
/// it adds: [`of`](Self::of) names the operation once and [`placement`](Self::placement) hands the
/// inner value back, while [`check`](Self::check) and [`record`](Self::record) are the pair this
/// type exists for — [`check_child_result`](crate::sysdb::SystemDatabase::check_child_result) and
/// [`record_child_result`](crate::sysdb::SystemDatabase::record_child_result), whose vocabulary
/// this borrows. It is the *result* half of what a parent records about a child; the *launch* half
/// is [`record_child_workflow`](crate::sysdb::SystemDatabase::record_child_workflow), carried by
/// [`InitWorkflowCaller`](crate::sysdb::types::InitWorkflowCaller).
///
/// **A type of its own rather than a bare placement**, because
/// [`run_with`](crate::WorkflowRef::run_with) holds two placements at once — the start's and this
/// one, claimed an instant apart and differing only in the integer inside. Handing
/// [`settle`](WorkflowHandle::settle) the wrong one would compile, and would record the await
/// under the start's id; the replay would meet a recorded step under the wrong name and there is
/// nothing before then to notice it.
///
/// A workflow awaiting some other workflow it did not itself start is treated exactly as a parent
/// awaiting its child, deliberately: it is learning an outcome it should not have to learn twice
/// either, and Python and Go checkpoint that case too. The name follows the layer below rather
/// than that distinction — every implementation stores the awaited id in `child_workflow_id`,
/// whoever started it.
pub(crate) struct ChildResultPlacement(StepPlacement);

impl ChildResultPlacement {
    /// Where this await stands, allocating its step id if it is to be recorded.
    ///
    /// The placement rules — and the argument for each of them — are
    /// [`StepPlacement::of`](crate::checkpoint::StepPlacement::of)'s, shared with every other
    /// library step in the crate. What stays here is only what an *await* does with the answer.
    pub(crate) fn of(conn: &Arc<Connection>) -> std::result::Result<Self, Error> {
        StepPlacement::of(conn, "awaiting a workflow's result").map(Self)
    }

    /// Where this await stands, for the [`PendingStep`] that has to ask again on every poll.
    pub(crate) fn placement(&self) -> &StepPlacement {
        &self.0
    }

    async fn check(
        &self,
        conn: &Connection,
        awaited_workflow_id: &str,
    ) -> std::result::Result<Option<StepRecord>, Error> {
        let Some((workflow_id, step_id)) = self.0.step() else {
            return Ok(None);
        };
        let Some(recorded) = conn
            .sysdb()
            .check_child_result(workflow_id, step_id)
            .await
            .map_err(Error::SystemDatabase)?
        else {
            return Ok(None);
        };
        // The mirror of the launch's own check, and the second half of one rule.
        // `check_child_result` compares the step *name*, which leaves the question of whose
        // outcome this is. For a child it cannot differ — the handle's id was read out of the
        // launch row moments earlier — but a workflow may also await a handle it did not start,
        // and there the recorded row is the only thing that knows which workflow answered.
        // Adopting some other workflow's outcome as this one's is what this refuses, and every
        // implementation writes the id needed to refuse it (Python's `record_get_result` stores
        // the awaited id as `child_workflow_id` too, `_sys_db.py:2851`).
        if recorded.child_workflow_id.as_deref() != Some(awaited_workflow_id) {
            return Err(Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                workflow_id: workflow_id.to_owned(),
                step_id,
                expected: format!("an await of {awaited_workflow_id}"),
                recorded: match &recorded.child_workflow_id {
                    Some(other) => format!("an await of {other}"),
                    None => "an await of no workflow at all".to_owned(),
                },
            }));
        }
        Ok(Some(recorded))
    }

    /// Records a *settled* outcome, and only that.
    ///
    /// A success, a failure and a cancellation are all things the child is finished doing and
    /// cannot take back, so the parent may safely be replayed straight past them. What is left
    /// unrecorded is whatever a replay must not be pinned to:
    ///
    /// - **the parent being interrupted** — shutdown aborted the task, nothing is decided, and a
    ///   recovered parent must await again (Go says the same in its own words: *"nothing is
    ///   checkpointed, so a resume re-executes the await"*);
    /// - **a system-database failure**, which is a statement about the substrate rather than about
    ///   the child;
    /// - **the child being parked** at `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, which is the one that
    ///   needs an argument, below.
    ///
    /// **A parked child still fails its parent** — all four implementations let that error out of
    /// the await, and none treats it as something a parent can wait out. What is withheld is only
    /// the *checkpoint*: parking is the one non-terminal verdict a workflow can carry, so freezing
    /// it into the parent's replay would outlive its own truth. A parent resumed after its child
    /// was resumed re-asks the child's row and sees what the child actually did; a parent holding
    /// a recorded "parked" would replay that answer forever. It is the same argument as the
    /// interrupted-await bullet above, one step further out.
    ///
    /// **Rust follows Go here, against the other three.** Go filters this case out of its await
    /// checkpoint deliberately and says so — *"either the workflow result proper (no dlq, no raw
    /// awaitWorkflowResult error) or the child's cancellation"* (`workflow.go:419`). Python,
    /// TypeScript and Java all record it, because in all three the await runs inside the generic
    /// step wrapper and that wrapper checkpoints whatever exception it caught.
    ///
    /// TODO(dbos-team): UPSTREAM item 20. Two implementations pin a parked child's verdict into
    /// the parent's replay and two do not, and none of the four argues for its side — the split
    /// falls exactly along "Go's await path filters on purpose" versus "a step wrapper records by
    /// default", which is not a decision anyone made twice. It is worth settling before v1,
    /// because it changes what a resumed parent sees. The same item covers a second half: Python
    /// and TypeScript raise a distinct *awaited* error here
    /// (`DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded`,
    /// `DBOSAwaitedWorkflowExceededMaxRecoveryAttempts`) while Go and Java reuse the error a
    /// workflow gets for its own parking — a two-two split the *cancellation* case does not have,
    /// where all four separate the two meanings and Rust followed them into
    /// [`Error::AwaitedWorkflowCancelled`].
    async fn record(
        &self,
        conn: &Connection,
        child_workflow_id: &str,
        settled: &std::result::Result<Option<String>, Failure>,
        started_at: Timestamp,
    ) -> std::result::Result<(), Error> {
        let Some((workflow_id, step_id)) = self.0.step() else {
            return Ok(());
        };
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
        conn.sysdb()
            .record_child_result(
                workflow_id,
                step_id,
                child_workflow_id,
                outcome,
                Some(conn.serializer().name()),
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
                    Provenance::Polling { .. } => &"polling",
                },
            )
            .finish_non_exhaustive()
    }
}
