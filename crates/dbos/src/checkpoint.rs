//! Where a call that is not a [`step`](crate::step) stands, and what that means for its replay.
//!
//! Several calls in this crate are *durable operations that are not steps*: awaiting a workflow's
//! result, waiting on a set of handles, reading an event. Each is a single durable act that a
//! replay must not perform twice, and each therefore takes a step id from the ambient workflow and
//! records its answer under it. None of them is a step in the [`step`](crate::step) sense — there
//! is no user body, no retry policy, no timeout — so none of them goes through `step_with`.
//!
//! What they share is not the recording but the **decision of whether to record at all**, and that
//! decision is subtle enough to be worth having in one place:
//!
//! - Outside a workflow there is no step counter, so nothing is checkpointed and the call is
//!   plain. That is the operator's case, and it is not an error.
//! - Inside a *step* nothing is checkpointed either, by the leaf rule the whole crate follows:
//!   the step's own checkpoint stands for everything its body did, and allocating an id inside one
//!   would shift every later step onto the wrong replay slot.
//! - Inside a workflow at a step boundary the call is a step of that workflow.
//! - Reached through a handle whose connection is not the ambient workflow's, the answer depends
//!   on *whose* connection it is — a [`Client`](crate::Client)'s degrades to the plain call, a
//!   second application instance's is [`Error::WrongInstance`].
//!
//! Those four cases and the argument for each were written once, for
//! [`WorkflowHandle::result`](crate::WorkflowHandle::result), and are now shared by every caller
//! that has the same question. The references keep the same logic in one place for the same
//! reason: Python's `call_function_as_step`, TypeScript's `runInternalStep` and Java's
//! `runDbosFunctionAsStep` are each one wrapper that every non-step durable call goes through.
//!
//! **What is *not* shared is the write**, and deliberately. A child await records the awaited id
//! alongside the outcome and reads it back through
//! [`check_child_result`](crate::sysdb::SystemDatabase::check_child_result); the management surface
//! commits its checkpoint inside the same transaction as the operation it records; a wait over
//! handles records a plain value. Each knows what its own row means. This module owns only the
//! placement and the generic value-shaped write, which is what the callers with nothing special to
//! say reach for.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use crate::connection::{Connection, Owner};
use crate::context::Ctx;
use crate::error::Error;
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp};

/// A durable call that has taken its step id and has not run.
///
/// The non-step counterpart of [`PendingStep`](crate::PendingStep), and the same bargain: the id
/// is spent at the call rather than at the first poll, so a set of launches or awaits built in
/// source order and then driven together — `tokio::join!` over three `start`s — takes the same
/// slots on a replay however their bodies interleave. Before this, every such call was an
/// `async fn` that read the counter when first polled, and a `join!` over them was documented as
/// a trap.
///
/// One type over a `T` rather than one per call, because the three calls that return it differ
/// only in what comes out: a handle, a result, or both. `Unpin` and `Send` for the reason
/// `PendingStep` is — a combinator can hold one by value and poll it through `&mut`.
#[must_use = "a durable call that is not awaited has spent its step id without running; await it,               or hand it to a combinator"]
pub struct Pending<'a, T> {
    step_id: Option<i32>,
    running: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
}

impl<'a, T> Pending<'a, T> {
    pub(crate) fn new(step_id: Option<i32>, running: impl Future<Output = T> + Send + 'a) -> Self {
        Self {
            step_id,
            running: Box::pin(running),
        }
    }

    /// The step id this call claimed when it was built, or `None` if it claimed none.
    ///
    /// `None` is not a failure: outside a workflow there is no counter, and a call that was refused
    /// at build — inside a step, or against the wrong instance — reports that when polled rather
    /// than here.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.step_id
    }
}

impl<T> Future for Pending<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<T> {
        self.get_mut().running.as_mut().poll(cx)
    }
}

impl<T> std::fmt::Debug for Pending<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("step_id", &self.step_id)
            .finish_non_exhaustive()
    }
}

/// Where the caller of a non-step durable operation stands.
///
/// Decides two independent things: whether the operation is checkpointed, and whether there is a
/// surrounding workflow that a *cancelled awaited workflow* would otherwise be confused with.
pub(crate) enum Placement {
    /// Not inside a workflow. Nothing is recorded, and there is no other workflow here for an
    /// awaited one to be confused with.
    Outside,
    /// Inside a workflow, with nothing to record against. The awaited-cancelled distinction
    /// applies, because there is a workflow to confuse it with, but nothing is checkpointed. Two
    /// ways to land here:
    ///
    /// - **Inside a step**: a step is a leaf, and an id-allocating call inside one would shift
    ///   every later step onto the wrong replay slot.
    /// - **Holding a [`Client`](crate::Client)'s connection**: a client has no step counter to
    ///   agree with this workflow's, and no execution of its own that a recorded call could belong
    ///   to. A handle from *another instance* is the third case and is not this one — that is
    ///   [`Error::WrongInstance`], because two instances each have a counter and the caller meant
    ///   one of them.
    Uncheckpointed,
    /// Inside a workflow at a step boundary: this call is a step of that workflow.
    Recorded { workflow_id: String, step_id: i32 },
}

impl Placement {
    /// Where a call reached through `conn` stands, allocating the step id if it is to be recorded.
    ///
    /// **Allocating is the point, and it happens here rather than at the write**: the position of
    /// this call in the workflow has to be the same on the replay as it was on the run, so the id
    /// is taken before anything that could fail and before the check it gates.
    ///
    /// `operation` names the call for [`Error::WrongInstance`], which is the one error this can
    /// return.
    pub(crate) fn of(conn: &Arc<Connection>, operation: &'static str) -> Result<Self, Error> {
        let Some(ctx) = Ctx::current() else {
            return Ok(Self::Outside);
        };
        // First, because inside a step nothing is checkpointed whoever the connection belongs to,
        // and there is then nothing for the halves below to disagree about. `DBOS::get_event`
        // orders its own two checks the same way.
        if ctx.in_step() {
            return Ok(Self::Uncheckpointed);
        }
        // Where the two halves would be combined: a step id is about to come from this workflow's
        // counter while the write goes through the caller's own connection. What that means
        // depends on whose connection it is, and it is the one question [`Owner`] exists for.
        //
        // The comparison is of *databases* rather than of executors, because the database is what
        // the two halves would disagree about, and it is the thing a connection from a
        // [`Client`](crate::Client) has in common with one from a running executor.
        if !Arc::ptr_eq(ctx.executor().connection(), conn) {
            return match conn.owner() {
                // **A client's is a plain call, not a refusal.** There is no second counter here
                // to have meant instead — a client has none — so the operation degrades to the
                // undurable version of itself, which is what `Client::enqueue` documents about a
                // client used from inside a workflow body and what `Client::get_event` already
                // does for the read.
                Owner::Client => Ok(Self::Uncheckpointed),
                // **Another instance's is the mistake the variant was raised for.** Both
                // instances have a step counter, the caller meant one of them, and the record
                // would land where the workflow that allocated the id cannot see it. Refused
                // rather than quietly downgraded, because a second instance in a process is
                // nearly always a wiring error and this is the only place it shows.
                Owner::Application => Err(Error::WrongInstance {
                    operation: operation.into(),
                }),
            };
        }
        Ok(Self::Recorded {
            workflow_id: ctx.workflow_id().to_owned(),
            step_id: ctx.next_step_id(),
        })
    }

    /// Refuses to run where this placement was not built.
    ///
    /// The id is a claim on *one position in one workflow*, taken at build, and the same two
    /// silent failures [`PendingStep`](crate::PendingStep) refuses apply here: built outside a
    /// workflow and polled inside one, the call would run unrecorded where a checkpoint was
    /// expected; built in one workflow and polled in another, the row would land under the wrong
    /// workflow's id. `Uncheckpointed` is not checked — it records nothing wherever it runs, and
    /// the client-connection case that produces it is legitimately driven from anywhere.
    ///
    /// `operation` is what a refusal names as the step.
    pub(crate) fn check_here(&self, operation: &str) -> Result<(), Error> {
        let here = Ctx::current();
        let built: Option<std::borrow::Cow<'static, str>> = match (self, here.as_ref()) {
            (Self::Uncheckpointed, _) => None,
            (Self::Outside, None) => None,
            (Self::Recorded { workflow_id, .. }, Some(here))
                if here.workflow_id() == workflow_id && here.step_marker().is_none() =>
            {
                None
            }
            (Self::Outside, _) => Some("outside a workflow".into()),
            (Self::Recorded { workflow_id, .. }, _) => {
                Some(format!("in workflow {workflow_id}").into())
            }
        };
        match built {
            None => Ok(()),
            Some(built) => Err(Error::StepBuiltElsewhere {
                step: operation.to_owned(),
                built,
                polled: crate::step::polled_in(here.as_ref()),
            }),
        }
    }

    /// Whether there is a surrounding workflow — true wherever a cancelled *awaited* workflow has
    /// to be distinguished from this caller being cancelled.
    pub(crate) fn inside_a_workflow(&self) -> bool {
        !matches!(self, Self::Outside)
    }

    /// The workflow and step id this call is recorded under, if it is recorded at all.
    pub(crate) fn step(&self) -> Option<(&str, i32)> {
        match self {
            Self::Recorded {
                workflow_id,
                step_id,
            } => Some((workflow_id.as_str(), *step_id)),
            _ => None,
        }
    }

    /// Reads back what this call recorded, if this workflow has run this far before.
    ///
    /// `check_step` compares the recorded name, so a call whose position now holds some other
    /// step is already [`Error::UnexpectedStep`](crate::sysdb::Error::UnexpectedStep) before this
    /// returns — which is the determinism check, not an incidental one.
    ///
    /// **Named for the layer below rather than for this type.** `check`/[`record`](Self::record)
    /// is the pair `sysdb` already uses — `check_step`/`record_step`,
    /// `check_child_result`/`record_child_result` — and this is the thin wrapper over the first of
    /// them. It also keeps the word `Recorded` meaning one thing: it is a
    /// [`Placement`] variant, describing where the caller stands, and a method of the same name
    /// describing a step row would be the same word for two unrelated things.
    pub(crate) async fn check(
        &self,
        conn: &Connection,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let Some((workflow_id, step_id)) = self.step() else {
            return Ok(None);
        };
        conn.sysdb()
            .check_step(workflow_id, step_id, step_name)
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Records what this call answered, so [`check`](Self::check) finds it.
    ///
    /// `started_at` is when the operation began rather than when it finished being written down,
    /// so a workflow's timeline shows the wait. It is paired with a completion stamped here, and
    /// the pair is what makes the write idempotent for this caller and a conflict for a rival —
    /// see [`StepTiming::completed_at`].
    ///
    /// A no-op where nothing is checkpointed, so a caller need not branch: the placement already
    /// decided, and repeating the decision at every call site is how the two halves drift apart.
    ///
    /// Pairs with [`check`](Self::check), as `record_step` pairs with `check_step`.
    pub(crate) async fn record(
        &self,
        conn: &Connection,
        step_name: &str,
        outcome: Outcome<'_>,
        started_at: Timestamp,
    ) -> Result<(), Error> {
        let Some((workflow_id, step_id)) = self.step() else {
            return Ok(());
        };
        conn.sysdb()
            .record_step(
                workflow_id,
                step_id,
                step_name,
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
