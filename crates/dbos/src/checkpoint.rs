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
use crate::context::{Ctx, StepMarker};
use crate::error::Error;
use crate::instance::Executor;
use crate::sysdb::types::{Outcome, StepRecord, StepTiming, Timestamp};

/// A durable call that has taken its step id and has not run.
///
/// Returned by [`step`](crate::step) and [`step_with`](crate::step_with), by
/// [`start`](crate::WorkflowRef::start) and [`start_with`](crate::WorkflowRef::start_with), by
/// [`WorkflowHandle::result`](crate::WorkflowHandle::result), by the waits
/// [`select_workflow`](fn@crate::select_workflow) and [`join_workflows`](fn@crate::join_workflows),
/// by
/// [`get_event`](crate::get_event), [`set_event`](crate::set_event) and [`sleep`](crate::sleep),
/// and by every checkpointed management call on [`DBOS`](crate::DBOS). Awaiting one runs it — so
/// `step(..).await?` and `child.start(n).await?` read as they always did. What is different is that
/// **the step id is spent at the call rather than at the first poll**, which is what lets a set of
/// them be built first and driven together: `tokio::join!` over three launches, or
/// [`select_step!`](crate::select_step) over a step and a child, takes the same slots on a replay
/// however the futures interleave.
///
/// One type for every producer rather than one each, because a combinator holding branches
/// has no reason to care which it holds. What every producer supplies is the same: a name, which
/// the replay compares against; the id it claimed, or `None` where there was none to claim; and
/// the work, which does not start until something polls it.
///
/// **`Future` rather than `IntoFuture`**, because a step has to be accepted everywhere a future is:
/// `tokio::time::timeout` around one, a combinator holding several. `IntoFuture` only ever reaches
/// the `.await` itself.
///
/// **`#[must_use]` is load-bearing rather than tidy.** A built call that is never polled has still
/// taken its id, so dropping one silently shifts nothing — every later id is what it would have
/// been — but the call itself never runs and never records.
///
/// **`Unpin`, and that is part of the contract rather than an accident.** The run is already
/// behind a `Pin<Box<..>>` and the other two fields are plain data, so a combinator can hold one
/// by value, move it into a `Vec`, and poll it through `&mut` without pinning it first. A
/// combinator that wants a whole set of branches is the caller this is for, and requiring it to
/// pin each one would be the difference between a poll loop and a `pin!` per branch.
///
/// **Polled where it was built.** The id is a claim on one position in one workflow, so a call
/// carried into another workflow, or built outside one and polled inside, is refused as
/// [`Error::StepBuiltElsewhere`] rather than run under the wrong id. Each producer makes that
/// check inside its own run, which is why nothing here knows how.
#[must_use = "a durable call that is not awaited has spent its step id without running; await it, \
              or hand it to a combinator"]
pub struct Pending<'a, T, E = crate::EngineOnly> {
    /// What the call is called — the step's name, the child workflow's, or `DBOS.getResult` —
    /// which is the name its checkpoint is checked against on replay.
    name: Arc<str>,
    /// The id this call claimed when it was built, or `None` where it claimed none.
    step_id: Option<i32>,
    /// The run, built by the constructor and driven by whatever polls this.
    ///
    /// An `async fn` body does not begin until it is polled, so the future is built where the id
    /// is taken and this field is the whole of what runs. The two above it are identity, not
    /// state: nothing reads them to decide what happens, and nothing mutates them.
    running: Pin<Box<dyn Future<Output = crate::Result<T, E>> + Send + 'a>>,
}

impl<'a, T, E> Pending<'a, T, E> {
    pub(crate) fn new(
        name: Arc<str>,
        step_id: Option<i32>,
        running: impl Future<Output = crate::Result<T, E>> + Send + 'a,
    ) -> Self {
        Self {
            name,
            step_id,
            running: Box::pin(running),
        }
    }

    /// A durable call at a placement already taken, refusing at the poll what was refused at the
    /// build.
    ///
    /// **The one constructor every non-step durable call goes through**, so the shape they share
    /// is written once: `built` is the placement taken at the call, with whatever the run needs
    /// beside it — an executor, a second id, an encoded value — or the error the build produced.
    /// Polled, it reports that error, refuses to run where it was not built, and only then hands
    /// the placement to `run`. The id was spent by whoever built `built`, which is why nothing
    /// here allocates.
    ///
    /// `name` is the cross-SDK step name the call records under, and what a refusal names.
    pub(crate) fn placed<C, F, Fut>(
        name: &'static str,
        built: Result<(C, Placement), Error>,
        run: F,
    ) -> Self
    where
        C: Send + 'a,
        F: FnOnce(C, Placement) -> Fut + Send + 'a,
        Fut: Future<Output = crate::Result<T, E>> + Send + 'a,
        T: 'a,
        E: 'a,
    {
        let step_id = built
            .as_ref()
            .ok()
            .and_then(|(_, placement)| placement.step_id());
        Self::new(Arc::from(name), step_id, async move {
            let (carried, placement) = built.map_err(Error::lift)?;
            placement.check_here(name).map_err(Error::lift)?;
            run(carried, placement).await
        })
    }

    /// What this call is called — the name its checkpoint will be checked against on replay.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The id this call claimed when it was built, or `None` if it claimed none.
    ///
    /// `None` is not a failure. Outside a workflow there is no counter; a step inside another step
    /// is a plain call by the leaf rule; an await through a [`Client`](crate::Client)'s connection
    /// has no counter to agree with. A call that was *refused* at build — a launch inside a step,
    /// or against the wrong instance — reports that when polled rather than here.
    ///
    /// **Readable here because the run cannot be asked.** Once the call is a future the id is
    /// sealed inside it, and the callers that need to *name* one are all outside it — a race
    /// reporting which branch a stale checkpoint meant, a dropped reservation saying which id it
    /// burned, a `Debug` that says something.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.step_id
    }
}

impl<T, E> Future for Pending<'_, T, E> {
    type Output = crate::Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.get_mut().running.as_mut().poll(cx)
    }
}

impl<T, E> std::fmt::Debug for Pending<'_, T, E> {
    /// Hand-written because the run is a boxed future with nothing to show. What is worth showing
    /// is the identity, which is why it is on the value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("name", &self.name)
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
    /// ways to land here, told apart by `marker`:
    ///
    /// - **Inside a step**, and `marker` is that step's: a step is a leaf, and an id-allocating
    ///   call inside one would shift every later step onto the wrong replay slot. The marker is
    ///   what [`check_here`](Self::check_here) holds the call to — built inside a step, it runs
    ///   inside that step or not at all, exactly as a nested step does.
    /// - **Holding a [`Client`](crate::Client)'s connection**, and `marker` is `None`: a client
    ///   has no step counter to agree with this workflow's, and no execution of its own that a
    ///   recorded call could belong to. Nothing pins it, because a client's call is legitimately
    ///   driven from anywhere. A handle from *another instance* is the third case and is not this
    ///   one — that is [`Error::WrongInstance`], because two instances each have a counter and
    ///   the caller meant one of them.
    Uncheckpointed { marker: Option<StepMarker> },
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
        // and there is then nothing for the halves below to disagree about.
        if let Some(marker) = ctx.step_marker() {
            return Ok(Self::Uncheckpointed {
                marker: Some(marker),
            });
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
                Owner::Client => Ok(Self::Uncheckpointed { marker: None }),
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

    /// Where a call served by `executor` stands, with the executor kept beside it for the run.
    ///
    /// [`of`](Self::of) with the executor threaded through, which is the pair every
    /// [`Pending::placed`] caller builds. `executor` is a `Result` so an instance that is not
    /// launched, or a context that is not a workflow, is carried into the call and reported when
    /// it is polled rather than at the build.
    pub(crate) fn taken(
        executor: Result<Arc<Executor>, Error>,
        operation: &'static str,
    ) -> Result<(Arc<Executor>, Self), Error> {
        let executor = executor?;
        let placement = Self::of(executor.connection(), operation)?;
        Ok((executor, placement))
    }

    /// Refuses to run where this placement was not built.
    ///
    /// The id is a claim on *one position in one workflow*, taken at build, and the same silent
    /// failures a step's own run refuses apply here: built outside a workflow and polled inside
    /// one, the call would run unrecorded where a checkpoint was expected; built in one workflow
    /// and polled in another, the row would land under the wrong workflow's id; built inside a
    /// step and polled in the workflow proper, it would run unrecorded at a position that should
    /// have been checkpointed, and every replay would run it again. That last is the case a
    /// nested step carries its [`StepMarker`] for, and the marker is held to the same way here.
    ///
    /// The one placement not checked is a client connection's — `Uncheckpointed` with no marker.
    /// It records nothing wherever it runs, and the client that produces it is legitimately
    /// driven from anywhere.
    ///
    /// `operation` is what a refusal names as the step.
    pub(crate) fn check_here(&self, operation: &str) -> Result<(), Error> {
        let here = Ctx::current();
        let built: Option<std::borrow::Cow<'static, str>> = match (self, here.as_ref()) {
            (Self::Uncheckpointed { marker: None }, _) => None,
            (
                Self::Uncheckpointed {
                    marker: Some(marker),
                },
                Some(here),
            ) if here.step_marker() == Some(*marker) => None,
            (Self::Outside, None) => None,
            (Self::Recorded { workflow_id, .. }, Some(here))
                if here.workflow_id() == workflow_id && here.step_marker().is_none() =>
            {
                None
            }
            (Self::Outside, _) => Some("outside a workflow".into()),
            (Self::Uncheckpointed { .. }, _) => Some("inside a step".into()),
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

    /// The step id this call is recorded under, if it is recorded at all — what a [`Pending`]
    /// built at this placement reports as its own.
    pub(crate) fn step_id(&self) -> Option<i32> {
        self.step().map(|(_, step_id)| step_id)
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
