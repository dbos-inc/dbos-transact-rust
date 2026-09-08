//! The durable race over steps: what it records, and the pair the macro's expansion calls.
//!
//! A workflow that races two steps has made a **choice**, and a choice a workflow makes has to be
//! recorded — a replay that raced again could see the other branch finish first and take a path
//! the first execution never took, which is the one thing a workflow may never do. That is the
//! whole reason this module exists where a `tokio::join!` over [steps](crate::step) needs nothing:
//! an all-wait decides nothing, so there is nothing for a replay to get differently.
//!
//! **A plain `tokio::select!` over steps is the trap this replaces.** Since a step takes its id
//! when it is built rather than at its first poll, the ids under a `select!` are already
//! deterministic — so it *looks* fixed. It is still not durable: nothing records which branch won,
//! and nothing stops the replay choosing again. The failure is silent, which is why the macro over
//! this refuses to be spelled like tokio's.
//!
//! # The split, and why the checkpoint is not in the macro
//!
//! [`check_select`] and [`record_select`] are ordinary `async fn`s dealing only in **indices**,
//! named for the `check`/`record` pair the crate uses at every other checkpoint. Everything that
//! touches [`StepPlacement`], the system database or serialization is here, in code that reads
//! without an expansion in the way and can be tested by calling it. What
//! [`select_step!`](crate::select_step) adds is the part that has to be written per call site: a
//! local per branch, a poll loop in source order, and one arm.
//!
//! **The poll loop is in the macro, and that is the point of a procedural macro.** A race's answer
//! is a choice among branches whose outputs differ in type, and Rust has no anonymous sum to
//! return one through — so a function owning the loop would need a sum type per arity, and a
//! declarative macro, which can neither invent an identifier nor count, would need a rule per
//! arity to match on it. A procedural macro writes a differently-named slot per branch instead, so
//! nothing here is written twice and there is no arity to run out of.
//!
//! # What may be a branch
//!
//! A [`PendingStep`] — a [`step`](crate::step), a
//! [`handle.result()`](crate::WorkflowHandle::result), a [`sleep`](crate::sleep), an event, a wait,
//! a management call. Every one of them **observes**: it claims one id, records one row, and a
//! replay of it reads that row back rather than doing the thing again.
//!
//! A [`PendingWorkflow`](crate::PendingWorkflow) — a child's
//! [`start`](crate::WorkflowRef::start) or [`run`](crate::WorkflowRef::run) — is **not** a
//! `PendingStep`, so [`Branches::push`] cannot be handed one and a race cannot be built over one.
//! That is the type doing what the prose alone could not: a start *creates* a workflow, only the
//! winner is polled on a replay, and so whether a child exists at all would follow the timing of
//! some other branch. Start outside the race and race what observes the result.

use std::marker::PhantomData;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, Timestamp, step_names};

/// What [`check_select`] found: a winner already recorded, or a race still to run.
///
/// Two states rather than an `Option<Recording>` beside a loose index, because the caller must
/// handle both and the type is what makes forgetting one a compile error.
pub enum Racing {
    /// This select already ran. Poll **only** this branch, which then replays from its own step
    /// row without running, and take its arm.
    Replay(usize),
    /// No checkpoint yet. Race the branches, then hand the winner to [`record_select`].
    Fresh(Recording),
}

/// A checkpoint that has been claimed and not yet written.
///
/// Named for the half-finished row rather than for the race, and it is what links the pair:
/// [`record_select`] cannot be reached without a [`check_select`] to hand one over, where the
/// crate's other `check`/`record` pairs are independent calls that trust their caller to order
/// them.
///
/// Holds the placement rather than re-deriving it, and the instant from *before* the wait rather
/// than taking a fresh one at the end: the recorded duration should cover the waiting, which is
/// where every other recorded call in this crate takes its `started_at`.
pub struct Recording {
    placement: StepPlacement,
    started_at: Timestamp,
}

/// The branches of one race: what each is called, what id it claimed, and how they all fail.
///
/// **Built by pushing rather than from a literal, because pushing is what unifies `E`.** A race's
/// branches differ in what they return — that is the whole reason it is a race — and agree on how
/// they fail, since one `Result` comes out the far end. Nothing in a macro expansion can state
/// that agreement; a `Vec<(String, Option<i32>)>` has already forgotten it, every branch's error
/// type would then be inferred alone, and the caller would be annotating a type that used to be
/// obvious. One generic method fixes it in one line.
///
/// The names and ids are read while the branches are still alive, because the losers are dropped
/// as soon as the race is decided and a stale-winner report has to be able to name a branch that
/// is gone.
pub struct Branches<E> {
    identities: Vec<(String, Option<i32>)>,
    // `fn() -> E` rather than `E`: this owns no error and must not inherit a `Send`/`Sync`
    // restriction from one, only the type.
    failure: PhantomData<fn() -> E>,
}

impl<E> Default for Branches<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Branches<E> {
    /// An empty set, which only an expansion ever holds — every race pushes at least two.
    #[must_use]
    pub fn new() -> Self {
        Self {
            identities: Vec::new(),
            failure: PhantomData,
        }
    }

    /// Records what this branch is called and which id it claimed, in build order.
    ///
    /// Takes the branch by reference: it is about to be raced, so this may not consume it, and `T`
    /// is free per call while `E` is fixed by the set.
    ///
    /// **A [`PendingStep`] and nothing else**, which is the refusal the module documentation
    /// describes: a child's start and a whole run are
    /// [`PendingWorkflow`](crate::PendingWorkflow)s, so this signature is what keeps them out of a
    /// race rather than a paragraph asking.
    pub fn push<T>(&mut self, branch: &PendingStep<'_, T, E>) {
        self.identities
            .push((branch.name().to_owned(), branch.step_id()));
    }

    /// How many branches this race has, which is the set a recorded winner is checked against.
    #[must_use]
    pub fn len(&self) -> usize {
        self.identities.len()
    }

    /// Whether no branch was pushed. Only reachable by calling [`Branches::new`] directly.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.identities.is_empty()
    }

    /// The branches this run built, as the stale-winner refusal names them.
    ///
    /// `"0: charge (step 3), 1: timeout (step 4)"` — the thing a reader has to compare a recorded
    /// index against, since "branch 2" alone says nothing about what the code in front of them
    /// does now.
    fn summarize(&self) -> String {
        self.identities
            .iter()
            .enumerate()
            .map(|(at, (name, step_id))| match step_id {
                Some(step_id) => format!("{at}: {name} (step {step_id})"),
                None => format!("{at}: {name} (uncheckpointed)"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Claims this select's step id and asks whether it already has a winner.
///
/// The `check` half of the pair [`check_step`](crate::sysdb::SystemDatabase::check_step) and
/// [`record_step`](crate::sysdb::SystemDatabase::record_step) name at the layer below, doing a
/// little more than they do: it takes the [`StepPlacement`] as well as reading the row, and it
/// holds a recorded winner to the branches that exist now.
///
/// **Called after every branch is built**, and that ordering is the contract rather than a
/// convenience: the branches take their ids as they are constructed and the select's own id
/// follows them, so a select that took its id first would leave every branch one slot higher than
/// the replay expects.
///
/// `branches` is read only to check a recorded winner against the set that exists now, and to fix
/// the error type. An empty race never arrives here: the macro refuses fewer than two branches at
/// compile time, which is the one refusal a macro can make that a `Vec`-taking function would have
/// had to make at run time.
///
/// **[`StepPlacement::here`], so a race inside a step body is a plain race.** The leaf rule the
/// whole crate follows: the step's own checkpoint stands for everything its body did, and outside
/// a workflow there is no counter at all. Both fall through to [`Racing::Fresh`] with a placement
/// that records nothing, which is what makes `select_step!` mean the same thing wherever it is
/// written.
pub async fn check_select<E: DurableError>(branches: &Branches<E>) -> Result<Racing, E> {
    let placement = StepPlacement::here();
    // Before the wait rather than after it, so the recorded duration covers the waiting.
    let started_at = Timestamp::now();

    if let Some(executor) = placement.executor()
        && let Some(recorded) = placement
            .check(executor.connection(), step_names::SELECT_STEP)
            .await
            .map_err(Error::lift)?
    {
        let winner: usize = decode(recorded.output.as_deref(), "the branch that won a select")?;
        // **A workflow that changed how many branches it races has changed what this position of
        // its code means**, and adopting a stale winner would silently take the branch the old
        // code took. The same check [`select_workflow`](crate::select_workflow()) makes against
        // its recorded id, for the same reason: the payload is the only thing a replay has to
        // check the current shape against.
        if winner >= branches.len() {
            // `Some` wherever a row was read at all — `check` answers `None` for every placement
            // that holds no step — so this arm knows both halves rather than defaulting them.
            let (workflow_id, step_id) = placement.step().unwrap_or(("", 0));
            return Err(Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                // `expected` is what this run asks for and `recorded` what the row holds, which is
                // the order `Error::UnexpectedStep` prints them in.
                workflow_id: workflow_id.to_owned(),
                step_id,
                expected: format!(
                    "a select over {} branches — {}",
                    branches.len(),
                    branches.summarize()
                ),
                recorded: format!("a select won by branch {winner}, which no longer exists"),
            }));
        }
        tracing::debug!(winner, "replaying select_step; the same branch wins again");
        return Ok(Racing::Replay(winner));
    }

    Ok(Racing::Fresh(Recording {
        placement,
        started_at,
    }))
}

/// Writes which branch won, once the race has one.
///
/// The `record` half, taking the [`Recording`] that [`check_select`] handed over — so the ordering
/// the crate's other pairs leave to their caller is a type error here instead.
///
/// **The position, not the branch's result.** The result is already recorded under the winning
/// step's own id, so writing it again here would keep one outcome in two places and leave a replay
/// to decide which is true. What only this step knows is which branch won.
///
/// Where nothing is checkpointed — outside a workflow, or inside a step body — this writes nothing
/// and the race was a plain one, which is the fall-through [`StepPlacement::record`] gives every
/// other call.
pub async fn record_select<E: DurableError>(recording: Recording, winner: usize) -> Result<(), E> {
    let Recording {
        placement,
        started_at,
    } = recording;
    let Some(executor) = placement.executor() else {
        return Ok(());
    };

    let encoded = encode(&winner, "the branch that won a select")?;
    placement
        .record(
            executor.connection(),
            step_names::SELECT_STEP,
            Outcome::Output(Some(&encoded)),
            started_at,
        )
        .await
        .map_err(Error::lift)
}

/// Takes a control signal out of the winning branch's slot, if that is what it holds.
///
/// **A control signal is not the race's decision.** A step that ends in a cancellation, an
/// interruption or a database failure records nothing — [`step`](crate::step) hands it back with
/// the row untouched, so the workflow stays pending and is recovered — and the race it won has to
/// do the same. Recording that branch as the winner would pin every recovery to a branch that
/// never ran its body and never race the others again; and a control signal tends to arrive
/// *fast*, one failed round trip ahead of any branch doing real work, so it would win exactly when
/// it matters.
///
/// An application error is different and is left where it is: the branch recorded it under its own
/// id, so recording it as the winner is a faithful account and a replay reproduces it.
///
/// Answers `None` where the slot holds anything else, leaving it in place for the arm.
pub fn control_error<T, E>(slot: &mut Option<Result<T, E>>) -> Option<Error<E>> {
    match slot {
        Some(Err(failure)) if failure.control().is_some() => slot.take().and_then(Result::err),
        _ => None,
    }
}
