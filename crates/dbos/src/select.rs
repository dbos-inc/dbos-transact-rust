//! The durable race over steps: what it records, and the pair the macro's expansion calls.
//!
//! A workflow that races two steps has made a **choice**, and a choice a workflow makes has to be
//! recorded — a replay that raced again could see the other branch finish first and take a path
//! the first execution never took, which is the one thing a workflow may never do. That is the
//! whole reason this module exists where a `tokio::join!` over [steps](crate::step()) needs nothing:
//! an all-wait decides nothing, so there is nothing for a replay to get differently.
//!
//! **A plain `tokio::select!` over steps is the trap this replaces.** Since a step takes its id
//! when it is built rather than at its first poll, the ids under a `select!` are already
//! deterministic — so it *looks* fixed. It is still not durable: nothing records which branch won,
//! and nothing stops the replay choosing again. The failure is silent, which is why the macro over
//! this refuses to be spelled like tokio's.
//!
//! # Two writes, and the window between them
//!
//! A decided race leaves **two** rows, and they are two commits. The winning branch records its
//! own outcome under its own id, because it is an ordinary durable call and does what one does;
//! this select then records *which* branch that was, under the id it claimed for itself. An
//! execution that stops between them leaves a branch row under a select with none — and a select
//! with no row is a select that has not run, so a recovery races again.
//!
//! **What that risks is a recovery disagreeing with the record**, not a path taken twice and not
//! work done twice. The select's row is written before any arm body, so an execution that stopped
//! in this window ran no arm at all; and a branch that recorded replays from its row rather than
//! running again. What can differ is which arm the recovery takes — a charge that succeeded and
//! recorded, raced against a timeout, and a second race that takes the timeout arm while the
//! charge's row says the money moved.
//!
//! **There is no timing argument that makes this rare.** A recovery usually comes long after the
//! crash, by which time a losing sleep's recorded wake time is in the past, so it replays as
//! immediately as the winner does and which lands first is a coin toss. The race that most wants
//! protection is the one least protected by luck.
//!
//! # Why this is documented rather than fixed
//!
//! Neither of the obvious repairs survives contact with the kinds of durable call a branch can
//! be:
//!
//! - *Infer the winner from the branch that recorded.* A row does not mean a branch finished.
//!   [`sleep`](crate::sleep()) checkpoints the instant it will wake at and waits afterwards, so a
//!   **losing** sleep leaves a row that is, in the row, indistinguishable from a winner's — and a
//!   losing sleep is what a timeout race has. Nothing in the row separates them: a durable sleep
//!   is stamped complete at its wake time, which a later recovery reads as long past, and a
//!   deadline is stamped complete the moment it is written. Making this sound needs a bit on the
//!   call itself, saying whether its row would mean it finished, which is a change to every
//!   durable call's contract in service of one caller.
//! - *Write both rows in one transaction.* There is no single write to join. An application step
//!   records through `record_step`, a child's result through `record_child_result`, a sleep
//!   through `record_sleep` before the wait it is checkpointing, and a child's start inside
//!   the transaction that creates the child. The select would have to reach into all four.
//!
//! So the window stands, with its shape written down. If it is closed later, the bit on the call
//! is the way in, and the losing sleep is the test that says whether it worked.
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
//! A [`PendingStep`] — a [`step`](crate::step()), a
//! [`handle.result()`](crate::WorkflowHandle::result), a [`sleep`](crate::sleep()), an event, a wait,
//! a management call. Every one of them **observes**: it claims one id, records one row, and a
//! replay of it reads that row back rather than doing the thing again.
//!
//! A child's [`start`](crate::WorkflowRef::start) or [`run`](crate::WorkflowRef::run) — a
//! [`PendingStart`](crate::PendingStart) and a [`PendingWorkflow`](crate::PendingWorkflow) — is
//! **not** a `PendingStep`, so [`Branches::push`] cannot be handed one and a race cannot be built
//! over one.
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
    /// describes: a child's start is a [`PendingStart`](crate::PendingStart) and a whole run a
    /// [`PendingWorkflow`](crate::PendingWorkflow), so this signature is what keeps them out of a
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
/// little more than they do: it takes the `StepPlacement` as well as reading the row, and it
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
/// **`StepPlacement::here`, so a race inside a step body is a plain race.** The leaf rule the
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
/// and the race was a plain one, which is the fall-through `StepPlacement::record` gives every
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
/// interruption or a database failure records nothing — [`step`](crate::step()) hands it back with
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

/// What a durable race is, and what writing one looks like.
///
/// Everything above is the contract — the id this call claims, the position it records, the
/// refusal a changed shape earns — and everything here drives it through
/// [`select_step!`](crate::select_step), because there is nothing else to drive it with: a race's
/// branches disagree about what they return, which is the whole point, and a `Vec` cannot hold
/// that.
///
/// **Every losing branch parks on [`std::future::pending`], never on a sleep.** Winning a race
/// costs the winner a `check_step` and a `record_step` — two database round trips — and on a
/// loaded runner those outlast any sleep short enough to keep a test quick, so a sleeping loser
/// wins and the test fails for a reason that has nothing to do with what it asserts.
#[cfg(all(test, feature = "macros"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::context::Ctx;
    use crate::step::step;
    use crate::sysdb::types::{NewWorkflow, Submission};
    use crate::{Config, DBOS};

    /// A launched instance and a workflow row for the race to record against.
    ///
    /// No `run` here on purpose: entering the same workflow id under two fresh contexts is
    /// precisely what a replay is, and it is the cheapest way to produce one without going through
    /// recovery.
    async fn workflow(id: &str) -> (DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = DBOS::new(Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..Config::new("select-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .init_workflow(&NewWorkflow::new(id), None, Submission::Fresh, None)
            .await
            .expect("could not create the workflow row");
        (dbos, db)
    }

    fn ctx(dbos: &DBOS, id: &str) -> Ctx {
        Ctx::new(dbos.executor("test").expect("launched"), id, None)
    }

    /// The ids a workflow's steps were recorded under, and what recorded them.
    async fn steps(dbos: &DBOS, id: &str) -> Vec<(i32, String)> {
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .list_workflow_steps(id, true, None, None, None)
            .await
            .expect("read failed")
            .into_iter()
            .map(|step| (step.step_id, step.step_name))
            .collect()
    }

    /// A branch that never answers, which is how every loser in this module loses.
    fn never(name: &'static str) -> PendingStep<'static, u32> {
        step(name, || async {
            std::future::pending::<()>().await;
            Ok::<_, crate::Error>(0u32)
        })
    }

    /// The macro over the core: heterogeneous branches, typed arms, one arm run.
    ///
    /// The branches return a `u32` and a `String`, which is the thing a `Vec`-taking core cannot
    /// express and the whole reason this is a macro.
    #[tokio::test]
    async fn a_race_runs_one_arm_and_the_loser_records_nothing() {
        let (dbos, _db) = workflow("wf-macro").await;
        let loser_ran = Arc::new(AtomicU32::new(0));

        let answer: crate::Result<String> = Ctx::scope(ctx(&dbos, "wf-macro"), {
            let loser_ran = Arc::clone(&loser_ran);
            async move {
                crate::select_step! {
                    slow = step("slow", {
                        let loser_ran = Arc::clone(&loser_ran);
                        move || {
                            let loser_ran = Arc::clone(&loser_ran);
                            async move {
                                loser_ran.fetch_add(1, Ordering::SeqCst);
                                std::future::pending::<()>().await;
                                Ok::<_, crate::Error>(1u32)
                            }
                        }
                    }) => format!("the counter won with {}", slow?),
                    quick = step("quick", || async {
                        Ok::<_, crate::Error>("hello".to_owned())
                    }) => format!("the namer won with {}", quick?)
                }
            }
        })
        .await;

        assert_eq!(answer.unwrap(), "the namer won with hello");
        // Polled once, in source order, before the winner was reached: the loser really did start,
        // so "records nothing" is about the drop rather than about never having run.
        assert_eq!(
            loser_ran.load(Ordering::SeqCst),
            1,
            "the loser was polled and started before the winner finished"
        );
        // The loser was *built*, so it spent id 0, and dropped without recording — which is what
        // keeps the numbering stable across a replay that never runs it.
        assert_eq!(
            steps(&dbos, "wf-macro").await,
            [(1, "quick".to_owned()), (2, "DBOS.selectStep".to_owned())],
            "the winner keeps its build-order id, the loser records nothing"
        );

        dbos.shutdown().await;
    }

    /// **Branches that fail differently agree at the build, through `map_error`.**
    ///
    /// The channel is fixed by `push` before anything is awaited, so `map_err` in an arm's body is
    /// too late — this is the conversion written where it still fits. What the loser's own row
    /// would hold is untouched by it: the winner here records its error under its own id in the
    /// channel it ran in, and only the arm sees the converted one.
    #[tokio::test]
    async fn branches_of_two_error_types_agree_through_map_error() {
        #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
        #[error("the gateway refused")]
        struct Refused;

        #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
        #[error("charging failed: {0}")]
        struct Charging(String);

        let (dbos, _db) = workflow("wf-two-channels").await;

        let answer: crate::Result<String, Charging> =
            Ctx::scope(ctx(&dbos, "wf-two-channels"), async {
                crate::select_step! {
                    // Its own error type, converted while it is still a call — which is what lets
                    // it stand beside a branch that fails as `Charging`.
                    refused = step("refused", || async { Err::<u32, Error<Refused>>(Refused.into()) })
                        .map_error(|refused: Refused| Charging(refused.to_string())) => {
                        format!("refused: {}", refused.unwrap_err())
                    }
                    slow = step("slow", || async {
                        std::future::pending::<()>().await;
                        Err::<u32, Error<Charging>>(Charging("never".to_owned()).into())
                    }) => format!("slow: {}", slow.unwrap_err())
                }
            })
            .await;

        assert_eq!(
            answer.unwrap(),
            "refused: charging failed: the gateway refused",
            "the arm sees the branch's error converted into the channel the race agreed on"
        );
        // An application error is the branch's outcome, so it recorded one — and the race recorded
        // it as the winner, which is the whole difference from a control signal.
        assert_eq!(
            steps(&dbos, "wf-two-channels").await,
            [(0, "refused".to_owned()), (2, "DBOS.selectStep".to_owned())],
            "the winner's row and the select's, with the loser's id spent and unrecorded"
        );

        dbos.shutdown().await;
    }

    /// A replay takes the same arm, and the loser is not run for the first time on the way past.
    #[tokio::test]
    async fn a_replayed_race_takes_the_recorded_branch_and_polls_no_other() {
        let (dbos, _db) = workflow("wf-macro-replay").await;
        let loser_ran = Arc::new(AtomicU32::new(0));

        let race = |loser_ran: Arc<AtomicU32>| async move {
            crate::select_step! {
                quick = step("quick", || async { Ok::<_, crate::Error>(7u32) }) => {
                    format!("quick: {}", quick?)
                }
                slow = step("slow", {
                    let loser_ran = Arc::clone(&loser_ran);
                    move || {
                        let loser_ran = Arc::clone(&loser_ran);
                        async move {
                            loser_ran.fetch_add(1, Ordering::SeqCst);
                            std::future::pending::<()>().await;
                            Ok::<_, crate::Error>("slow".to_owned())
                        }
                    }
                }) => format!("slow: {}", slow?)
            }
        };

        let first: crate::Result<String> =
            Ctx::scope(ctx(&dbos, "wf-macro-replay"), race(Arc::clone(&loser_ran))).await;
        assert_eq!(first.unwrap(), "quick: 7");
        let started_once = loser_ran.load(Ordering::SeqCst);

        let again: crate::Result<String> =
            Ctx::scope(ctx(&dbos, "wf-macro-replay"), race(Arc::clone(&loser_ran))).await;
        assert_eq!(again.unwrap(), "quick: 7", "the replay takes the same arm");
        assert_eq!(
            loser_ran.load(Ordering::SeqCst),
            started_once,
            "the loser recorded nothing the first time, so a replay must not run it now"
        );

        dbos.shutdown().await;
    }

    /// **The window between the winner's row and the select's**, from the recovery's side: the
    /// race is run again, and the branch that recorded replays rather than running a second time.
    ///
    /// The state is planted rather than reached by crashing an execution, because what has to be
    /// tested is what a recovery *finds* — a branch row under a select with none — however it got
    /// there. What this pins is the half that holds whatever the timing does: no step runs twice.
    /// Which arm a recovery takes is the part the module documentation describes and this cannot
    /// assert, since with an elapsed deadline in the race it is a coin toss.
    #[tokio::test]
    async fn a_race_re_run_in_the_window_replays_the_branch_that_recorded() {
        let (dbos, _db) = workflow("wf-window").await;
        let executor = dbos.executor("test").expect("launched");
        let quick_ran = Arc::new(AtomicU32::new(0));

        // What the interrupted execution left behind: the branch that won recorded, and nothing
        // recorded that it won.
        executor
            .sysdb()
            .record_step(
                "wf-window",
                1,
                "quick",
                Outcome::Output(Some(
                    &encode::<_, crate::Error>(&"hello".to_owned(), "the winner").expect("encodes"),
                )),
                Some(executor.connection().serializer().name()),
                None,
            )
            .await
            .expect("could not plant the winner's row");

        let answer: crate::Result<String> = Ctx::scope(ctx(&dbos, "wf-window"), {
            let quick_ran = Arc::clone(&quick_ran);
            async move {
                crate::select_step! {
                    slow = never("slow") => format!("slow: {}", slow?),
                    quick = step("quick", {
                        let quick_ran = Arc::clone(&quick_ran);
                        move || {
                            let quick_ran = Arc::clone(&quick_ran);
                            async move {
                                quick_ran.fetch_add(1, Ordering::SeqCst);
                                Ok::<_, crate::Error>("hello".to_owned())
                            }
                        }
                    }) => format!("the namer won with {}", quick?)
                }
            }
        })
        .await;

        assert_eq!(answer.unwrap(), "the namer won with hello");
        assert_eq!(
            quick_ran.load(Ordering::SeqCst),
            0,
            "the branch that recorded replays from its row; the window costs no second execution"
        );
        // And the select has the row it was missing, so the next recovery reads a decision rather
        // than racing a third time.
        assert_eq!(
            steps(&dbos, "wf-window").await,
            [(1, "quick".to_owned()), (2, "DBOS.selectStep".to_owned())],
            "the winner's row, and the select row that was missing"
        );

        dbos.shutdown().await;
    }

    /// **A losing sleep leaves a row**, which is the fact behind the module documentation's
    /// refusal to read branch rows as the race's answer.
    ///
    /// `record_sleep` writes when the sleep is first polled and the wait comes after it, so
    /// what this asserts is a row for a wait that was abandoned — beside the winner's, and
    /// indistinguishable from one.
    #[tokio::test]
    async fn a_losing_sleep_leaves_a_row_for_a_wait_it_never_finished() {
        let (dbos, _db) = workflow("wf-losing-sleep").await;

        let answer: crate::Result<String> = Ctx::scope(ctx(&dbos, "wf-losing-sleep"), async {
            crate::select_step! {
                timeout = crate::sleep(std::time::Duration::from_secs(30)) => {
                    timeout?;
                    "timed out".to_owned()
                }
                quick = step("quick", || async { Ok::<_, crate::Error>("hello".to_owned()) })
                    => format!("the namer won with {}", quick?)
            }
        })
        .await;

        assert_eq!(answer.unwrap(), "the namer won with hello");
        assert_eq!(
            steps(&dbos, "wf-losing-sleep").await,
            [
                (0, "DBOS.sleep".to_owned()),
                (1, "quick".to_owned()),
                (2, "DBOS.selectStep".to_owned())
            ],
            "the sleep lost and still recorded, which is why a branch row is not a winner"
        );

        dbos.shutdown().await;
    }

    /// A recorded winner that no longer exists is a changed shape, not a branch to take.
    ///
    /// The select has to land on the same step id both times, so the same three calls are built —
    /// only two of them are raced the second time.
    #[tokio::test]
    async fn a_recorded_winner_outside_the_current_set_is_reported() {
        let (dbos, _db) = workflow("wf-shrunk").await;

        let first: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
            crate::select_step! {
                b = never("b") => b?,
                c = never("c") => c?,
                a = step("a", || async { Ok::<_, crate::Error>(1u32) }) => a?
            }
        })
        .await;
        assert_eq!(first.unwrap(), 1, "the branch built third finished first");

        let shrunk: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
            // Built and dropped, so the select still lands on step 3 and the refusal is about the
            // race's shape rather than about which slot it read.
            let _third = step("a", || async { Ok::<_, crate::Error>(1u32) });
            crate::select_step! {
                b = step("b", || async { Ok::<_, crate::Error>(2u32) }) => b?,
                c = step("c", || async { Ok::<_, crate::Error>(3u32) }) => c?
            }
        })
        .await;

        match shrunk {
            Err(crate::Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                expected,
                recorded,
                ..
            })) => {
                assert!(recorded.contains("branch 2"), "{recorded}");
                // The branches this run built, named — "branch 2" alone says nothing about what
                // the code in front of the reader does now.
                assert!(
                    expected.contains("0: b (step 1), 1: c (step 2)"),
                    "{expected}"
                );
            }
            other => panic!("a stale winner must be reported, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// Outside a workflow the branches run plainly and nothing is recorded — which is what keeps a
    /// function built from steps ordinarily callable and ordinarily testable.
    #[tokio::test]
    async fn outside_a_workflow_the_race_is_plain() {
        let answer: crate::Result<u32> = async {
            crate::select_step! {
                quick = step("quick", || async { Ok::<_, crate::Error>(1u32) }) => quick?,
                slow = never("slow") => slow?
            }
        }
        .await;
        assert_eq!(answer.unwrap(), 1);
    }

    /// Inside a step body it is plain too, by the leaf rule the whole crate follows: the step's
    /// own checkpoint stands for everything its body did.
    ///
    /// This is the case the module documentation blesses — *put the race inside a step* — so it is
    /// worth pinning that it records nothing of its own rather than a second row under the step.
    #[tokio::test]
    async fn inside_a_step_body_the_race_is_plain() {
        let (dbos, _db) = workflow("wf-in-step").await;

        let answer: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-in-step"), async {
            step("outer", || async {
                crate::select_step! {
                    quick = step("inner-quick", || async { Ok::<_, crate::Error>(5u32) }) => quick?,
                    slow = never("inner-slow") => slow?
                }
            })
            .await
        })
        .await;

        assert_eq!(answer.unwrap(), 5);
        assert_eq!(
            steps(&dbos, "wf-in-step").await,
            [(0, "outer".to_owned())],
            "the step's own row stands for the race inside it"
        );

        dbos.shutdown().await;
    }

    /// **No arity to run out of.** Ten branches, where a declarative form would stop at the last
    /// sum type somebody wrote.
    #[tokio::test]
    async fn a_race_wider_than_any_hand_written_arity() {
        let (dbos, _db) = workflow("wf-macro-wide").await;

        let answer: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-macro-wide"), async {
            crate::select_step! {
                a = never("a") => a?,
                b = never("b") => b?,
                c = never("c") => c?,
                d = never("d") => d?,
                e = never("e") => e?,
                f = never("f") => f?,
                g = never("g") => g?,
                h = never("h") => h?,
                i = never("i") => i?,
                j = step("j", || async { Ok::<_, crate::Error>(10u32) }) => j?,
            }
        })
        .await;

        assert_eq!(answer.unwrap(), 10, "the tenth branch finished first");
        assert_eq!(
            steps(&dbos, "wf-macro-wide").await,
            [(9, "j".to_owned()), (10, "DBOS.selectStep".to_owned())],
            "nine losers spend their ids and record nothing"
        );

        dbos.shutdown().await;
    }

    /// The comma rule is `match`'s: a block body ends its own arm, including mid-list.
    ///
    /// Outside a workflow, because what is under test is the grammar rather than the checkpoint —
    /// this compiling at all is most of the assertion.
    #[tokio::test]
    async fn a_block_arm_ends_itself_like_a_match_arm() {
        let answer: crate::Result<u32> = async {
            crate::select_step! {
                quick = step("quick", || async { Ok::<_, crate::Error>(1u32) }) => {
                    let won = quick?;
                    won + 10
                }
                slow = never("slow") => match slow {
                    Ok(value) => value + 1,
                    Err(_) => 0,
                }
            }
        }
        .await;
        assert_eq!(answer.unwrap(), 11);
    }

    /// **An engine-channel call joins a race in the application's own channel**, which is what
    /// [`PendingStep::lift`] is for.
    ///
    /// The management surface answers in the engine's channel, a workflow body usually does not,
    /// and the branches have to agree on how they fail *before* any of them is awaited — so
    /// `map_err(Error::lift)` inside an arm is too late, converting the branch's output where what
    /// has to change is its type. This compiling is most of the assertion.
    #[tokio::test]
    async fn an_engine_channel_call_joins_a_race_in_the_applications_channel() {
        #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
        #[error("the application failed")]
        struct AppError;

        let (dbos, _db) = workflow("wf-lifted").await;
        let filter = crate::sysdb::types::WorkflowFilter::default();

        let listed: crate::Result<bool, AppError> = Ctx::scope(ctx(&dbos, "wf-lifted"), async {
            crate::select_step! {
                never = crate::step("never", || async {
                    std::future::pending::<()>().await;
                    Ok::<bool, crate::Error<AppError>>(false)
                }) => never?,
                rows = dbos.list_workflows(&filter).lift() => !rows?.is_empty()
            }
        })
        .await;

        assert!(
            listed.unwrap(),
            "the listing won and saw this workflow's row"
        );

        dbos.shutdown().await;
    }

    /// **The race's error type comes from its branches, with nothing written down.**
    ///
    /// [`Branches`] is what carries it: the branches disagree about `T` — that is the point of a
    /// race — and agree about `E`, and pushing each one is what states the agreement. Without it
    /// the only thing tying the expansion's error type to the branches' would be a `?` inside an
    /// arm, so a race whose arms handle their own failures would need an annotation. No annotation
    /// here is the whole test.
    #[tokio::test]
    async fn the_error_type_is_inferred_from_the_branches() {
        let (dbos, _db) = workflow("wf-macro-inferred").await;

        let answer = Ctx::scope(ctx(&dbos, "wf-macro-inferred"), async {
            crate::select_step! {
                counted = never("counted") => counted.is_ok(),
                named = step("named", || async {
                    Ok::<_, crate::Error>("hello".to_owned())
                }) => named.is_ok(),
            }
        })
        .await;

        assert!(answer.unwrap(), "the branch that finished first succeeded");

        dbos.shutdown().await;
    }
}
