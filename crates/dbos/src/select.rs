//! The durable race over steps: its checkpoint, and the poll loop the macro wraps.
//!
//! A workflow that races two steps has made a **choice**, and a choice a workflow makes has to be
//! recorded — a replay that raced again could see the other branch finish first and take a path
//! the first execution never took, which is the one thing a workflow may never do. That is the
//! whole reason this exists where a `tokio::join!` over [steps](crate::step) needs nothing: an
//! all-wait decides nothing, so there is nothing for a replay to get differently.
//!
//! **A plain `tokio::select!` over steps is the trap this replaces.** Since a step takes its id
//! when it is built rather than at its first poll, the ids under a `select!` are already
//! deterministic — so it *looks* fixed. It is still not durable: nothing records which branch won,
//! and nothing stops the replay choosing again. The failure is silent, which is why the macro over
//! this refuses to be spelled like tokio's.
//!
//! # The split, and why the checkpoint is not in the macro
//!
//! [`check_select`] and [`record_select`] are ordinary async functions dealing only in
//! **indices**, named for the `check`/`record` pair the crate uses at every other checkpoint.
//! Everything that touches `Placement`, the system database, or serialization lives here, in
//! code that can be read without macro expansion in the way, and can be tested by calling it.
//! What [`select_step!`](crate::select_step) adds is the part that has to be written per call
//! site: a local per branch, a poll loop in source order, and one arm.
//!
//! **The poll loop is in the macro, not here, and that is the point of a procedural macro.** A
//! race's answer is a choice among branches whose outputs differ in type, and Rust has no
//! anonymous sum to return one through — so a function that owned the loop would need a sum type
//! per arity, and a declarative macro (which can neither invent an identifier nor count) would
//! need a rule per arity to match on it. A procedural macro writes a differently-named slot per
//! branch instead, so nothing here is written twice and there is no arity to run out of.
//!
//! A `Vec`-taking form was tried as a public surface and withdrawn: a `Vec` forces every branch to
//! share `T` and `E`, which a race almost never wants — "the fetch returned" and "the timeout
//! fired" are different types — and a position is not a handler. [`Branches`] is what is left of
//! it, and it carries only the half that *is* uniform: how the branches fail.

use std::marker::PhantomData;

use crate::checkpoint::Pending;
use crate::checkpoint::Placement;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, Timestamp, step_names};

/// What [`check_select`] found: a winner already recorded, or a race still to run.
///
/// Two states rather than an `Option<Recording>` plus a loose index, because the caller must
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
/// Named for the half-finished row rather than for the race, and it is what links the pair: a
/// [`record_select`] cannot be reached without a [`check_select`] to hand one over, where the
/// crate's other `check`/`record` pairs are independent calls that trust their caller to order
/// them.
///
/// Holds the placement rather than re-deriving it, and the start instant rather than taking a
/// fresh one at the end: the recorded duration should cover the waiting, which is where every
/// other recorded call in this crate takes its `started_at`.
pub struct Recording {
    ctx: Option<Ctx>,
    placement: Placement,
    started_at: Timestamp,
}

/// The branches of one race: what each is called, what id it claimed, and how they all fail.
///
/// **Built by pushing, not from a literal, because pushing is what unifies `E`.** A race's
/// branches differ in what they return — that is the whole reason it is a race — and agree on how
/// they fail, since one `Result` comes out the far end. Nothing in a macro expansion can state
/// that agreement; a `Vec<(String, Option<i32>)>` has already forgotten it, and every branch's
/// error type would then be inferred alone, leaving the race's own error type unconstrained and
/// the caller annotating a type that used to be obvious. One generic method fixes it in one line.
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
    /// An empty set, which only a macro expansion ever holds — every race pushes at least two.
    #[must_use]
    pub fn new() -> Self {
        Branches {
            identities: Vec::new(),
            failure: PhantomData,
        }
    }

    /// Records what this branch is called and which id it claimed, in build order.
    ///
    /// Takes the branch by reference: it is about to be raced, so this may not consume it, and
    /// `T` is free per call while `E` is fixed by the set.
    ///
    /// **Any [`Pending`], which is a step, a launch, or an await** — and not a
    /// [`PendingRun`](crate::PendingRun), which is a different type for exactly this reason.
    pub fn push<T>(&mut self, step: &Pending<'_, T, E>) {
        self.identities
            .push((step.name().to_owned(), step.step_id()));
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
}

/// Claims this select's step id and asks whether it already has a winner.
///
/// The `check` half of the pair [`check_step`](crate::sysdb::SystemDatabase::check_step) and
/// [`record_step`](crate::sysdb::SystemDatabase::record_step) name at the layer below, doing a
/// little more than they do: it takes the `Placement` as well as reading the row, and it checks
/// the recorded winner against the branches that exist now.
///
/// **Called after every branch is built**, and that ordering is the contract rather than a
/// convenience: the branches take their ids as they are constructed, and the select's own id
/// follows them. A select that took its id first would leave its branches' ids one higher than the
/// replay expects.
///
/// `branches` is only used to check a recorded winner against the set that exists now, and to fix
/// the error type. An empty race never reaches here — the macro refuses fewer than two branches at
/// compile time, which is the one refusal a macro can make that the withdrawn `Vec`-taking form
/// had to make at run time.
pub async fn check_select<E: DurableError>(branches: &Branches<E>) -> Result<Racing, E> {
    // The connection is the ambient executor's, so the placement can only be `Outside`,
    // `Uncheckpointed` or `Recorded` — never `WrongInstance`, which needs two connections to
    // disagree and there is only one here.
    let ctx = Ctx::current();
    let placement = match &ctx {
        Some(ctx) => {
            Placement::of(ctx.executor().connection(), "select_step").map_err(Error::lift)?
        }
        None => Placement::Outside,
    };
    let started_at = Timestamp::now();

    if let Some(ctx) = &ctx
        && let Some(recorded) = placement
            .check(ctx.executor().connection(), step_names::SELECT_STEP)
            .await
            .map_err(Error::lift)?
    {
        let winner: usize = decode(recorded.output.as_deref(), "the branch that won a select")?;
        // **A workflow that changed how many branches it races has changed what this position of
        // its code means**, and adopting a stale winner would silently take the branch the old
        // code took. The same check `select_workflow` makes against its recorded id, and the same
        // reason: the payload is the only thing a replay can check the current shape against.
        if winner >= branches.identities.len() {
            let (workflow_id, step_id) = placement.step().unwrap_or(("", 0));
            // Names the branches this run actually built, which is the thing a reader has to
            // compare against the recorded index — "branch 2" alone says nothing about what the
            // code in front of them now does.
            let now = branches
                .identities
                .iter()
                .enumerate()
                .map(|(at, (name, id))| match id {
                    Some(id) => format!("{at}: {name} (step {id})"),
                    None => format!("{at}: {name} (uncheckpointed)"),
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                workflow_id: workflow_id.to_owned(),
                step_id,
                expected: format!(
                    "a select over {} branches — {now}",
                    branches.identities.len()
                ),
                recorded: format!("a select won by branch {winner}, which no longer exists"),
            }));
        }
        tracing::debug!(winner, "replaying select_step; the same branch wins again");
        return Ok(Racing::Replay(winner));
    }

    Ok(Racing::Fresh(Recording {
        ctx,
        placement,
        started_at,
    }))
}

/// Writes which branch won, once the race has one.
///
/// The `record` half, taking the [`Recording`] that [`check_select`] handed over — so the order the
/// crate's other pairs leave to their caller is a type error here instead.
///
/// **The position, not the branch's result.** The result is already recorded under the winning
/// step's own id, so writing it again here would store one outcome in two places and leave a
/// replay to decide which is true. What only this step knows is which branch won.
///
/// Outside a workflow this records nothing and the race was a plain one, which is the same
/// fall-through an ordinary step takes.
pub async fn record_select<E: DurableError>(recording: Recording, winner: usize) -> Result<(), E> {
    let Recording {
        ctx,
        placement,
        started_at,
    } = recording;
    let Some(ctx) = ctx else { return Ok(()) };

    let encoded = encode(&winner, "the branch that won a select")?;
    placement
        .record(
            ctx.executor().connection(),
            step_names::SELECT_STEP,
            Outcome::Output(Some(&encoded)),
            started_at,
        )
        .await
        .map_err(Error::lift)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::sysdb::types::{NewWorkflow, Submission};
    use crate::{Config, DBOS, step::step};

    /// A launched instance with one workflow row, so a step has somewhere to record.
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
            .init_workflow(&NewWorkflow::new(id), None, Submission::Fresh)
            .await
            .expect("could not create the workflow row");
        (dbos, db)
    }

    fn ctx(dbos: &DBOS, id: &str) -> Ctx {
        Ctx::new(dbos.executor("test").expect("launched"), id, None)
    }

    /// The step rows a workflow left, in id order.
    async fn steps(dbos: &DBOS, id: &str) -> Vec<(i32, String)> {
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .list_workflow_steps(id, false, None, None, None)
            .await
            .expect("could not read the steps")
            .into_iter()
            .map(|s| (s.step_id, s.step_name))
            .collect()
    }

    /// The id is readable on the value, in build order — and `None` where none was claimed.
    ///
    /// The run cannot be asked once the step is a future, and every caller that needs to *name* a
    /// branch is outside it.
    #[tokio::test]
    async fn a_built_step_carries_its_name_and_id() {
        let (dbos, _db) = workflow("wf-identity").await;

        Ctx::scope(ctx(&dbos, "wf-identity"), async {
            let first = step("first", || async { Ok::<_, crate::Error>(1u32) });
            let second = step("second", || async { Ok::<_, crate::Error>(2u32) });
            assert_eq!((first.name(), first.step_id()), ("first", Some(0)));
            assert_eq!((second.name(), second.step_id()), ("second", Some(1)));
            // Awaited so neither reservation is burned, which is what `#[must_use]` is about.
            let _ = (first.await, second.await);
        })
        .await;

        // **The name is owned, not borrowed.** `Arc<str>` is an owned allocation — `Arc` has no
        // lifetime parameter — so `Arc::from(&str)` copies. Building from a `String` that is
        // dropped before the step is awaited is the demonstration: if the name borrowed, this
        // would not compile, and the step's `'a` comes from its body rather than from the name.
        let owned = {
            let temporary = String::from("built-from-a-temporary");
            let s = step(&temporary, || async { Ok::<_, crate::Error>(4u32) });
            drop(temporary);
            s
        };
        assert_eq!(owned.name(), "built-from-a-temporary");
        assert_eq!(owned.await.unwrap(), 4);

        // Outside a workflow there is no checkpoint to make, so no id is claimed.
        let plain = step("plain", || async { Ok::<_, crate::Error>(3u32) });
        assert_eq!(plain.step_id(), None);
        assert_eq!(plain.name(), "plain");
        assert_eq!(plain.await.unwrap(), 3);

        dbos.shutdown().await;
    }

    /// **The check must not fire on legitimate concurrent siblings**, which is the whole reason it
    /// compares workflow identity and the per-call-stack step marker rather than anything shared.
    ///
    /// A workflow-wide flag would be set by the first branch's running body while the second is
    /// still being first-polled, and a check that consulted it would refuse the very thing the
    /// eager id exists to permit — so this races two steps whose bodies overlap and asserts both
    /// are checkpointed.
    #[tokio::test]
    async fn overlapping_siblings_are_not_mistaken_for_foreign_steps() {
        let (dbos, _db) = workflow("wf-siblings").await;

        let outcomes = Ctx::scope(ctx(&dbos, "wf-siblings"), async {
            // Both sleep, so each is polled while the other's body is suspended mid-flight.
            let a = step("a", || async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok::<_, crate::Error>(1u32)
            });
            let b = step("b", || async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok::<_, crate::Error>(2u32)
            });
            tokio::join!(a, b)
        })
        .await;

        assert_eq!(outcomes.0.unwrap(), 1, "the first sibling was refused");
        assert_eq!(outcomes.1.unwrap(), 2, "the second sibling was refused");
        assert_eq!(
            steps(&dbos, "wf-siblings").await,
            [(0, "a".to_owned()), (1, "b".to_owned())],
            "both siblings checkpoint, under the ids they were built with"
        );

        dbos.shutdown().await;
    }

    /// **A step built while a sibling's body is in flight still takes an id.** Build A, poll A,
    /// then build B once A's body has started: a workflow-wide in-step flag would read A's body as
    /// B's own, classify B as nested, and let it run plainly with no checkpoint. The marker is per
    /// call stack, so the workflow proper is still the workflow proper.
    #[tokio::test]
    async fn a_step_built_while_a_sibling_runs_is_still_checkpointed() {
        let (dbos, _db) = workflow("wf-built-mid-flight").await;

        let outcomes = Ctx::scope(ctx(&dbos, "wf-built-mid-flight"), async {
            let (started, mut started_rx) = tokio::sync::oneshot::channel::<()>();
            let mut started = Some(started);
            let a = step("a", move || {
                let started = started.take();
                async move {
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok::<_, crate::Error>(1u32)
                }
            });
            let mut a = std::pin::pin!(a);
            // Poll A until its body has started and parked on its sleep.
            let a_result = tokio::select! {
                result = &mut a => Some(result),
                _ = &mut started_rx => None,
            };
            assert!(
                a_result.is_none(),
                "A must still be in flight when B is built"
            );

            let b = step("b", || async { Ok::<_, crate::Error>(2u32) });
            assert_eq!(
                b.step_id(),
                Some(1),
                "built in the workflow proper, so it takes an id"
            );
            tokio::join!(a, b)
        })
        .await;

        assert_eq!(outcomes.0.unwrap(), 1);
        assert_eq!(outcomes.1.unwrap(), 2);
        assert_eq!(
            steps(&dbos, "wf-built-mid-flight").await,
            [(0, "a".to_owned()), (1, "b".to_owned())],
            "both checkpoint: a running sibling does not make the workflow proper look nested"
        );

        dbos.shutdown().await;
    }

    /// Built outside a workflow, polled inside one: the `Ctx::scope(ctx, step(..))` trap, now loud.
    #[tokio::test]
    async fn a_step_built_outside_and_polled_inside_is_refused() {
        let (dbos, _db) = workflow("wf-smuggled-in").await;

        // Built here, with no workflow around it, so it claimed no id.
        let orphan = step("orphan", || async { Ok::<_, crate::Error>(1u32) });
        assert_eq!(orphan.step_id(), None);

        let refused = Ctx::scope(ctx(&dbos, "wf-smuggled-in"), orphan).await;
        match refused {
            Err(crate::Error::StepBuiltElsewhere {
                step,
                built,
                polled,
            }) => {
                assert_eq!(step, "orphan");
                assert_eq!(built, "outside a workflow");
                assert_eq!(polled, "in workflow wf-smuggled-in");
            }
            other => panic!("expected a built-elsewhere refusal, got {other:?}"),
        }

        assert!(
            steps(&dbos, "wf-smuggled-in").await.is_empty(),
            "a refused step records nothing"
        );

        dbos.shutdown().await;
    }

    /// **The gap the scope id closes.** A step built inside another step's body takes no id by
    /// the leaf rule; carried out of that body and awaited in the workflow proper it would run
    /// undurably, where the caller plainly expected a checkpoint.
    ///
    /// Both places have the same workflow id, so only the per-body scope tells them apart.
    #[tokio::test]
    async fn a_step_built_inside_a_step_and_awaited_outside_it_is_refused() {
        let (dbos, _db) = workflow("wf-carried-out").await;

        // The inner step cannot be the outer one's *result* — a step's output must serialize — so
        // it leaves the body the way a real mistake would, through a slot the body can reach.
        type Slot = Arc<std::sync::Mutex<Option<crate::Pending<'static, u32, crate::EngineOnly>>>>;
        let smuggled: Slot = Arc::new(std::sync::Mutex::new(None));

        Ctx::scope(ctx(&dbos, "wf-carried-out"), {
            let smuggled = Arc::clone(&smuggled);
            async move {
                step("outer", move || {
                    let smuggled = Arc::clone(&smuggled);
                    async move {
                        *smuggled.lock().unwrap() =
                            Some(step("inner", || async { Ok::<_, crate::Error>(1u32) }));
                        Ok::<_, crate::Error>(0u32)
                    }
                })
                .await
            }
        })
        .await
        .expect("the outer step failed");

        let carried = smuggled.lock().unwrap().take().expect("built in the body");
        // It took no id, being nested — that part is the leaf rule working correctly.
        assert_eq!(carried.step_id(), None);

        let refused = Ctx::scope(ctx(&dbos, "wf-carried-out"), carried).await;
        match refused {
            Err(crate::Error::StepBuiltElsewhere {
                step,
                built,
                polled,
            }) => {
                assert_eq!(step, "inner");
                assert_eq!(built, "inside a step of workflow wf-carried-out");
                assert_eq!(polled, "in workflow wf-carried-out");
            }
            other => panic!("expected a built-elsewhere refusal, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// The other half of the same rule: a step claimed in the workflow proper and carried *into*
    /// a step body would checkpoint beneath a step whose own row already covers whatever its body
    /// did.
    #[tokio::test]
    async fn a_step_carried_into_a_step_body_is_refused() {
        let (dbos, _db) = workflow("wf-carried-in").await;

        let inside = Ctx::scope(ctx(&dbos, "wf-carried-in"), async {
            let claimed = step("claimed", || async { Ok::<_, crate::Error>(1u32) });
            assert_eq!(claimed.step_id(), Some(0), "claimed at a step boundary");

            // Handed into another step's body and awaited there instead of where it was built.
            let mut carried = Some(claimed);
            step("outer", move || {
                let taken = carried.take();
                async move {
                    match taken {
                        Some(inner) => inner.await,
                        None => Ok::<_, crate::Error>(0u32),
                    }
                }
            })
            .await
        })
        .await;

        match inside {
            Err(crate::Error::StepBuiltElsewhere {
                step,
                built,
                polled,
            }) => {
                assert_eq!(step, "claimed");
                assert_eq!(built, "in workflow wf-carried-in");
                assert_eq!(polled, "inside a step of workflow wf-carried-in");
            }
            other => panic!("expected a built-elsewhere refusal, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// Built in one workflow, polled in another: the id names a position in the first.
    #[tokio::test]
    async fn a_step_built_in_another_workflow_is_refused() {
        let (dbos, _db) = workflow("wf-donor").await;
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .init_workflow(&NewWorkflow::new("wf-thief"), None, Submission::Fresh)
            .await
            .expect("could not create the second workflow row");

        // Built in the donor, and never awaited there — which is what makes its id a claim nobody
        // honours. Yielding the un-awaited step out of the scope is exactly the smuggling under
        // test, so the lint against it is the thing being demonstrated.
        #[allow(clippy::async_yields_async)]
        let smuggled = Ctx::scope(ctx(&dbos, "wf-donor"), async {
            step("borrowed", || async { Ok::<_, crate::Error>(7u32) })
        })
        .await;
        assert_eq!(smuggled.step_id(), Some(0));

        let refused = Ctx::scope(ctx(&dbos, "wf-thief"), smuggled).await;
        match refused {
            Err(crate::Error::StepBuiltElsewhere {
                step,
                built,
                polled,
            }) => {
                assert_eq!(step, "borrowed");
                assert_eq!(built, "in workflow wf-donor");
                assert_eq!(polled, "in workflow wf-thief");
            }
            other => panic!("expected a built-elsewhere refusal, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// The macro over the core, behind the feature that re-exports it.
    ///
    /// Nested rather than gated test by test, because the split is worth stating once: above
    /// is what a durable race *is*, and here is what writing one looks like.
    #[cfg(feature = "macros")]
    mod races {
        use std::sync::atomic::{AtomicU32, Ordering};

        use super::*;

        /// A recorded winner that no longer exists is a changed shape, not a branch to take.
        ///
        /// The select has to land on the same step id both times, so the same three steps are
        /// built — only two of them are raced the second time.
        #[tokio::test]
        async fn a_recorded_winner_outside_the_current_set_is_reported() {
            let (dbos, _db) = workflow("wf-shrunk").await;

            let first: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
                crate::select_step! {
                    b = step("b", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(2u32)
                    }) => b?,
                    c = step("c", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(3u32)
                    }) => c?,
                    a = step("a", || async { Ok::<_, crate::Error>(1u32) }) => a?
                }
            })
            .await;
            assert_eq!(first.unwrap(), 1, "the branch built third finished first");

            let shrunk: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
                let _third = step("a", || async { Ok::<_, crate::Error>(1u32) });
                crate::select_step! {
                    b = step("b", || async { Ok::<_, crate::Error>(2u32) }) => b?,
                    c = step("c", || async { Ok::<_, crate::Error>(3u32) }) => c?
                }
            })
            .await;

            match shrunk {
                Err(crate::Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                    recorded,
                    ..
                })) => assert!(recorded.contains("branch 2"), "{recorded}"),
                other => panic!("a stale winner must be reported, got {other:?}"),
            }

            dbos.shutdown().await;
        }

        /// Outside a workflow the branches run plainly and nothing is recorded — which is what
        /// keeps a function built from steps ordinarily callable and ordinarily testable.
        #[tokio::test]
        async fn outside_a_workflow_the_race_is_plain() {
            let answer: crate::Result<u32> = async {
                crate::select_step! {
                    quick = step("quick", || async { Ok::<_, crate::Error>(1u32) }) => quick?,
                    slow = step("slow", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(2u32)
                    }) => slow?
                }
            }
            .await;
            assert_eq!(answer.unwrap(), 1);
        }

        /// The macro over the core: heterogeneous branches, typed arms, one arm run.
        ///
        /// The branches return different types — a `u32` and a `String` — which is the thing the
        /// `Vec`-taking core cannot express and the whole reason this is a macro.
        #[tokio::test]
        async fn the_macro_races_heterogeneous_branches_and_runs_one_arm() {
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
                                    tokio::time::sleep(Duration::from_millis(300)).await;
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

            // The loser was built — so it spent id 0 — and dropped without recording.
            assert_eq!(
                steps(&dbos, "wf-macro").await,
                [(1, "quick".to_owned()), (2, "DBOS.selectStep".to_owned())],
                "the winner keeps its build-order id, the loser records nothing"
            );

            dbos.shutdown().await;
        }

        /// A replay takes the same arm, and the loser is not run for the first time on the way
        /// through.
        #[tokio::test]
        async fn a_replayed_macro_race_takes_the_same_arm() {
            let (dbos, _db) = workflow("wf-macro-replay").await;
            let loser_ran = Arc::new(AtomicU32::new(0));

            let race = |loser_ran: Arc<AtomicU32>| async move {
                crate::select_step! {
                    quick = step("quick", || async { Ok::<_, crate::Error>(7u32) }) => {
                        format!("quick: {}", quick?)
                    },
                    slow = step("slow", {
                        let loser_ran = Arc::clone(&loser_ran);
                        move || {
                            let loser_ran = Arc::clone(&loser_ran);
                            async move {
                                loser_ran.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(300)).await;
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
                "the loser recorded nothing first time, so a replay must not run it now"
            );

            dbos.shutdown().await;
        }

        /// **No arity to run out of.** Ten branches, where the declarative form stopped at the
        /// last sum type somebody wrote — the macro names a slot per branch, so the only bound
        /// is patience.
        #[tokio::test]
        async fn a_race_wider_than_any_hand_written_arity() {
            let (dbos, _db) = workflow("wf-macro-wide").await;

            // Nine that sleep and one that does not, so the winner is unambiguous and last.
            let slow = |name: &'static str| {
                step(name, || async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Ok::<_, crate::Error>(0u32)
                })
            };

            let answer: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-macro-wide"), async {
                crate::select_step! {
                    a = slow("a") => a?,
                    b = slow("b") => b?,
                    c = slow("c") => c?,
                    d = slow("d") => d?,
                    e = slow("e") => e?,
                    f = slow("f") => f?,
                    g = slow("g") => g?,
                    h = slow("h") => h?,
                    i = slow("i") => i?,
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
        /// Run outside a workflow because what is under test is the grammar, not the checkpoint —
        /// this compiling at all is the assertion.
        #[tokio::test]
        async fn a_block_arm_ends_itself_like_a_match_arm() {
            let answer: crate::Result<u32> = async {
                crate::select_step! {
                    quick = step("quick", || async { Ok::<_, crate::Error>(1u32) }) => {
                        let won = quick?;
                        won + 10
                    }
                    slow = step("slow", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(2u32)
                    }) => match slow {
                        Ok(value) => value + 1,
                        Err(_) => 0,
                    }
                }
            }
            .await;
            assert_eq!(answer.unwrap(), 11);
        }

        /// **The race's error type comes from its branches, with nothing written down.**
        ///
        /// `Branches` is what carries it: the branches disagree about `T` — that is the point
        /// of a race — and agree about `E`, and pushing each one is what states the agreement.
        /// Without it the only thing tying the expansion's error type to the branches' would be
        /// a `?` inside an arm, so a race whose arms handle their own failures would need an
        /// annotation. No annotation here is the whole test.
        #[tokio::test]
        async fn the_error_type_is_inferred_from_the_branches() {
            let (dbos, _db) = workflow("wf-macro-inferred").await;

            let answer = Ctx::scope(ctx(&dbos, "wf-macro-inferred"), async {
                crate::select_step! {
                    counted = step("counted", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(1u32)
                    }) => counted.is_ok(),
                    named = step("named", || async {
                        Ok::<_, crate::Error>("hello".to_owned())
                    }) => named.is_ok(),
                }
            })
            .await;

            assert!(answer.unwrap(), "the branch that finished first succeeded");

            dbos.shutdown().await;
        }

        /// Three branches, to show the arity rules are not a two-branch special case.
        #[tokio::test]
        async fn the_macro_handles_more_than_two_branches() {
            let (dbos, _db) = workflow("wf-macro-three").await;

            let answer: crate::Result<u32> = Ctx::scope(ctx(&dbos, "wf-macro-three"), async {
                crate::select_step! {
                    a = step("a", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(1u32)
                    }) => a? + 100,
                    b = step("b", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>("b".to_owned())
                    }) => b.map(|_| 200u32)?,
                    c = step("c", || async { Ok::<_, crate::Error>(3u32) }) => c? + 300
                }
            })
            .await;

            assert_eq!(answer.unwrap(), 303, "the third branch finished first");

            dbos.shutdown().await;
        }
    }
}
