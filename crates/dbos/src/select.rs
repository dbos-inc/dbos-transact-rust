//! The durable race over steps: its checkpoint, and the poll loop the macro wraps.
//!
//! A workflow that races two steps has made a **choice**, and a choice a workflow makes has to be
//! recorded — a replay that raced again could see the other branch finish first and take a path
//! the first execution never took, which is the one thing a workflow may never do. That is the
//! whole reason this exists where [`join_steps`-shaped code](crate::step) needs nothing: an
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
//! [`begin`] and [`finish`] are ordinary async functions dealing only in **indices**. Everything
//! that touches [`Placement`], the system database, or serialization lives here, in code that can
//! be read and tested without macro hygiene in the way; what is left for the macro is a poll loop
//! over branches whose types differ. That split is also why [`select_indexed`] exists: it is the
//! same core driven by a homogeneous `Vec`, so the checkpoint half can be tested directly.
//!
//! [`select_indexed`] is deliberately **not public**. A `Vec` forces every branch to share `T` and
//! `E`, which a race almost never wants — "the fetch returned" and "the timeout fired" are
//! different types — and it answers with a position rather than running the winner's arm. It was
//! tried as a public surface and withdrawn for exactly that; it survives as the core's test seam.

// **Nothing calls this yet, and `select_step!` is what will.** The checkpoint half is built and
// tested first, deliberately: it is ordinary async code that can be read and exercised without
// macro hygiene in the way, where the macro over it is a poll loop across branches of differing
// type. This allow comes off in the same commit that adds the macro — if it is still here after
// that, something that was meant to have a caller does not.
#![allow(dead_code)]

use std::task::Poll;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::checkpoint::Placement;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::step::PendingStep;
use crate::sysdb::types::{Outcome, Timestamp, step_names};

/// What [`begin`] found: a winner already recorded, or a race still to run.
///
/// Two states rather than an `Option<Select>` plus a loose index, because the caller must handle
/// both and the type is what makes forgetting one a compile error.
pub(crate) enum Racing {
    /// This select already ran. Poll **only** this branch, which then replays from its own step
    /// row without running, and take its arm.
    Replay(usize),
    /// No checkpoint yet. Race the branches, then hand the winner to [`finish`].
    Fresh(Select),
}

/// A race in progress, carrying what [`finish`] needs to record it.
///
/// Holds the placement rather than re-deriving it, and the start instant rather than taking a
/// fresh one at the end: the recorded duration should cover the waiting, which is where every
/// other recorded call in this crate takes its `started_at`.
pub(crate) struct Select {
    ctx: Option<Ctx>,
    placement: Placement,
    started_at: Timestamp,
}

/// Claims this select's step id and asks whether it already has a winner.
///
/// **Called after every branch is built**, and that ordering is the contract rather than a
/// convenience: the branches take their ids as they are constructed, and the select's own id
/// follows them. A select that took its id first would leave its branches' ids one higher than the
/// replay expects.
///
/// `branches` is only used to check a recorded winner against the set that exists now. An empty
/// race never reaches here — the macro refuses it at compile time, which is the one refusal a
/// macro can make that the withdrawn function had to make at run time.
pub(crate) async fn begin<E: DurableError>(
    branches: &[(String, Option<i32>)],
) -> Result<Racing, E> {
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
        if winner >= branches.len() {
            let (workflow_id, step_id) = placement.step().unwrap_or(("", 0));
            // Names the branches this run actually built, which is the thing a reader has to
            // compare against the recorded index — "branch 2" alone says nothing about what the
            // code in front of them now does.
            let now = branches
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
                expected: format!("a select over {} branches — {now}", branches.len()),
                recorded: format!("a select won by branch {winner}, which no longer exists"),
            }));
        }
        tracing::debug!(winner, "replaying select_step; the same branch wins again");
        return Ok(Racing::Replay(winner));
    }

    Ok(Racing::Fresh(Select {
        ctx,
        placement,
        started_at,
    }))
}

/// Records which branch won, once the race has one.
///
/// **The position, not the branch's result.** The result is already recorded under the winning
/// step's own id, so writing it again here would store one outcome in two places and leave a
/// replay to decide which is true. What only this step knows is which branch won.
///
/// Outside a workflow this records nothing and the race was a plain one, which is the same
/// fall-through an ordinary step takes.
pub(crate) async fn finish<E: DurableError>(select: Select, winner: usize) -> Result<(), E> {
    let Select {
        ctx,
        placement,
        started_at,
    } = select;
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

/// The core driven by a homogeneous `Vec`, so that it can be tested without the macro.
///
/// Answers with the winning position and that branch's outcome. Not public, and the module doc
/// says why: a `Vec` forces one `T` on every branch and a position is not a handler.
pub(crate) async fn select_indexed<'a, T, E>(
    mut branches: Vec<PendingStep<'a, T, E>>,
) -> Result<(usize, Result<T, E>), E>
where
    T: Serialize + DeserializeOwned + Send + 'a,
    E: DurableError + Send + 'a,
{
    // Refused before the placement, so a call that cannot be answered does not move the step
    // counter: a workflow fixed to pass branches would otherwise replay onto a different slot than
    // it recorded. The macro makes this a compile error instead.
    if branches.is_empty() {
        return Err(Error::Config(
            "select_step was given no branches to choose between".to_owned(),
        ));
    }

    // Read before the race, because the losers are dropped once it is decided and a stale-winner
    // report has to be able to name a branch that is gone.
    let identities: Vec<_> = branches
        .iter()
        .map(|b| (b.name().to_owned(), b.step_id()))
        .collect();

    match begin(&identities).await? {
        // **Only the winner is polled.** The losers recorded nothing on the first execution, so
        // running them now would be running them for the first time — side effects the original
        // never had. The branches before it were still *built*, which is what keeps their ids
        // spent and every later id where the replay expects it.
        Racing::Replay(winner) => {
            tracing::debug!(
                winner,
                step = %identities[winner].0,
                "replaying select_step; only the winning branch is polled"
            );
            let outcome = branches.swap_remove(winner).await;
            Ok((winner, outcome))
        }
        Racing::Fresh(select) => {
            let mut running: Vec<_> = branches
                .into_iter()
                .map(PendingStep::into_running)
                .collect();
            let (winner, outcome) = std::future::poll_fn(|cx| {
                for (index, branch) in running.iter_mut().enumerate() {
                    // Returned from inside the loop, so nothing is polled after it went `Ready`.
                    // Fixed source order, not tokio's randomised one: fairness is exactly the
                    // property a replay cannot reproduce, so two branches ready in the same instant
                    // resolve to the earlier one — and a replay reads the winner rather than racing
                    // at all, which makes that bias a tie-break rather than something to depend on.
                    if let Poll::Ready(outcome) = branch.as_mut().poll(cx) {
                        return Poll::Ready((index, outcome));
                    }
                }
                Poll::Pending
            })
            .await;
            // Dropped before the checkpoint is written, so every loser is stopped at its next
            // suspension point and its destructors have run before anything records that the race
            // is over. Whatever a loser did before that, it did once and invisibly — the same trade
            // a step timeout makes, and what Go says of its own `Select`.
            drop(running);

            finish(select, winner).await?;
            Ok((winner, outcome))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
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

    /// **The claim S1 exists for.** The *second* branch finishes first, and the ids are still the
    /// order the branches were built in — which a design that allocated at the first poll passes
    /// only by luck.
    #[tokio::test]
    async fn a_race_keeps_the_ids_the_branches_were_built_with() {
        let (dbos, _db) = workflow("wf-order").await;

        let (winner, outcome) = Ctx::scope(ctx(&dbos, "wf-order"), async {
            // Built first, finishes last.
            let slow = step("slow", || async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<_, crate::Error>(1u32)
            });
            // Built second, finishes first.
            let quick = step("quick", || async { Ok::<_, crate::Error>(2u32) });
            select_indexed(vec![slow, quick]).await
        })
        .await
        .expect("the select failed");

        assert_eq!(winner, 1, "the second branch finished first");
        assert_eq!(outcome.unwrap(), 2);

        // **The hole at id 0 is the point.** `slow` was built first and so took id 0, then lost
        // and was dropped before it could record — so the rows skip straight to 1. That the winner
        // is at 1 rather than 0 is exactly the claim: the id came from where the branch was
        // *built*, not from the order the bodies finished in. A design that allocated at the first
        // poll would have put `quick` at 0.
        let rows = steps(&dbos, "wf-order").await;
        assert_eq!(
            rows,
            [(1, "quick".to_owned()), (2, "DBOS.selectStep".to_owned())],
            "the winner keeps its build-order id, the loser records nothing, and the select's own \
             id follows both"
        );

        dbos.shutdown().await;
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
    /// compares workflow identity and never the in-step flag.
    ///
    /// That flag is one `AtomicBool` for the workflow, so the first branch's running body sets it
    /// while the second is still being first-polled. A check that consulted it would refuse the
    /// very thing the eager id exists to permit — so this races two steps whose bodies overlap and
    /// asserts both are checkpointed.
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
        type Slot =
            Arc<std::sync::Mutex<Option<crate::PendingStep<'static, u32, crate::EngineOnly>>>>;
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

    /// A replay takes the branch it recorded, and never runs the loser.
    #[tokio::test]
    async fn a_replayed_race_reuses_the_winner_and_leaves_the_loser_alone() {
        let (dbos, _db) = workflow("wf-replay").await;
        let loser_ran = Arc::new(AtomicU32::new(0));

        let race = |loser_ran: Arc<AtomicU32>| async move {
            let quick = step("quick", || async { Ok::<_, crate::Error>(7u32) });
            let slow = step("slow", {
                let loser_ran = loser_ran.clone();
                move || {
                    let loser_ran = loser_ran.clone();
                    async move {
                        loser_ran.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, crate::Error>(8u32)
                    }
                }
            });
            select_indexed(vec![quick, slow]).await
        };

        let (first, _) = Ctx::scope(ctx(&dbos, "wf-replay"), race(loser_ran.clone()))
            .await
            .expect("the select failed");
        assert_eq!(first, 0);
        let started_once = loser_ran.load(Ordering::SeqCst);

        // A fresh context over the same workflow id: step ids start again from zero.
        let (again, outcome) = Ctx::scope(ctx(&dbos, "wf-replay"), race(loser_ran.clone()))
            .await
            .expect("the select failed");
        assert_eq!(again, 0, "the replay takes the branch it recorded");
        assert_eq!(
            outcome.unwrap(),
            7,
            "and that branch replays from its own row"
        );
        assert_eq!(
            loser_ran.load(Ordering::SeqCst),
            started_once,
            "the loser recorded nothing on the first run, so a replay must not run it for the \
             first time now"
        );

        dbos.shutdown().await;
    }

    /// A recorded winner that no longer exists is a changed shape, not a branch to take.
    #[tokio::test]
    async fn a_recorded_winner_outside_the_current_set_is_reported() {
        let (dbos, _db) = workflow("wf-shrunk").await;

        let (winner, _) = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
            let b = step("b", || async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<_, crate::Error>(2u32)
            });
            let c = step("c", || async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<_, crate::Error>(3u32)
            });
            let a = step("a", || async { Ok::<_, crate::Error>(1u32) });
            select_indexed(vec![b, c, a]).await
        })
        .await
        .expect("the select failed");
        assert_eq!(winner, 2, "the branch built third finished first");

        // The same three steps are built, so the select lands on the same id — but only two of
        // them are raced, so the recorded winner names a branch that is no longer there.
        let shrunk = Ctx::scope(ctx(&dbos, "wf-shrunk"), async {
            let b = step("b", || async { Ok::<_, crate::Error>(2u32) });
            let c = step("c", || async { Ok::<_, crate::Error>(3u32) });
            let _dropped = step("a", || async { Ok::<_, crate::Error>(1u32) });
            select_indexed(vec![b, c]).await
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

    /// Nothing to choose between has no answer it could ever give.
    #[tokio::test]
    async fn an_empty_race_is_refused_and_spends_no_id() {
        let (dbos, _db) = workflow("wf-empty").await;

        let refused = Ctx::scope(ctx(&dbos, "wf-empty"), async {
            let empty: Vec<crate::PendingStep<'_, u32, crate::Error>> = Vec::new();
            select_indexed(empty).await
        })
        .await;
        assert!(matches!(refused, Err(crate::Error::Config(_))));

        assert!(
            steps(&dbos, "wf-empty").await.is_empty(),
            "a refused select must not move the step counter"
        );

        dbos.shutdown().await;
    }

    /// Outside a workflow the branches run plainly and nothing is recorded.
    #[tokio::test]
    async fn outside_a_workflow_the_race_is_plain() {
        let quick = step("quick", || async { Ok::<_, crate::Error>(1u32) });
        let slow = step("slow", || async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, crate::Error>(2u32)
        });
        let (winner, outcome) = select_indexed(vec![quick, slow])
            .await
            .expect("the select failed");
        assert_eq!(winner, 0);
        assert_eq!(outcome.unwrap(), 1);
    }
}
