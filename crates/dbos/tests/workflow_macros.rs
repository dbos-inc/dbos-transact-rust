//! The macro forms of the two waits, against real databases.
//!
//! What these have to show is not that the wait works — [`waits.rs`](waits) covers that — but that
//! wrapping it in a macro records nothing extra and awaits nothing extra. So every test here reads
//! the parent's step rows back and asserts the whole sequence: the launches, the one wait, and
//! exactly the `DBOS.getResult`s the shape calls for. A macro that quietly awaited a loser, or
//! checkpointed itself, would show up there and nowhere else.

use std::sync::Arc;
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::{Client, ClientConfig, Config, DBOS, Error, ForkFrom, WorkflowHandle, WorkflowRef};

use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        // Short, because every test here is waiting on a poll loop.
        outcome_poll_interval: Some(Duration::from_millis(50)),
        ..Config::new(app_name, db.url())
    }
}

/// A second handle on the database, for reading what a call actually wrote.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// The parent's steps, as `(id, name)` pairs in order.
async fn steps(reader: &PostgresSystemDatabase, workflow_id: &str) -> Vec<(i32, String)> {
    reader
        .list_workflow_steps(workflow_id, true, None, None, None)
        .await
        .expect("read failed")
        .into_iter()
        .map(|step| (step.step_id, step.step_name))
        .collect()
}

/// The three workflows the select tests share: a child that finishes at once, a child held at a
/// gate the test controls, and a parent that races them with the macro.
///
/// The race is decided by construction rather than by timing — the loser cannot settle until the
/// test releases it — so a run that answered the other way would be a real failure and not a slow
/// machine.
fn register_race(dbos: &DBOS, gate: &Arc<tokio::sync::Notify>) -> WorkflowRef<(), String> {
    let counter = dbos
        .register_workflow("counter", |n: u32| async move { Ok::<_, Error>(n + 1) })
        .unwrap();
    let namer = dbos
        .register_workflow("namer", {
            let gate = Arc::clone(gate);
            move |()| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.notified().await;
                    Ok::<_, Error>("namer".to_owned())
                }
            }
        })
        .unwrap();
    dbos.register_workflow("parent", {
        move |()| {
            let (counter, namer) = (counter.clone(), namer.clone());
            async move {
                let count = counter.start(3).await?;
                let name = namer.start(()).await?;
                // Annotated where every workflow body in the suite is: a closure's error type has
                // nothing else to pin it, and the `?`s in the arms need it known.
                let won = dbos::select_workflow! {
                    n = count => format!("counter: {}", n?),
                    s = name => format!("namer: {}", s?),
                }?;
                Ok::<_, Error>(won)
            }
        }
    })
    .unwrap()
}

/// The step rows a completed race leaves behind: two launches, the wait the macro expands to, and
/// the winner's result alone.
fn race_steps() -> [(i32, String); 4] {
    [
        (0, "counter".to_owned()),
        (1, "namer".to_owned()),
        (2, "DBOS.selectWorkflow".to_owned()),
        (3, "DBOS.getResult".to_owned()),
    ]
}

/// The winning arm runs, with the winner's own typed result, and the loser is never awaited.
///
/// The two children return different types, which is the whole reason the macro exists: a set of
/// ids has no one type to hand back, where a fixed list of handles has one per branch.
///
/// The step rows are what rule out a macro that quietly awaited the loser as well — there would be
/// a second `DBOS.getResult` — or that checkpointed itself on top of the wait it expands to.
#[tokio::test]
async fn select_workflow_runs_the_winners_arm_and_awaits_nobody_else() {
    let db = test_database().await;
    let dbos = DBOS::new(config("select-macro-app", &db));
    let reader = reader(&db).await;

    let gate = Arc::new(tokio::sync::Notify::new());
    let parent = register_race(&dbos, &gate);
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    let won = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the parent never finished")
        .expect("the parent failed");
    assert_eq!(
        won, "counter: 4",
        "the arm that ran is the winner's, and it holds the winner's own typed result"
    );

    assert_eq!(
        steps(&reader, &workflow_id).await,
        race_steps(),
        "the two launches, the wait the macro expands to, and the winner's result alone"
    );

    // Released only now, which is what makes the answer above decided rather than raced: the
    // loser could not have settled first however slowly the winner ran.
    gate.notify_waiters();
    dbos.shutdown().await;
}

/// A replay takes the arm it took the first time, because the winner is read back rather than
/// raced for again.
///
/// Forked from the `DBOS.getResult`, so the two launches and the `DBOS.selectWorkflow` all replay
/// from their rows and only the arm's own await runs afresh. The loser is still sitting at its gate
/// while this happens, which is the sharper half of the claim: a fork that re-raced would have to
/// wait for it, and this one does not wait at all.
#[tokio::test]
async fn a_replayed_select_takes_the_same_arm() {
    let db = test_database().await;
    let dbos = DBOS::new(config("select-replay-app", &db));
    let reader = reader(&db).await;

    let gate = Arc::new(tokio::sync::Notify::new());
    let parent = register_race(&dbos, &gate);
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    let won = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the parent never finished")
        .expect("the parent failed");

    let forked: WorkflowHandle<String> = dbos
        .fork(&workflow_id, ForkFrom::Step(3))
        .await
        .expect("fork failed");
    let forked_id = forked.workflow_id().to_owned();
    let again = tokio::time::timeout(DEADLINE, forked.result())
        .await
        .expect("the fork never finished")
        .expect("the fork failed");
    assert_eq!(again, won, "the replay took the arm the run took");

    assert_eq!(
        steps(&reader, &forked_id).await,
        race_steps(),
        "the same sequence, with the launches and the wait inherited"
    );

    gate.notify_waiters();
    dbos.shutdown().await;
}

/// Every result comes back, typed, in the order the handles were written.
///
/// The step rows are the claim: nothing for the wait itself, which records nowhere, and then one
/// `DBOS.getResult` per handle in source order. Those reads are what carries the durability — each
/// records the outcome the parent goes on to use — and sequential is not a concession, since the
/// set is already settled by the time the first one is read.
#[tokio::test]
async fn join_workflows_returns_every_result_in_source_order() {
    let db = test_database().await;
    let dbos = DBOS::new(config("join-macro-app", &db));
    let reader = reader(&db).await;

    let counter = dbos
        .register_workflow("counter", |n: u32| async move { Ok::<_, Error>(n + 1) })
        .unwrap();
    let namer = dbos
        .register_workflow(
            "namer",
            |()| async move { Ok::<_, Error>("namer".to_owned()) },
        )
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let (counter, namer) = (counter.clone(), namer.clone());
            move |()| {
                let (counter, namer) = (counter.clone(), namer.clone());
                async move {
                    let count = counter.start(3).await?;
                    let name = namer.start(()).await?;
                    let (count, name) = dbos::join_workflows!(count, name)?;
                    Ok::<_, Error>(format!("{name} counted to {count}"))
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    let both = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the parent never finished")
        .expect("the parent failed");
    assert_eq!(
        both, "namer counted to 4",
        "both results come back, each with its own type"
    );

    assert_eq!(
        steps(&reader, &workflow_id).await,
        [
            (0, "counter".to_owned()),
            (1, "namer".to_owned()),
            (2, "DBOS.getResult".to_owned()),
            (3, "DBOS.getResult".to_owned()),
        ],
        "the two launches and a result read per handle in source order, the wait recording nothing"
    );

    dbos.shutdown().await;
}

/// An instance before the semicolon is the outside-a-workflow form, and it works for anything that
/// has the two waits on it.
///
/// Every combination runs here — each macro against a [`DBOS`] and against a [`Client`] — because
/// the claim being made is that the expansion names the *method* rather than a type. One test
/// rather than four, since all of them need the same pair of connections and the same children.
///
/// This is the case the bare form reports `NotInWorkflow` for: there is no ambient context
/// anywhere in it.
#[tokio::test]
async fn an_instance_before_the_semicolon_waits_from_outside_a_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("instance-macro-app", &db));

    let counter = dbos
        .register_workflow("counter", |n: u32| async move { Ok::<_, Error>(n + 1) })
        .unwrap();
    let labeler = dbos
        .register_workflow("labeler", |()| async move {
            Ok::<_, Error>("labeler".to_owned())
        })
        .unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    let namer = dbos
        .register_workflow("namer", {
            let gate = Arc::clone(&gate);
            move |()| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.notified().await;
                    Ok::<_, Error>("namer".to_owned())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let client = Client::connect(ClientConfig {
        app_name: Some("instance-macro-app".to_owned()),
        outcome_poll_interval: Some(Duration::from_millis(50)),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");

    // Every race here is decided by construction: the losing child sits at a gate this test does
    // not open until both races have already answered.
    let count = counter.start(3).await.expect("start failed");
    let name = namer.start(()).await.expect("start failed");
    // Annotated because `lift` leaves the engine's channel free to be any application's: inside a
    // workflow the body's own return type pins it, and out here nothing else does.
    let won: dbos::Result<String> = dbos::select_workflow! { &dbos;
        n = count => format!("counter: {}", n.expect("the counter failed")),
        s = name => format!("namer: {}", s.expect("the namer failed")),
    };
    assert_eq!(
        won.expect("the instance's race failed"),
        "counter: 4",
        "the instance form answers with the winner's own result, as the ambient form does"
    );

    let count = counter.start(4).await.expect("start failed");
    let name = namer.start(()).await.expect("start failed");
    let won: dbos::Result<String> = dbos::select_workflow! { client;
        n = count => format!("counter: {}", n.expect("the counter failed")),
        s = name => format!("namer: {}", s.expect("the namer failed")),
    };
    assert_eq!(
        won.expect("the client's race failed"),
        "counter: 5",
        "a client races the same way, on the same expansion"
    );

    // Both losers are still at the gate, which is what made the two answers above decided.
    gate.notify_waiters();

    let count = counter.start(9).await.expect("start failed");
    let label = labeler.start(()).await.expect("start failed");
    let joined: dbos::Result<(u32, String)> = tokio::time::timeout(DEADLINE, async {
        dbos::join_workflows!(client; count, label)
    })
    .await
    .expect("the client's join never resolved");
    assert_eq!(
        joined.expect("the client's join failed"),
        (10, "labeler".to_owned())
    );

    let count = counter.start(19).await.expect("start failed");
    let label = labeler.start(()).await.expect("start failed");
    let joined: dbos::Result<(u32, String)> = tokio::time::timeout(DEADLINE, async {
        dbos::join_workflows!(&dbos; count, label)
    })
    .await
    .expect("the instance's join never resolved");
    assert_eq!(
        joined.expect("the instance's join failed"),
        (20, "labeler".to_owned())
    );

    dbos.shutdown().await;
}
