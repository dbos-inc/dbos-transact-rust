//! Child workflows: a workflow started from inside another one, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// The child's id is `{parent}-{step_id}`, its row records the parent, and the parent records the
/// launch as a step.
///
/// Zero-based, matching Go, TypeScript and Java: a parent's first child is `parent-0`. Python is
/// the one-based outlier, which is the step-numbering inconsistency showing up in an id.
#[tokio::test]
async fn a_child_is_named_for_its_parent_and_the_step_that_started_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-id-app", &db));
    let child = dbos
        .register_workflow("child", |n: u32| async move { Ok::<u32, Error>(n * 2) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                let first = child.run(21).await?;
                let second = child.run(50).await?;
                Ok::<u32, Error>(first + second)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "the-parent";
    let total = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(total, 142, "both children ran");

    let reader = reader(&db).await;
    for (step_id, expected) in [(0, "the-parent-0"), (1, "the-parent-1")] {
        let row = reader
            .get_workflow(expected)
            .await
            .expect("read failed")
            .unwrap_or_else(|| panic!("no row for {expected}"));
        assert_eq!(row.status, WorkflowStatus::Success);
        assert_eq!(
            row.parent_workflow_id.as_deref(),
            Some(id),
            "the child's row points back at its parent"
        );
        let steps = reader
            .list_workflow_steps(id, false, None, None)
            .await
            .expect("read failed");
        let step = steps
            .iter()
            .find(|step| step.step_id == step_id)
            .unwrap_or_else(|| panic!("no step {step_id} on the parent"));
        assert_eq!(step.step_name, "child");
        assert_eq!(
            step.child_workflow_id.as_deref(),
            Some(expected),
            "the launch is recorded against the parent"
        );
    }

    // The relationship is queryable from the parent's side too.
    let children = reader.get_workflow_children(id).await.expect("read failed");
    assert_eq!(children.len(), 2, "two children: {children:?}");

    dbos.shutdown().await;
}

/// **The acceptance test.** A parent that dies between its children adopts the ones it already
/// started rather than starting a second set.
///
/// The first process starts two of three children and is killed by shutdown, leaving the parent
/// PENDING. The second process recovers it, and the deterministic id is what makes the launches
/// idempotent: the recovered parent re-derives `parent-0` and `parent-1`, finds them recorded, and
/// runs only the third child's body.
#[tokio::test]
async fn a_recovered_parent_adopts_the_children_it_already_started() {
    let db = test_database().await;
    let id = "crashes-between-children";

    // How many times a child body actually ran, across both processes.
    let bodies = Arc::new(AtomicU32::new(0));

    // First process: two children finish, the third blocks until shutdown kills the parent.
    {
        let dbos = DBOS::new(config("recover-children-app", &db));
        let bodies = Arc::clone(&bodies);
        let child = dbos
            .register_workflow("child", move |n: u32| {
                let bodies = Arc::clone(&bodies);
                async move {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    if n == 2 {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Ok::<u32, Error>(n)
                }
            })
            .unwrap();
        let parent = dbos
            .register_workflow("parent", move |()| {
                let child = child.clone();
                async move {
                    let mut total = 0;
                    for n in 0..3 {
                        total += child.run(n).await?;
                    }
                    Ok::<u32, Error>(total)
                }
            })
            .unwrap();
        dbos.launch().await.expect("launch failed");
        parent
            .start_with(
                (),
                StartOptions {
                    workflow_id: Some(id),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("start failed");

        // Wait until the first two children are recorded, so the crash lands mid-parent.
        let reader = reader(&db).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let steps = reader
                .list_workflow_steps(id, false, None, None)
                .await
                .expect("read failed");
            if steps.len() >= 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the parent never got as far as its third child; steps: {steps:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        dbos.shutdown().await;
    }

    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Pending,
        "shutdown left the parent for recovery"
    );
    let ran_before = bodies.load(Ordering::SeqCst);
    assert_eq!(
        ran_before, 3,
        "three bodies entered, the third still blocked"
    );

    // Second process: recovery re-runs the parent, which must adopt rather than re-launch.
    let dbos = DBOS::new(config("recover-children-app", &db));
    let recovered_bodies = Arc::clone(&bodies);
    let child = dbos
        .register_workflow("child", move |n: u32| {
            let bodies = Arc::clone(&recovered_bodies);
            async move {
                bodies.fetch_add(1, Ordering::SeqCst);
                Ok::<u32, Error>(n)
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                let mut total = 0;
                for n in 0..3 {
                    total += child.run(n).await?;
                }
                Ok::<u32, Error>(total)
            }
        })
        .unwrap();
    // Registered so recovery can find it; nothing here starts it, recovery does.
    dbos.launch().await.expect("launch failed");
    drop(parent);

    // The parent finishes, and its children are the same three rows.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let row = reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing");
        if row.status == WorkflowStatus::Success {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the recovered parent did not finish; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let children = reader.get_workflow_children(id).await.expect("read failed");
    assert_eq!(
        children.len(),
        3,
        "the recovered parent started no fourth child: {children:?}"
    );
    let steps = reader
        .list_workflow_steps(id, false, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 3, "one launch per child: {steps:?}");
    assert_eq!(
        bodies.load(Ordering::SeqCst),
        ran_before + 1,
        "only the unfinished child ran again: the two that succeeded were adopted, not re-run"
    );

    dbos.shutdown().await;
}

/// The fan-out shape: children are launched one at a time and awaited one at a time, but they
/// **run** concurrently.
///
/// Sequential bookkeeping is the step counter's determinism constraint reaching a second caller —
/// each launch and each await allocates a step id, and concurrent allocation would replay against
/// the wrong slots. It costs nothing in wall-clock: three children that each sleep are all in
/// flight together, so the parent takes about as long as the slowest rather than the sum. A
/// `join!` over the *launches* is the same trap a `join!` over steps is, and is unsound for the
/// same reason.
#[tokio::test]
async fn children_launched_in_a_loop_run_concurrently() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fan-out-app", &db));
    let child = dbos
        .register_workflow("child", |n: u32| async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok::<u32, Error>(n)
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                // Launch all three first — each `start` returns as soon as the child is recorded
                // and spawned — then collect. Awaiting inside the first loop would serialize them.
                let mut handles = Vec::new();
                for n in 0..3 {
                    handles.push(child.start(n).await.map_err(Error::lift)?);
                }
                let mut total = 0;
                for handle in handles {
                    total += handle.result().await?;
                }
                Ok::<u32, Error>(total)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let began = std::time::Instant::now();
    let total = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some("fans-out"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    let took = began.elapsed();

    assert_eq!(total, 3, "0 + 1 + 2");
    assert!(
        took < Duration::from_millis(1_200),
        "three 400ms children took {took:?}, which is serial rather than concurrent"
    );

    let children = reader(&db)
        .await
        .get_workflow_children("fans-out")
        .await
        .expect("read failed");
    assert_eq!(children.len(), 3);

    dbos.shutdown().await;
}

/// A child started and never awaited is still recorded, so a recovered parent adopts it.
///
/// The launch row is what makes a child adoptable, and it is written by `start` alone — nothing
/// about awaiting the handle is what records the relationship.
#[tokio::test]
async fn a_child_that_is_never_awaited_is_still_recorded() {
    let db = test_database().await;
    let dbos = DBOS::new(config("unawaited-child-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                // Started, and the handle dropped without ever being awaited.
                child.start(()).await.map_err(Error::lift)?;
                Ok::<u32, Error>(0)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "forgets-its-child";
    parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the parent failed");

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(id, false, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "the launch is recorded: {steps:?}");
    assert_eq!(
        steps[0].child_workflow_id.as_deref(),
        Some("forgets-its-child-0")
    );

    // The child outlives the parent's interest in it and finishes on its own.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = reader
            .get_workflow("forgets-its-child-0")
            .await
            .expect("read failed")
            .expect("the child has no row");
        if row.status == WorkflowStatus::Success {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the abandoned child never finished; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    dbos.shutdown().await;
}

/// A child cannot be started from inside a step, in every implementation.
///
/// A step is a leaf, and an id-allocating call inside one shifts every later step onto the wrong
/// replay slot. There is no plain version of a durable launch to degrade to, so this is an error
/// rather than the quiet fallback a nested *step* gets.
#[tokio::test]
async fn a_child_cannot_be_started_from_inside_a_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-in-step-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                dbos::step::<u32, dbos::EngineOnly, _, _>("tries_to_spawn", || {
                    let child = child.clone();
                    async move { child.run(()).await }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let error = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some("spawns-in-a-step"),
                ..StartOptions::default()
            },
        )
        .await
        .expect_err("the parent succeeded");
    assert!(
        matches!(&error, Error::InsideStep { operation } if operation.contains("workflow")),
        "expected an inside-a-step refusal, got {error:?}"
    );

    dbos.shutdown().await;
}

/// An application-assigned id wins over the derivation, and the launch is still recorded.
#[tokio::test]
async fn an_assigned_child_id_wins_over_the_derived_one() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-assigned-id-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                child
                    .run_with(
                        (),
                        StartOptions {
                            workflow_id: Some("i-named-this-one"),
                            ..StartOptions::default()
                        },
                    )
                    .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "names-its-child";
    assert_eq!(
        parent
            .run_with(
                (),
                StartOptions {
                    workflow_id: Some(id),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("the parent failed"),
        7
    );

    let reader = reader(&db).await;
    let row = reader
        .get_workflow("i-named-this-one")
        .await
        .expect("read failed")
        .expect("the assigned id has no row");
    assert_eq!(row.parent_workflow_id.as_deref(), Some(id));
    assert!(
        reader
            .get_workflow("names-its-child-0")
            .await
            .expect("read failed")
            .is_none(),
        "the derived id was not used as well"
    );

    dbos.shutdown().await;
}

/// A workflow started outside any workflow is a root: no parent, and no launch recorded anywhere.
#[tokio::test]
async fn a_workflow_started_outside_a_workflow_has_no_parent() {
    let db = test_database().await;
    let dbos = DBOS::new(config("root-app", &db));
    let root = dbos
        .register_workflow("root", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "a-root";
    root.run_with(
        (),
        StartOptions {
            workflow_id: Some(id),
            ..StartOptions::default()
        },
    )
    .await
    .expect("the workflow failed");

    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert!(row.parent_workflow_id.is_none());

    dbos.shutdown().await;
}
