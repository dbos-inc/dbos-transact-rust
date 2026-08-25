//! Child workflows: a workflow started from inside another one, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{Outcome, WorkflowStatus};
use dbos::{Config, DBOS, Error, StartOptions, Timeout};

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
///
/// **A `run` costs two step ids** — the launch and the await — so consecutive children are `-0` and
/// `-2` rather than `-0` and `-1`. Every reference numbers them the same way, for the same reason:
/// one counter, and both halves take from it.
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
    for (step_id, expected) in [(0, "the-parent-0"), (2, "the-parent-2")] {
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
            // Two children launched and awaited, and the third launched: five steps, with the
            // third child's await still outstanding.
            if steps.len() >= 5 {
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
    assert_eq!(
        steps.len(),
        6,
        "a launch and an await per child, and no second set: {steps:?}"
    );
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

/// Awaiting a child is itself a step: the parent records what the child returned.
///
/// `DBOS.getResult` is the name all four implementations write, so a step listing reads the same
/// whichever SDK ran the parent. The row carries the child's own encoded output, not a re-encoding
/// of it, and the child id beside it.
#[tokio::test]
async fn awaiting_a_child_is_recorded_as_a_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("await-checkpoint-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(99) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move { child.run(()).await }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "records-its-await";
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
        99
    );

    let steps = reader(&db)
        .await
        .list_workflow_steps(id, true, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 2, "one launch, one await: {steps:?}");
    assert_eq!(steps[0].step_name, "child", "the launch");
    assert_eq!(steps[1].step_name, "DBOS.getResult", "the await");
    assert_eq!(steps[1].output.as_deref(), Some("99"));
    assert_eq!(
        steps[1].child_workflow_id.as_deref(),
        Some("records-its-await-0"),
        "the await says which child it was waiting on"
    );

    dbos.shutdown().await;
}

/// A replayed parent takes the recorded outcome instead of waiting on the child again.
///
/// The child is deleted out from under the recovered parent — a workflow that no longer exists
/// cannot be awaited, so finishing anyway is only possible from the recorded value.
#[tokio::test]
async fn a_replayed_parent_reads_the_recorded_outcome_rather_than_waiting_again() {
    let db = test_database().await;
    let id = "already-knows";
    let reader = reader(&db).await;

    // First process: the child finishes and its result is recorded, then the parent is killed
    // before it can finish.
    {
        let dbos = DBOS::new(config("replay-await-app", &db));
        let child = dbos
            .register_workflow("child", |()| async move { Ok::<u32, Error>(5) })
            .unwrap();
        let parent = dbos
            .register_workflow("parent", move |()| {
                let child = child.clone();
                async move {
                    let value = child.run(()).await?;
                    // Long enough that shutdown lands after the await is recorded.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok::<u32, Error>(value)
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

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let steps = reader
                .list_workflow_steps(id, false, None, None)
                .await
                .expect("read failed");
            if steps.len() == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the await was never recorded: {steps:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        dbos.shutdown().await;
    }

    // The child is gone. Only the parent's recorded copy of its result is left.
    reader
        .delete_workflows(&[&format!("{id}-0")], false)
        .await
        .expect("delete failed");
    assert!(
        reader
            .get_workflow(&format!("{id}-0"))
            .await
            .expect("read failed")
            .is_none(),
        "the child was deleted"
    );

    // Second process: recovery replays the parent, which must not go looking for the child.
    let dbos = DBOS::new(config("replay-await-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(5) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move { child.run(()).await }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    drop(parent);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let row = reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing");
        if row.status == WorkflowStatus::Success {
            assert_eq!(row.output.as_deref(), Some("5"), "from the recorded await");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replayed parent did not finish; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    dbos.shutdown().await;
}

/// A cancelled child is reported to its parent as an *awaited* cancellation, not as the parent
/// being cancelled — and that outcome is recorded like any other.
#[tokio::test]
async fn a_cancelled_child_is_an_awaited_cancellation_in_the_parent() {
    let db = test_database().await;
    let dbos = DBOS::new(config("cancelled-child-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, Error>(1)
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                // Its own budget, so the child cancels itself while the parent waits.
                child
                    .run_with(
                        (),
                        StartOptions {
                            timeout: Timeout::Explicit(Duration::from_millis(300)),
                            ..StartOptions::default()
                        },
                    )
                    .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "waits-on-a-doomed-child";
    let error = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect_err("the parent succeeded");
    match &error {
        Error::AwaitedWorkflowCancelled { workflow_id } => {
            assert_eq!(
                workflow_id, "waits-on-a-doomed-child-0",
                "the child, not the parent"
            );
        }
        other => panic!("expected an awaited cancellation, got {other:?}"),
    }

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(id, true, None, None)
        .await
        .expect("read failed");
    let await_step = steps
        .iter()
        .find(|step| step.step_name == "DBOS.getResult")
        .expect("the await was not recorded");
    let recorded = await_step.error.as_deref().expect("no error recorded");
    assert!(
        recorded.contains("AwaitedWorkflowCancelled"),
        "the row says the awaited workflow was cancelled: {recorded}"
    );

    // The parent failed with it rather than being cancelled itself.
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.status,
        WorkflowStatus::Error,
        "the parent failed; it was not itself cancelled"
    );

    dbos.shutdown().await;
}

/// A child with no budget of its own inherits its parent's deadline, as the same instant.
///
/// Not "the same duration again": the child's row carries the parent's deadline verbatim, so a
/// parent an hour into a two-hour budget hands its child one hour, not two.
#[tokio::test]
async fn a_child_inherits_its_parents_deadline() {
    let db = test_database().await;
    let dbos = DBOS::new(config("inherit-deadline-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move { child.run(()).await }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "hands-down-its-deadline";
    parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
            },
        )
        .await
        .expect("the parent failed");

    let reader = reader(&db).await;
    let parent_row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    let child_row = reader
        .get_workflow(&format!("{id}-0"))
        .await
        .expect("read failed")
        .expect("the child has no row");
    assert!(parent_row.deadline.is_some(), "the parent has a deadline");
    assert_eq!(
        child_row.deadline, parent_row.deadline,
        "the same instant, not a fresh budget"
    );
    assert!(
        child_row.timeout.is_none(),
        "the child was given no timeout of its own; it has a deadline because its parent had one"
    );

    dbos.shutdown().await;
}

/// A child's own timeout replaces the inherited deadline, even when it is the longer of the two.
///
/// Python and TypeScript carry this rule and its comment identically: *"If a timeout is explicitly
/// specified, use it over any propagated deadline"*. The visible consequence is that such a child
/// **outlives its parent** — which is the point of asking for a timeout on one specific child.
#[tokio::test]
async fn a_childs_own_timeout_replaces_the_inherited_deadline() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-timeout-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                child
                    .run_with(
                        (),
                        StartOptions {
                            // Longer than what the parent has left.
                            timeout: Timeout::Explicit(Duration::from_secs(3_600)),
                            ..StartOptions::default()
                        },
                    )
                    .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "child-asks-for-more";
    parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(60)),
            },
        )
        .await
        .expect("the parent failed");

    let reader = reader(&db).await;
    let parent_deadline = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing")
        .deadline
        .expect("the parent has a deadline");
    let child_row = reader
        .get_workflow(&format!("{id}-0"))
        .await
        .expect("read failed")
        .expect("the child has no row");
    assert!(
        child_row.deadline.expect("the child has a deadline") > parent_deadline,
        "the child's own timeout won: it outlives its parent"
    );
    assert_eq!(child_row.timeout, Some(Duration::from_secs(3_600)));

    dbos.shutdown().await;
}

/// A child can decline the inherited deadline outright, which no `Option<Duration>` could say.
///
/// [`Timeout::Inherit`] is a caller who said nothing and [`Timeout::None`] is a caller who decided,
/// and only the second detaches a child from a parent that has a budget. All four references carry
/// the same three states — Java's `Timeout.None`, a `null` timeout in TypeScript, Python's
/// `SetWorkflowTimeout(None)` — and each clears the propagated deadline rather than merely leaving
/// it unset.
///
/// The row is the assertion: no deadline, and no timeout either. A `workflow_timeout_ms` here
/// would be a budget nobody asked for, and it is the column a queue recomputes a deadline from.
#[tokio::test]
async fn a_child_can_decline_the_inherited_deadline() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-no-timeout-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                // Two children under one bounded parent: the first says nothing, the second
                // declines. Together they are the difference the enum exists for.
                let inherited = child.run(()).await?;
                let detached = child
                    .run_with(
                        (),
                        StartOptions {
                            timeout: Timeout::None,
                            ..StartOptions::default()
                        },
                    )
                    .await?;
                Ok::<u32, Error>(inherited + detached)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "one-child-opts-out";
    let total = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(total, 2, "both children ran");

    let reader = reader(&db).await;
    let parent_deadline = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing")
        .deadline
        .expect("the parent has a deadline");

    let inherited = reader
        .get_workflow(&format!("{id}-0"))
        .await
        .expect("read failed")
        .expect("the first child has no row");
    assert_eq!(
        inherited.deadline,
        Some(parent_deadline),
        "silence inherits the parent's instant verbatim"
    );

    let detached = reader
        .get_workflow(&format!("{id}-2"))
        .await
        .expect("read failed")
        .expect("the second child has no row");
    assert_eq!(
        detached.deadline, None,
        "Timeout::None declined the parent's deadline"
    );
    assert_eq!(
        detached.timeout, None,
        "and took no budget of its own in its place"
    );

    dbos.shutdown().await;
}

/// **The propagated deadline is the cascade.** A parent that runs out of time and the child that
/// inherited its deadline are cancelled at the same instant, independently.
///
/// Nothing signals the child. It holds the same instant, its own `select!` fires on it, and it
/// writes its own `CANCELLED`. That is why this slice adds no cancellation cascade: for deadlines
/// there is nothing left for one to do. (An explicit `cancel` with children is a different
/// question, and belongs with the management surface.)
#[tokio::test]
async fn a_parent_and_its_child_hit_an_inherited_deadline_independently() {
    let db = test_database().await;
    let dbos = DBOS::new(config("deadline-cascade-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, Error>(1)
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move { child.run(()).await }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "runs-out-of-time";
    let error = parent
        .run_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_millis(400)),
            },
        )
        .await
        .expect_err("the parent succeeded");
    assert!(
        matches!(error, Error::WorkflowCancelled { .. }),
        "the parent was cancelled by its own deadline, got {error:?}"
    );

    // The child cancels itself on the same instant, with nothing telling it to.
    let reader = reader(&db).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = reader
            .get_workflow(&format!("{id}-0"))
            .await
            .expect("read failed")
            .expect("the child has no row");
        if row.status == WorkflowStatus::Cancelled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the child was not cancelled by the deadline it inherited; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    dbos.shutdown().await;
}

/// A recorded await that belongs to a *different* workflow is refused, not adopted.
///
/// The other half of the launch position's own check, and the reason both exist: `check_step`
/// compares the step name, which leaves open whose outcome the row actually holds. For a child
/// the two cannot disagree — the handle's id came out of the launch row moments earlier — so this
/// plants the disagreement directly, which is what a parent awaiting a handle it did not itself
/// start could otherwise reach by changing its code.
#[tokio::test]
async fn a_recorded_await_of_another_workflow_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("stale-await-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();

    // The launch happens first, so its row is on disk before the gate; the await is what waits.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let gate = Arc::new(std::sync::Mutex::new(Some(rx)));
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            let gate = Arc::clone(&gate);
            async move {
                let handle = child.start(()).await.map_err(Error::lift)?;
                let rx = gate.lock().unwrap().take().expect("the parent runs once");
                let _ = rx.await;
                handle.result().await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "awaits-the-wrong-one";
    let handle = parent
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");

    let reader = reader(&db).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let steps = reader
            .list_workflow_steps(id, false, None, None)
            .await
            .expect("read failed");
        if !steps.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parent never recorded its launch"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // An await recorded at the position this parent is about to reach, naming a workflow that is
    // not the one it holds a handle to.
    reader
        .record_child_result(
            id,
            1,
            "somebody-elses-workflow",
            Outcome::Output(Some("7")),
            None,
            None,
        )
        .await
        .expect("planting the await failed");
    tx.send(()).expect("the parent is waiting on the gate");

    let error = handle.result().await.expect_err("the parent succeeded");
    match &error {
        Error::SystemDatabase(dbos::sysdb::Error::UnexpectedStep {
            step_id,
            expected,
            recorded,
            ..
        }) => {
            assert_eq!(*step_id, 1, "the await position");
            assert!(
                expected.contains("awaits-the-wrong-one-0"),
                "says which workflow it was awaiting: {expected}"
            );
            assert!(
                recorded.contains("somebody-elses-workflow"),
                "and whose outcome it found: {recorded}"
            );
        }
        other => panic!("expected an unexpected-step refusal, got {other:?}"),
    }

    dbos.shutdown().await;
}
