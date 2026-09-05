//! Child workflows: a workflow started from inside another one, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{Outcome, WorkflowStatus};
use dbos::{
    Config, DBOS, DuplicationPolicy, Enqueue, Error, QueueConflict, QueueOptions, RunOptions,
    StartOptions, Timeout, WorkflowRef,
};

use dbos_test_support::{TestDatabase, test_database};

/// The version each instance in this file launches with, derived from its application name.
///
/// DBOS computes none, so a launch without one fails, and it has to be stable across a relaunch of
/// the same application or the relaunch would recover nothing. It cannot simply be a shared
/// constant, though: `application_versions` still carries a global `UNIQUE (version_name)`, so two
/// differently-named applications sharing one database cannot both register `1.0.0` — see the
/// UPSTREAM notes on `resolve_owning_application`, and
/// `lifecycle::a_launch_that_fails_after_connecting_closes_the_database`, which contests a version
/// on purpose. Keying on the name gives both properties at once.
fn app_version(app_name: &str) -> String {
    format!("{app_name}-1.0.0")
}

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(app_version(app_name)),
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
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
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
            .list_workflow_steps(id, false, None, None, None)
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

/// A child that joins a held deduplication key is recorded as the workflow it joined.
///
/// The interesting half is the launch record. The child id this call derived — `{parent}-{step}` —
/// names no row, because the insert lost the key and nothing was written under it. What the parent
/// records at that step is the *holder's* id, so a replay of this position resolves to the same
/// workflow instead of trying to start a child that never existed. Go records the same mapping at
/// the same reserved step id, for the reason it states at `workflow.go:1465`.
#[tokio::test]
async fn a_child_joining_a_held_key_is_recorded_as_the_workflow_it_joined() {
    let db = test_database().await;
    let dbos = DBOS::new(config("child-dedup-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(9) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let child = child.clone();
            move |()| {
                let child = child.clone();
                async move {
                    child
                        .start_with(
                            (),
                            StartOptions {
                                queue: Some(Enqueue {
                                    deduplication_id: Some("order-42"),
                                    duplication_policy: DuplicationPolicy::ReturnExisting,
                                    ..Enqueue::new("demo-queue")
                                }),
                                ..StartOptions::default()
                            },
                        )
                        .await?
                        .result()
                        .await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "demo-queue",
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    // The holder, enqueued before the parent runs and still waiting when the child starts.
    let holder = child
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("the-holder"),
                queue: Some(Enqueue {
                    deduplication_id: Some("order-42"),
                    delay: Some(Duration::from_secs(3)),
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the holder was not enqueued");

    let id = "the-joining-parent";
    let result = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(result, 9, "the parent read the joined workflow's output");

    let reader = reader(&db).await;
    assert!(
        reader
            .get_workflow("the-joining-parent-0")
            .await
            .expect("read failed")
            .is_none(),
        "the derived child id names no row: the insert lost the key",
    );
    let steps = reader
        .list_workflow_steps(id, false, None, None, None)
        .await
        .expect("read failed");
    let launch = steps
        .iter()
        .find(|step| step.step_id == 0)
        .expect("no launch step on the parent");
    assert_eq!(launch.step_name, "child");
    assert_eq!(
        launch.child_workflow_id.as_deref(),
        Some("the-holder"),
        "the launch records the workflow that was joined",
    );

    // The other half of the relationship is deliberately absent. `parent_workflow_id` names the
    // one owner of a row, and the holder has one already -- so a joined workflow is resolved by
    // the parent's replay and named among its steps, but is not listed among its children, and a
    // cascade following that column never reaches it.
    assert!(
        reader
            .get_workflow_children(id)
            .await
            .expect("read failed")
            .is_empty(),
        "a joined workflow is not the joining parent's child",
    );
    assert_eq!(
        reader
            .get_workflow("the-holder")
            .await
            .expect("read failed")
            .expect("the holder is missing")
            .parent_workflow_id,
        None,
        "the holder keeps its own parentage: joining does not re-parent it",
    );

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(20), holder.result())
            .await
            .expect("the holder never ran")
            .expect("the holder failed"),
        9
    );

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
        let counted = Arc::clone(&bodies);
        let child = dbos
            .register_workflow("child", move |n: u32| {
                let counted = Arc::clone(&counted);
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
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

        // Wait until the third child's body has entered, so the crash lands mid-parent: two
        // children finished, one in flight.
        //
        // On the counter rather than on the parent's step count, which is the tempting version
        // and is racy. The launch row is written *before* the child is spawned — that ordering is
        // what makes a crash in the gap adoptable — so a parent showing five steps can have a
        // third child that has not been polled once, and shutdown would abort it before it
        // counted. Waiting on the thing the next assertion reads leaves no window between them.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while bodies.load(Ordering::SeqCst) < 3 {
            assert!(
                std::time::Instant::now() < deadline,
                "the parent never got as far as its third child"
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
        "three bodies entered, the third still blocked when the process died"
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
        .list_workflow_steps(id, false, None, None, None)
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

/// The fan-out shape: children launched in one loop and awaited in another, and they **run**
/// concurrently.
///
/// Each launch and each await allocates a step id, and here they are allocated in the caller's
/// own sequential order. It costs nothing in wall-clock: three children that each sleep are all
/// in flight together, so the parent takes about as long as the slowest rather than the sum. The
/// test after this one drives the launches with a `join!` instead, which is sound for the reason
/// a `join!` over steps is: the id is taken when the call is built.
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
            RunOptions {
                workflow_id: Some("fans-out"),
                ..RunOptions::default()
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

/// **A `join!` over launches takes ids in build order**, because a launch claims its id when it
/// is built rather than when it is first polled. Three `start`s built in source order and driven
/// together are `{parent}-0`, `{parent}-1`, `{parent}-2` whatever order the futures reach the
/// database — and a `join!` over the `result()`s numbers the awaits the same way.
#[tokio::test]
async fn launches_driven_together_take_ids_in_build_order() {
    let db = test_database().await;
    let dbos = DBOS::new(config("joined-fan-out-app", &db));
    let child = dbos
        .register_workflow("child", |n: u32| async move {
            // The later-built children finish first, so a poll-time id would be assigned in the
            // opposite order to the build.
            tokio::time::sleep(Duration::from_millis(100 * (3 - n as u64))).await;
            Ok::<u32, Error>(n)
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                let (a, b, c) = (child.start(0), child.start(1), child.start(2));
                assert_eq!(
                    (a.step_id(), b.step_id(), c.step_id()),
                    (Some(0), Some(1), Some(2)),
                    "the ids are taken at the call, in source order"
                );
                let (a, b, c) = tokio::join!(a, b, c);
                let (a, b, c) = (
                    a.map_err(Error::lift)?,
                    b.map_err(Error::lift)?,
                    c.map_err(Error::lift)?,
                );
                assert_eq!(
                    [a.workflow_id(), b.workflow_id(), c.workflow_id()],
                    ["joined-0", "joined-1", "joined-2"],
                    "each child is named for the slot its launch was built into"
                );
                let (ra, rb, rc) = (a.result(), b.result(), c.result());
                assert_eq!(
                    (ra.step_id(), rb.step_id(), rc.step_id()),
                    (Some(3), Some(4), Some(5)),
                    "the awaits are numbered after every launch, in build order"
                );
                let (ra, rb, rc) = tokio::join!(ra, rb, rc);
                Ok::<u32, Error>(ra? + rb? + rc?)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let total = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some("joined"),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(total, 3);

    // The recorded positions say the same thing the handles did, and the replay would read them.
    let steps = reader(&db)
        .await
        .list_workflow_steps("joined", false, None, None, None)
        .await
        .expect("read failed");
    let launches: Vec<_> = steps
        .iter()
        .filter_map(|step| {
            step.child_workflow_id
                .as_deref()
                .map(|child| (step.step_id, child))
        })
        .collect();
    assert_eq!(
        launches,
        [
            (0, "joined-0"),
            (1, "joined-1"),
            (2, "joined-2"),
            (3, "joined-0"),
            (4, "joined-1"),
            (5, "joined-2"),
        ],
        "three launches then three awaits, each under the id it was built with"
    );

    dbos.shutdown().await;
}

/// **A `run` takes both of its ids at the call**, so a `join!` over `run`s does not let one
/// child's await id depend on which child was recorded first.
#[tokio::test]
async fn runs_driven_together_take_both_ids_in_build_order() {
    let db = test_database().await;
    let dbos = DBOS::new(config("joined-runs-app", &db));
    let child = dbos
        .register_workflow("child", |n: u32| async move {
            tokio::time::sleep(Duration::from_millis(100 * (2 - n as u64))).await;
            Ok::<u32, Error>(n)
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                let (a, b) = (child.run(0), child.run(1));
                assert_eq!((a.step_id(), b.step_id()), (Some(0), Some(2)), "launch ids");
                let (a, b) = tokio::join!(a, b);
                Ok::<u32, Error>(a? + b?)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let total = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some("joined-runs"),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(total, 1);

    let steps = reader(&db)
        .await
        .list_workflow_steps("joined-runs", false, None, None, None)
        .await
        .expect("read failed");
    let positions: Vec<_> = steps
        .iter()
        .map(|step| {
            (
                step.step_id,
                step.child_workflow_id.as_deref().unwrap_or(""),
            )
        })
        .collect();
    assert_eq!(
        positions,
        [
            (0, "joined-runs-0"),
            (1, "joined-runs-0"),
            (2, "joined-runs-2"),
            (3, "joined-runs-2"),
        ],
        "each run is a {{launch, await}} pair at the ids it claimed when built, and the second \
         child is named for its launch's id, not its ordinal"
    );

    dbos.shutdown().await;
}

/// **A launch is polled where it was built.** Built outside a workflow and polled inside one —
/// `Ctx::scope`-shaped code, or a `start` built before the parent's body and moved in — it would
/// start a *root* workflow where the caller expected a child, with no launch record for the
/// parent to replay. It is refused instead, as a step built the same way is.
#[tokio::test]
async fn a_launch_built_outside_and_polled_inside_a_workflow_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("smuggled-launch-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    // A launch borrows the reference it was built from, and the parent's body has to be
    // `'static` to hold it — so the reference is leaked for the life of the test.
    let child: &'static WorkflowRef<(), u32> = Box::leak(Box::new(child));

    // Filled in after launch, since a launch needs a launched instance and a registration needs
    // one that is not yet launched.
    type Smuggled =
        Arc<std::sync::Mutex<Option<dbos::Pending<'static, dbos::WorkflowHandle<u32>>>>>;
    let smuggled: Smuggled = Arc::default();
    let parent = dbos
        .register_workflow("parent", {
            let smuggled = Arc::clone(&smuggled);
            move |()| {
                let smuggled = Arc::clone(&smuggled);
                async move {
                    let launch = smuggled.lock().unwrap().take().expect("built by the test");
                    let refused = launch.await.expect_err("a smuggled launch is refused");
                    match refused {
                        Error::StepBuiltElsewhere {
                            step,
                            built,
                            polled,
                        } => {
                            assert_eq!(step, "child");
                            assert_eq!(built, "outside a workflow");
                            assert_eq!(polled, "in workflow smuggled");
                        }
                        other => panic!("expected a built-elsewhere refusal, got {other:?}"),
                    }
                    Ok::<(), Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // Built here, with no workflow around it, so it claimed no id and would launch a root.
    let launch = child.start(());
    assert_eq!(launch.step_id(), None);
    *smuggled.lock().unwrap() = Some(launch);

    parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some("smuggled"),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent itself succeeds");

    assert!(
        reader(&db)
            .await
            .get_workflow_children("smuggled")
            .await
            .expect("read failed")
            .is_empty(),
        "nothing was launched"
    );

    dbos.shutdown().await;
}

/// **A race across a step and a child launch**, which `select_step!` accepts because both are
/// `Pending`. The launch wins here; its row and the parent's launch record are written, the
/// losing step records nothing, and the race records the winner's position. The winning arm then
/// awaits the handle, which is a step of its own.
#[tokio::test]
async fn a_select_step_can_race_a_step_against_a_child_launch() {
    let db = test_database().await;
    let dbos = DBOS::new(config("race-child-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                let outcome: u32 = dbos::select_step! {
                    slow = dbos::step("slow", || async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<u32, Error>(0)
                    }) => slow?,
                    started = child.start(()) => {
                        // The launch won; awaiting its handle is the next step of the parent.
                        started.map_err(Error::lift)?.result().await?
                    }
                }?;
                Ok::<u32, Error>(outcome)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let outcome = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some("raced"),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(outcome, 7, "the launch won and the arm awaited the child");

    let steps = reader(&db)
        .await
        .list_workflow_steps("raced", false, None, None, None)
        .await
        .expect("read failed");
    let positions: Vec<_> = steps
        .iter()
        .map(|step| {
            (
                step.step_id,
                step.step_name.as_str(),
                step.child_workflow_id.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        positions,
        [
            // Id 0 is the losing step, built and dropped without a row.
            (1, "child", Some("raced-1")),
            (2, "DBOS.selectStep", None),
            (3, "DBOS.getResult", Some("raced-1")),
        ],
        "the launch keeps its build-order id, the race records the winner, the arm's await follows"
    );

    dbos.shutdown().await;
}

/// **A control signal winning a race is not the race's decision.** A step that ends in a
/// cancellation, an interruption or a database failure records nothing, so the workflow stays
/// pending and is recovered — and the race it won has to do the same. Recording the branch as
/// the winner would pin every recovery to a branch that never ran its body, and never race the
/// other again.
#[tokio::test]
async fn a_control_signal_winning_a_select_step_records_no_winner() {
    let db = test_database().await;
    let dbos = DBOS::new(config("control-race-app", &db));
    let raced = Arc::new(tokio::sync::Notify::new());
    let parent = {
        let raced = Arc::clone(&raced);
        dbos.register_workflow("parent", move |()| {
            let raced = Arc::clone(&raced);
            async move {
                // The arms hand the branch's own result out rather than `?`-ing it, so the
                // workflow reaches the notify whichever way the race went.
                let outcome: Result<Result<u32, Error>, Error> = dbos::select_step! {
                    // The shape a cancelled or interrupted body reports in: a control signal,
                    // which the step returns without checkpointing.
                    interrupted = dbos::step("interrupted", || async {
                        Err::<u32, Error>(Error::Interrupted {
                            workflow_id: "raced".to_owned(),
                        })
                    }) => interrupted,
                    slow = dbos::step("slow", || async {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        Ok::<u32, Error>(1)
                    }) => slow,
                };
                raced.notify_one();
                outcome.and_then(|won| won)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let handle = parent
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("raced"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the parent failed to start");
    tokio::time::timeout(Duration::from_secs(5), raced.notified())
        .await
        .expect("the race did not finish");

    let steps: Vec<_> = reader(&db)
        .await
        .list_workflow_steps("raced", false, None, None, None)
        .await
        .expect("read failed")
        .iter()
        .map(|step| (step.step_id, step.step_name.clone()))
        .collect();
    assert_eq!(
        steps,
        [],
        "neither the interrupted step nor the race it won left a row"
    );
    drop(handle);

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
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(id, false, None, None, None)
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
            RunOptions {
                workflow_id: Some("spawns-in-a-step"),
                ..RunOptions::default()
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
                        RunOptions {
                            workflow_id: Some("i-named-this-one"),
                            ..RunOptions::default()
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
                RunOptions {
                    workflow_id: Some(id),
                    ..RunOptions::default()
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
        RunOptions {
            workflow_id: Some(id),
            ..RunOptions::default()
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
                RunOptions {
                    workflow_id: Some(id),
                    ..RunOptions::default()
                },
            )
            .await
            .expect("the parent failed"),
        99
    );

    let steps = reader(&db)
        .await
        .list_workflow_steps(id, true, None, None, None)
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
                .list_workflow_steps(id, false, None, None, None)
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
        .delete_workflows(&[&format!("{id}-0")], false, None)
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
                        RunOptions {
                            timeout: Timeout::Explicit(Duration::from_millis(300)),
                            ..RunOptions::default()
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
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
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
        .list_workflow_steps(id, true, None, None, None)
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
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
                ..Default::default()
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
                        RunOptions {
                            // Longer than what the parent has left.
                            timeout: Timeout::Explicit(Duration::from_secs(3_600)),
                            ..RunOptions::default()
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
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(60)),
                ..Default::default()
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
                        RunOptions {
                            timeout: Timeout::None,
                            ..RunOptions::default()
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
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
                ..Default::default()
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
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_millis(400)),
                ..Default::default()
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

    // And the parent, cancelled *while awaiting*, checkpointed nothing for that await. The wait
    // was interrupted rather than answered, so a resumed parent asks the child's then-settled row
    // again instead of replaying a verdict it never actually received. Go states the same rule in
    // its own words at the same point.
    let steps = reader
        .list_workflow_steps(id, false, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(
        steps.len(),
        1,
        "the launch, and nothing for the interrupted await: {steps:?}"
    );
    assert_eq!(steps[0].step_name, "child", "the launch");

    dbos.shutdown().await;
}

/// A launch position that already holds a *plain* step is an error, not a second child.
///
/// **Stricter than every reference, deliberately.** Python falls through and starts a fresh child,
/// and Go's `CheckChildWorkflow` returns nothing for such a row; both then collide on the write a
/// moment later, so both end up loud rather than wrong — but only after creating and orphaning a
/// child workflow. Refusing before anything is created leaves nothing behind to clean up.
///
/// The situation is a parent whose code changed under it: `step("child")` at position 0 became
/// `child.run(())`. Planted directly here rather than staged across two processes, because the
/// database is the whole of what the replay reads and the gate is what makes the write land in
/// the same place a crash would have left it.
#[tokio::test]
async fn a_launch_position_holding_a_plain_step_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("stale-launch-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();

    // The parent waits before its first step id is allocated, which is the window the plain step
    // is planted in. Nothing before this is a step, so the launch still lands on position 0.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let gate = Arc::new(std::sync::Mutex::new(Some(rx)));
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            let gate = Arc::clone(&gate);
            async move {
                let rx = gate.lock().unwrap().take().expect("the parent runs once");
                let _ = rx.await;
                child.run(()).await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "changed-under-itself";
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

    // The row exists as soon as `start_with` returns, which is what this write needs.
    let reader = reader(&db).await;
    reader
        .record_step(id, 0, "child", Outcome::Output(Some("1")), None, None)
        .await
        .expect("planting the step failed");
    tx.send(()).expect("the parent is waiting on the gate");

    let error = handle.result().await.expect_err("the parent succeeded");
    match &error {
        Error::SystemDatabase(dbos::sysdb::Error::UnexpectedStep {
            step_id,
            expected,
            recorded,
            ..
        }) => {
            assert_eq!(*step_id, 0, "the launch position");
            assert!(
                expected.contains("child workflow launch"),
                "says what it wanted: {expected}"
            );
            assert!(
                recorded.contains("plain step"),
                "and what it found: {recorded}"
            );
        }
        other => panic!("expected an unexpected-step refusal, got {other:?}"),
    }

    // The point of refusing early: nothing was created to be orphaned.
    assert!(
        reader
            .get_workflow(&format!("{id}-0"))
            .await
            .expect("read failed")
            .is_none(),
        "no child row was created before the refusal"
    );

    dbos.shutdown().await;
}

/// A `WorkflowRef` from another instance cannot start a child of this workflow.
///
/// The second place [`Error::WrongInstance`] is reachable from, and the reason it exists: the step
/// id would come from this workflow's counter while the launch record went through the other
/// instance's system database, landing where the workflow that allocated it cannot see it.
/// `DBOS::get_event` refuses the same combination for the same reason, and is where the variant
/// was first raised.
#[tokio::test]
async fn a_child_started_through_another_instance_is_refused() {
    let db = test_database().await;
    let other = DBOS::new(config("other-instance-app", &db));
    let owner = DBOS::new(config("owner-instance-app", &db));

    let child = other
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = {
        let child = child.clone();
        owner
            .register_workflow("parent", move |()| {
                let child = child.clone();
                async move { child.run(()).await }
            })
            .unwrap()
    };
    other.launch().await.expect("launch failed");
    owner.launch().await.expect("launch failed");

    let id = "borrows-another-instance";
    let error = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect_err("the parent succeeded");
    assert!(
        matches!(&error, Error::WrongInstance { operation } if operation.contains("workflow")),
        "expected a wrong-instance refusal, got {error:?}"
    );

    // Refused before anything was written: no child, and no launch on the parent.
    let reader = reader(&db).await;
    assert!(
        reader
            .get_workflow(&format!("{id}-0"))
            .await
            .expect("read failed")
            .is_none(),
        "no child row"
    );
    assert!(
        reader
            .list_workflow_steps(id, false, None, None, None)
            .await
            .expect("read failed")
            .is_empty(),
        "and no launch recorded against the parent"
    );

    owner.shutdown().await;
    other.shutdown().await;
}

/// Awaiting a child from inside a step checkpoints nothing of its own, and is not an error.
///
/// The asymmetry with *starting* a child is deliberate, and both halves follow from a step being a
/// leaf. A launch inside a step allocates ids that would shift every later step onto the wrong
/// replay slot, and there is no undurable version of it to fall back to, so it raises
/// [`Error::InsideStep`]. An await has such a version: the enclosing step's own checkpoint already
/// stands for whatever its body did, including the waiting, so the await simply runs plainly.
/// `DBOS::get_event` degrades in exactly the same way.
#[tokio::test]
async fn awaiting_a_child_inside_a_step_is_covered_by_that_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("await-in-step-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(41) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                // Started at a step boundary, where a launch has to happen...
                let handle = child.start(()).await.map_err(Error::lift)?;
                // ...and awaited from inside a step, where there is no id to allocate. The handle
                // is not `Clone` and `result` consumes it, so it reaches the retryable closure
                // through a slot it takes from once.
                let slot = Arc::new(std::sync::Mutex::new(Some(handle)));
                let value = dbos::step("collect", move || {
                    let slot = Arc::clone(&slot);
                    async move {
                        let handle = slot.lock().unwrap().take().expect("the step runs once");
                        handle.result().await
                    }
                })
                .await?;
                Ok::<u32, Error>(value)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "collects-inside-a-step";
    assert_eq!(
        parent
            .run_with(
                (),
                RunOptions {
                    workflow_id: Some(id),
                    ..RunOptions::default()
                },
            )
            .await
            .expect("the parent failed"),
        41
    );

    let steps = reader(&db)
        .await
        .list_workflow_steps(id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 2, "the launch and the step: {steps:?}");
    assert_eq!(steps[0].step_name, "child", "the launch");
    assert_eq!(
        steps[0].child_workflow_id.as_deref(),
        Some("collects-inside-a-step-0")
    );
    assert_eq!(
        steps[1].step_name, "collect",
        "the enclosing step, under its own name"
    );
    assert_eq!(
        steps[1].output.as_deref(),
        Some("41"),
        "which carries the child's value"
    );
    assert!(
        steps.iter().all(|step| step.step_name != "DBOS.getResult"),
        "the await added no checkpoint of its own: {steps:?}"
    );

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
            .list_workflow_steps(id, false, None, None, None)
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
