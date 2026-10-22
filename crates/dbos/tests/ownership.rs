//! Execution ownership: a workflow handed to a new execution stops the old one at its next write.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
use dbos::{Config, DBOS, Error, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// Holds the first execution to pass it until released; every later one passes at once. What
/// lets a test hand a workflow to a new execution while the old one is still inside it.
#[derive(Default)]
struct Gate {
    passes: AtomicU32,
    entered: Notify,
    release: Notify,
}

impl Gate {
    /// Which pass this is, from 1, after waiting for the release if it is the first.
    async fn pass(&self) -> u32 {
        let pass = self.passes.fetch_add(1, Ordering::SeqCst) + 1;
        if pass == 1 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        pass
    }

    fn passes(&self) -> u32 {
        self.passes.load(Ordering::SeqCst)
    }
}

/// How a workflow is taken from the execution running it.
#[derive(Clone, Copy, Debug)]
enum HandOff {
    /// An operator resumes it while it runs.
    Resume,
    /// Recovery re-enqueues it, as for an executor presumed dead.
    Recovery,
}

impl HandOff {
    async fn take(self, dbos: &DBOS, db: &TestDatabase, workflow_id: &str) {
        match self {
            HandOff::Resume => {
                dbos.resume::<u32, Error>(workflow_id)
                    .await
                    .expect("resume failed");
            }
            HandOff::Recovery => {
                let moved = reader(db)
                    .await
                    .reenqueue_for_recovery(&["local"], APP_VERSION, INTERNAL_QUEUE)
                    .await
                    .expect("re-enqueue failed");
                assert_eq!(moved, [workflow_id]);
            }
        }
    }
}

/// Waits up to ten seconds for `id` to reach `status`.
async fn wait_for_status(db: &TestDatabase, id: &str, status: WorkflowStatus) {
    let reader = reader(db).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = reader.get_workflow(id).await.unwrap().expect("row missing");
        if row.status == status {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{id} is still {} rather than {status}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn step_rows(db: &TestDatabase, id: &str) -> usize {
    reader(db)
        .await
        .list_workflow_steps(id, false, None, None, None)
        .await
        .unwrap()
        .len()
}

/// **A hand-off while a step is in flight.** The new owner runs the step again and finishes; the
/// old execution's late result is refused, it never reaches its next step, and its caller gets the
/// owner's outcome. Exactly the owner's two steps are recorded.
async fn a_hand_off_during_a_step_is_finished_by_the_new_owner(hand_off: HandOff) {
    let db = test_database().await;
    let dbos = DBOS::new(config("hand-off-app", &db));
    let gate = Arc::new(Gate::default());
    let seconds = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("handed-off", {
            let (gate, seconds) = (Arc::clone(&gate), Arc::clone(&seconds));
            move |()| {
                let (gate, seconds) = (Arc::clone(&gate), Arc::clone(&seconds));
                async move {
                    let first = dbos::step("first", || {
                        let gate = Arc::clone(&gate);
                        async move { Ok::<_, Error>(gate.pass().await) }
                    })
                    .await?;
                    let second = dbos::step("second", || {
                        let seconds = Arc::clone(&seconds);
                        async move { Ok::<_, Error>(seconds.fetch_add(1, Ordering::SeqCst) + 10) }
                    })
                    .await?;
                    Ok::<_, Error>(first + second)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "handed-off-wf";
    let stale = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    gate.entered.notified().await;
    hand_off.take(&dbos, &db, id).await;
    wait_for_status(&db, id, WorkflowStatus::Success).await;

    gate.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(10), stale.result())
        .await
        .expect("the stale execution never ended")
        .expect("the stale execution failed");
    assert_eq!(outcome, 2 + 10, "the stale caller gets the owner's outcome");
    assert_eq!(gate.passes(), 2, "the new owner ran the step again");
    assert_eq!(
        seconds.load(Ordering::SeqCst),
        1,
        "the stale execution never reached its next step"
    );
    assert_eq!(step_rows(&db, id).await, 2, "only the owner's steps");

    dbos.shutdown().await;
}

#[tokio::test]
async fn a_resume_during_a_step_is_finished_by_the_new_owner() {
    a_hand_off_during_a_step_is_finished_by_the_new_owner(HandOff::Resume).await;
}

#[tokio::test]
async fn a_recovery_during_a_step_is_finished_by_the_new_owner() {
    a_hand_off_during_a_step_is_finished_by_the_new_owner(HandOff::Recovery).await;
}

/// **A cancel during a step.** The step's late result is not recorded, the row has no owner, and
/// a later resume runs the step again.
#[tokio::test]
async fn a_step_that_outlives_a_cancel_records_nothing() {
    let db = test_database().await;
    let dbos = DBOS::new(config("cancel-step-app", &db));
    let gate = Arc::new(Gate::default());
    let workflow = dbos
        .register_workflow("cancelled-mid-step", {
            let gate = Arc::clone(&gate);
            move |()| {
                let gate = Arc::clone(&gate);
                async move {
                    dbos::step("slow", || {
                        let gate = Arc::clone(&gate);
                        async move { Ok::<_, Error>(gate.pass().await) }
                    })
                    .await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "cancelled-mid-step-wf";
    let stale = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    gate.entered.notified().await;
    dbos.cancel(id).await.expect("cancel failed");
    gate.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(10), stale.result())
        .await
        .expect("the cancelled execution never ended")
        .expect_err("the cancelled workflow succeeded");
    assert!(
        matches!(error, Error::WorkflowCancelled { .. }),
        "{error:?}"
    );
    assert_eq!(
        step_rows(&db, id).await,
        0,
        "the late result is not recorded"
    );
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.owner_xid, None);

    let resumed = dbos.resume::<u32, Error>(id).await.expect("resume failed");
    let outcome = tokio::time::timeout(Duration::from_secs(10), resumed.result())
        .await
        .expect("the resumed workflow never ended")
        .expect("the resumed workflow failed");
    assert_eq!(outcome, 2, "the resume ran the step again");

    dbos.shutdown().await;
}

/// **A hand-off while blocked in `recv`.** The new owner gets the message; the old execution's
/// consume rolls back, so a second message stays unconsumed.
#[tokio::test]
async fn a_hand_off_during_recv_leaves_the_message_for_the_new_owner() {
    let db = test_database().await;
    let dbos = DBOS::new(config("hand-off-recv-app", &db));
    let gate = Arc::new(Gate::default());
    let workflow = dbos
        .register_workflow("receiver", {
            let gate = Arc::clone(&gate);
            move |()| {
                let gate = Arc::clone(&gate);
                async move {
                    // Counted, but never held: the first execution blocks in the receive itself.
                    let pass = gate.passes.fetch_add(1, Ordering::SeqCst) + 1;
                    if pass == 1 {
                        gate.entered.notify_one();
                    }
                    let message: Option<u32> =
                        dbos::recv(Some("topic"), Duration::from_secs(30)).await?;
                    Ok::<_, Error>(message.unwrap_or_default())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "receiver-wf";
    let stale = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    gate.entered.notified().await;
    // Until the first receive has recorded its deadline and is waiting: resumed any earlier, it
    // would be refused at that record, and its consume never reached.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while step_rows(&db, id).await < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the receive never recorded its deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    dbos.resume::<u32, Error>(id).await.expect("resume failed");
    // Until the new owner is running and waiting too.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while gate.passes() < 2 {
        assert!(tokio::time::Instant::now() < deadline, "no new owner ran");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    for n in [7u32, 8] {
        dbos.send_with(
            dbos::SendMessage::new(id, &n).topic("topic"),
            dbos::SendOptions::default(),
        )
        .await
        .expect("send failed");
    }
    let outcome = tokio::time::timeout(Duration::from_secs(15), stale.result())
        .await
        .expect("the stale execution never ended")
        .expect("the stale execution failed");
    assert_eq!(outcome, 7, "the owner received the first message");
    let unconsumed: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM "dbos"."notifications"
           WHERE destination_uuid = $1 AND consumed_by_function_id IS NULL"#,
    )
    .bind(id)
    .fetch_one(&db.pool().await)
    .await
    .unwrap();
    assert_eq!(unconsumed, 1, "the stale consume took nothing");

    dbos.shutdown().await;
}

/// **A stale parent starts nothing**: no child row, no child run, and no step of its own.
#[tokio::test]
async fn a_stale_parent_starts_no_child() {
    let db = test_database().await;
    let dbos = DBOS::new(config("stale-parent-app", &db));
    let gate = Arc::new(Gate::default());
    let child_runs = Arc::new(AtomicU32::new(0));
    let child = dbos
        .register_workflow("child", {
            let child_runs = Arc::clone(&child_runs);
            move |()| {
                let child_runs = Arc::clone(&child_runs);
                async move {
                    child_runs.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let gate = Arc::clone(&gate);
            move |()| {
                let (gate, child) = (Arc::clone(&gate), child.clone());
                async move {
                    // Not a step: the first execution waits here having written nothing.
                    if gate.pass().await == 1 {
                        child
                            .run_with(
                                (),
                                dbos::RunOptions {
                                    workflow_id: Some("stale-child"),
                                    ..Default::default()
                                },
                            )
                            .await?;
                    }
                    Ok::<_, Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "stale-parent-wf";
    let stale = parent
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    gate.entered.notified().await;
    dbos.resume::<(), Error>(id).await.expect("resume failed");
    wait_for_status(&db, id, WorkflowStatus::Success).await;
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), stale.result())
        .await
        .expect("the stale parent never ended")
        .expect("the stale parent failed");

    let reader = reader(&db).await;
    assert!(
        reader.get_workflow("stale-child").await.unwrap().is_none(),
        "the stale parent created a child"
    );
    assert_eq!(child_runs.load(Ordering::SeqCst), 0);
    assert_eq!(step_rows(&db, id).await, 0);

    dbos.shutdown().await;
}

/// **Concurrent steps under a hand-off.** Both of the old execution's steps are refused; the new
/// owner records both and finishes, and the old caller gets the owner's outcome.
#[tokio::test]
async fn concurrent_steps_under_a_hand_off_are_all_the_new_owners() {
    let db = test_database().await;
    let dbos = DBOS::new(config("hand-off-join-app", &db));
    let left = Arc::new(Gate::default());
    let right = Arc::new(Gate::default());
    let workflow = dbos
        .register_workflow("joined", {
            let (left, right) = (Arc::clone(&left), Arc::clone(&right));
            move |()| {
                let (left, right) = (Arc::clone(&left), Arc::clone(&right));
                async move {
                    let (l, r) = tokio::join!(
                        dbos::step("left", || {
                            let left = Arc::clone(&left);
                            async move { Ok::<_, Error>(left.pass().await) }
                        }),
                        dbos::step("right", || {
                            let right = Arc::clone(&right);
                            async move { Ok::<_, Error>(right.pass().await * 10) }
                        }),
                    );
                    Ok::<_, Error>(l? + r?)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "joined-wf";
    let stale = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    left.entered.notified().await;
    right.entered.notified().await;
    dbos.resume::<u32, Error>(id).await.expect("resume failed");
    wait_for_status(&db, id, WorkflowStatus::Success).await;
    left.release.notify_one();
    right.release.notify_one();

    let outcome = tokio::time::timeout(Duration::from_secs(10), stale.result())
        .await
        .expect("the stale execution never ended")
        .expect("the stale execution failed");
    assert_eq!(outcome, 2 + 20, "the owner's outcome");
    assert_eq!(step_rows(&db, id).await, 2);

    dbos.shutdown().await;
}

/// **A conflict the workflow swallows changes nothing.** Its later writes and its outcome are
/// refused, and its caller gets the owner's outcome; the fallback is never recorded.
#[tokio::test]
async fn a_swallowed_conflict_still_yields_the_owners_outcome() {
    let db = test_database().await;
    let dbos = DBOS::new(config("swallowed-app", &db));
    let gate = Arc::new(Gate::default());
    let workflow = dbos
        .register_workflow("swallowing", {
            let gate = Arc::clone(&gate);
            move |()| {
                let gate = Arc::clone(&gate);
                async move {
                    let value = dbos::step("guarded", || {
                        let gate = Arc::clone(&gate);
                        async move { Ok::<_, Error>(gate.pass().await) }
                    })
                    .await
                    .unwrap_or(1000);
                    Ok::<_, Error>(value)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "swallowing-wf";
    let stale = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    gate.entered.notified().await;
    dbos.resume::<u32, Error>(id).await.expect("resume failed");
    wait_for_status(&db, id, WorkflowStatus::Success).await;
    gate.release.notify_one();

    let outcome = tokio::time::timeout(Duration::from_secs(10), stale.result())
        .await
        .expect("the stale execution never ended")
        .expect("the stale execution failed");
    assert_eq!(outcome, 2, "the owner's outcome, not the fallback");
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.output.as_deref(), Some("2"));

    dbos.shutdown().await;
}
