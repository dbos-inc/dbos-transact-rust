//! Workflow timeouts: the durable deadline, against real databases.

use std::sync::Arc;
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{NewWorkflow, Outcome, Submission, Timestamp, WorkflowStatus};
use dbos::{Config, DBOS, Error, RunOptions, StartOptions, Timeout};

use dbos_test_support::{Backend, TestDatabase, raw_database, test_database};

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails — and sharing it is what lets a relaunch recover what the previous launch left.
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

/// A workflow past its deadline is cancelled, and the row says so.
#[tokio::test]
async fn a_workflow_past_its_deadline_is_cancelled() {
    let db = test_database().await;
    let dbos = DBOS::new(config("deadline-app", &db));
    let workflow = dbos
        .register_workflow("runs_forever", |()| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, dbos::Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "too-slow";
    let error = workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_millis(100)),
                ..Default::default()
            },
        )
        .await
        .expect_err("the workflow succeeded");
    assert!(
        matches!(error, Error::WorkflowCancelled { .. }),
        "expected a cancellation, got {error:?}"
    );

    // Durable, unlike every other stop the engine performs.
    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Cancelled);

    dbos.shutdown().await;
}

/// A workflow that finishes inside its deadline is unaffected.
#[tokio::test]
async fn a_workflow_within_its_deadline_is_unaffected() {
    let db = test_database().await;
    let dbos = DBOS::new(config("within-deadline-app", &db));
    let workflow = dbos
        .register_workflow("quick", |()| async move { Ok::<u32, dbos::Error>(7) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "fast-enough";
    let value = workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(30)),
                ..Default::default()
            },
        )
        .await
        .expect("the workflow failed");
    assert_eq!(value, 7);

    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Success);

    dbos.shutdown().await;
}

/// The deadline is an instant, so a recovery gets what is left of the budget, not the whole of it.
///
/// Checked on the row: the deadline the recovered workflow is cancelled with is the one the first
/// launch stored. Its status alone can't tell, since a fresh budget would also run out inside the
/// wait, and the sweep cancels an expired row whatever recovery does.
#[tokio::test]
async fn a_recovered_workflow_keeps_the_deadline_it_already_had() {
    let db = test_database().await;
    let id = "keeps-its-deadline";

    // First process: start with a short budget and let it die mid-flight, leaving PENDING.
    {
        let dbos = DBOS::new(config("recover-deadline-app", &db));
        let workflow = dbos
            .register_workflow("slow", |()| async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<u32, dbos::Error>(1)
            })
            .unwrap();
        dbos.launch().await.expect("launch failed");
        workflow
            .start_with(
                (),
                StartOptions {
                    workflow_id: Some(id),
                    timeout: Timeout::Explicit(Duration::from_millis(400)),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("start failed");
        // Shutdown aborts it, leaving the row PENDING with its deadline stored.
        dbos.shutdown().await;
    }

    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.status,
        WorkflowStatus::Pending,
        "shutdown left it PENDING"
    );
    let stored = row.deadline.expect("the deadline is stored on the row");

    // Let the original deadline pass before the second process starts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second process: recovery reads the stored deadline, which is now in the past.
    let dbos = DBOS::new(config("recover-deadline-app", &db));
    dbos.register_workflow("slow", |()| async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok::<u32, dbos::Error>(1)
    })
    .unwrap();
    dbos.launch().await.expect("launch failed");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing");
        if row.status == WorkflowStatus::Cancelled {
            assert_eq!(
                row.deadline,
                Some(stored),
                "recovery kept the deadline it already had"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the recovered workflow was not cancelled; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    dbos.shutdown().await;
}

/// Shutdown must not durably cancel: the row stays PENDING so a later executor recovers it.
///
/// The failure mode is Go's #426: a shutdown that cancels a context, a context that fires the
/// durable cancel hook, and workflows that should wait for recovery written CANCELLED instead. Here
/// shutdown aborts the task and the deadline is a `select!` branch, so there is no shared hook to
/// misfire — this test is what keeps it that way.
#[tokio::test]
async fn shutdown_does_not_durably_cancel_a_workflow_that_has_a_deadline() {
    let db = test_database().await;
    let dbos = DBOS::new(config("shutdown-deadline-app", &db));
    let started = Arc::new(tokio::sync::Notify::new());
    let dbos_started = Arc::clone(&started);
    let workflow = dbos
        .register_workflow("slow", move |()| {
            let started = Arc::clone(&dbos_started);
            async move {
                started.notify_one();
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<u32, dbos::Error>(1)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "interrupted-not-cancelled";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                // Generous, so the deadline is nowhere near firing when shutdown arrives.
                timeout: Timeout::Explicit(Duration::from_secs(300)),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    started.notified().await;

    dbos.shutdown().await;

    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.status,
        WorkflowStatus::Pending,
        "shutdown durably cancelled a workflow it should have left for recovery"
    );
}

/// A deadline that fires after another execution has already finished the workflow reports **that
/// outcome**, not a cancellation.
///
/// A timeout cancels only an unsettled row, so the one this execution attempts moves nothing.
/// Reporting `WorkflowCancelled` anyway would hand this caller an answer no other caller can see:
/// the row says `SUCCESS`, and so does `handle.status()` on the very same handle.
#[tokio::test]
async fn a_deadline_that_loses_to_a_recorded_outcome_reports_that_outcome() {
    let db = test_database().await;
    let dbos = DBOS::new(config("deadline-race-app", &db));
    let workflow = dbos
        .register_workflow("runs_forever", |()| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, dbos::Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "finished-elsewhere";
    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_millis(600)),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");

    // A rival execution — a recovery after this process was presumed dead — records the outcome
    // while the body here is still sleeping, and before the deadline fires.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let reader = reader(&db).await;
    reader
        .record_workflow_outcome(id, Outcome::Output(Some("7")))
        .await
        .expect("the rival could not record its outcome");

    assert_eq!(
        handle
            .result()
            .await
            .expect("the deadline reported a cancellation over a recorded result"),
        7,
        "the caller must read what the row holds"
    );

    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.status,
        WorkflowStatus::Success,
        "the deadline overwrote a terminal row"
    );

    dbos.shutdown().await;
}

/// A configuration for a test that waits on the timeout sweep, which then runs every 100
/// milliseconds rather than every second. The interval is process-wide, so every launch in this
/// binary from then on sweeps that often, which only makes the other tests' timeouts land sooner.
fn sweeping_config(app_name: &str, db: &TestDatabase) -> Config {
    dbos::__private::set_workflow_timeout_sweep_interval(Duration::from_millis(100));
    config(app_name, db)
}

/// A system database handle that writes rows as `app_name` does, for rows no workflow of this
/// process will run.
async fn writer(db: &TestDatabase, app_name: &'static str) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            application_name: Some(app_name),
            ..Settings::default()
        },
    )
}

/// Inserts an unsettled workflow whose deadline is long past, owned by an executor that isn't
/// running, and queued on `queue` when one is given.
async fn insert_expired(sys: &PostgresSystemDatabase, id: &str, queue: Option<&str>) {
    sys.init_workflow(
        &NewWorkflow {
            name: Some("never-registered"),
            executor_id: Some("dead-executor"),
            queue_name: queue,
            deadline: Some(Timestamp::from_epoch_ms(1_000)),
            ..NewWorkflow::new(id)
        },
        None,
        Submission::Fresh,
        None,
    )
    .await
    .expect("insert failed");
}

/// Waits up to five seconds for `id` to reach `status`, and says what it held if it never does.
async fn wait_for_status(reader: &PostgresSystemDatabase, id: &str, status: WorkflowStatus) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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

/// **The sweep cancels an expired workflow nobody is running**: one queued where no worker
/// polls, and a `PENDING` one whose executor is gone. Another application's is left alone.
#[tokio::test]
async fn the_sweep_cancels_expired_workflows_nobody_is_running() {
    let db = test_database().await;
    let dbos = DBOS::new(sweeping_config("sweep-app", &db));
    dbos.launch().await.expect("launch failed");

    let ours = writer(&db, "sweep-app").await;
    insert_expired(&ours, "queued-nowhere", Some("unpolled-queue")).await;
    insert_expired(&ours, "orphaned", None).await;
    insert_expired(&writer(&db, "another-app").await, "not-ours", None).await;

    let reader = reader(&db).await;
    for id in ["queued-nowhere", "orphaned"] {
        wait_for_status(&reader, id, WorkflowStatus::Cancelled).await;
    }
    let queued = reader
        .get_workflow("queued-nowhere")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        queued.queue_name, None,
        "a cancelled workflow leaves its queue"
    );
    assert_eq!(
        reader
            .get_workflow("not-ours")
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkflowStatus::Pending,
        "another application's workflow is its own sweep's to cancel"
    );

    dbos.shutdown().await;
}

/// **A failed pass doesn't stop the sweep.** Passes fail while the deadline column is renamed
/// away; once it is back, the next pass cancels as before.
#[tokio::test]
async fn a_failed_sweep_pass_does_not_stop_the_sweep() {
    // A database of its own, never pooled: the test renames a column, and one left renamed by a
    // failure part-way would break whichever test drew the database next.
    let db = raw_database().await;
    if db.backend() == Backend::Cockroach {
        // CockroachDB won't rename a column a SQL function (`enqueue_workflow`) depends on, and
        // what this checks is the loop, which is the same on both backends.
        return;
    }
    let dbos = DBOS::new(Config {
        migrate: true,
        ..sweeping_config("sweep-failure-app", &db)
    });
    dbos.launch().await.expect("launch failed");

    let pool = db.pool().await;
    let rename = |from: &'static str, to: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                r#"ALTER TABLE "dbos"."workflow_status" RENAME COLUMN {from} TO {to}"#
            )))
            .execute(&pool)
            .await
            .expect("rename failed");
        }
    };
    rename("workflow_deadline_epoch_ms", "deadline_hidden").await;
    // Several passes' worth, every one of which fails.
    tokio::time::sleep(Duration::from_millis(500)).await;
    rename("deadline_hidden", "workflow_deadline_epoch_ms").await;

    insert_expired(
        &writer(&db, "sweep-failure-app").await,
        "after-the-failure",
        None,
    )
    .await;
    wait_for_status(
        &reader(&db).await,
        "after-the-failure",
        WorkflowStatus::Cancelled,
    )
    .await;

    dbos.shutdown().await;
}

/// **The sweep stops at shutdown, and a relaunch starts a fresh one.**
#[tokio::test]
async fn the_sweep_stops_at_shutdown_and_restarts_on_relaunch() {
    let db = test_database().await;
    let dbos = DBOS::new(sweeping_config("sweep-relaunch-app", &db));
    dbos.launch().await.expect("launch failed");
    dbos.shutdown().await;

    insert_expired(&writer(&db, "sweep-relaunch-app").await, "while-down", None).await;
    // Several passes' worth, none of which runs.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow("while-down")
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkflowStatus::Pending,
        "nothing sweeps once the executor is shut down"
    );

    dbos.launch().await.expect("relaunch failed");
    wait_for_status(&reader, "while-down", WorkflowStatus::Cancelled).await;
    dbos.shutdown().await;
}

/// **A run whose deadline passes records its own timeout**, so it doesn't depend on the sweep
/// reaching the row.
///
/// The row is recorded under another application's name with no owner, which this process will
/// run when started under the same id, and which its sweep, scoped to its own application's rows,
/// never cancels. The run still ends `CANCELLED` soon after its deadline.
#[tokio::test]
async fn a_timed_out_run_cancels_its_own_row_where_the_sweep_does_not_reach() {
    let db = test_database().await;
    let dbos = DBOS::new(sweeping_config("deadline-scope-app", &db));
    let workflow = dbos
        .register_workflow("runs_forever", |()| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, dbos::Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "out-of-scope";
    writer(&db, "another-app")
        .await
        .init_workflow(
            &NewWorkflow {
                name: Some("runs_forever"),
                input: Some("null"),
                serialization: Some("rust_serde"),
                deadline: Timestamp::now().checked_add(Duration::from_millis(300)),
                ..NewWorkflow::new(id)
            },
            None,
            Submission::Fresh,
            None,
        )
        .await
        .expect("insert failed");
    sqlx::query(r#"UPDATE "dbos"."workflow_status" SET owner_xid = NULL WHERE workflow_uuid = $1"#)
        .bind(id)
        .execute(&db.pool().await)
        .await
        .unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(10),
        workflow.run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the timed-out run never ended")
    .expect_err("the workflow succeeded");
    assert!(
        matches!(error, Error::WorkflowCancelled { .. }),
        "expected a cancellation, got {error:?}"
    );
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Cancelled);
    assert_eq!(
        row.application_name.as_deref(),
        Some("another-app"),
        "the row stayed outside this application's sweep"
    );

    dbos.shutdown().await;
}
