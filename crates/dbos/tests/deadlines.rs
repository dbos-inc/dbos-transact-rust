//! Workflow timeouts: the durable deadline, against real databases.

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
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_millis(100)),
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
            StartOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(30)),
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
/// The recovered run is given a body that would outlive a *fresh* budget but not the elapsed one:
/// if the clock restarted, the workflow would finish; because it does not, it is cancelled.
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
    assert!(row.deadline.is_some(), "the deadline is stored on the row");

    // Let the original deadline pass before the second process starts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second process: recovery reads the stored deadline, which is now in the past.
    let dbos = DBOS::new(config("recover-deadline-app", &db));
    let runs = Arc::new(AtomicU32::new(0));
    let dbos_runs = Arc::clone(&runs);
    dbos.register_workflow("slow", move |()| {
        let runs = Arc::clone(&dbos_runs);
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<u32, dbos::Error>(1)
        }
    })
    .unwrap();
    dbos.launch().await.expect("launch failed");

    // The recovered run is cancelled almost at once rather than being given a fresh 400ms.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing");
        if row.status == WorkflowStatus::Cancelled {
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
/// This is go #426's bug, which cost Go a fix: its shutdown cancelled a context, the context fired
/// the durable cancel hook, and workflows that should have waited for recovery were written
/// CANCELLED instead. Here shutdown aborts the task and the deadline is a `select!` branch, so
/// there is no shared hook to misfire — this test is what keeps it that way.
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
/// `cancel_batch` leaves a finished row alone, so the cancellation this execution attempts moves
/// nothing. Reporting `WorkflowCancelled` anyway would hand this caller an answer no other caller
/// can see: the row says `SUCCESS`, and so does `handle.status()` on the very same handle.
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
