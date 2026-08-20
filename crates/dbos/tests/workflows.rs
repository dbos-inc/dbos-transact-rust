//! Running workflows, against real databases.

use std::error::Error as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error};

use dbos_test_support::{TestDatabase, test_database};

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        ..Config::new(app_name, db.url())
    }
}

/// A handle for reading rows behind the instance's back.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

async fn double(n: u32) -> dbos::Result<u32> {
    Ok(n * 2)
}

async fn greet(_: ()) -> dbos::Result<String> {
    Ok("hello".to_owned())
}

/// The ordinary case: a workflow runs, returns, and its row records what it returned.
#[tokio::test]
async fn a_workflow_runs_and_records_its_output() {
    let db = test_database().await;
    let dbos = DBOS::new(config("run-app", &db));
    let double = dbos.register_workflow("double", double).unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(double.run(21).await.expect("the workflow failed"), 42);

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let [row] = &rows[..] else {
        panic!("expected exactly one workflow, got {}", rows.len())
    };
    assert_eq!(row.status, WorkflowStatus::Success);
    assert_eq!(row.name.as_deref(), Some("double"));
    assert_eq!(row.output.as_deref(), Some("42"));
    assert_eq!(row.input.as_deref(), Some("21"));
    assert_eq!(row.serialization.as_deref(), Some("rust_serde"));
    assert_eq!(
        row.application_version.as_deref(),
        Some(&*dbos.application_version().unwrap())
    );
    assert_eq!(row.executor_id.as_deref(), Some("local"));

    dbos.shutdown().await;
}

/// A zero-argument workflow stores no input, and must not need a literal `null` to run.
#[tokio::test]
async fn a_zero_argument_workflow_records_no_input() {
    let db = test_database().await;
    let dbos = DBOS::new(config("noarg-app", &db));
    let greet = dbos.register_workflow("greet", greet).unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(greet.run(()).await.expect("the workflow failed"), "hello");

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(
        rows[0].input.as_deref(),
        Some("null"),
        "`()` encodes as JSON null"
    );
    assert_eq!(rows[0].output.as_deref(), Some("\"hello\""));

    dbos.shutdown().await;
}

/// A workflow that fails in the application's own terms records ERROR, and the row carries the
/// application's message rather than a wrapper around it.
///
/// The error type here is deliberately not one of ours: a real workflow fails with its own error,
/// converted at the boundary with `.map_err(Error::application)`, and what must reach the database
/// is what *it* said.
#[tokio::test]
async fn a_failing_workflow_records_the_applications_own_error() {
    #[derive(Debug, thiserror::Error)]
    #[error("the card was declined")]
    struct CardDeclined;

    async fn charge() -> Result<(), CardDeclined> {
        Err(CardDeclined)
    }

    async fn fails(_: ()) -> dbos::Result<()> {
        charge().await.map_err(Error::application)?;
        Ok(())
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("fail-app", &db));
    let fails = dbos.register_workflow("fails", fails).unwrap();
    dbos.launch().await.expect("launch failed");

    // The caller that ran it gets the application's error *object* back, not a string round-tripped
    // through the database. Only a caller that adopts someone else's run reads back a
    // `WorkflowFailed` built from the recorded message, because by then the object is gone.
    let err = fails.run(()).await.unwrap_err();
    assert!(matches!(err, Error::Application(_)), "{err}");
    assert_eq!(err.to_string(), "the card was declined");
    assert!(
        err.source().is_none(),
        "transparent forwards the application's own source chain, and it has none"
    );

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(rows[0].status, WorkflowStatus::Error);
    assert_eq!(
        rows[0].error.as_deref(),
        Some("the card was declined"),
        "the application's message, not a DBOS wrapper around it"
    );

    dbos.shutdown().await;
}

/// The workflow is recorded before its body runs, which is what makes a crash recoverable.
#[tokio::test]
async fn the_row_exists_before_the_body_starts() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("early-row-app", &db));
    let blocked = {
        let (started, release) = (Arc::clone(&started), Arc::clone(&release));
        dbos.register_workflow("blocked", move |()| {
            let (started, release) = (Arc::clone(&started), Arc::clone(&release));
            async move {
                started.notify_one();
                release.notified().await;
                dbos::Result::Ok(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let running = tokio::spawn(async move { blocked.run(()).await });
    started.notified().await;

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(
        rows.len(),
        1,
        "the row is written before the body is entered"
    );
    assert_eq!(rows[0].status, WorkflowStatus::Pending);

    release.notify_one();
    running
        .await
        .expect("the task panicked")
        .expect("the workflow failed");
    dbos.shutdown().await;
}

/// Shutdown cancels what is still running and leaves it PENDING for a later executor.
#[tokio::test]
async fn shutdown_cancels_a_running_workflow_and_leaves_it_pending() {
    let started = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(AtomicU32::new(0));

    let db = test_database().await;
    let dbos = DBOS::new(config("cancel-app", &db));
    let forever = {
        let (started, finished) = (Arc::clone(&started), Arc::clone(&finished));
        dbos.register_workflow("forever", move |()| {
            let (started, finished) = (Arc::clone(&started), Arc::clone(&finished));
            async move {
                started.notify_one();
                tokio::time::sleep(Duration::from_secs(3600)).await;
                finished.fetch_add(1, Ordering::SeqCst);
                dbos::Result::Ok(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let running = tokio::spawn(async move { forever.run(()).await });
    started.notified().await;

    dbos.shutdown().await;

    let err = running.await.expect("the task panicked").unwrap_err();
    assert!(matches!(err, Error::Interrupted { .. }), "{err}");
    assert_eq!(
        finished.load(Ordering::SeqCst),
        0,
        "the body never got past the sleep"
    );

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(
        rows[0].status,
        WorkflowStatus::Pending,
        "left PENDING on purpose: a later executor recovers it, so nothing is lost"
    );

    dbos.shutdown().await;
}

/// Dropping the returned future does not stop the workflow — awaiting is a convenience over a
/// durable run, not the thing that makes it durable.
#[tokio::test]
async fn dropping_the_future_does_not_stop_the_workflow() {
    let started = Arc::new(tokio::sync::Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("detached-app", &db));
    let slow = {
        let started = Arc::clone(&started);
        dbos.register_workflow("slow", move |()| {
            let started = Arc::clone(&started);
            async move {
                started.notify_one();
                tokio::time::sleep(Duration::from_millis(200)).await;
                dbos::Result::Ok(7u32)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    {
        let mut future = Box::pin(slow.run(()));
        // Poll it far enough to start the workflow, then throw the future away.
        tokio::select! {
            _ = &mut future => panic!("it should not have finished yet"),
            () = started.notified() => {}
        }
    }

    let reader = reader(&db).await;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rows = reader
            .list_workflows(&Default::default())
            .await
            .expect("read failed");
        if rows[0].status == WorkflowStatus::Success {
            assert_eq!(rows[0].output.as_deref(), Some("7"));
            dbos.shutdown().await;
            return;
        }
    }
    panic!("the workflow never finished after its caller stopped waiting");
}

/// A workflow started against an instance that is not launched is an error, not a panic.
#[tokio::test]
async fn running_before_launch_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("unlaunched-app", &db));
    let double = dbos.register_workflow("double", double).unwrap();

    let err = double.run(1).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::NotLaunched {
                operation: "run a workflow"
            }
        ),
        "{err}"
    );
}
