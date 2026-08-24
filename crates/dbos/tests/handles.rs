//! Non-blocking starts and the workflow handle, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);

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

/// The demo's `POST /workflow/:taskid`, posted twice: the id is an idempotency key, so the second
/// start joins the run already going instead of failing — and both callers get the same answer.
#[tokio::test]
async fn starting_a_taken_id_joins_the_existing_run() {
    let entered = Arc::new(AtomicU32::new(0));
    let release = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("join-app", &db));
    let slow = {
        let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
        dbos.register_workflow("slow", move |()| {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok::<_, dbos::Error>(7u32)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let options = || StartOptions {
        workflow_id: Some("task-42"),
        ..Default::default()
    };
    // Returns at once, before the workflow finishes: it is blocked at the gate.
    let first = slow.start_with((), options()).await.expect("start failed");
    assert_eq!(first.workflow_id(), "task-42", "the id is the caller's");
    assert_eq!(first.status().await.expect("status failed"), {
        WorkflowStatus::Pending
    });

    // The double-click: same id while the first run is still going.
    let second = slow.start_with((), options()).await.expect("start failed");
    assert_eq!(second.workflow_id(), "task-42");

    release.notify_one();
    let results = tokio::time::timeout(DEADLINE, async {
        (first.result().await, second.result().await)
    })
    .await
    .expect("the handles never resolved");
    assert_eq!(results.0.expect("the first caller failed"), 7);
    assert_eq!(results.1.expect("the joining caller failed"), 7);
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "one id, one execution — the second start ran nothing"
    );

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let [row] = &rows[..] else {
        panic!("expected exactly one workflow, got {}", rows.len())
    };
    assert_eq!(row.workflow_id, "task-42");
    assert_eq!(row.status, WorkflowStatus::Success);

    dbos.shutdown().await;
}

/// A polling handle decodes a recorded failure with the same fidelity the local one does: the
/// workflow's own error type, fields and all, out of a row this handle never watched being
/// written.
#[tokio::test]
async fn a_polling_handle_returns_the_typed_error_the_run_recorded() {
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    enum CheckoutError {
        #[error("the card was declined after {attempts} attempts")]
        CardDeclined { attempts: u32 },
    }

    async fn checkout(_: ()) -> dbos::Result<u32, CheckoutError> {
        Err(CheckoutError::CardDeclined { attempts: 3 })?
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("polling-app", &db));
    let checkout = dbos.register_workflow("checkout", checkout).unwrap();
    dbos.launch().await.expect("launch failed");

    let options = || StartOptions {
        workflow_id: Some("order-1"),
        ..Default::default()
    };
    let err = checkout
        .run_with((), options())
        .await
        .expect_err("the checkout must fail");
    assert!(
        matches!(
            err,
            Error::Application(CheckoutError::CardDeclined { attempts: 3 })
        ),
        "{err:?}"
    );

    // The same id again, after the run finished: this start owns nothing and polls the row.
    let joined = checkout
        .start_with((), options())
        .await
        .expect("start failed");
    assert_eq!(
        joined.status().await.expect("status failed"),
        WorkflowStatus::Error
    );
    let err = joined
        .result()
        .await
        .expect_err("the row records a failure");
    assert!(
        matches!(
            err,
            Error::Application(CheckoutError::CardDeclined { attempts: 3 })
        ),
        "the recorded error decodes whole through the polling path: {err:?}"
    );

    dbos.shutdown().await;
}

/// Dropping the handle stops the watching, not the workflow.
#[tokio::test]
async fn dropping_the_handle_does_not_stop_the_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("drop-app", &db));
    let quick = dbos
        .register_workflow("quick", |()| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, dbos::Error>(9u32)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = quick.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    drop(handle);

    let reader = reader(&db).await;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(&workflow_id)
                .await
                .expect("read failed")
                .expect("the row exists");
            if row.status == WorkflowStatus::Success {
                assert_eq!(row.output.as_deref(), Some("9"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the workflow never finished after its handle was dropped");

    dbos.shutdown().await;
}
