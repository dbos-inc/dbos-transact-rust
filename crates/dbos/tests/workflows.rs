//! Running workflows, against real databases.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::sysdb::{BackendError, BackendErrorKind};
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

/// An application's own error type survives the database intact.
///
/// This is the point of the error being a type parameter: `CardDeclined` comes back as
/// `CardDeclined`, matchable, rather than as a name and a message describing it.
#[tokio::test]
async fn an_application_error_type_round_trips_as_itself() {
    // Two derives and nothing else. No variant holding a `dbos::Error`, no `From` impl, no trait
    // to implement: the engine wraps this type rather than this type making room for the engine.
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    enum CheckoutError {
        #[error("the card was declined after {attempts} attempts")]
        CardDeclined { attempts: u32 },
    }

    async fn checkout(_: ()) -> dbos::Result<u32, CheckoutError> {
        // `?` lifts the application's error into ours through the blanket conversion the type
        // parameter buys.
        Err(CheckoutError::CardDeclined { attempts: 3 })?
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("typed-error-app", &db));
    let checkout = dbos.register_workflow("checkout", checkout).unwrap();
    dbos.launch().await.expect("launch failed");

    // The run that failed gets its own error, structure and all.
    let err = checkout.run(()).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Application(CheckoutError::CardDeclined { attempts: 3 })
        ),
        "{err:?}"
    );

    // And so does a caller adopting the finished workflow, which reads it back out of the column.
    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(rows[0].status, WorkflowStatus::Error);
    let recorded: Error<CheckoutError> =
        serde_json::from_str(rows[0].error.as_deref().expect("an error was recorded"))
            .expect("the column holds the application's own error");
    assert!(
        matches!(
            recorded,
            Error::Application(CheckoutError::CardDeclined { attempts: 3 })
        ),
        "the field survives, not just the message: {recorded:?}"
    );

    dbos.shutdown().await;
}

/// A workflow converts a foreign error at the boundary, because a durable one must serialize.
///
/// The case a library error lands in: a third party's type has no serde derives and cannot cross a
/// column, so it becomes the workflow's own error where the two meet. One `map_err` at the call
/// site, and everything downstream — the row, the replay, the caller — gets full fidelity.
#[tokio::test]
async fn a_foreign_error_is_converted_at_the_boundary() {
    // Somebody else's error type: `Debug + Display` and nothing more.
    #[derive(Debug, thiserror::Error)]
    #[error("the gateway refused the card")]
    struct GatewayRefused;

    async fn charge() -> Result<(), GatewayRefused> {
        Err(GatewayRefused)
    }

    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    enum PaymentError {
        #[error("charging failed: {reason}")]
        Gateway { reason: String },
    }

    async fn pay(_: ()) -> dbos::Result<(), PaymentError> {
        charge().await.map_err(|e| PaymentError::Gateway {
            reason: e.to_string(),
        })?;
        Ok(())
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("fail-app", &db));
    let pay = dbos.register_workflow("pay", pay).unwrap();
    dbos.launch().await.expect("launch failed");

    let err = pay.run(()).await.unwrap_err();
    assert!(
        matches!(err, Error::Application(PaymentError::Gateway { .. })),
        "{err}"
    );
    assert_eq!(
        err.to_string(),
        "charging failed: the gateway refused the card"
    );

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(rows[0].status, WorkflowStatus::Error);
    // The column holds the failure encoded, not stringified, so what comes back out of it is the
    // error itself rather than a description of it.
    let recorded: Error<PaymentError> =
        serde_json::from_str(rows[0].error.as_deref().expect("an error was recorded"))
            .expect("the error column holds an encoded error");
    assert!(matches!(
        recorded,
        Error::Application(PaymentError::Gateway { .. })
    ));
    assert_eq!(recorded.to_string(), err.to_string());

    dbos.shutdown().await;
}

/// A database failure leaves the workflow `PENDING` instead of recording it as failed.
///
/// The blip case: a connection drops while the engine checkpoints, the error propagates out
/// through the workflow body, and the workflow had not failed — the database had. Recording ERROR
/// would assert something false about work that could still be finished, and the row is the only
/// copy of that fact.
#[tokio::test]
async fn a_database_failure_is_not_the_workflows_outcome() {
    async fn blips(_: ()) -> dbos::Result<()> {
        Err(Error::SystemDatabase(dbos::sysdb::Error::Backend(
            BackendError {
                message: "connection reset by peer".to_owned(),
                sqlstate: None,
                kind: BackendErrorKind::Connection,
            },
        )))
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("blip-app", &db));
    let blips = dbos.register_workflow("blips", blips).unwrap();
    dbos.launch().await.expect("launch failed");

    let err = blips.run(()).await.unwrap_err();
    assert!(matches!(err, Error::SystemDatabase(_)), "{err}");

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(
        rows[0].status,
        WorkflowStatus::Pending,
        "left for a later executor to recover, not recorded as failed"
    );
    assert!(
        rows[0].error.is_none(),
        "nothing terminal was written: {:?}",
        rows[0].error
    );

    dbos.shutdown().await;
}

/// Steps inside a real `run` are checkpointed under the workflow that ran them.
#[tokio::test]
async fn a_workflow_records_the_steps_it_took() {
    async fn three_steps(_: ()) -> dbos::Result<u32> {
        let mut total = 0;
        for (n, name) in ["one", "two", "three"].into_iter().enumerate() {
            total += dbos::step(name, || async move { dbos::Result::Ok(n as u32 + 1) }).await?;
        }
        Ok(total)
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("steps-app", &db));
    let workflow = dbos.register_workflow("three_steps", three_steps).unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(workflow.run(()).await.expect("the workflow failed"), 6);

    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let steps = reader
        .list_workflow_steps(&rows[0].workflow_id, true, None, None)
        .await
        .expect("read failed");
    let seen: Vec<(i32, &str, Option<&str>)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str(), s.output.as_deref()))
        .collect();
    assert_eq!(
        seen,
        [
            (0, "one", Some("1")),
            (1, "two", Some("2")),
            (2, "three", Some("3"))
        ]
    );

    dbos.shutdown().await;
}

/// A step's error type round-trips the same way, so a replay resumes with the error it recorded.
#[tokio::test]
async fn a_step_error_type_round_trips_as_itself() {
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    enum ChargeError {
        #[error("insufficient funds: short by {short}")]
        InsufficientFunds { short: u32 },
    }

    async fn pay(_: ()) -> dbos::Result<u32, ChargeError> {
        // A step shares the workflow's error channel, so the body needs no type of its own.
        dbos::step("charge", || async {
            Err(ChargeError::InsufficientFunds { short: 12 })?
        })
        .await
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("step-error-app", &db));
    let pay = dbos.register_workflow("pay", pay).unwrap();
    dbos.launch().await.expect("launch failed");

    let err = pay.run(()).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Application(ChargeError::InsufficientFunds { short: 12 })
        ),
        "{err:?}"
    );

    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let steps = reader
        .list_workflow_steps(&rows[0].workflow_id, true, None, None)
        .await
        .expect("read failed");
    let recorded: Error<ChargeError> = serde_json::from_str(
        steps[0]
            .error
            .as_deref()
            .expect("the step recorded an error"),
    )
    .expect("the column holds the application's own error");
    assert!(
        matches!(
            recorded,
            Error::Application(ChargeError::InsufficientFunds { short: 12 })
        ),
        "{recorded:?}"
    );

    dbos.shutdown().await;
}

/// A panic is not an outcome: nothing is recorded, and an awaiting caller gets the panic back.
///
/// The row staying PENDING is the point — a panic is treated as a crash, not a failure, so a
/// later executor recovers the workflow instead of it being permanently failed by a bug.
#[tokio::test]
async fn a_panicking_workflow_leaves_its_row_pending() {
    async fn explodes(_: ()) -> dbos::Result<()> {
        panic!("a bug, not an outcome")
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("panic-app", &db));
    let explodes = dbos.register_workflow("explodes", explodes).unwrap();
    dbos.launch().await.expect("launch failed");

    // On its own task, because the panic is re-raised in the awaiting caller.
    let joined = tokio::spawn(async move { explodes.run(()).await }).await;
    let panicked = joined.expect_err("the panic must reach the awaiting caller");
    assert!(panicked.is_panic(), "{panicked}");

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    assert_eq!(
        rows[0].status,
        WorkflowStatus::Pending,
        "a panic records nothing: the row is left for recovery, like a crash"
    );
    assert!(rows[0].error.is_none(), "{:?}", rows[0].error);

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
                Ok::<_, dbos::Error>(())
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
                Ok::<_, dbos::Error>(())
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
                Ok::<_, dbos::Error>(7u32)
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

    // `run` is `start` plus awaiting, so the refusal names the start — the half that was refused.
    let err = double.run(1).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::NotLaunched {
                operation: Cow::Borrowed("start a workflow")
            }
        ),
        "{err}"
    );
}
