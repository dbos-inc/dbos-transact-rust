//! Queue registration and enqueueing, against real databases.
//!
//! Nothing here dequeues — the runner arrives with the next commit — so these assert the row and
//! the handle, which is exactly what a workflow left on a queue *is* until someone polls for it.

use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error, QueueConflict, QueueOptions, RunOptions, StartOptions, Timeout};

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

/// A registered queue is a row, and the handle reports what the row holds.
#[tokio::test]
async fn registering_a_queue_writes_a_row() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-register-app", &db));
    dbos.launch().await.expect("launch failed");

    let queue = dbos
        .register_queue(
            "demo-queue",
            QueueOptions {
                worker_concurrency: Some(3),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert_eq!(queue.name(), "demo-queue");
    assert_eq!(queue.worker_concurrency(), Some(3));
    assert_eq!(
        queue.concurrency(),
        None,
        "no fleet-wide limit was asked for"
    );
    assert_eq!(queue.polling_interval(), Duration::from_secs(1));

    let row = reader(&db)
        .await
        .get_queue("demo-queue")
        .await
        .expect("read failed")
        .expect("the queue has no row");
    assert_eq!(row.worker_concurrency, Some(3));
    assert_eq!(
        row.application_name.as_deref(),
        Some("queue-register-app"),
        "the queue belongs to the application that registered it"
    );

    dbos.shutdown().await;
}

/// Registering the same queue again updates it — this process is running the latest version, which
/// is the default policy's whole question.
#[tokio::test]
async fn re_registering_updates_the_stored_limits() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-update-app", &db));
    dbos.launch().await.expect("launch failed");

    dbos.register_queue(
        "demo-queue",
        QueueOptions {
            worker_concurrency: Some(3),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    let updated = dbos
        .register_queue(
            "demo-queue",
            QueueOptions {
                worker_concurrency: Some(7),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("re-registration failed");
    assert_eq!(updated.worker_concurrency(), Some(7));

    dbos.shutdown().await;
}

/// `NeverUpdate` leaves the stored limits alone, and reports them rather than what was asked for.
///
/// The read-back is the point: a caller that declined to overwrite still needs to know what its
/// dequeues will actually honour, which is the row and not the request.
#[tokio::test]
async fn declining_to_update_reports_what_is_stored() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-leave-app", &db));
    dbos.launch().await.expect("launch failed");

    dbos.register_queue(
        "demo-queue",
        QueueOptions {
            worker_concurrency: Some(3),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    let second = dbos
        .register_queue(
            "demo-queue",
            QueueOptions {
                worker_concurrency: Some(99),
                on_conflict: QueueConflict::NeverUpdate,
                ..QueueOptions::default()
            },
        )
        .await
        .expect("re-registration failed");
    assert_eq!(
        second.worker_concurrency(),
        Some(3),
        "the stored limit stands, and is what is reported"
    );

    dbos.shutdown().await;
}

/// The engine's internal queue is not a queue anybody registers.
#[tokio::test]
async fn the_internal_queue_name_is_reserved() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-reserved-app", &db));
    dbos.launch().await.expect("launch failed");

    let error = dbos
        .register_queue("_dbos_internal_queue", QueueOptions::default())
        .await
        .expect_err("the reserved name was accepted");
    assert!(
        matches!(&error, Error::Config(message) if message.contains("reserved")),
        "expected a configuration refusal, got {error:?}"
    );

    dbos.shutdown().await;
}

/// A configuration that cannot mean anything is refused before it reaches the row.
///
/// The zeroes matter more than they look: the dequeue clamps its budgets with `.max(0)`, so a
/// limit of nothing that got stored would read back as a queue that never dequeues, with nothing
/// anywhere saying why.
#[tokio::test]
async fn incoherent_limits_are_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-invalid-app", &db));
    dbos.launch().await.expect("launch failed");

    let cases = [
        (
            "a fleet-wide limit of nothing",
            QueueOptions {
                concurrency: Some(0),
                ..QueueOptions::default()
            },
            "`concurrency` must be at least 1",
        ),
        (
            "a per-process limit of nothing",
            QueueOptions {
                worker_concurrency: Some(0),
                ..QueueOptions::default()
            },
            "`worker_concurrency` must be at least 1",
        ),
        (
            "a negative limit",
            QueueOptions {
                worker_concurrency: Some(-1),
                ..QueueOptions::default()
            },
            "`worker_concurrency` must be at least 1",
        ),
        (
            "one process allowed more than the whole fleet",
            QueueOptions {
                concurrency: Some(2),
                worker_concurrency: Some(3),
                ..QueueOptions::default()
            },
            "must be greater than or equal to `worker_concurrency`",
        ),
        (
            "a queue polled continuously",
            QueueOptions {
                polling_interval: Duration::ZERO,
                ..QueueOptions::default()
            },
            "`polling_interval` cannot be zero",
        ),
    ];

    for (what, options, expected) in cases {
        let error = match dbos.register_queue("demo-queue", options).await {
            Ok(_) => panic!("{what} was accepted"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, Error::Config(message) if message.contains(expected)),
            "{what}: expected a refusal mentioning {expected:?}, got {error:?}"
        );
    }

    assert!(
        reader(&db)
            .await
            .get_queue("demo-queue")
            .await
            .expect("read failed")
            .is_none(),
        "a refused registration wrote a row anyway"
    );

    dbos.shutdown().await;
}

/// Equal limits are coherent: one process may run the whole fleet's allowance.
#[tokio::test]
async fn a_per_process_limit_may_equal_the_fleet_limit() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-equal-limits-app", &db));
    dbos.launch().await.expect("launch failed");

    let queue = dbos
        .register_queue(
            "demo-queue",
            QueueOptions {
                concurrency: Some(3),
                worker_concurrency: Some(3),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert_eq!(queue.concurrency(), Some(3));
    assert_eq!(queue.worker_concurrency(), Some(3));

    dbos.shutdown().await;
}

/// Registering a queue needs a launched instance, because it is a write.
#[tokio::test]
async fn registering_before_launch_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-unlaunched-app", &db));

    let error = dbos
        .register_queue("demo-queue", QueueOptions::default())
        .await
        .expect_err("an unlaunched instance registered a queue");
    assert!(
        matches!(error, Error::NotLaunched { .. }),
        "expected a not-launched refusal, got {error:?}"
    );
}

/// An enqueued workflow is recorded and **not run**: the row says `ENQUEUED`, and its body never
/// enters.
#[tokio::test]
async fn an_enqueued_workflow_is_recorded_and_not_started() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-app", &db));
    let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&ran);
    let workflow = dbos
        .register_workflow("queued", move |()| {
            let flag = std::sync::Arc::clone(&flag);
            async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<u32, Error>(1)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let id = "left-on-the-queue";
    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some("demo-queue"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.workflow_id(), id);

    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Enqueued);
    assert_eq!(row.queue_name.as_deref(), Some("demo-queue"));

    // Nothing dequeues yet, and nothing should have run it here.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "an enqueued workflow ran in the process that enqueued it"
    );

    dbos.shutdown().await;
}

/// **A queued workflow's budget becomes a deadline on dequeue, not at enqueue.**
///
/// The timeout is recorded and the deadline left null, so a workflow that waits an hour in a queue
/// still gets the whole budget when it finally runs. Python and TypeScript branch on the queue in
/// the same place.
#[tokio::test]
async fn an_explicit_timeout_on_a_queued_workflow_records_no_deadline_yet() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-timeout-app", &db));
    let workflow = dbos
        .register_workflow("queued", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let id = "queued-with-a-budget";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some("demo-queue"),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
            },
        )
        .await
        .expect("enqueue failed");

    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.timeout,
        Some(Duration::from_secs(300)),
        "the budget is recorded"
    );
    assert!(
        row.deadline.is_none(),
        "the deadline waits for the dequeue that starts the clock"
    );

    dbos.shutdown().await;
}

/// An **inherited** deadline reaches a queued child, unlike an explicit budget.
///
/// The two differ in what they mean: a budget promises how long the *work* may take, so the queue
/// wait cannot count against it, while an inherited deadline is an instant the parent is already
/// bound by — and a child does not escape it by being queued. Python returns the propagated
/// deadline whether or not there is a queue.
#[tokio::test]
async fn an_inherited_deadline_reaches_a_queued_child() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-inherit-app", &db));
    let child = dbos
        .register_workflow("child", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |()| {
            let child = child.clone();
            async move {
                child
                    .start_with(
                        (),
                        StartOptions {
                            queue: Some("demo-queue"),
                            ..StartOptions::default()
                        },
                    )
                    .await
                    .map_err(Error::lift)?;
                Ok::<u32, Error>(0)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let id = "queues-its-child";
    parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
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
    assert_eq!(child_row.status, WorkflowStatus::Enqueued);
    assert_eq!(
        child_row.deadline,
        Some(parent_deadline),
        "the child is bound by the instant its parent is bound by, queued or not"
    );
    assert!(
        child_row.timeout.is_none(),
        "it inherited an instant, not a budget"
    );

    dbos.shutdown().await;
}
