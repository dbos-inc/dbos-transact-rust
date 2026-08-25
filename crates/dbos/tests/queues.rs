//! Queue registration and enqueueing, against real databases.
//!
//! Nothing here dequeues — the runner arrives with the next commit — so these assert the row and
//! the handle, which is exactly what a workflow left on a queue *is* until someone polls for it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{NewQueue, OnExistingQueue, WorkflowStatus};
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
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

/// **The starter app's Queues tab, which is this slice's acceptance test.**
///
/// Register a queue with `worker_concurrency: 3`, enqueue five workflows that each hold for a
/// moment, and watch three run while two wait. The Go starter's tab says exactly this, and it is
/// the scenario the whole runner exists to serve.
#[tokio::test]
async fn worker_concurrency_bounds_what_one_process_runs_at_once() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-worker-concurrency-app", &db));

    // Counts concurrent bodies and remembers the high-water mark, which is the assertion.
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let workflow = dbos
        .register_workflow("held", {
            let live = Arc::clone(&live);
            let peak = Arc::clone(&peak);
            move |()| {
                let live = Arc::clone(&live);
                let peak = Arc::clone(&peak);
                async move {
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    live.fetch_sub(1, Ordering::SeqCst);
                    Ok::<u32, Error>(1)
                }
            }
        })
        .unwrap();
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

    let mut handles = Vec::new();
    for n in 0..5 {
        handles.push(
            workflow
                .start_with(
                    (),
                    StartOptions {
                        workflow_id: Some(&format!("fanned-out-{n}")),
                        queue: Some("demo-queue"),
                        ..StartOptions::default()
                    },
                )
                .await
                .expect("enqueue failed"),
        );
    }

    for handle in handles {
        let id = handle.workflow_id().to_owned();
        assert_eq!(
            handle.result().await.expect("the workflow failed"),
            1,
            "{id} did not produce its result"
        );
    }
    assert_eq!(
        peak.load(Ordering::SeqCst),
        3,
        "the queue ran {} at once against a worker_concurrency of 3",
        peak.load(Ordering::SeqCst)
    );

    dbos.shutdown().await;
}

/// A workflow left on a queue is dispatched by the runner, not by the process that enqueued it.
///
/// The enqueue records `ENQUEUED` and returns a polling handle — the caller may not be the process
/// that runs it — and the handle resolves once whichever executor dequeued it finishes.
#[tokio::test]
async fn an_enqueued_workflow_is_dispatched_by_the_runner() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-dequeue-app", &db));
    let workflow = dbos
        .register_workflow("queued", |()| async move { Ok::<u32, Error>(7) })
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
    assert_eq!(handle.result().await.expect("the workflow failed"), 7);

    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Success);
    assert_eq!(
        row.queue_name.as_deref(),
        Some("demo-queue"),
        "the row remembers the queue it came off"
    );

    dbos.shutdown().await;
}

/// A queue registered **after** launch is picked up without a restart.
///
/// The supervisor rebuilds its set from the table on every reconcile, which is the same mechanism
/// that lets a limit change at runtime. Without it, `register_queue` after launch would write a
/// row nothing ever polls.
#[tokio::test]
async fn a_queue_registered_after_launch_is_dequeued_from() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-late-registration-app", &db));
    let workflow = dbos
        .register_workflow("late", |()| async move { Ok::<u32, Error>(2) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // Long enough that the supervisor has already reconciled without this queue in the set.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    dbos.register_queue("late-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("enqueued-late"),
                queue: Some("late-queue"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 2);

    dbos.shutdown().await;
}

/// **A queued workflow's budget becomes a deadline on dequeue, not at enqueue.**
///
/// The timeout is recorded and the deadline left null, so a workflow that waits an hour in a queue
/// still gets the whole budget when it finally runs. Python and TypeScript branch on the queue in
/// the same place.
///
/// **Enqueued onto a queue nothing polls**, which is what makes the assertion stable: a queue name
/// is an address and needs no registration, so a name no runner has in its set leaves the row in
/// the state the enqueue wrote. The dequeue's half of this rule is
/// `a_dequeue_stamps_the_deadline_an_enqueue_left_open`.
#[tokio::test]
async fn an_explicit_timeout_on_a_queued_workflow_records_no_deadline_yet() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-timeout-app", &db));
    let workflow = dbos
        .register_workflow("queued", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "queued-with-a-budget";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some("unpolled-queue"),
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

/// **A queue this process never registered is still one it dequeues from.**
///
/// The consequence of every queue being a row: the worker set is rebuilt from the table, so what
/// this process called `register_queue` on has no bearing on what it polls. Here the row is
/// written straight to the database — no `register_queue` anywhere — and the workflow still runs.
///
/// That is the point of a fleet sharing a backlog, and it is why `listen_queues` exists as the
/// way to narrow it.
#[tokio::test]
async fn a_queue_this_process_never_registered_is_dequeued_from() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-unregistered-app", &db));
    let workflow = dbos
        .register_workflow(
            "unregistered-queue",
            |()| async move { Ok::<u32, Error>(5) },
        )
        .unwrap();

    // Written as this application's, but by something that is not this instance.
    reader(&db)
        .await
        .upsert_queue(
            &NewQueue {
                application_name: Some("queue-unregistered-app"),
                ..NewQueue::new("written-by-someone-else")
            },
            OnExistingQueue::Update,
        )
        .await
        .expect("could not write the queue row");

    dbos.launch().await.expect("launch failed");

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("on-a-queue-nobody-here-registered"),
                queue: Some("written-by-someone-else"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 5);

    dbos.shutdown().await;
}

/// **A stored row for the internal queue is ignored, not honoured.**
///
/// `register_queue` refuses the name, but nothing else does — every peer implementation's client
/// accepts it, and this crate's own `sysdb::upsert_queue` is public and unguarded. Honouring such
/// a row would let anything sharing the database throttle the queue `resume` and `fork` land on.
///
/// The row here asks for a five-minute polling interval. If it were honoured the workflow below
/// would not run for five minutes; the assertion is that it runs at once.
#[tokio::test]
async fn a_stored_row_cannot_redefine_the_internal_queue() {
    let db = test_database().await;
    let dbos = DBOS::new(config("internal-queue-row-app", &db));
    let workflow = dbos
        .register_workflow("internal", |()| async move { Ok::<u32, Error>(9) })
        .unwrap();

    reader(&db)
        .await
        .upsert_queue(
            &NewQueue {
                polling_interval: Duration::from_secs(300),
                worker_concurrency: Some(1),
                application_name: Some("internal-queue-row-app"),
                ..NewQueue::new(INTERNAL_QUEUE)
            },
            OnExistingQueue::Update,
        )
        .await
        .expect("could not write the queue row");

    dbos.launch().await.expect("launch failed");

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("on-the-internal-queue"),
                queue: Some(INTERNAL_QUEUE),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let ran = tokio::time::timeout(Duration::from_secs(20), handle.result())
        .await
        .expect("the internal queue took the stored row's polling interval")
        .expect("the workflow failed");
    assert_eq!(ran, 9);

    dbos.shutdown().await;
}

/// **But only within the application that owns it.** A queue owned by another application is not
/// this one's to poll.
///
/// `list_queues` scopes an unset search to `application_name = <this app> OR application_name IS
/// NULL`, so a peer application's queue never enters the worker set — taking its work would be
/// exactly the redirection that makes registering over a peer's queue name an error.
#[tokio::test]
async fn another_applications_queue_is_not_dequeued_from() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-owner-scope-app", &db));
    let ran = Arc::new(AtomicUsize::new(0));
    let workflow = dbos
        .register_workflow("scoped", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(1)
                }
            }
        })
        .unwrap();

    reader(&db)
        .await
        .upsert_queue(
            &NewQueue {
                application_name: Some("some-other-application"),
                ..NewQueue::new("belongs-to-a-peer")
            },
            OnExistingQueue::Update,
        )
        .await
        .expect("could not write the queue row");

    dbos.launch().await.expect("launch failed");

    let id = "enqueued-onto-a-peers-queue";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some("belongs-to-a-peer"),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    // Several reconciles' worth: if this queue were going to enter the set, it would have.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "this application ran a workflow from a queue it does not own"
    );
    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.status,
        WorkflowStatus::Enqueued,
        "the workflow left the queue it was enqueued on"
    );

    dbos.shutdown().await;
}

/// **The other half of the budget rule: the dequeue is what starts the clock.**
///
/// The claim statement carries
/// `workflow_deadline_epoch_ms = CASE WHEN workflow_timeout_ms IS NOT NULL AND
/// workflow_deadline_epoch_ms IS NULL THEN now + workflow_timeout_ms ELSE ... END`, which is the
/// arm the queued branch of the deadline rule reserved and had nothing to exercise it until a
/// runner existed. A budget survives the queue wait intact and becomes an instant when the
/// workflow is actually picked up.
#[tokio::test]
async fn a_dequeue_stamps_the_deadline_an_enqueue_left_open() {
    let db = test_database().await;
    let dbos = DBOS::new(config("dequeue-deadline-app", &db));
    let workflow = dbos
        .register_workflow("budgeted", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let id = "budget-starts-on-dequeue";
    let handle = workflow
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
    assert_eq!(handle.result().await.expect("the workflow failed"), 1);

    let row = reader(&db)
        .await
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        row.timeout,
        Some(Duration::from_secs(300)),
        "the budget is still what was asked for"
    );
    assert!(
        row.deadline.is_some(),
        "the dequeue left the budget without an expiry"
    );

    dbos.shutdown().await;
}

/// An **inherited** deadline reaches a queued child, unlike an explicit budget.
///
/// The two differ in what they mean: a budget promises how long the *work* may take, so the queue
/// wait cannot count against it, while an inherited deadline is an instant the parent is already
/// bound by — and a child does not escape it by being queued. Python returns the propagated
/// deadline whether or not there is a queue.
///
/// The child is enqueued onto a queue nothing polls, for the reason
/// `an_explicit_timeout_on_a_queued_workflow_records_no_deadline_yet` gives: the assertion is
/// about what the enqueue wrote, so the row has to stay as the enqueue left it.
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
                            queue: Some("unpolled-queue"),
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
