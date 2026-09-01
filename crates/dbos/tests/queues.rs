//! Queue registration and enqueueing, against real databases.
//!
//! Nothing here dequeues — the runner arrives with the next commit — so these assert the row and
//! the handle, which is exactly what a workflow left on a queue *is* until someone polls for it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{NewQueue, OnExistingQueue, WorkflowStatus};
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
use dbos::{
    Change, Config, DBOS, DuplicationPolicy, Enqueue, Error, QueueChange, QueueConflict,
    QueueOptions, RateLimit, RunOptions, StartOptions, Timeout,
};

use dbos_test_support::{TestDatabase, test_database};

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
                        queue: Some(Enqueue::new("demo-queue")),
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
                queue: Some(Enqueue::new("demo-queue")),
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
                queue: Some(Enqueue::new("late-queue")),
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
                queue: Some(Enqueue::new("unpolled-queue")),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
                ..Default::default()
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
                queue: Some(Enqueue::new("written-by-someone-else")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 5);

    dbos.shutdown().await;
}

/// **`listen_queues` narrows what this process dequeues from, and nothing else changes.**
///
/// Both queues have rows and both hold work; only the listened one is drained. The other's
/// workflow stays `ENQUEUED` for a peer that does listen to it — which is the point, and why this
/// is a split of one application's fleet rather than a way to disable a queue.
#[tokio::test]
async fn listen_queues_narrows_what_this_process_dequeues() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        listen_queues: Some(vec!["fast".to_owned()]),
        ..config("listen-queues-app", &db)
    });
    let workflow = dbos
        .register_workflow("either", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    for name in ["fast", "slow"] {
        dbos.register_queue(name, QueueOptions::default())
            .await
            .expect("registration failed");
    }

    let listened = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("on-the-listened-queue"),
                queue: Some(Enqueue::new("fast")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("on-the-other-queue"),
                queue: Some(Enqueue::new("slow")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    assert_eq!(listened.result().await.expect("the workflow failed"), 1);

    // By now several sweeps have run; the unlistened queue has not been touched.
    assert_eq!(
        reader(&db)
            .await
            .get_workflow("on-the-other-queue")
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Enqueued,
        "a queue this process does not listen to was drained anyway"
    );

    dbos.shutdown().await;
}

/// The internal queue is dequeued from whatever `listen_queues` says.
///
/// `resume`, `fork` and recovery all put work there, so a filter that excluded it would strand
/// them silently. Here the filter names one unrelated queue, and a workflow enqueued onto the
/// internal queue still runs.
#[tokio::test]
async fn listen_queues_never_excludes_the_internal_queue() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        listen_queues: Some(vec!["something-else".to_owned()]),
        ..config("listen-internal-app", &db)
    });
    let workflow = dbos
        .register_workflow("internal", |()| async move { Ok::<u32, Error>(4) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("internal-under-a-filter"),
                queue: Some(Enqueue::new(INTERNAL_QUEUE)),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 4);

    dbos.shutdown().await;
}

/// An empty list listens to nothing, which is a setting rather than a mistake.
///
/// Distinct from `None`, which is every queue. Go's empty set means "all"; Rust's `Option` carries
/// that meaning instead, so an empty slice can mean what it says. The internal queue still runs,
/// as it always does.
#[tokio::test]
async fn an_empty_listen_set_dequeues_from_no_registered_queue() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        listen_queues: Some(Vec::new()),
        ..config("listen-none-app", &db)
    });
    let workflow = dbos
        .register_workflow("nothing", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("ignored", QueueOptions::default())
        .await
        .expect("registration failed");

    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("never-drained"),
                queue: Some(Enqueue::new("ignored")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    // The internal queue proves the loop is running at all, rather than the assertion below
    // passing because nothing works.
    let internal = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("still-internal"),
                queue: Some(Enqueue::new(INTERNAL_QUEUE)),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(internal.result().await.expect("the workflow failed"), 1);

    assert_eq!(
        reader(&db)
            .await
            .get_workflow("never-drained")
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Enqueued,
        "an empty listen set drained a registered queue"
    );

    dbos.shutdown().await;
}

/// **A limit changed at runtime takes effect without a restart, which is the tab's whole point.**
///
/// The queue starts with `worker_concurrency: 1`, so a fan-out runs one at a time. Raising it to
/// three mid-flight is picked up by the worker on its next pass, and the rest run three at once.
#[tokio::test]
async fn updating_a_queue_changes_what_a_running_worker_honours() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-update-limits-app", &db));
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
                    tokio::time::sleep(Duration::from_millis(600)).await;
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
            worker_concurrency: Some(1),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    let mut handles = Vec::new();
    for n in 0..6 {
        handles.push(
            workflow
                .start_with(
                    (),
                    StartOptions {
                        workflow_id: Some(&format!("fanned-{n}")),
                        queue: Some(Enqueue::new("demo-queue")),
                        ..StartOptions::default()
                    },
                )
                .await
                .expect("enqueue failed"),
        );
    }

    // One at a time to begin with.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "the queue exceeded a worker_concurrency of 1"
    );

    let updated = dbos
        .update_queue(
            "demo-queue",
            QueueChange {
                worker_concurrency: Change::Set(Some(3)),
                ..QueueChange::default()
            },
        )
        .await
        .expect("update failed");
    assert_eq!(updated.worker_concurrency(), Some(3));

    for handle in handles {
        handle.result().await.expect("the workflow failed");
    }
    assert_eq!(
        peak.load(Ordering::SeqCst),
        3,
        "the raised limit was not picked up by the running worker"
    );

    dbos.shutdown().await;
}

/// An update is validated as a whole, against what is already stored.
///
/// `worker_concurrency` on its own is always fine; it is only wrong beside the
/// `concurrency` the row already holds, which is why the merged result is what gets checked.
#[tokio::test]
async fn an_update_cannot_leave_a_queue_incoherent() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-update-invalid-app", &db));
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "demo-queue",
        QueueOptions {
            concurrency: Some(2),
            worker_concurrency: Some(2),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    let error = dbos
        .update_queue(
            "demo-queue",
            QueueChange {
                worker_concurrency: Change::Set(Some(5)),
                ..QueueChange::default()
            },
        )
        .await
        .expect_err("an incoherent update was accepted");
    assert!(
        matches!(&error, Error::Config(message)
            if message.contains("must be greater than or equal to `worker_concurrency`")),
        "expected a refusal about the pair, got {error:?}"
    );
    assert_eq!(
        dbos.queue("demo-queue")
            .await
            .expect("read failed")
            .expect("the queue is missing")
            .worker_concurrency(),
        Some(2),
        "the refused update was written anyway"
    );

    // Raising both together is coherent, and accepted.
    let updated = dbos
        .update_queue(
            "demo-queue",
            QueueChange {
                concurrency: Change::Set(Some(5)),
                worker_concurrency: Change::Set(Some(5)),
                ..QueueChange::default()
            },
        )
        .await
        .expect("a coherent update was refused");
    assert_eq!(updated.concurrency(), Some(5));

    dbos.shutdown().await;
}

/// Reading, listing and deleting a queue, and the internal queue's absence from all three.
#[tokio::test]
async fn the_queue_registry_can_be_read_and_deleted() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-registry-app", &db));
    dbos.launch().await.expect("launch failed");
    for name in ["alpha", "beta"] {
        dbos.register_queue(name, QueueOptions::default())
            .await
            .expect("registration failed");
    }

    assert!(
        dbos.queue("nothing-here")
            .await
            .expect("read failed")
            .is_none()
    );
    let mut names: Vec<String> = dbos
        .list_queues()
        .await
        .expect("list failed")
        .into_iter()
        .map(|queue| queue.name().to_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["alpha", "beta"],
        "the internal queue is not a queue anybody registered"
    );

    dbos.delete_queue("alpha").await.expect("delete failed");
    assert!(dbos.queue("alpha").await.expect("read failed").is_none());
    // Deleting what is not there is the end state asked for, not an error.
    dbos.delete_queue("alpha")
        .await
        .expect("second delete failed");

    for name in [INTERNAL_QUEUE] {
        assert!(
            matches!(
                dbos.delete_queue(name).await,
                Err(Error::Config(message)) if message.contains("reserved")
            ),
            "the internal queue was deletable"
        );
        assert!(
            matches!(
                dbos.update_queue(name, QueueChange::default()).await,
                Err(Error::Config(message)) if message.contains("reserved")
            ),
            "the internal queue was updatable"
        );
    }

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
                queue: Some(Enqueue::new(INTERNAL_QUEUE)),
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
                queue: Some(Enqueue::new("belongs-to-a-peer")),
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
                queue: Some(Enqueue::new("demo-queue")),
                timeout: Timeout::Explicit(Duration::from_secs(300)),
                ..Default::default()
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
                            queue: Some(Enqueue::new("unpolled-queue")),
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

/// **The four queue-only options are refused only where nesting could not rule them out.**
///
/// A delay, a priority, a deduplication id or a partition key without a queue is not a runtime
/// error here — it does not compile, because [`Enqueue`] owns them and there is no queue-less
/// value to hang them on. Go returns `InvalidOptionError` for each of those four
/// (`workflow.go:1178`–`1199`). What is left is the pair no shape can express.
#[tokio::test]
async fn an_incoherent_enqueue_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-validation-app", &db));
    let workflow = dbos
        .register_workflow("checked", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let cases = [
        (
            "a deduplication id beside a partition key",
            Enqueue {
                deduplication_id: Some("key"),
                partition_key: Some("shard-1"),
                ..Enqueue::new("demo-queue")
            },
            "`deduplication_id` and `partition_key` cannot both be set",
        ),
        (
            "the unprioritised sentinel spelled as a priority",
            Enqueue {
                priority: Some(0),
                ..Enqueue::new("demo-queue")
            },
            "`priority` must be at least 1",
        ),
        (
            "a priority past what the column holds",
            Enqueue {
                priority: Some(i32::MAX as u32 + 1),
                ..Enqueue::new("demo-queue")
            },
            "`priority` must be at most 2147483647",
        ),
        (
            "a policy for resolving collisions on a key that does not exist",
            Enqueue {
                duplication_policy: DuplicationPolicy::ReturnExisting,
                ..Enqueue::new("demo-queue")
            },
            "`DuplicationPolicy::ReturnExisting` needs a `deduplication_id`",
        ),
    ];

    for (what, enqueue, expected) in cases {
        let error = match workflow
            .start_with(
                (),
                StartOptions {
                    queue: Some(enqueue),
                    ..StartOptions::default()
                },
            )
            .await
        {
            Ok(_) => panic!("{what} was accepted"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, Error::Config(message) if message.contains(expected)),
            "{what}: expected a refusal mentioning {expected:?}, got {error:?}"
        );
    }

    // Refused before anything is written: a bad enqueue costs a round trip, not a row.
    assert!(
        reader(&db)
            .await
            .list_workflows(&dbos::sysdb::types::WorkflowFilter {
                queue_names: vec!["demo-queue"],
                ..Default::default()
            })
            .await
            .expect("list failed")
            .is_empty(),
        "a refused enqueue wrote a row anyway"
    );

    dbos.shutdown().await;
}

/// **A delay holds the workflow `DELAYED` until it expires, then the supervisor releases it.**
///
/// The status is not the caller's to choose — `NewWorkflow::initial_status` derives it from the
/// queue-and-delay pair — so what is asserted is that the row starts `DELAYED`, that nothing runs
/// it early, and that it still runs.
#[tokio::test]
async fn a_delayed_enqueue_waits_before_it_is_dequeued() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-delay-app", &db));
    let ran = Arc::new(AtomicUsize::new(0));
    let workflow = dbos
        .register_workflow("delayed", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(7)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("held-back"),
                queue: Some(Enqueue {
                    delay: Some(Duration::from_secs(3)),
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    assert_eq!(
        handle.status().await.expect("status failed"),
        WorkflowStatus::Delayed,
        "a delayed enqueue must not be ENQUEUED yet"
    );
    // Comfortably inside the delay, and after several supervisor sweeps: the row is not eligible,
    // so no worker may have taken it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "the workflow ran before its delay expired"
    );

    let result = tokio::time::timeout(Duration::from_secs(20), handle.result())
        .await
        .expect("the delayed workflow was never released")
        .expect("the workflow failed");
    assert_eq!(result, 7);

    dbos.shutdown().await;
}

/// **A deduplication id is unique among a queue's *waiting* workflows, and freed when one ends.**
///
/// The key deduplicates a backlog rather than a history, which is the half worth asserting: the
/// same key enqueued again after the first finished is a new workflow, not a duplicate.
#[tokio::test]
async fn a_deduplication_id_admits_one_waiting_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-dedup-app", &db));
    let workflow = dbos
        .register_workflow("deduped", |()| async move { Ok::<u32, Error>(3) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    // Delayed, so the first workflow is still holding the key when the second arrives rather than
    // racing the runner to finish before it.
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    let held = Enqueue {
        deduplication_id: Some("order-42"),
        delay: Some(Duration::from_secs(3)),
        ..Enqueue::new("demo-queue")
    };
    let first = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("dedup-first"),
                queue: Some(held.clone()),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the first enqueue failed");

    let error = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("dedup-second"),
                queue: Some(held),
                ..StartOptions::default()
            },
        )
        .await
        .expect_err("a second workflow took a held deduplication key");
    assert!(
        format!("{error}").contains("order-42"),
        "the refusal must name the key, got {error:?}"
    );

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(20), first.result())
            .await
            .expect("the first workflow never ran")
            .expect("the workflow failed"),
        3
    );

    // Finishing released the key, so the same one is enqueueable again.
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("dedup-third"),
                queue: Some(Enqueue {
                    deduplication_id: Some("order-42"),
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the key was not released when the holder finished");

    dbos.shutdown().await;
}

/// `DuplicationPolicy::ReturnExisting` joins the holder instead of refusing, and the join is idempotent.
///
/// The enqueue that loses the key does not write a row at all: it takes a handle to the workflow
/// that holds it, so a retried request waits on the first caller's workflow rather than being told
/// no. The key is released when the holder finishes, so the same key afterwards is a new workflow
/// — the same backlog-not-history rule the rejecting default follows.
#[tokio::test]
async fn return_existing_joins_the_workflow_holding_the_key() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-join-app", &db));
    let workflow = dbos
        .register_workflow("deduped", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    // Delayed, so the holder is still waiting when the second caller arrives.
    let joining = Enqueue {
        deduplication_id: Some("order-42"),
        delay: Some(Duration::from_secs(3)),
        duplication_policy: DuplicationPolicy::ReturnExisting,
        ..Enqueue::new("demo-queue")
    };
    let first = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("join-first"),
                queue: Some(joining.clone()),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the first enqueue failed");

    let second = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("join-second"),
                queue: Some(joining),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the second enqueue was refused rather than joined");
    assert_eq!(
        second.workflow_id(),
        "join-first",
        "the handle names the workflow holding the key, not the id this call offered",
    );

    let reader = reader(&db).await;
    assert!(
        reader
            .get_workflow("join-second")
            .await
            .expect("read failed")
            .is_none(),
        "the losing enqueue writes no row of its own",
    );

    for handle in [first, second] {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(20), handle.result())
                .await
                .expect("the workflow never ran")
                .expect("the workflow failed"),
            7,
            "both handles resolve to the one workflow that ran",
        );
    }

    // The holder has finished, so the key is free and the same policy claims it rather than joining.
    let third = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("join-third"),
                queue: Some(Enqueue {
                    deduplication_id: Some("order-42"),
                    duplication_policy: DuplicationPolicy::ReturnExisting,
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the released key was not claimable");
    assert_eq!(third.workflow_id(), "join-third");

    dbos.shutdown().await;
}

/// **Priority orders a queue's backlog, lower first, and the unprioritised sort ahead of all.**
///
/// One worker at a time, and every workflow enqueued before the runner can drain any of them, so
/// what is measured is the order the dequeue chose rather than the order they were submitted.
#[tokio::test]
async fn priority_orders_the_backlog_lower_first() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-priority-app", &db));
    let order: Arc<Mutex<Vec<String>>> = Arc::default();
    let workflow = dbos
        .register_workflow("ordered", {
            let order = Arc::clone(&order);
            move |name: String| {
                let order = Arc::clone(&order);
                async move {
                    order.lock().unwrap().push(name.clone());
                    Ok::<String, Error>(name)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // **Enqueued before the queue is registered, which is what holds the backlog back.** A worker
    // exists only for a queue that has a row, so until `register_queue` below there is nothing
    // polling `demo-queue` and the four rows accumulate untouched — otherwise the first one
    // enqueued is simply the first one available, whatever its priority.
    //
    // A delay on each enqueue was the previous way of arranging this, and it does not hold: the
    // delay is relative to its own enqueue, so four sequential enqueues get four deadlines
    // staggered by a round trip apiece, and the release sweep runs on its own second-granularity
    // tick. A tick landing inside that stagger releases the earliest-enqueued row on its own,
    // which then runs first however low its priority — reliably enough to fail on CockroachDB,
    // where the round trips are slow enough to widen the window.
    let submitted = [
        ("low", Some(9)),
        ("high", Some(1)),
        ("none", None),
        ("mid", Some(5)),
    ];
    let mut handles = Vec::new();
    for (name, priority) in submitted {
        handles.push(
            workflow
                .start_with(
                    name.to_owned(),
                    StartOptions {
                        workflow_id: Some(name),
                        queue: Some(Enqueue {
                            priority,
                            ..Enqueue::new("demo-queue")
                        }),
                        ..StartOptions::default()
                    },
                )
                .await
                .expect("enqueue failed"),
        );
    }

    // The backlog is complete, so registering the queue is what starts its worker: the supervisor
    // picks the row up on its next pass and every row is already there to be ranked.
    dbos.register_queue(
        "demo-queue",
        QueueOptions {
            worker_concurrency: Some(1),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(30), handle.result())
            .await
            .expect("a prioritised workflow never ran")
            .expect("the workflow failed");
    }

    assert_eq!(
        order.lock().unwrap().as_slice(),
        ["none", "high", "mid", "low"],
        "the backlog did not run in priority order"
    );

    dbos.shutdown().await;
}

/// **A partition key is recorded even on a queue that is not partitioned.**
///
/// The column is the workflow's, not the queue's, so it is written wherever the caller names one —
/// which matters because partitioning can be turned on later, and because a peer implementation
/// may own the queue. Nothing dequeues by it here: an unpartitioned queue's dequeue names no
/// partition, so this asserts the row rather than a run.
#[tokio::test]
async fn a_partition_key_is_recorded_on_the_row() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-partition-app", &db));
    let workflow = dbos
        .register_workflow("partitioned", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("sharded"),
                queue: Some(Enqueue {
                    partition_key: Some("tenant-7"),
                    priority: Some(4),
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let row = reader(&db)
        .await
        .get_workflow("sharded")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.queue_name.as_deref(), Some("demo-queue"));
    assert_eq!(row.queue_partition_key.as_deref(), Some("tenant-7"));
    assert_eq!(row.priority, 4);

    dbos.shutdown().await;
}

/// A workflow that names no priority stores the sentinel, which is what makes `None` mean
/// "unprioritised" rather than "priority zero" — and what an unqueued workflow stores too.
#[tokio::test]
async fn an_unprioritised_workflow_stores_the_sentinel() {
    let db = test_database().await;
    let dbos = DBOS::new(config("enqueue-sentinel-app", &db));
    let workflow = dbos
        .register_workflow("plain", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("demo-queue", QueueOptions::default())
        .await
        .expect("registration failed");

    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("no-priority"),
                queue: Some(Enqueue {
                    delay: Some(Duration::from_secs(30)),
                    ..Enqueue::new("demo-queue")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let row = reader(&db)
        .await
        .get_workflow("no-priority")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.priority, 0);
    assert_eq!(row.deduplication_id, None);
    assert_eq!(row.queue_partition_key, None);

    dbos.shutdown().await;
}
/// A rate limit and priority ordering are stored, reported, and changeable at runtime.
///
/// The dequeue already honoured both — `start_queued_workflows` counts a window's starts and
/// orders by priority — so what was missing was only the way to ask for them.
#[tokio::test]
async fn a_queue_carries_a_rate_limit_and_priority_ordering() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-limits-app", &db));
    dbos.launch().await.expect("launch failed");

    let limit = RateLimit {
        limit: 5,
        period: Duration::from_secs(30),
    };
    let queue = dbos
        .register_queue(
            "limited-queue",
            QueueOptions {
                rate_limit: Some(limit),
                priority_enabled: true,
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert_eq!(queue.rate_limit(), Some(limit));
    assert!(queue.priority_enabled());
    assert!(!queue.is_partitioned());

    // Changed at runtime, like every other limit: cleared, and priority turned back off.
    let updated = dbos
        .update_queue(
            "limited-queue",
            QueueChange {
                rate_limit: Change::Set(None),
                priority_enabled: Change::Set(false),
                ..QueueChange::default()
            },
        )
        .await
        .expect("update failed");
    assert_eq!(updated.rate_limit(), None);
    assert!(!updated.priority_enabled());

    dbos.shutdown().await;
}

/// A queue configuration no dequeue could honour is refused at registration.
///
/// The partitioned cases are the ones worth catching here rather than at poll time: the sweep
/// answers `InvalidInput` for each, and a worker polling once a second would turn that into an
/// error per tick for as long as the row existed.
#[tokio::test]
async fn an_unhonourable_queue_configuration_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-limit-validation-app", &db));
    dbos.launch().await.expect("launch failed");

    let cases = [
        (
            "a rate limit admitting nothing",
            QueueOptions {
                rate_limit: Some(RateLimit {
                    limit: 0,
                    period: Duration::from_secs(1),
                }),
                ..QueueOptions::default()
            },
            "`rate_limit.limit` must be at least 1",
        ),
        (
            "a rate limit over no window",
            QueueOptions {
                rate_limit: Some(RateLimit {
                    limit: 1,
                    period: Duration::ZERO,
                }),
                ..QueueOptions::default()
            },
            "`rate_limit.period` cannot be zero",
        ),
        (
            "no workflows at all per partition",
            QueueOptions {
                partition_concurrency: Some(0),
                ..QueueOptions::default()
            },
            "`partition_concurrency` must be at least 1",
        ),
        (
            "a per-partition rate limit over no window",
            QueueOptions {
                partition_rate_limit: Some(RateLimit {
                    limit: 1,
                    period: Duration::ZERO,
                }),
                ..QueueOptions::default()
            },
            "`partition_rate_limit.period` cannot be zero",
        ),
        (
            "a partition allowed more than the whole queue",
            QueueOptions {
                concurrency: Some(2),
                partition_concurrency: Some(4),
                ..QueueOptions::default()
            },
            "`concurrency` must be greater than or equal to `partition_concurrency`",
        ),
        (
            "a partition's worker limit above the partition's own",
            QueueOptions {
                partition_concurrency: Some(2),
                partition_worker_concurrency: Some(4),
                ..QueueOptions::default()
            },
            "`partition_concurrency` must be greater than or equal to \
             `partition_worker_concurrency`",
        ),
        (
            "a partition's worker limit above this process's own",
            QueueOptions {
                worker_concurrency: Some(2),
                partition_worker_concurrency: Some(4),
                ..QueueOptions::default()
            },
            "`worker_concurrency` must be greater than or equal to \
             `partition_worker_concurrency`",
        ),
        (
            "a partition allowed to start faster than the whole queue",
            QueueOptions {
                rate_limit: Some(RateLimit {
                    limit: 10,
                    period: Duration::from_secs(1),
                }),
                partition_rate_limit: Some(RateLimit {
                    limit: 100,
                    period: Duration::from_secs(1),
                }),
                ..QueueOptions::default()
            },
            "`rate_limit` must allow at least the rate `partition_rate_limit` does",
        ),
        (
            // The counts alone say the opposite — 5 is below 10 — so only comparing the two as
            // rates catches this one.
            "a partition faster than the queue over a different window",
            QueueOptions {
                rate_limit: Some(RateLimit {
                    limit: 10,
                    period: Duration::from_secs(60),
                }),
                partition_rate_limit: Some(RateLimit {
                    limit: 5,
                    period: Duration::from_secs(1),
                }),
                ..QueueOptions::default()
            },
            "`rate_limit` must allow at least the rate `partition_rate_limit` does",
        ),
    ];

    for (what, options, expected) in cases {
        let error = match dbos.register_queue("checked-queue", options).await {
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
            .get_queue("checked-queue")
            .await
            .expect("read failed")
            .is_none(),
        "a refused registration wrote a row anyway"
    );

    // The other side of the rate comparison: a larger count over a longer window is the *slower*
    // rate, and slower is what a per-partition limit is allowed to be. Registering it is the
    // assertion — the pair the counts would refuse is the pair the rates accept.
    dbos.register_queue(
        "checked-queue",
        QueueOptions {
            rate_limit: Some(RateLimit {
                limit: 10,
                period: Duration::from_secs(1),
            }),
            partition_rate_limit: Some(RateLimit {
                limit: 100,
                period: Duration::from_secs(60),
            }),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("a slower per-partition rate should be honoured");

    dbos.shutdown().await;
}

/// Per-partition limits sit beside the queue-wide ones, and partitioning is derived from them.
///
/// There is no `partition_queue` switch on this surface: setting a partition limit is the
/// statement that partitions exist, and the stored flag — which is what an implementation still
/// reading it sees — follows the limits in both directions.
#[tokio::test]
async fn per_partition_limits_partition_a_queue() {
    let db = test_database().await;
    let dbos = DBOS::new(config("queue-partition-limits-app", &db));
    dbos.launch().await.expect("launch failed");

    let queue = dbos
        .register_queue(
            "sharded",
            QueueOptions {
                concurrency: Some(60),
                worker_concurrency: Some(10),
                partition_concurrency: Some(4),
                partition_worker_concurrency: Some(2),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert!(queue.is_partitioned(), "a partition limit partitions it");
    assert_eq!(queue.concurrency(), Some(60), "both scopes are kept");
    assert_eq!(queue.partition_concurrency(), Some(4));
    assert_eq!(queue.partition_worker_concurrency(), Some(2));

    let stored = reader(&db)
        .await
        .get_queue("sharded")
        .await
        .expect("read failed")
        .expect("no row");
    assert!(
        stored.partition_queue,
        "the derived flag is written for implementations that still read it"
    );

    // Clearing the last partition limit un-partitions the queue, flag included.
    let updated = dbos
        .update_queue(
            "sharded",
            QueueChange {
                partition_concurrency: Change::Set(None),
                partition_worker_concurrency: Change::Set(None),
                ..QueueChange::default()
            },
        )
        .await
        .expect("update failed");
    assert!(!updated.is_partitioned());
    assert_eq!(
        updated.concurrency(),
        Some(60),
        "the queue-wide limits are untouched"
    );
    let stored = reader(&db)
        .await
        .get_queue("sharded")
        .await
        .expect("read failed")
        .expect("no row");
    assert!(
        !stored.partition_queue,
        "the flag follows the limits back off"
    );
}

/// A row a peer wrote with the deprecated flag is read as the per-partition limits it means.
///
/// Under `partition_queue` every queue-wide limit applies per partition, so that is where they are
/// reported — matching Python's `_resolve_limits` and TypeScript's `resolveQueueLimits`. Go and
/// Java still write rows in this shape.
#[tokio::test]
async fn a_legacy_partitioned_row_is_read_as_per_partition_limits() {
    let db = test_database().await;
    let sys = reader(&db).await;
    sys.upsert_queue(
        &NewQueue {
            concurrency: Some(1),
            worker_concurrency: Some(1),
            partition_queue: true,
            ..NewQueue::new("legacy")
        },
        OnExistingQueue::Update,
    )
    .await
    .expect("write failed");

    let dbos = DBOS::new(config("queue-legacy-app", &db));
    dbos.launch().await.expect("launch failed");
    let queue = dbos
        .queue("legacy")
        .await
        .expect("read failed")
        .expect("no queue");

    assert!(queue.is_partitioned());
    assert_eq!(
        queue.partition_concurrency(),
        Some(1),
        "the flag re-scopes the queue-wide limit rather than adding to it"
    );
    assert_eq!(queue.partition_worker_concurrency(), Some(1));
    assert_eq!(
        queue.concurrency(),
        None,
        "nothing is enforced queue-wide on a legacy row"
    );

    // Adding a per-partition limit to such a row is refused rather than leaving two answers in it.
    let error = dbos
        .update_queue(
            "legacy",
            QueueChange {
                partition_concurrency: Change::Set(Some(4)),
                ..QueueChange::default()
            },
        )
        .await
        .expect_err("the update was accepted");
    assert!(
        matches!(&error, Error::Config(message) if message.contains("deprecated `partition_queue`")),
        "got {error:?}"
    );

    dbos.shutdown().await;
}

/// **A partitioned queue runs one workflow per key at a time, and different keys concurrently.**
///
/// This is the batched read side: `partition_concurrency: 1` and nothing else is the one shape
/// `start_queued_partitioned_workflows` can sweep, so this exercises it. It claims every
/// partition's head-of-line workflow in one transaction, admitting a head only while no `PENDING`
/// row holds that partition — the mutual exclusion is the data's, not a count's.
///
/// Two keys with two workflows each. The assertion is both halves of what partitioning means: no
/// key ever has two bodies live at once, and the two keys do overlap — otherwise a queue that
/// simply ran everything serially would pass.
#[tokio::test]
async fn a_partitioned_queue_runs_one_workflow_per_key_at_a_time() {
    let db = test_database().await;
    let dbos = DBOS::new(config("partition-runner-app", &db));

    // Per-key live counts, their high-water marks, and the peak across keys.
    let live: Arc<Mutex<std::collections::HashMap<String, usize>>> = Arc::default();
    let per_key_peak = Arc::new(AtomicUsize::new(0));
    let overlap_peak = Arc::new(AtomicUsize::new(0));
    let workflow = dbos
        .register_workflow("sharded", {
            let live = Arc::clone(&live);
            let per_key_peak = Arc::clone(&per_key_peak);
            let overlap_peak = Arc::clone(&overlap_peak);
            move |key: String| {
                let live = Arc::clone(&live);
                let per_key_peak = Arc::clone(&per_key_peak);
                let overlap_peak = Arc::clone(&overlap_peak);
                async move {
                    {
                        let mut live = live.lock().unwrap();
                        let mine = live.entry(key.clone()).or_default();
                        *mine += 1;
                        per_key_peak.fetch_max(*mine, Ordering::SeqCst);
                        overlap_peak.fetch_max(live.len(), Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    let mut live = live.lock().unwrap();
                    if let Some(mine) = live.get_mut(&key) {
                        *mine -= 1;
                        if *mine == 0 {
                            live.remove(&key);
                        }
                    }
                    Ok::<String, Error>(key)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let queue = dbos
        .register_queue(
            "partitioned-queue",
            QueueOptions {
                partition_concurrency: Some(1),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert!(queue.is_partitioned());
    assert_eq!(
        queue.concurrency(),
        None,
        "nothing queue-wide, which is what keeps this on the batched sweep"
    );

    let mut handles = Vec::new();
    for key in ["tenant-a", "tenant-b"] {
        for n in 0..2 {
            handles.push(
                workflow
                    .start_with(
                        key.to_owned(),
                        StartOptions {
                            workflow_id: Some(&format!("{key}-{n}")),
                            queue: Some(Enqueue {
                                partition_key: Some(key),
                                ..Enqueue::new("partitioned-queue")
                            }),
                            ..StartOptions::default()
                        },
                    )
                    .await
                    .expect("enqueue failed"),
            );
        }
    }

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(30), handle.result())
            .await
            .expect("a partitioned workflow never ran")
            .expect("the workflow failed");
    }

    assert_eq!(
        per_key_peak.load(Ordering::SeqCst),
        1,
        "two workflows sharing a partition key ran at once"
    );
    assert_eq!(
        overlap_peak.load(Ordering::SeqCst),
        2,
        "the two partitions never overlapped, so nothing was gained by partitioning"
    );

    dbos.shutdown().await;
}

/// A partitioned queue carrying limits the sweep cannot honour is walked partition by partition.
///
/// `partition_concurrency: 2` needs counting, so this takes the other path — and the assertion is
/// that the limit is real on it: two workflows sharing a key do overlap, three never do.
#[tokio::test]
async fn a_counted_partitioned_queue_runs_its_limit_per_key() {
    let db = test_database().await;
    let dbos = DBOS::new(config("partition-counted-app", &db));

    let live: Arc<Mutex<std::collections::HashMap<String, usize>>> = Arc::default();
    let per_key_peak = Arc::new(AtomicUsize::new(0));
    let workflow = dbos
        .register_workflow("sharded", {
            let live = Arc::clone(&live);
            let per_key_peak = Arc::clone(&per_key_peak);
            move |key: String| {
                let live = Arc::clone(&live);
                let per_key_peak = Arc::clone(&per_key_peak);
                async move {
                    {
                        let mut live = live.lock().unwrap();
                        let mine = live.entry(key.clone()).or_default();
                        *mine += 1;
                        per_key_peak.fetch_max(*mine, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    let mut live = live.lock().unwrap();
                    if let Some(mine) = live.get_mut(&key) {
                        *mine -= 1;
                        if *mine == 0 {
                            live.remove(&key);
                        }
                    }
                    Ok::<String, Error>(key)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    dbos.register_queue(
        "counted-queue",
        QueueOptions {
            partition_concurrency: Some(2),
            ..QueueOptions::default()
        },
    )
    .await
    .expect("registration failed");

    let mut handles = Vec::new();
    for key in ["tenant-a", "tenant-b"] {
        for n in 0..3 {
            handles.push(
                workflow
                    .start_with(
                        key.to_owned(),
                        StartOptions {
                            workflow_id: Some(&format!("{key}-{n}")),
                            queue: Some(Enqueue {
                                partition_key: Some(key),
                                ..Enqueue::new("counted-queue")
                            }),
                            ..StartOptions::default()
                        },
                    )
                    .await
                    .expect("enqueue failed"),
            );
        }
    }

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(30), handle.result())
            .await
            .expect("a partitioned workflow never ran")
            .expect("the workflow failed");
    }

    assert_eq!(
        per_key_peak.load(Ordering::SeqCst),
        2,
        "the per-partition limit was not what bounded a key: got {}",
        per_key_peak.load(Ordering::SeqCst)
    );

    dbos.shutdown().await;
}
