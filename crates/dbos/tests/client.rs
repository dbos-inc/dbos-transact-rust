//! The client, against real databases.
//!
//! Two shapes of test here, and the split is the point of the surface. Most of these use a client
//! **alone**, with no application anywhere: what they assert is the row, because a row is all a
//! client can produce — it names a workflow nothing in this process has ever heard of. The rest
//! stand a real [`DBOS`] instance up beside the client, and assert the thing that makes the client
//! worth having: work handed over by one process and run by another.

use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::sysdb::{Error as SysdbError, SystemDatabase};
use dbos::{
    Change, Client, ClientConfig, Config, DBOS, Duplication, Enqueue, EnqueueOptions, Error, Forks,
    Message, QueueChange, QueueConflict, QueueOptions, WorkflowHandle,
};
use dbos_test_support::{TestDatabase, raw_database, test_database};

/// A client for `db`, acting for `app_name`.
async fn client(app_name: &str, db: &TestDatabase) -> Client {
    Client::connect(ClientConfig {
        app_name: Some(app_name.to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed")
}

/// A client for `db` that speaks for no application.
async fn nameless(db: &TestDatabase) -> Client {
    Client::connect(ClientConfig::new(db.url()))
        .await
        .expect("connect failed")
}

/// The version an instance in this file launches with unless it says otherwise: DBOS computes
/// none, so a launch without one fails.
const APP_VERSION: &str = "1.0.0";

/// An application whose database is already migrated, so launching only connects.
fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

/// A second handle on the database, for reading what a call actually wrote.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// The client hands work over and the application runs it — the whole point of the surface.
///
/// Nothing about the enqueue mentions a function: the client names the workflow by string, and the
/// executor that dequeues it is the only process that has the code. The handle is a polling one,
/// and it resolves when that other process finishes.
#[tokio::test]
async fn an_application_runs_what_a_client_enqueues() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-handover", &db));
    dbos.register_workflow("double", |n: u32| async move { Ok::<u32, Error>(n * 2) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue("work", QueueOptions::default())
        .await
        .expect("registration failed");

    let client = client("client-handover", &db).await;
    let handle: WorkflowHandle<u32> = client
        .enqueue("double", "work", 21u32)
        .await
        .expect("enqueue failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 42);

    client.close().await;
    dbos.shutdown().await;
}

/// A client enqueues onto a queue nothing is draining, and the row is still exactly right.
///
/// This is the case a client is *usually* in: the application it is handing work to is somewhere
/// else, or not running yet. Nothing here can run `never_registered`, and the enqueue is no less
/// successful for it.
#[tokio::test]
async fn an_enqueue_writes_a_row_nothing_here_could_run() {
    let db = test_database().await;
    let client = client("client-rows", &db).await;

    let handle: WorkflowHandle<()> = client
        .enqueue_with(
            "never_registered",
            "payload",
            EnqueueOptions {
                workflow_id: Some("row-under-test"),
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.workflow_id(), "row-under-test");

    let row = reader(&db)
        .await
        .get_workflow("row-under-test")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Enqueued);
    assert_eq!(row.name.as_deref(), Some("never_registered"));
    assert_eq!(row.queue_name.as_deref(), Some("work"));
    assert_eq!(row.input.as_deref(), Some("\"payload\""));
    assert_eq!(
        row.application_name.as_deref(),
        Some("client-rows"),
        "a named client owns what it writes"
    );
    assert_eq!(
        row.executor_id, None,
        "a client claims nothing: the executor that dequeues it stamps its own id"
    );
    assert_eq!(
        row.application_version, None,
        "a client has no version of its own to pin the row to"
    );
    assert_eq!(row.priority, 0, "unprioritised is stored as the sentinel");

    client.close().await;
}

/// Everything an enqueue may say reaches the row it writes.
///
/// One workflow rather than one per option, because what is being checked is the mapping — every
/// field of `EnqueueOptions` and `Enqueue` landing in the column it names.
#[tokio::test]
async fn every_option_reaches_the_row() {
    let db = test_database().await;
    let client = client("client-options", &db).await;
    let mut attributes = serde_json::Map::new();
    attributes.insert("tenant".to_owned(), serde_json::json!("acme"));

    let handle: WorkflowHandle<()> = client
        .enqueue_with(
            "charge",
            (),
            EnqueueOptions {
                workflow_id: Some("fully-specified"),
                class_name: Some("Billing"),
                config_name: Some("eu"),
                app_name: Some("some-other-app"),
                app_version: Some("v1.2.3"),
                timeout: Some(Duration::from_secs(90)),
                attributes: Some(&attributes),
                queue: Enqueue {
                    priority: Some(5),
                    partition_key: Some("acme"),
                    ..Enqueue::new("billing")
                },
                ..EnqueueOptions::new("billing")
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.workflow_id(), "fully-specified");

    let row = reader(&db)
        .await
        .get_workflow("fully-specified")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.name.as_deref(), Some("charge"));
    assert_eq!(row.class_name.as_deref(), Some("Billing"));
    assert_eq!(row.config_name.as_deref(), Some("eu"));
    assert_eq!(
        row.application_name.as_deref(),
        Some("some-other-app"),
        "an enqueue may hand work to an application this client is not"
    );
    assert_eq!(row.application_version.as_deref(), Some("v1.2.3"));
    assert_eq!(row.timeout, Some(Duration::from_secs(90)));
    assert_eq!(
        row.deadline, None,
        "a queued workflow's deadline is computed on dequeue: the wait is not part of the budget"
    );
    assert_eq!(row.priority, 5);
    assert_eq!(row.queue_partition_key.as_deref(), Some("acme"));
    assert!(
        row.attributes
            .as_deref()
            .is_some_and(|json| json.contains("acme")),
        "{:?}",
        row.attributes
    );

    client.close().await;
}

/// A nameless client writes rows no application owns.
///
/// Unclaimed is not a failure state: every application matches such a row, and the first executor
/// to dequeue it claims it. It is what a cross-application tool wants, and what a client sharing a
/// database with several applications must not do by accident — which is why the bare constructor
/// is the nameless one and a name has to be asked for.
#[tokio::test]
async fn a_nameless_client_writes_unclaimed_rows() {
    let db = test_database().await;
    let client = nameless(&db).await;
    assert_eq!(client.app_name(), None);

    let handle: WorkflowHandle<()> = client
        .enqueue("anything", "work", ())
        .await
        .expect("enqueue failed");

    let row = reader(&db)
        .await
        .get_workflow(handle.workflow_id())
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.application_name, None);

    client.close().await;
}

/// A delay leaves the workflow `DELAYED`, not `ENQUEUED`.
///
/// The supervisor moves it across when the delay expires, so this is the one status a fresh
/// enqueue can have that is not `ENQUEUED`.
#[tokio::test]
async fn a_delayed_enqueue_is_held_back() {
    let db = test_database().await;
    let client = client("client-delay", &db).await;

    let handle: WorkflowHandle<()> = client
        .enqueue_with(
            "later",
            (),
            EnqueueOptions {
                queue: Enqueue {
                    delay: Some(Duration::from_secs(3600)),
                    ..Enqueue::new("work")
                },
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect("enqueue failed");

    let row = reader(&db)
        .await
        .get_workflow(handle.workflow_id())
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(row.status, WorkflowStatus::Delayed);
    assert!(row.delay_until.is_some(), "the database stamped the moment");

    client.close().await;
}

/// The same id twice is one workflow, which is what makes a retried request safe.
#[tokio::test]
async fn the_workflow_id_is_an_idempotency_key() {
    let db = test_database().await;
    let client = client("client-idempotent", &db).await;
    let options = || EnqueueOptions {
        workflow_id: Some("request-7"),
        ..EnqueueOptions::new("work")
    };

    let first: WorkflowHandle<()> = client
        .enqueue_with("handle_request", (), options())
        .await
        .expect("enqueue failed");
    let second: WorkflowHandle<()> = client
        .enqueue_with("handle_request", (), options())
        .await
        .expect("the second enqueue should join the first, not fail");
    assert_eq!(first.workflow_id(), second.workflow_id());

    let rows = reader(&db)
        .await
        .list_workflows(&Default::default())
        .await
        .expect("list failed");
    assert_eq!(rows.len(), 1, "two enqueues, one workflow");

    client.close().await;
}

/// A deduplication key is refused by default and joined when the caller asks for it.
///
/// The two halves of the same collision: `Reject` reports it, `ReturnExisting` hands back a handle
/// to the workflow already holding the key. The second is idempotent enqueue — the first caller's
/// workflow is the one that runs, and everyone else waits on it.
#[tokio::test]
async fn a_held_deduplication_key_is_refused_or_joined() {
    let db = test_database().await;
    let client = client("client-dedup", &db).await;
    let options = |duplication| EnqueueOptions {
        duplication,
        queue: Enqueue {
            deduplication_id: Some("order-42"),
            ..Enqueue::new("work")
        },
        ..EnqueueOptions::new("work")
    };

    let first: WorkflowHandle<()> = client
        .enqueue_with("process_order", (), options(Duplication::Reject))
        .await
        .expect("enqueue failed");

    let error = client
        .enqueue_with::<_, (), Error>("process_order", (), options(Duplication::Reject))
        .await
        .expect_err("the key is held, and rejecting is the default");
    assert!(
        matches!(
            error,
            Error::SystemDatabase(SysdbError::QueueDeduplicated { .. })
        ),
        "{error}"
    );

    let joined: WorkflowHandle<()> = client
        .enqueue_with("process_order", (), options(Duplication::ReturnExisting))
        .await
        .expect("returning the existing workflow should not fail");
    assert_eq!(
        joined.workflow_id(),
        first.workflow_id(),
        "the handle is to the workflow already holding the key"
    );

    client.close().await;
}

/// Asking to join a holder with no key to be held is refused rather than ignored.
#[tokio::test]
async fn returning_the_existing_workflow_needs_a_key() {
    let db = test_database().await;
    let client = client("client-dedup-guard", &db).await;

    let error = client
        .enqueue_with::<_, (), Error>(
            "process_order",
            (),
            EnqueueOptions {
                duplication: Duplication::ReturnExisting,
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect_err("there is no collision to resolve without a deduplication id");
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert!(error.to_string().contains("deduplication_id"), "{error}");

    client.close().await;
}

/// An enqueue no queue could honour costs a round trip, not a row.
#[tokio::test]
async fn an_impossible_enqueue_is_refused_before_it_is_written() {
    let db = test_database().await;
    let client = client("client-validate", &db).await;

    let error = client
        .enqueue_with::<_, (), Error>(
            "anything",
            (),
            EnqueueOptions {
                workflow_id: Some("never-written"),
                queue: Enqueue {
                    deduplication_id: Some("key"),
                    partition_key: Some("part"),
                    ..Enqueue::new("work")
                },
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect_err("a deduplication id and a partition key cannot both be set");
    assert!(matches!(error, Error::Config(_)), "{error}");

    assert!(
        reader(&db)
            .await
            .get_workflow("never-written")
            .await
            .expect("read failed")
            .is_none(),
        "nothing should have been written"
    );

    client.close().await;
}

/// A message sent from a client waits in the database for its destination to read it.
///
/// A client's send is not a step — there is no workflow to checkpoint it against — so what makes a
/// resend safe is the idempotency key, and that is what the second half asserts: the same key twice
/// is one message.
#[tokio::test]
async fn a_message_waits_for_its_destination() {
    let db = test_database().await;
    let client = client("client-send", &db).await;
    let destination: WorkflowHandle<()> = client
        .enqueue("receiver", "work", ())
        .await
        .expect("enqueue failed");
    let id = destination.workflow_id().to_owned();

    client
        .send(Message {
            topic: Some("approvals"),
            ..Message::new(&id, &"approved")
        })
        .await
        .expect("send failed");
    client
        .send(Message {
            idempotency_key: Some("once"),
            ..Message::new(&id, &"only once")
        })
        .await
        .expect("send failed");
    client
        .send(Message {
            idempotency_key: Some("once"),
            ..Message::new(&id, &"only once")
        })
        .await
        .expect("a resend under a held key is a no-op, not an error");

    let notifications = reader(&db)
        .await
        .get_all_notifications(&id)
        .await
        .expect("read failed");
    assert_eq!(
        notifications.len(),
        2,
        "three sends, two messages: the key deduplicated the resend"
    );
    let approval = notifications
        .iter()
        .find(|notification| notification.topic.as_deref() == Some("approvals"))
        .expect("the topicked message is missing");
    assert_eq!(approval.message, "\"approved\"");
    assert!(!approval.consumed, "nothing has received it");

    client.close().await;
}

/// A batch is one transaction: either every message is delivered or none is.
#[tokio::test]
async fn a_batch_of_messages_lands_together() {
    let db = test_database().await;
    let client = client("client-send-bulk", &db).await;
    let first: WorkflowHandle<()> = client
        .enqueue("receiver", "work", ())
        .await
        .expect("enqueue failed");
    let second: WorkflowHandle<()> = client
        .enqueue("receiver", "work", ())
        .await
        .expect("enqueue failed");
    let (first, second) = (
        first.workflow_id().to_owned(),
        second.workflow_id().to_owned(),
    );

    client
        .send_all(
            &[
                Message::new(&first, &"one"),
                Message::new(&second, &"two"),
                Message::new(&first, &"three"),
            ],
            Forks::Skip,
        )
        .await
        .expect("send failed");

    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_all_notifications(&first)
            .await
            .expect("read failed")
            .len(),
        2
    );
    assert_eq!(
        reader
            .get_all_notifications(&second)
            .await
            .expect("read failed")
            .len(),
        1
    );

    // Nothing is delivered when one destination does not exist: the foreign key fails the whole
    // insert, which is the guarantee a batch buys over a loop of sends.
    let error = client
        .send_all(
            &[
                Message::new(&first, &"four"),
                Message::new("no-such-workflow", &"five"),
            ],
            Forks::Skip,
        )
        .await
        .expect_err("a message addressed to nothing should fail");
    assert!(matches!(error, Error::SystemDatabase(_)), "{error}");
    assert_eq!(
        reader
            .get_all_notifications(&first)
            .await
            .expect("read failed")
            .len(),
        2,
        "the failed batch delivered nothing, not a prefix"
    );

    client.close().await;
}

/// A client reads the events a running workflow publishes.
///
/// The progress channel every implementation's client has: the application sets a key as it goes,
/// and whatever is watching from outside reads it. A key that is not there when the deadline passes
/// is `None` rather than an error, and a zero timeout makes the read a poll.
#[tokio::test]
async fn a_client_reads_a_workflow_s_events() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-events", &db));
    let workflow = dbos
        .register_workflow("publish", |()| async move {
            dbos::set_event("progress", &"done").await?;
            Ok::<(), Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let client = client("client-events", &db).await;
    let handle = workflow.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    handle.result().await.expect("the workflow failed");

    let published: Option<String> = client
        .get_event(&id, "progress", Duration::from_secs(5))
        .await
        .expect("read failed");
    assert_eq!(published.as_deref(), Some("done"));

    let missing: Option<String> = client
        .get_event(&id, "not-published", Duration::ZERO)
        .await
        .expect("read failed");
    assert_eq!(missing, None, "absence is a value, and zero is a poll");

    client.close().await;
    dbos.shutdown().await;
}

/// A client sees a workflow's status, and says so plainly when there is none.
#[tokio::test]
async fn a_client_reads_workflow_status() {
    let db = test_database().await;
    let client = client("client-status", &db).await;
    let handle: WorkflowHandle<()> = client
        .enqueue("waiting", "work", ())
        .await
        .expect("enqueue failed");

    assert_eq!(
        client
            .workflow_status(handle.workflow_id())
            .await
            .expect("read failed"),
        Some(WorkflowStatus::Enqueued)
    );
    assert_eq!(
        client
            .workflow_status("no-such-workflow")
            .await
            .expect("read failed"),
        None,
        "a client asking after an id it was given deserves a plain answer"
    );

    // A handle to a workflow that does not exist is not an error until it is used: nothing is read
    // when it is built.
    let dangling: WorkflowHandle<()> = client.retrieve_workflow("no-such-workflow");
    let error = dangling.status().await.expect_err("there is no such row");
    assert!(matches!(error, Error::WorkflowNotFound { .. }), "{error}");

    client.close().await;
}

/// The queues a fleet drains can be registered, adjusted and removed by a client.
///
/// The reason this belongs on a client at all: the process that decides a queue should slow down is
/// rarely one of the processes draining it.
#[tokio::test]
async fn a_client_manages_queues() {
    let db = test_database().await;
    let client = client("client-queues", &db).await;

    let queue = client
        .register_queue(
            "fleet",
            QueueOptions {
                worker_concurrency: Some(3),
                on_conflict: Some(QueueConflict::AlwaysUpdate),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("registration failed");
    assert_eq!(queue.name(), "fleet");
    assert_eq!(queue.worker_concurrency(), Some(3));

    assert_eq!(
        client
            .queue("fleet")
            .await
            .expect("read failed")
            .expect("the queue is missing")
            .worker_concurrency(),
        Some(3)
    );
    assert_eq!(
        client
            .list_queues()
            .await
            .expect("list failed")
            .iter()
            .map(dbos::Queue::name)
            .collect::<Vec<_>>(),
        vec!["fleet"]
    );

    let updated = client
        .update_queue(
            "fleet",
            QueueChange {
                worker_concurrency: Change::Set(Some(1)),
                ..QueueChange::default()
            },
        )
        .await
        .expect("update failed");
    assert_eq!(updated.worker_concurrency(), Some(1));

    client.delete_queue("fleet").await.expect("delete failed");
    assert!(client.queue("fleet").await.expect("read failed").is_none());

    client.close().await;
}

/// A client that is dropped rather than closed lets go of its connections.
///
/// Dropping is the ordinary end of a client — it is a value in someone's application state, not
/// something with a lifecycle — and it used to leak: the listener task holds its own clone of the
/// pool, so the pool could not close itself, and the listener's only way out of its loop is that
/// close. One abandoned client meant a `LISTEN` connection and two tasks for the life of the
/// process.
#[tokio::test]
async fn a_dropped_client_releases_its_connections() {
    // Tagged so this counts its own connections and not another test's, on a shared server.
    const TAG: &str = "dbos-client-drop-probe";

    let db = test_database().await;
    let client = Client::connect(ClientConfig {
        app_name: Some("client-drop".to_owned()),
        ..ClientConfig::new(format!("{}?application_name={TAG}", db.url()))
    })
    .await
    .expect("connect failed");

    // One call, so the pool has actually connected and the listener has had a reason to.
    assert!(
        client
            .queue("nothing")
            .await
            .expect("read failed")
            .is_none()
    );
    assert!(db.connection_count(TAG).await > 0);

    drop(client);

    // The server drops a session shortly after its client goes away, so this is a bounded wait
    // rather than a single look. A leak never converges.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let open = db.connection_count(TAG).await;
        if open == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{open} connections from a dropped client are still open"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A client cannot ask for the conflict policy that needs an application version.
///
/// It has none — it runs none of the application's code — so "update if I am the latest version"
/// has no answer. Python refuses the same combination rather than guessing at one.
#[tokio::test]
async fn a_client_cannot_register_a_queue_by_version() {
    let db = test_database().await;
    let client = client("client-queue-policy", &db).await;

    let error = client
        .register_queue(
            "fleet",
            QueueOptions {
                on_conflict: Some(QueueConflict::UpdateIfLatestVersion),
                ..QueueOptions::default()
            },
        )
        .await
        .expect_err("a client has no version to be the latest of");
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert!(error.to_string().contains("AlwaysUpdate"), "{error}");

    client.close().await;
}

/// A client registering without naming a policy overwrites what is stored.
///
/// The bare call has to work: `QueueOptions::default()` leaves `on_conflict` unstated, and the
/// policy an application would fall back to is the one a client is refused. So a client falls back
/// to `AlwaysUpdate` instead, which is also Python's client default.
#[tokio::test]
async fn a_client_registers_a_queue_with_default_options() {
    let db = test_database().await;
    let client = client("client-queue-default", &db).await;

    let queue = client
        .register_queue("fleet", QueueOptions::default())
        .await
        .expect("a client's default policy has to be one it can use");
    assert_eq!(queue.name(), "fleet");

    // Registered again with a limit this time: the unstated policy is an update, so the stored row
    // is the second registration's, not the first's.
    let queue = client
        .register_queue(
            "fleet",
            QueueOptions {
                worker_concurrency: Some(2),
                ..QueueOptions::default()
            },
        )
        .await
        .expect("re-registration failed");
    assert_eq!(queue.worker_concurrency(), Some(2));

    client.delete_queue("fleet").await.expect("delete failed");
    client.close().await;
}

/// A client reads the version registry an application writes at launch.
#[tokio::test]
async fn a_client_reads_application_versions() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        app_version: Some("v9".to_owned()),
        ..config("client-versions", &db)
    });
    dbos.launch().await.expect("launch failed");

    let client = client("client-versions", &db).await;
    let versions = client
        .list_application_versions()
        .await
        .expect("list failed");
    assert_eq!(
        versions
            .iter()
            .map(|version| version.version_name.as_str())
            .collect::<Vec<_>>(),
        vec!["v9"]
    );
    assert_eq!(
        client
            .latest_application_version()
            .await
            .expect("read failed")
            .expect("a version was registered at launch")
            .version_name,
        "v9"
    );

    client.close().await;
    dbos.shutdown().await;
}

/// A client never migrates — and so refuses a database no application has created.
///
/// The refusal is the interesting half. Connecting with migrations off *verifies* instead of
/// skipping, so a schema that is missing or behind what this build's queries are written against
/// fails at connect, naming the version it found. **Go's client does the same** — `NewClient` sets
/// `SkipMigrations: true` and fails on an unmigrated database. Python's, TypeScript's and Java's
/// clients neither migrate nor verify, and report a missing schema as whatever SQL error the first
/// real call happens to raise.
#[tokio::test]
async fn connecting_to_a_database_no_application_has_created_is_refused() {
    let db = raw_database().await;
    let error = Client::connect(ClientConfig::new(db.url()))
        .await
        .expect_err("there is no DBOS schema to work with");
    assert!(matches!(error, Error::SystemDatabase(_)), "{error}");
    assert!(
        error.to_string().contains("migration 0"),
        "the error should name what it found: {error}"
    );

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'dbos'",
    )
    .fetch_one(&db.pool().await)
    .await
    .expect("failed to count tables");
    assert_eq!(tables, 0, "the client should have created nothing");
}

/// Closing is deliberate, and what it closes is the pool every clone shares.
#[tokio::test]
async fn closing_ends_the_connection_for_every_clone() {
    let db = test_database().await;
    let client = client("client-close", &db).await;
    let clone = client.clone();

    client.close().await;
    client.close().await; // idempotent

    let error = clone
        .list_queues()
        .await
        .expect_err("the pool the clone shares is closed");
    assert!(matches!(error, Error::SystemDatabase(_)), "{error}");
}

/// A configuration that cannot work is refused before anything is connected.
#[tokio::test]
async fn a_bad_configuration_is_refused_at_connect() {
    let refused = |config: ClientConfig| async move {
        Client::connect(config)
            .await
            .expect_err("the configuration should have been refused")
    };

    assert!(matches!(
        refused(ClientConfig::new("")).await,
        Error::Config(_)
    ));
    assert!(matches!(
        refused(ClientConfig {
            app_name: Some("No".to_owned()),
            ..ClientConfig::new("postgres://localhost/nothing")
        })
        .await,
        Error::Config(_)
    ));
    assert!(matches!(
        refused(ClientConfig {
            max_connections: 0,
            ..ClientConfig::new("postgres://localhost/nothing")
        })
        .await,
        Error::Config(_)
    ));
}
