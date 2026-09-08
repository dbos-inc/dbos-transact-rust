//! The client, against real databases.
//!
//! Two shapes of test here, and the split is the point of the surface. Most of these use a client
//! **alone**, with no application anywhere: what they assert is the row, because a row is all a
//! client can produce — it names a workflow nothing in this process has ever heard of. The rest
//! stand a real [`DBOS`] instance up beside the client, and assert the thing that makes the client
//! worth having: work handed over by one process and run by another.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowFilter;
use dbos::sysdb::types::WorkflowStatus;
use dbos::sysdb::{Error as SysdbError, SystemDatabase};
use dbos::{
    Change, Children, Client, ClientConfig, Config, DBOS, DuplicationPolicy, EngineOnly, Enqueue,
    EnqueueOptions, Error, ForkFrom, ForkOptions, Message, QueueChange, QueueConflict,
    QueueOptions, SendOptions, StartOptions, WorkflowHandle,
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
    dbos.register_queue(
        "work",
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
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
                // Every field named, which is the point of this test -- so there is nothing left
                // for a functional update to fill in.
                queue: Enqueue {
                    priority: Some(5),
                    partition_key: Some("acme"),
                    ..Enqueue::new("billing")
                },
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
            EnqueueOptions::on(Enqueue {
                delay: Some(Duration::from_secs(3600)),
                ..Enqueue::new("work")
            }),
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
        .list_workflows(&Default::default(), None)
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
    let options = |duplication_policy| {
        EnqueueOptions::on(Enqueue {
            deduplication_id: Some("order-42"),
            duplication_policy,
            ..Enqueue::new("work")
        })
    };

    let first: WorkflowHandle<()> = client
        .enqueue_with("process_order", (), options(DuplicationPolicy::Reject))
        .await
        .expect("enqueue failed");

    let error = client
        .enqueue_with::<_, (), Error>("process_order", (), options(DuplicationPolicy::Reject))
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
        .enqueue_with(
            "process_order",
            (),
            options(DuplicationPolicy::ReturnExisting),
        )
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
            EnqueueOptions::on(Enqueue {
                duplication_policy: DuplicationPolicy::ReturnExisting,
                ..Enqueue::new("work")
            }),
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
                ..EnqueueOptions::on(Enqueue {
                    deduplication_id: Some("key"),
                    partition_key: Some("part"),
                    ..Enqueue::new("work")
                })
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
        .send_with(
            &id,
            &"approved",
            SendOptions {
                topic: Some("approvals"),
                ..Default::default()
            },
        )
        .await
        .expect("send failed");
    let once = SendOptions {
        idempotency_key: Some("once"),
        ..Default::default()
    };
    client
        .send_with(&id, &"only once", once)
        .await
        .expect("send failed");
    client
        .send_with(&id, &"only once", once)
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
        .send_bulk(&[
            Message::new(&first, &"one"),
            Message::new(&second, &"two"),
            Message::new(&first, &"three"),
        ])
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
        .send_bulk(&[
            Message::new(&first, &"four"),
            Message::new("no-such-workflow", &"five"),
        ])
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

    // The other half answers differently, and deliberately: a wait treats a missing row as one
    // that has not been enqueued yet and keeps polling for it, which is what all four references
    // default to and what a client -- the caller most likely to hold an id before its owner has
    // committed -- needs. Nothing here ever creates the row, so the wait is still running when the
    // timeout takes it.
    let dangling: WorkflowHandle<()> = client.retrieve_workflow("no-such-workflow");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(750), dangling.result())
            .await
            .is_err(),
        "awaiting an id with no row should wait for it to appear, not report it"
    );

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
                ..QueueOptions::default()
            },
            QueueConflict::AlwaysUpdate,
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
/// something with a lifecycle — and it is the case that leaks unless the drop aborts the listener:
/// that task holds its own clone of the pool, so the pool cannot close itself, and the listener's
/// only way out of its loop is that close. An abandoned client would mean a `LISTEN` connection
/// and two tasks for the life of the process.
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

/// A client is refused the conflict policy that needs an application version.
///
/// It has none — it runs none of the application's code — so "update if I am the latest version"
/// has no answer. Both surfaces take the same `QueueConflict`, as Python's and TypeScript's do,
/// and both refuse this combination rather than guessing at an answer.
#[tokio::test]
async fn a_client_cannot_register_a_queue_by_version() {
    let db = test_database().await;
    let client = client("client-queue-policy", &db).await;

    let error = client
        .register_queue(
            "fleet",
            QueueOptions::default(),
            QueueConflict::UpdateIfLatestVersion,
        )
        .await
        .expect_err("a client has no version to be the latest of");
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert!(error.to_string().contains("AlwaysUpdate"), "{error}");

    client.close().await;
}

/// A client's `AlwaysUpdate` re-registration replaces what is stored.
///
/// The policy an application reaches for first — update only if I am the latest version — is one a
/// client is refused, so `AlwaysUpdate` is what an operator's registration says instead, and this
/// is what it has to mean: the limits named last are the ones in force.
#[tokio::test]
async fn a_clients_registration_replaces_the_stored_limits() {
    let db = test_database().await;
    let client = client("client-queue-default", &db).await;

    let queue = client
        .register_queue(
            "fleet",
            QueueOptions::default(),
            QueueConflict::AlwaysUpdate,
        )
        .await
        .expect("a client's default policy has to be one it can use");
    assert_eq!(queue.name(), "fleet");

    // Registered again with a limit this time: the policy is an update, so the stored row is the
    // second registration's, not the first's.
    let queue = client
        .register_queue(
            "fleet",
            QueueOptions {
                worker_concurrency: Some(2),
                ..QueueOptions::default()
            },
            QueueConflict::AlwaysUpdate,
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

/// Promoting an older version rolls a deploy back.
///
/// The latest version is the one with the newest *timestamp*, not the one registered last, which
/// is the whole reason promotion is a write an operator can make: moving `v1`'s timestamp forward
/// puts the fleet back on `v1` without redeploying it.
#[tokio::test]
async fn a_client_promotes_an_older_version_to_roll_a_deploy_back() {
    let db = test_database().await;

    // Two deployments in order, so the registry's latest is the second.
    for version in ["v1", "v2"] {
        let dbos = DBOS::new(Config {
            app_version: Some(version.to_owned()),
            ..config("client-promote", &db)
        });
        dbos.launch().await.expect("launch failed");
        dbos.shutdown().await;
    }

    let client = client("client-promote", &db).await;
    let latest = async || {
        client
            .latest_application_version()
            .await
            .expect("read failed")
            .expect("a version was registered at launch")
            .version_name
    };
    assert_eq!(latest().await, "v2", "the newer deployment is the latest");

    client
        .set_latest_application_version("v1")
        .await
        .expect("promote failed");
    assert_eq!(latest().await, "v1", "promoting rolls the fleet back");

    // Promotion moves a timestamp; it registers nothing.
    let mut names = client
        .list_application_versions()
        .await
        .expect("list failed")
        .into_iter()
        .map(|version| version.version_name)
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["v1", "v2"]);

    client.close().await;
}

/// Promoting on behalf of a named application — which is the whole reason the setter takes a name.
///
/// One operator tool, pointed at a shared system database, rolls a peer application back without
/// connecting a second client for it. Ownership still holds: the bare form refuses the peer's
/// version, and naming the peer is what makes the same call legal.
#[tokio::test]
async fn a_client_promotes_a_version_for_a_named_application() {
    let db = test_database().await;

    for (app, versions) in [
        ("promote-own", ["v1", "v2"]),
        ("promote-peer", ["p1", "p2"]),
    ] {
        for version in versions {
            let dbos = DBOS::new(Config {
                app_version: Some(version.to_owned()),
                ..config(app, &db)
            });
            dbos.launch().await.expect("launch failed");
            dbos.shutdown().await;
        }
    }

    let own = client("promote-own", &db).await;
    let peer = client("promote-peer", &db).await;
    let latest = async |client: &dbos::Client| {
        client
            .latest_application_version()
            .await
            .expect("read failed")
            .expect("a version was registered at launch")
            .version_name
    };

    // Unnamed, the call is scoped to this client's own application, so a peer's version is not its
    // to move.
    assert!(
        own.set_latest_application_version("p1").await.is_err(),
        "promoting a peer's version without naming the peer should be refused"
    );
    assert_eq!(latest(&peer).await, "p2", "the refusal moved nothing");

    // Naming the peer is what makes it legal.
    own.set_latest_application_version_for("p1", "promote-peer")
        .await
        .expect("promote failed");
    assert_eq!(latest(&peer).await, "p1", "the peer rolled back");
    assert_eq!(
        latest(&own).await,
        "v2",
        "this client's own latest is untouched"
    );

    peer.close().await;
    own.close().await;
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

/// **The operator's case: a client stops work it did not start, and starts it again.**
///
/// The application enqueues onto a queue nothing polls, so the row sits still. A client — a
/// separate process, holding nothing but a connection — cancels it, and the application never runs
/// it. Resuming moves it onto the internal queue, which the application does poll, and it runs.
#[tokio::test]
async fn a_client_cancels_and_resumes_a_workflow_it_did_not_start() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-cancel", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("interruptible", {
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

    let id = "stopped-from-outside";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let client = client("client-cancel", &db).await;
    let cancelled = client
        .cancel_all(&[id], Children::Skip)
        .await
        .expect("cancel failed");
    assert_eq!(cancelled, [id], "the cancel did not move the row it named");
    assert_eq!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Cancelled,
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0, "a cancelled workflow ran");

    // And back again, from the same process that stopped it.
    let handle = client
        .resume::<u32, EngineOnly>(id)
        .await
        .expect("resume failed");
    assert_eq!(handle.workflow_id(), id, "resuming changed the id");
    assert_eq!(handle.result().await.expect("the workflow failed"), 7);
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    client.close().await;
    dbos.shutdown().await;
}

/// A client forks a workflow, and the application dequeues the fork and runs it.
///
/// The job an operator reaches for a client to do: re-run finished work without the application's
/// code and without touching the original.
#[tokio::test]
async fn a_client_forks_a_workflow_the_application_then_runs() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-fork", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("repeatable", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(9)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let source = "already-run";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(source),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    let client = client("client-fork", &db).await;
    let fork = client
        .fork_with::<u32, EngineOnly>(
            source,
            ForkFrom::Beginning,
            ForkOptions {
                app_version: Some(APP_VERSION),
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_ne!(
        fork.workflow_id(),
        source,
        "the fork reused the source's id"
    );
    assert_eq!(fork.result().await.expect("the fork failed"), 9);
    assert_eq!(ran.load(Ordering::SeqCst), 2, "the fork did not run");

    client.close().await;
    dbos.shutdown().await;
}

/// A chosen id names one fork, so the bulk form refuses it — the same refusal `DBOS::fork_all`
/// makes, checked here because it is the one rule this surface enforces before any I/O.
#[tokio::test]
async fn a_clients_bulk_fork_refuses_a_chosen_id() {
    let db = test_database().await;
    let client = client("client-fork-refusal", &db).await;

    let error = client
        .fork_all::<u32, EngineOnly>(
            &["a", "b"],
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("only-one-of-me"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect_err("one id cannot name two forks");
    assert!(matches!(error, Error::InvalidArgument { .. }), "{error}");

    client.close().await;
}

/// A searched fork point cannot name its step, so a chosen id has nothing to attach to and the
/// client refuses it rather than forking under a generated one — the same refusal `DBOS::fork_with`
/// makes, checked here because the two surfaces enforce it independently.
#[tokio::test]
async fn a_clients_fork_refuses_a_chosen_id_without_a_step() {
    let db = test_database().await;
    let client = client("client-fork-searched", &db).await;

    for from in [
        ForkFrom::LastFailure,
        ForkFrom::LastStep,
        ForkFrom::StepNamed("only"),
    ] {
        let error = client
            .fork_with::<u32, EngineOnly>(
                "has-a-step",
                from,
                ForkOptions {
                    forked_id: Some("chosen"),
                    ..ForkOptions::default()
                },
            )
            .await
            .expect_err("a chosen id was accepted for a searched fork point");
        assert!(
            matches!(&error, Error::InvalidArgument { detail, .. } if detail.contains("forked_id")),
            "expected an argument refusal for {from:?}, got {error:?}"
        );
    }

    client.close().await;
}

/// A client deletes a workflow, and the row and its steps go with it.
#[tokio::test]
async fn a_client_deletes_a_workflow_it_did_not_start() {
    let db = test_database().await;
    let client = client("client-delete", &db).await;

    let handle: WorkflowHandle<()> = client
        .enqueue_with(
            "never_registered",
            (),
            EnqueueOptions {
                workflow_id: Some("to-be-deleted"),
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(handle.workflow_id(), "to-be-deleted");

    let deleted = client
        .delete_all(&["to-be-deleted"], Children::Skip)
        .await
        .expect("delete failed");
    assert_eq!(deleted, 1, "the delete removed the wrong number of rows");
    assert!(
        reader(&db)
            .await
            .get_workflow("to-be-deleted")
            .await
            .expect("read failed")
            .is_none(),
        "the row survived its delete"
    );

    client.close().await;
}

/// Listing, tagging and delaying: the reads and small writes an operator's tool makes.
///
/// One test because they share a row, and because what each asserts is narrow. The attributes are
/// a **replacement** rather than a merge, and `None` clears them, which is the half worth checking.
#[tokio::test]
async fn a_client_lists_tags_and_delays_workflows() {
    let db = test_database().await;
    let client = client("client-reads", &db).await;

    let id = "under-inspection";
    let _: WorkflowHandle<()> = client
        .enqueue_with(
            "never_registered",
            (),
            EnqueueOptions {
                workflow_id: Some(id),
                ..EnqueueOptions::new("work")
            },
        )
        .await
        .expect("enqueue failed");

    let listed = client
        .list_workflows(&WorkflowFilter {
            workflow_ids: vec![id],
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert_eq!(
        listed
            .iter()
            .map(|w| w.workflow_id.as_str())
            .collect::<Vec<_>>(),
        [id],
    );
    assert!(
        client
            .list_workflow_steps(id)
            .await
            .expect("step listing failed")
            .is_empty(),
        "a workflow nothing has run has no steps"
    );

    // Containment, not equality: one key out of two matches.
    let tags = serde_json::json!({ "tenant": "acme", "tier": "gold" });
    client
        .update_workflow_attributes(id, tags.as_object())
        .await
        .expect("update failed");
    let found = client
        .list_workflows(&WorkflowFilter {
            attributes: Some(r#"{"tenant":"acme"}"#),
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert_eq!(
        found
            .iter()
            .map(|w| w.workflow_id.as_str())
            .collect::<Vec<_>>(),
        [id],
    );

    // `None` clears, rather than leaving what was there.
    client
        .update_workflow_attributes(id, None)
        .await
        .expect("clear failed");
    assert!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .attributes
            .is_none(),
        "the attributes were not cleared"
    );

    // A delay only moves a DELAYED row, and this one is ENQUEUED -- so the call is accepted and
    // changes nothing, which is the reference behaviour rather than an error.
    client
        .set_workflow_delay(id, dbos::WorkflowDelay::For(Duration::ZERO))
        .await
        .expect("delay failed");

    client.close().await;
}

/// **A client's management call inside a workflow is not a step, where the same call on `DBOS`
/// is.**
///
/// The line this crate already draws for a client's handle and its `get_event`, checked for the
/// operator surface: the step id would have to come from an ambient context belonging to an
/// instance the client is not. So the workflow records nothing, and a replay would make the call
/// again.
#[tokio::test]
async fn a_clients_management_call_inside_a_workflow_is_not_a_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-inside", &db));
    let client = client("client-inside", &db).await;
    let target = dbos
        .register_workflow("target", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let operator = dbos
        .register_workflow("operator", {
            let client = client.clone();
            move |id: String| {
                let client = client.clone();
                async move {
                    client.cancel(&id).await?;
                    Ok::<(), Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let target_id = "cancelled-through-a-client";
    target
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(target_id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let operator_id = "the-client-operator";
    operator
        .run_with(
            target_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(operator_id),
                ..Default::default()
            },
        )
        .await
        .expect("the operator workflow failed");

    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow(target_id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Cancelled,
        "the client's cancel did not reach the row",
    );
    assert!(
        reader
            .list_workflow_steps(operator_id, true, None, None, None)
            .await
            .expect("read failed")
            .is_empty(),
        "a client's call was checkpointed into the calling workflow's step sequence",
    );

    client.close().await;
    dbos.shutdown().await;
}
