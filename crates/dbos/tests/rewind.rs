//! Rewinding a finished workflow, against real databases.
//!
//! A rewind leaves the workflow `ENQUEUED`, and the dequeue loop does the re-run. Where a test
//! checks what the rewind left in the database before the re-run touches it, the workflow is
//! rewound onto a queue nothing polls — a queue with no row — and then resumed onto the internal
//! queue to let it run. A resume of an `ENQUEUED` workflow only moves its queue, so what the re-run
//! finds is what the rewind left.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowRecord, WorkflowStatus};
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
use dbos::{
    Client, ClientConfig, Config, DBOS, EngineOnly, Enqueue, Error, RewindOptions, RunOptions,
    StartOptions,
};
use sqlx::Row;

use dbos_test_support::{TestDatabase, test_database};

/// Nothing legitimate takes this long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

/// A queue with no row, which no executor polls: a workflow rewound onto it stays `ENQUEUED`.
const PARKING: &str = "a-queue-with-no-row";

/// The version an instance in this file launches with, derived from its application name so two
/// instances sharing a database never contest one version row.
fn app_version(app_name: &str) -> String {
    format!("{app_name}-1.0.0")
}

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(app_version(app_name)),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

async fn row(reader: &PostgresSystemDatabase, workflow_id: &str) -> WorkflowRecord {
    reader
        .get_workflow(workflow_id)
        .await
        .expect("read failed")
        .unwrap_or_else(|| panic!("no row for {workflow_id}"))
}

/// Polls until the workflow reaches `status`, returning its row.
async fn await_status(
    reader: &PostgresSystemDatabase,
    workflow_id: &str,
    status: WorkflowStatus,
) -> WorkflowRecord {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = row(reader, workflow_id).await;
            if row.status == status {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{workflow_id} never reached {status:?}"))
}

/// The step ids a workflow has recorded, in order.
async fn step_ids(reader: &PostgresSystemDatabase, workflow_id: &str) -> Vec<i32> {
    reader
        .list_workflow_steps(workflow_id, true, None, None, None)
        .await
        .expect("read failed")
        .into_iter()
        .map(|step| step.step_id)
        .collect()
}

/// How many rows `table` holds for this workflow, keyed on `column`.
async fn count(db: &TestDatabase, table: &str, column: &str, workflow_id: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*)::INT8 FROM dbos.{table} WHERE {column} = $1"
    )))
    .bind(workflow_id)
    .fetch_one(&db.pool().await)
    .await
    .expect("count failed")
}

/// A counter shared between a test and the workflow bodies it registers.
fn counter() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

/// **The whole feature: a finished workflow runs again under its own id.**
///
/// The workflow returns how many times its body has run, so the result the handle reports is the
/// re-run's, not the original's. The default rewinds from step 0, and the version is left as it
/// was.
#[tokio::test]
async fn a_rewound_workflow_runs_again_under_the_same_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-app", &db));
    let runs = counter();
    let workflow = dbos
        .register_workflow("counted", {
            let runs = Arc::clone(&runs);
            move |()| {
                let runs = Arc::clone(&runs);
                async move { Ok::<u32, Error>(runs.fetch_add(1, Ordering::SeqCst) + 1) }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "rewound";
    let first = workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");
    assert_eq!(first, 1);
    let reader = reader(&db).await;
    let before = row(&reader, id).await;

    let handle = dbos
        .rewind::<u32, EngineOnly>(id)
        .await
        .expect("rewind failed");
    assert_eq!(handle.workflow_id(), id, "rewinding changed the id");
    assert_eq!(handle.result().await.expect("the re-run failed"), 2);
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    let after = row(&reader, id).await;
    assert_eq!(after.status, WorkflowStatus::Success);
    assert_eq!(
        after.application_version, before.application_version,
        "a rewind with no version keeps the one the workflow had"
    );
    assert_eq!(after.created_at, before.created_at);

    dbos.shutdown().await;
}

/// **Steps below the cut replay from their checkpoints, and the cut and everything after it run
/// again.**
#[tokio::test]
async fn steps_below_the_cut_replay_and_the_rest_run_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-steps-app", &db));
    let counts = [counter(), counter(), counter()];
    let workflow = dbos
        .register_workflow("three-steps", {
            let counts = counts.clone();
            move |()| {
                let counts = counts.clone();
                async move {
                    for (name, count) in ["zero", "one", "two"].into_iter().zip(counts.iter()) {
                        dbos::step(name, || {
                            let count = Arc::clone(count);
                            async move {
                                count.fetch_add(1, Ordering::SeqCst);
                                Ok::<(), Error>(())
                            }
                        })
                        .await?;
                    }
                    Ok::<(), Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "three-steps-run";
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");

    dbos.rewind_with::<(), EngineOnly>(
        id,
        RewindOptions {
            start_step: 1,
            ..RewindOptions::default()
        },
    )
    .await
    .expect("rewind failed")
    .result()
    .await
    .expect("the re-run failed");

    let ran: Vec<u32> = counts.iter().map(|c| c.load(Ordering::SeqCst)).collect();
    assert_eq!(ran, [1, 2, 2], "step 0 replayed; steps 1 and 2 ran again");
    assert_eq!(step_ids(&reader(&db).await, id).await, [0, 1, 2]);

    dbos.shutdown().await;
}

/// **What a rewind leaves behind, before the re-run touches it.**
///
/// Every column the rewind resets is first given a value, so a column it forgot is caught rather
/// than already being NULL. The legacy `output` and `error` columns are checked directly, since
/// the record reads the payload table first.
#[tokio::test]
async fn a_rewind_resets_the_row_and_discards_the_history() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-state-app", &db));
    let workflow = dbos
        .register_workflow("busy", |()| async {
            dbos::step("work", || async { Ok::<u32, Error>(1) }).await?;
            dbos::set_event("progress", &"done").await?;
            let message: Option<String> = dbos::recv(Some("inbox"), DEADLINE).await?;
            Ok::<_, Error>(message)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "busy-run";
    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    dbos.send_with(
        id,
        &"hello",
        dbos::SendOptions {
            topic: Some("inbox"),
            ..Default::default()
        },
    )
    .await
    .expect("send failed");
    assert_eq!(
        handle.result().await.expect("the first run failed"),
        Some("hello".to_owned())
    );

    let pool = db.pool().await;
    sqlx::query(
        "UPDATE dbos.workflow_status SET recovery_attempts = 3, owner_xid = 'stale-owner', \
         workflow_deadline_epoch_ms = 1, deduplication_id = 'stale-dedup', \
         queue_partition_key = 'stale-key', workflow_timeout_ms = 600000, \
         output = '\"legacy\"', error = '\"legacy\"' \
         WHERE workflow_uuid = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("seeding the row failed");
    let reader = reader(&db).await;
    let before = row(&reader, id).await;

    dbos.rewind_with::<Option<String>, EngineOnly>(
        id,
        RewindOptions {
            queue: Some(PARKING),
            queue_partition_key: Some("new-key"),
            ..RewindOptions::default()
        },
    )
    .await
    .expect("rewind failed");

    let after = row(&reader, id).await;
    assert_eq!(after.status, WorkflowStatus::Enqueued);
    assert_eq!(after.queue_name.as_deref(), Some(PARKING));
    assert_eq!(after.queue_partition_key.as_deref(), Some("new-key"));
    assert_eq!(after.recovery_attempts, 0);
    assert_eq!(after.started_at, None);
    assert_eq!(after.completed_at, None);
    assert_eq!(after.deduplication_id, None);
    assert_eq!(after.owner_xid, None);
    assert_eq!(after.name, before.name);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(
        after.timeout,
        Some(Duration::from_secs(600)),
        "the timeout is kept, for the dequeue to derive a fresh deadline from"
    );

    let legacy = sqlx::query(
        "SELECT workflow_deadline_epoch_ms, output, error FROM dbos.workflow_status \
         WHERE workflow_uuid = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .expect("read failed");
    assert_eq!(
        legacy.get::<Option<i64>, _>("workflow_deadline_epoch_ms"),
        None
    );
    assert_eq!(legacy.get::<Option<String>, _>("output"), None);
    assert_eq!(legacy.get::<Option<String>, _>("error"), None);

    for (table, column) in [
        ("operation_outputs", "workflow_uuid"),
        ("workflow_events_history", "workflow_uuid"),
        ("workflow_events", "workflow_uuid"),
        ("workflow_output", "workflow_uuid"),
        ("notifications", "destination_uuid"),
    ] {
        assert_eq!(
            count(&db, table, column, id).await,
            0,
            "{table} still has rows for the rewound workflow"
        );
    }
    // The input is the workflow's identity, not its history.
    assert_eq!(count(&db, "workflow_input", "workflow_uuid", id).await, 1);

    dbos.shutdown().await;
}

/// **A recv records which step consumed its message**, and a message nobody received has none.
///
/// Then the rewind to the second recv: the message the first recv took stays, since that recv
/// replays; the one the second took goes, and so does one sent after the workflow finished. The
/// replayed second recv then waits for a new message and gets it.
#[tokio::test]
async fn a_rewind_discards_the_messages_the_discarded_steps_consumed() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-messages-app", &db));
    let workflow = dbos
        .register_workflow("two-recvs", |()| async {
            // A recv is two steps, the receive and its deadline: these are 0 and 2.
            let a: Option<String> = dbos::recv(Some("a"), DEADLINE).await?;
            let b: Option<String> = dbos::recv(Some("b"), DEADLINE).await?;
            Ok::<_, Error>((a, b))
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "two-recvs-run";
    let send = |topic: &'static str, message: &'static str| {
        let dbos = dbos.clone();
        async move {
            dbos.send_with(
                id,
                &message,
                dbos::SendOptions {
                    topic: Some(topic),
                    ..Default::default()
                },
            )
            .await
            .expect("send failed")
        }
    };

    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    send("a", "first").await;
    send("b", "second").await;
    assert_eq!(
        handle.result().await.expect("the first run failed"),
        (Some("first".to_owned()), Some("second".to_owned()))
    );
    send("b", "stray").await;

    let pool = db.pool().await;
    let mailbox = || async {
        let rows = sqlx::query(
            "SELECT message, consumed, consumed_by_function_id FROM dbos.notifications \
             WHERE destination_uuid = $1 ORDER BY message",
        )
        .bind(id)
        .fetch_all(&pool)
        .await
        .expect("read failed");
        rows.iter()
            .map(|r| {
                (
                    r.get::<String, _>("message"),
                    r.get::<bool, _>("consumed"),
                    r.get::<Option<i32>, _>("consumed_by_function_id"),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        mailbox().await,
        [
            ("\"first\"".to_owned(), true, Some(0)),
            ("\"second\"".to_owned(), true, Some(2)),
            ("\"stray\"".to_owned(), false, None),
        ],
        "each consumed message names the recv that took it"
    );

    dbos.rewind_with::<(Option<String>, Option<String>), EngineOnly>(
        id,
        RewindOptions {
            start_step: 2,
            queue: Some(PARKING),
            ..RewindOptions::default()
        },
    )
    .await
    .expect("rewind failed");
    assert_eq!(
        mailbox().await,
        [("\"first\"".to_owned(), true, Some(0))],
        "only the message consumed below the cut survives"
    );

    send("b", "fresh").await;
    let rerun = dbos
        .resume::<(Option<String>, Option<String>), EngineOnly>(id)
        .await
        .expect("resume failed");
    assert_eq!(
        rerun.result().await.expect("the re-run failed"),
        (Some("first".to_owned()), Some("fresh".to_owned())),
        "the first recv replayed and the second took the new message"
    );

    dbos.shutdown().await;
}

/// **Events revert to what they were below the cut.**
///
/// Three keys, published around a cut between the two writes to `both`: `below` is untouched,
/// `both` goes back to its first value, and `above`, published only past the cut, is gone. A peer
/// reading them sees the reverted values. The re-run then publishes them again.
#[tokio::test]
async fn a_rewind_reverts_the_events_published_past_the_cut() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-events-app", &db));
    let workflow = dbos
        .register_workflow("publishes", |()| async {
            dbos::set_event("below", &1).await?;
            dbos::set_event("both", &1).await?;
            dbos::set_event("both", &2).await?;
            dbos::set_event("above", &3).await?;
            Ok::<(), Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "publishes-run";
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");
    let reader = reader(&db).await;
    assert_eq!(step_ids(&reader, id).await, [0, 1, 2, 3]);

    dbos.rewind_with::<(), EngineOnly>(
        id,
        RewindOptions {
            start_step: 2,
            queue: Some(PARKING),
            ..RewindOptions::default()
        },
    )
    .await
    .expect("rewind failed");

    let event = |key: &'static str| {
        let dbos = dbos.clone();
        async move {
            dbos.get_event::<u32>(id, key, Duration::ZERO)
                .await
                .expect("get_event failed")
        }
    };
    assert_eq!(event("below").await, Some(1), "a key below the cut is kept");
    assert_eq!(
        event("both").await,
        Some(1),
        "a key reverts to its value below the cut"
    );
    assert_eq!(
        event("above").await,
        None,
        "a key set only past the cut is gone"
    );

    let history: Vec<i32> = sqlx::query_scalar(
        "SELECT function_id FROM dbos.workflow_events_history WHERE workflow_uuid = $1 \
         ORDER BY function_id",
    )
    .bind(id)
    .fetch_all(&db.pool().await)
    .await
    .expect("read failed");
    assert_eq!(history, [0, 1], "only history below the cut remains");

    dbos.resume::<(), EngineOnly>(id)
        .await
        .expect("resume failed")
        .result()
        .await
        .expect("the re-run failed");
    assert_eq!(event("both").await, Some(2));
    assert_eq!(event("above").await, Some(3));

    dbos.shutdown().await;
}

/// **With no queue named, a rewound workflow goes to the internal queue and its partition key is
/// cleared; with a version nobody runs, it waits.**
///
/// Then cancelled, and rewound again under the version this executor runs, which it does.
#[tokio::test]
async fn a_rewind_chooses_the_queue_and_version_the_rerun_gets() {
    let db = test_database().await;
    let app = "rewind-version-app";
    let dbos = DBOS::new(config(app, &db));
    let runs = counter();
    let workflow = dbos
        .register_workflow("versioned", {
            let runs = Arc::clone(&runs);
            move |()| {
                let runs = Arc::clone(&runs);
                async move { Ok::<u32, Error>(runs.fetch_add(1, Ordering::SeqCst) + 1) }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "versioned-run";
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");
    sqlx::query(
        "UPDATE dbos.workflow_status SET queue_partition_key = 'old-key' WHERE workflow_uuid = $1",
    )
    .bind(id)
    .execute(&db.pool().await)
    .await
    .expect("seeding the row failed");

    dbos.rewind_with::<u32, EngineOnly>(
        id,
        RewindOptions {
            app_version: Some("not-deployed"),
            ..RewindOptions::default()
        },
    )
    .await
    .expect("rewind failed");
    let reader = reader(&db).await;
    let parked = row(&reader, id).await;
    assert_eq!(parked.queue_name.as_deref(), Some(INTERNAL_QUEUE));
    assert_eq!(parked.queue_partition_key, None, "the old key is not kept");
    assert_eq!(parked.application_version.as_deref(), Some("not-deployed"));

    // The internal queue is polled here, but only for this executor's version.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(row(&reader, id).await.status, WorkflowStatus::Enqueued);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    dbos.cancel(id).await.expect("cancel failed");
    let rerun = dbos
        .rewind_with::<u32, EngineOnly>(
            id,
            RewindOptions {
                app_version: Some(&app_version(app)),
                ..RewindOptions::default()
            },
        )
        .await
        .expect("rewind failed");
    assert_eq!(rerun.result().await.expect("the re-run failed"), 2);

    dbos.shutdown().await;
}

/// **A rewound parent adopts the child it started, rather than starting it again.**
///
/// Nothing touches the child: the parent replays to the child's deterministic id and finds the
/// finished row there.
#[tokio::test]
async fn a_rewound_parent_adopts_its_child() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-parent-app", &db));
    let child_runs = counter();
    let parent_runs = counter();
    let child = dbos
        .register_workflow("child", {
            let child_runs = Arc::clone(&child_runs);
            move |n: u32| {
                let child_runs = Arc::clone(&child_runs);
                async move {
                    child_runs.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(n * 2)
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let parent_runs = Arc::clone(&parent_runs);
            move |()| {
                let (child, parent_runs) = (child.clone(), Arc::clone(&parent_runs));
                async move {
                    parent_runs.fetch_add(1, Ordering::SeqCst);
                    child.run(21).await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "adopting-parent";
    parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");

    let rerun = dbos
        .rewind::<u32, EngineOnly>(id)
        .await
        .expect("rewind failed");
    assert_eq!(rerun.result().await.expect("the re-run failed"), 42);
    assert_eq!(
        parent_runs.load(Ordering::SeqCst),
        2,
        "the parent ran again"
    );
    assert_eq!(child_runs.load(Ordering::SeqCst), 1, "the child did not");

    dbos.shutdown().await;
}

/// **A failed child is repaired by rewinding it, then rewinding its parent to the step that
/// awaited it.**
///
/// The parent's start of the child is step 0 and its await step 1, so the parent's rewind to
/// step 1 replays the start, adopting the child, and awaits the child's new result.
#[tokio::test]
async fn a_failed_child_is_repaired_by_rewinding_it_and_then_its_parent() {
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("the child is broken")]
    struct Broken;

    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-repair-app", &db));
    let broken = Arc::new(AtomicBool::new(true));
    let child_runs = counter();
    let child = dbos
        .register_workflow("flaky-child", {
            let (broken, child_runs) = (Arc::clone(&broken), Arc::clone(&child_runs));
            move |()| {
                let (broken, child_runs) = (Arc::clone(&broken), Arc::clone(&child_runs));
                async move {
                    child_runs.fetch_add(1, Ordering::SeqCst);
                    if broken.load(Ordering::SeqCst) {
                        return Err(Error::Application(Broken));
                    }
                    Ok::<u32, Error<Broken>>(5)
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("waiting-parent", move |()| {
            let child = child.clone();
            async move { child.run(()).await }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "repaired-parent";
    let failed = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await;
    assert!(
        matches!(failed, Err(Error::Application(Broken))),
        "expected the child's failure, got {failed:?}"
    );
    let reader = reader(&db).await;
    let child_id = format!("{id}-0");
    assert_eq!(row(&reader, &child_id).await.status, WorkflowStatus::Error);

    broken.store(false, Ordering::SeqCst);
    let repaired = dbos
        .rewind::<u32, Broken>(&child_id)
        .await
        .expect("rewinding the child failed");
    assert_eq!(repaired.result().await.expect("the child failed again"), 5);

    let rerun = dbos
        .rewind_with::<u32, Broken>(
            id,
            RewindOptions {
                start_step: 1,
                ..RewindOptions::default()
            },
        )
        .await
        .expect("rewinding the parent failed");
    assert_eq!(rerun.result().await.expect("the parent failed again"), 5);
    assert_eq!(
        child_runs.load(Ordering::SeqCst),
        2,
        "the child ran once broken and once repaired, and the parent's re-run adopted it"
    );

    dbos.shutdown().await;
}

/// **Only a finished workflow can be rewound**, and every refusal writes nothing.
///
/// A missing id, an `ENQUEUED` workflow and a `PENDING` one are each refused with their own
/// error, and a negative step is refused before the database is asked anything.
#[tokio::test]
async fn a_rewind_is_refused_for_a_workflow_that_has_not_finished() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-refused-app", &db));
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let workflow = dbos
        .register_workflow("gated", {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            move |()| {
                let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
                async move {
                    dbos::step("before-the-gate", || async { Ok::<(), Error>(()) }).await?;
                    reached.notify_one();
                    release.notified().await;
                    Ok::<u32, Error>(1)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let missing = dbos
        .rewind::<u32, EngineOnly>("never-existed")
        .await
        .expect_err("a missing workflow was rewound");
    assert!(
        matches!(
            &missing,
            Error::SystemDatabase(dbos::sysdb::Error::NonExistentWorkflow { workflow_ids })
                if workflow_ids == &["never-existed"]
        ),
        "{missing:?}"
    );

    let enqueued = "parked-workflow";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(enqueued),
                queue: Some(Enqueue::new(PARKING)),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    let refused = dbos
        .rewind::<u32, EngineOnly>(enqueued)
        .await
        .expect_err("an enqueued workflow was rewound");
    assert!(
        matches!(
            &refused,
            Error::SystemDatabase(dbos::sysdb::Error::WorkflowNotRewindable { status, .. })
                if status == "ENQUEUED"
        ),
        "{refused:?}"
    );
    assert_eq!(
        row(&reader, enqueued).await.queue_name.as_deref(),
        Some(PARKING)
    );

    let pending = "running-workflow";
    let running = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(pending),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the workflow never reached its gate");
    let refused = dbos
        .rewind::<u32, EngineOnly>(pending)
        .await
        .expect_err("a running workflow was rewound");
    assert!(
        matches!(
            &refused,
            Error::SystemDatabase(dbos::sysdb::Error::WorkflowNotRewindable { status, .. })
                if status == "PENDING"
        ),
        "{refused:?}"
    );
    assert_eq!(
        step_ids(&reader, pending).await,
        [0],
        "the history is untouched"
    );
    release.notify_one();
    assert_eq!(running.result().await.expect("the workflow failed"), 1);

    let negative = dbos
        .rewind_with::<u32, EngineOnly>(
            pending,
            RewindOptions {
                start_step: -1,
                ..RewindOptions::default()
            },
        )
        .await
        .expect_err("a negative step was accepted");
    assert!(
        matches!(negative, Error::InvalidArgument { .. }),
        "{negative:?}"
    );
    let finished = row(&reader, pending).await;
    assert_eq!(
        finished.status,
        WorkflowStatus::Success,
        "nothing was written"
    );
    assert_eq!(step_ids(&reader, pending).await, [0]);

    dbos.shutdown().await;
}

/// **From inside a workflow, a rewind is a step, and a recovered caller does not rewind its
/// target a second time.**
///
/// The operator rewinds the target, waits for the re-run to finish, and is abandoned at a gate.
/// The relaunch recovers it: had the rewind not been checkpointed, the replay would rewind the
/// finished target again and run it a third time.
#[tokio::test]
async fn a_recovered_caller_does_not_rewind_its_target_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-operator-app", &db));
    let target_runs = counter();
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let target = dbos
        .register_workflow("target", {
            let target_runs = Arc::clone(&target_runs);
            move |()| {
                let target_runs = Arc::clone(&target_runs);
                async move { Ok::<u32, Error>(target_runs.fetch_add(1, Ordering::SeqCst) + 1) }
            }
        })
        .unwrap();
    let operator = dbos
        .register_workflow("operator", {
            let dbos = dbos.clone();
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            move |target_id: String| {
                let dbos = dbos.clone();
                let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
                async move {
                    let handle = dbos.rewind::<u32, EngineOnly>(&target_id).await?;
                    let rerun = handle.result().await?;
                    reached.notify_one();
                    release.notified().await;
                    Ok::<u32, Error>(rerun)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let target_id = "rewind-target";
    target
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(target_id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the target failed");

    let operator_id = "rewind-operator";
    let running = tokio::spawn({
        let operator = operator.clone();
        async move {
            operator
                .run_with(
                    target_id.to_owned(),
                    RunOptions {
                        workflow_id: Some(operator_id),
                        ..RunOptions::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the operator never reached its gate");
    assert_eq!(
        target_runs.load(Ordering::SeqCst),
        2,
        "the rewind re-ran the target"
    );

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(operator_id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps[0].step_id, 0);
    assert_eq!(steps[0].step_name, "DBOS.rewindWorkflow");
    assert!(steps[0].error.is_none(), "{:?}", steps[0]);

    dbos.shutdown().await;
    let interrupted = running.await.expect("the task panicked").unwrap_err();
    assert!(
        matches!(interrupted, Error::Interrupted { .. }),
        "{interrupted}"
    );

    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the recovered operator never reached its gate");
    release.notify_one();
    let done = await_status(&reader, operator_id, WorkflowStatus::Success).await;
    assert_eq!(done.output.as_deref(), Some("2"));
    assert_eq!(
        target_runs.load(Ordering::SeqCst),
        2,
        "the recovered operator replayed its rewind rather than rewinding again"
    );

    dbos.shutdown().await;
}

/// **A negative step refused inside a workflow moves no step counter.**
///
/// The refusal comes before the step id is taken, so the step after it gets id 0.
#[tokio::test]
async fn a_refused_rewind_inside_a_workflow_takes_no_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("rewind-negative-app", &db));
    let operator = dbos
        .register_workflow("careless-operator", {
            let dbos = dbos.clone();
            move |()| {
                let dbos = dbos.clone();
                async move {
                    let refused = dbos
                        .rewind_with::<u32, EngineOnly>(
                            "anything",
                            RewindOptions {
                                start_step: -1,
                                ..RewindOptions::default()
                            },
                        )
                        .await;
                    assert!(
                        matches!(refused, Err(Error::InvalidArgument { .. })),
                        "{refused:?}"
                    );
                    dbos::step("after", || async { Ok::<(), Error>(()) }).await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "careless-run";
    operator
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the operator failed");
    let steps = reader(&db)
        .await
        .list_workflow_steps(id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect::<Vec<_>>(),
        [(0, "after")]
    );

    dbos.shutdown().await;
}

/// **A client rewinds as an application does**, from step 0 by default, and its handle reports
/// the re-run's result.
#[tokio::test]
async fn a_client_can_rewind_a_workflow() {
    let db = test_database().await;
    let app = "rewind-client-app";
    let dbos = DBOS::new(config(app, &db));
    let runs = counter();
    let workflow = dbos
        .register_workflow("client-rewound", {
            let runs = Arc::clone(&runs);
            move |()| {
                let runs = Arc::clone(&runs);
                async move { Ok::<u32, Error>(runs.fetch_add(1, Ordering::SeqCst) + 1) }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "client-rewound-run";
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the first run failed");

    let client = Client::connect(ClientConfig {
        app_name: Some(app.to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");
    let handle = client
        .rewind::<u32, EngineOnly>(id)
        .await
        .expect("rewind failed");
    assert_eq!(handle.result().await.expect("the re-run failed"), 2);

    let negative = client
        .rewind_with::<u32, EngineOnly>(
            id,
            RewindOptions {
                start_step: -1,
                ..RewindOptions::default()
            },
        )
        .await
        .expect_err("a negative step was accepted");
    assert!(
        matches!(negative, Error::InvalidArgument { .. }),
        "{negative:?}"
    );

    client.close().await;
    dbos.shutdown().await;
}
