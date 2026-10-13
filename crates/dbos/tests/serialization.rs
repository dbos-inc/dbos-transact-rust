//! How rows are read by their `serialization` tag, against real databases: a row in an encoding
//! this SDK cannot read is reported rather than misread, and a row with no tag is this SDK's own.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::{
    Client, ClientConfig, Config, DBOS, EngineOnly, Error, QueueConflict, QueueOptions,
    StartOptions,
};
use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);
const QUEUE: &str = "tagq";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(format!("{app_name}-1.0.0")),
        ..Config::new(app_name, db.url())
    }
}

async fn client(db: &TestDatabase) -> Client {
    Client::connect(ClientConfig::new(db.url()))
        .await
        .expect("connect failed")
}

async fn status(db: &TestDatabase, workflow_id: &str) -> Option<String> {
    sqlx::query_scalar(r#"SELECT status FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#)
        .bind(workflow_id)
        .fetch_one(&db.pool().await)
        .await
        .unwrap()
}

/// Inserts a finished row the way another writer would, with `tag` in its `serialization` column.
async fn insert_finished(db: &TestDatabase, id: &str, output: &str, tag: Option<&str>) {
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status"
             (workflow_uuid, name, status, output, created_at, serialization)
           VALUES ($1, 'elsewhere', 'SUCCESS', $2, (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT,
                   $3)"#,
    )
    .bind(id)
    .bind(output)
    .bind(tag)
    .execute(&db.pool().await)
    .await
    .unwrap();
}

/// A result tagged with an encoding this SDK does not read is reported by its tag, not decoded as
/// though it were this SDK's own.
#[tokio::test]
async fn a_result_in_an_unreadable_encoding_is_reported() {
    let db = test_database().await;
    insert_finished(&db, "wf-pickled", "gASVAgAAAAAAAABLKi4=", Some("py_pickle")).await;

    let error = client(&db)
        .await
        .retrieve_workflow::<u32, EngineOnly>("wf-pickled")
        .result()
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::UnknownSerialization { name } if name == "py_pickle"),
        "{error:?}"
    );
}

/// A row with no tag, or an empty one, was written by this SDK before tags were.
#[tokio::test]
async fn a_row_with_no_tag_is_read_as_this_sdks_own() {
    let db = test_database().await;
    insert_finished(&db, "wf-untagged", "42", None).await;
    insert_finished(&db, "wf-empty-tag", "43", Some("")).await;

    let client = client(&db).await;
    for (id, expected) in [("wf-untagged", 42), ("wf-empty-tag", 43)] {
        let result = client
            .retrieve_workflow::<u32, EngineOnly>(id)
            .result()
            .await
            .unwrap();
        assert_eq!(result, expected, "{id}");
    }
}

/// A dequeued row in an encoding this SDK cannot read is not run, and nothing is recorded for it.
#[tokio::test]
async fn a_dequeued_row_in_an_unreadable_encoding_is_not_run() {
    let db = test_database().await;
    let dbos = DBOS::new(config("tag-unreadable", &db));
    let runs = Arc::new(AtomicU32::new(0));
    {
        let runs = Arc::clone(&runs);
        dbos.register_workflow("echo", move |text: String| {
            runs.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, Error>(text) }
        })
        .unwrap();
    }
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        QUEUE,
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status"
             (workflow_uuid, name, queue_name, status, inputs, created_at, serialization)
           VALUES ('wf-pickled-input', 'echo', $1, 'ENQUEUED', '"valid JSON all the same"',
                   (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, 'py_pickle')"#,
    )
    .bind(QUEUE)
    .execute(&db.pool().await)
    .await
    .unwrap();

    // Claimed — the dequeue moved it to PENDING — and then left alone, though its input happens
    // to be readable JSON: the tag says it is not this SDK's to read.
    tokio::time::timeout(DEADLINE, async {
        while status(&db, "wf-pickled-input").await.as_deref() != Some("PENDING") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the row is never claimed");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 0, "the body never ran");
    assert_eq!(
        status(&db, "wf-pickled-input").await.as_deref(),
        Some("PENDING"),
        "no outcome is recorded"
    );
    dbos.shutdown().await;
}

/// A start that claims a row it did not create runs the row's input, not the one it was called
/// with: only the creator writes a row's input, and a recovery of the row would run what it holds.
/// The same whether the row's tag is the caller's own or absent.
#[tokio::test]
async fn a_start_of_an_existing_row_runs_the_rows_input() {
    let db = test_database().await;
    let dbos = DBOS::new(config("tag-existing", &db));
    let echo = dbos
        .register_workflow("echo", |text: String| async move { Ok::<_, Error>(text) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    for (id, tag) in [("wf-tagged", Some("rust_serde")), ("wf-untagged", None)] {
        // Pending and unowned, as a row whose first run never got going: the start claims it.
        sqlx::query(
            r#"INSERT INTO "dbos"."workflow_status"
                 (workflow_uuid, name, status, inputs, created_at, serialization)
               VALUES ($1, 'echo', 'PENDING', '"stored"',
                       (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, $2)"#,
        )
        .bind(id)
        .bind(tag)
        .execute(&db.pool().await)
        .await
        .unwrap();

        let handle = echo
            .start_with(
                "caller".to_owned(),
                StartOptions {
                    workflow_id: Some(id),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(DEADLINE, handle.result())
            .await
            .expect("the workflow finishes")
            .unwrap();
        assert_eq!(result, "stored", "{id}");
    }
    dbos.shutdown().await;
}
