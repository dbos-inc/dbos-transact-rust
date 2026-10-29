//! Portable serialization, against real databases: the rows a portable workflow writes, and the
//! rows another SDK writes, compared as exact strings.
//!
//! The golden strings are the ones every DBOS SDK's interop test checks its own rows against, so a
//! row that matches here is one the other SDKs read.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use dbos::{
    Client, ClientConfig, Config, DBOS, EngineOnly, Enqueue, EnqueueOptions, Error,
    PortableWorkflowArgs, QualifiedWorkflowName, QueueConflict, QueueOptions, ScheduleSpec,
    ScheduledWorkflowInput, SendMessage, SendOptions, SerializationFormat, SetEventOptions,
    StartOptions, WorkflowOptions, WriteStreamOptions,
};
use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);
const QUEUE: &str = "interopq";

/// The canonical workflow's input: seven positional arguments, no named ones.
const GOLDEN_INPUTS_JSON: &str = concat!(
    r#"{"positionalArgs":["hello-interop",42,"2025-06-15T10:30:00.000Z","#,
    r#"["alpha","beta","gamma"],"#,
    r#"{"key1":"value1","key2":99,"nested":{"deep":true}},"#,
    r#"true,null]}"#,
);
const GOLDEN_MESSAGE_JSON: &str = r#"{"sender":"test","payload":[1,2,3]}"#;
const GOLDEN_OUTPUT_JSON: &str = concat!(
    r#"{"echo_text":"hello-interop","echo_num":42,"#,
    r#""echo_dt":"2025-06-15T10:30:00.000Z","#,
    r#""items_count":3,"meta_keys":["key1","key2","nested"],"#,
    r#""flag":true,"empty":null,"#,
    r#""received":{"sender":"test","payload":[1,2,3]}}"#,
);
const GOLDEN_EVENT_JSON: &str = r#"{"text":"hello-interop","num":42,"flag":true}"#;
const GOLDEN_STREAM_JSON: &str = r#"{"item":"hello-interop"}"#;

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

fn portable() -> WorkflowOptions {
    WorkflowOptions {
        serialization: SerializationFormat::Portable,
    }
}

/// The canonical workflow's seven arguments, as one Rust value: a tuple takes several positional
/// arguments.
type CanonicalInput = (
    String,
    i64,
    String,
    Vec<String>,
    Map<String, Value>,
    bool,
    Option<Value>,
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct InteropMessage {
    sender: String,
    payload: Vec<u32>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CanonicalOutput {
    echo_text: String,
    echo_num: i64,
    echo_dt: String,
    items_count: usize,
    meta_keys: Vec<String>,
    flag: bool,
    empty: Option<Value>,
    received: InteropMessage,
}

#[derive(Serialize)]
struct Status<'a> {
    text: &'a str,
    num: i64,
    flag: bool,
}

#[derive(Serialize)]
struct Item<'a> {
    item: &'a str,
}

fn expected_output() -> CanonicalOutput {
    serde_json::from_str(GOLDEN_OUTPUT_JSON).unwrap()
}

/// Registers the canonical workflow every SDK's interop test runs, launches, and registers its
/// queue.
async fn launch_canonical(app_name: &str, db: &TestDatabase) -> DBOS {
    let dbos = DBOS::new(config(app_name, db));
    dbos.register_workflow_with(
        QualifiedWorkflowName::method("canonicalWorkflow", "interop"),
        portable(),
        |(text, num, dt, items, meta, flag, empty): CanonicalInput| async move {
            dbos::set_event(
                "interop_status",
                &Status {
                    text: &text,
                    num,
                    flag,
                },
            )
            .await?;
            dbos::write_stream("interop_stream", &Item { item: &text }).await?;
            let received =
                dbos::recv::<InteropMessage, EngineOnly>(Some("interop_topic"), DEADLINE)
                    .await?
                    .expect("the message arrives");
            Ok::<_, Error>(CanonicalOutput {
                echo_text: text,
                echo_num: num,
                echo_dt: dt,
                items_count: items.len(),
                meta_keys: meta.keys().cloned().collect(),
                flag,
                empty,
                received,
            })
        },
    )
    .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        QUEUE,
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .unwrap();
    dbos
}

async fn scalar(db: &TestDatabase, sql: &str, workflow_id: &str) -> Option<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
        .bind(workflow_id)
        .fetch_one(&db.pool().await)
        .await
        .unwrap()
}

/// The workflow every SDK's interop test runs, enqueued and messaged as another SDK's client does,
/// writes the rows every SDK writes — byte for byte, apart from where `namedArgs` sits.
#[tokio::test]
async fn the_canonical_workflow_writes_the_golden_rows() {
    let db = test_database().await;
    let dbos = launch_canonical("portable-canonical", &db).await;
    let client = client(&db).await;

    let args: PortableWorkflowArgs = serde_json::from_str(GOLDEN_INPUTS_JSON).unwrap();
    let handle = client
        .enqueue_portable::<CanonicalOutput, EngineOnly>(
            QualifiedWorkflowName::method("canonicalWorkflow", "interop"),
            args,
            EnqueueOptions::new(QUEUE),
        )
        .await
        .unwrap();
    client
        .send_with(
            SendMessage {
                topic: Some("interop_topic"),
                ..SendMessage::new(
                    handle.workflow_id(),
                    &InteropMessage {
                        sender: "test".to_owned(),
                        payload: vec![1, 2, 3],
                    },
                )
            },
            SendOptions {
                serialization: SerializationFormat::Portable,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id = handle.workflow_id().to_owned();
    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow finishes")
        .unwrap();
    assert_eq!(result, expected_output());

    let status = |column: &str| {
        format!(r#"SELECT {column} FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#)
    };
    assert_eq!(
        scalar(&db, &status("serialization"), &id).await.as_deref(),
        Some("portable_json")
    );
    assert_eq!(
        scalar(&db, &status("status"), &id).await.as_deref(),
        Some("SUCCESS")
    );
    let inputs = scalar(
        &db,
        r#"SELECT inputs FROM "dbos"."workflow_input" WHERE workflow_uuid = $1"#,
        &id,
    )
    .await
    .unwrap();
    assert_eq!(inputs.replace(r#","namedArgs":{}"#, ""), GOLDEN_INPUTS_JSON);
    assert_eq!(
        scalar(
            &db,
            r#"SELECT output FROM "dbos"."workflow_output" WHERE workflow_uuid = $1"#,
            &id
        )
        .await
        .as_deref(),
        Some(GOLDEN_OUTPUT_JSON)
    );
    for table in ["workflow_events", "workflow_events_history"] {
        let (value, serialization): (String, String) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                r#"SELECT value, serialization FROM "dbos"."{table}"
               WHERE workflow_uuid = $1 AND key = 'interop_status'"#
            )))
            .bind(&id)
            .fetch_one(&db.pool().await)
            .await
            .unwrap();
        assert_eq!(value, GOLDEN_EVENT_JSON, "{table}");
        assert_eq!(serialization, "portable_json", "{table}");
    }
    let (value, serialization, offset): (String, String, i32) = sqlx::query_as(
        r#"SELECT value, serialization, "offset" FROM "dbos"."streams"
           WHERE workflow_uuid = $1 AND key = 'interop_stream'"#,
    )
    .bind(&id)
    .fetch_one(&db.pool().await)
    .await
    .unwrap();
    assert_eq!(
        (value.as_str(), serialization.as_str(), offset),
        (GOLDEN_STREAM_JSON, "portable_json", 0)
    );
    let (message, serialization): (String, String) = sqlx::query_as(
        r#"SELECT message, serialization FROM "dbos"."notifications" WHERE destination_uuid = $1"#,
    )
    .bind(&id)
    .fetch_one(&db.pool().await)
    .await
    .unwrap();
    assert_eq!(
        (message.as_str(), serialization.as_str()),
        (GOLDEN_MESSAGE_JSON, "portable_json")
    );
    dbos.shutdown().await;
}

/// The rows another SDK writes — inserted here as that SDK's interop test inserts them — run this
/// SDK's workflow to the same result.
#[tokio::test]
async fn rows_another_sdk_writes_run_here() {
    let db = test_database().await;
    let dbos = launch_canonical("portable-inserted", &db).await;
    let pool = db.pool().await;

    let id = "wf-inserted";
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status"
             (workflow_uuid, name, class_name, queue_name, status, inputs, created_at, serialization)
           VALUES ($1, 'canonicalWorkflow', 'interop', $2, 'ENQUEUED', $3,
                   (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, 'portable_json')"#,
    )
    .bind(id)
    .bind(QUEUE)
    .bind(GOLDEN_INPUTS_JSON)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO "dbos"."notifications" (destination_uuid, topic, message, serialization)
           VALUES ($1, 'interop_topic', $2, 'portable_json')"#,
    )
    .bind(id)
    .bind(GOLDEN_MESSAGE_JSON)
    .execute(&pool)
    .await
    .unwrap();

    let result = tokio::time::timeout(
        DEADLINE,
        dbos.retrieve_workflow::<CanonicalOutput, EngineOnly>(id)
            .unwrap()
            .result(),
    )
    .await
    .expect("the workflow finishes")
    .unwrap();
    assert_eq!(result, expected_output());
    dbos.shutdown().await;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    sku: String,
    quantity: u32,
}

/// The SQL `enqueue_workflow` function is how any language enqueues without an SDK, and it writes
/// the envelope spaced out, with named arguments as given. Named arguments alone reach a struct's
/// fields; several positional ones reach a tuple.
#[tokio::test]
async fn the_sql_enqueue_function_reaches_struct_and_tuple_inputs() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-sql", &db));
    dbos.register_workflow_with("order", portable(), |order: Order| async move {
        Ok::<_, Error>(order)
    })
    .unwrap();
    dbos.register_workflow_with(
        "pair",
        portable(),
        |(sku, quantity): (String, u32)| async move { Ok::<_, Error>(format!("{quantity} x {sku}")) },
    )
    .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        QUEUE,
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .unwrap();
    let pool = db.pool().await;

    let named: String = sqlx::query_scalar(
        r#"SELECT "dbos".enqueue_workflow('order', $1, ARRAY[]::JSON[],
             '{"sku":"tea","quantity":2}'::JSON)"#,
    )
    .bind(QUEUE)
    .fetch_one(&pool)
    .await
    .unwrap();
    let positional: String = sqlx::query_scalar(
        r#"SELECT "dbos".enqueue_workflow('pair', $1, ARRAY['"tea"'::JSON, '2'::JSON])"#,
    )
    .bind(QUEUE)
    .fetch_one(&pool)
    .await
    .unwrap();

    let order = tokio::time::timeout(
        DEADLINE,
        dbos.retrieve_workflow::<Order, EngineOnly>(&named)
            .unwrap()
            .result(),
    )
    .await
    .expect("the workflow finishes")
    .unwrap();
    assert_eq!(
        order,
        Order {
            sku: "tea".to_owned(),
            quantity: 2
        }
    );
    let pair = tokio::time::timeout(
        DEADLINE,
        dbos.retrieve_workflow::<String, EngineOnly>(&positional)
            .unwrap()
            .result(),
    )
    .await
    .expect("the workflow finishes")
    .unwrap();
    assert_eq!(pair, "2 x tea");
    dbos.shutdown().await;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, PartialEq)]
#[error("the card was declined after {attempts} attempts")]
struct CardDeclined {
    attempts: u32,
}

/// A portable workflow's error is the envelope every SDK reads, with the error's fields as its
/// `data`. A reader whose error type it is gets the error itself back; any other reader gets
/// [`Error::Portable`].
#[tokio::test]
async fn a_portable_workflows_error_is_the_envelope_and_reads_back_as_itself() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-error", &db));
    let declines = dbos
        .register_workflow_with("declines", portable(), |(): ()| async {
            Err::<(), _>(Error::Application(CardDeclined { attempts: 3 }))
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = declines.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    let error = handle.result().await.unwrap_err();
    assert!(
        matches!(error, Error::Application(CardDeclined { attempts: 3 })),
        "{error:?}"
    );
    assert_eq!(
        scalar(
            &db,
            r#"SELECT error FROM "dbos"."workflow_output" WHERE workflow_uuid = $1"#,
            &id
        )
        .await
        .as_deref(),
        Some(
            r#"{"name":"portable::CardDeclined","message":"the card was declined after 3 attempts","code":null,"data":{"attempts":3}}"#
        )
    );

    let elsewhere = dbos
        .retrieve_workflow::<(), EngineOnly>(&id)
        .unwrap()
        .result()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &elsewhere,
            Error::Portable { name, data: Some(data), .. }
                if name == "portable::CardDeclined" && *data == json!({"attempts": 3})
        ),
        "{elsewhere:?}"
    );
    dbos.shutdown().await;
}

/// A workflow another SDK failed, read by a client here: the envelope's four fields, as written.
#[tokio::test]
async fn another_sdks_error_reads_as_portable() {
    let db = test_database().await;
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status"
             (workflow_uuid, name, status, error, created_at, serialization)
           VALUES ('wf-python-failed', 'pythonWorkflow', 'ERROR',
                   '{"name":"ValueError","message":"bad input","code":42,"data":null}',
                   (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, 'portable_json')"#,
    )
    .execute(&db.pool().await)
    .await
    .unwrap();

    let error = client(&db)
        .await
        .retrieve_workflow::<Value, EngineOnly>("wf-python-failed")
        .result()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            Error::Portable { name, message, code: Some(code), data: None }
                if name == "ValueError" && message == "bad input" && *code == json!(42)
        ),
        "{error:?}"
    );
}

/// A parent's record of a child it awaited holds the child's bytes under the child's tag, so the
/// parent reads back the child's error as itself on a replay too.
///
/// The replay is a fork of the parent after its await: the fork inherits the recorded start and
/// await, so its body gets the child's error from the record, and the child does not run again.
#[tokio::test]
async fn a_parent_records_a_portable_childs_error_as_the_child_wrote_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-parent", &db));
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let declines = dbos
        .register_workflow_with("declines", portable(), move |(): ()| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<(), _>(Error::Application(CardDeclined { attempts: 3 })) }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", move |(): ()| {
            let declines = declines.clone();
            async move {
                match declines.run(()).await {
                    Err(Error::Application(declined)) => Ok::<_, Error>(declined.attempts),
                    // Anything else finishes as 0, which the assertions below refuse. A panic
                    // would leave the workflow without an outcome, and its result waiting forever.
                    _ => Ok(0),
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    assert_eq!(handle.result().await.unwrap(), 3);

    let (awaited_at, serialization, error): (i32, String, String) = sqlx::query_as(
        r#"SELECT function_id, serialization, error FROM "dbos"."operation_outputs"
           WHERE workflow_uuid = $1 AND function_name = 'DBOS.get_result'"#,
    )
    .bind(&id)
    .fetch_one(&db.pool().await)
    .await
    .unwrap();
    assert_eq!(serialization, "portable_json");
    let recorded: Value = serde_json::from_str(&error).unwrap();
    assert_eq!(recorded["name"], "portable::CardDeclined", "{error}");

    let replayed: dbos::WorkflowHandle<u32> =
        dbos.fork(&id, awaited_at + 1).await.expect("fork failed");
    assert_eq!(
        replayed.result().await.unwrap(),
        3,
        "the replayed await gives back the child's error as itself"
    );
    assert_eq!(
        runs.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the replay read the record rather than running the child again"
    );
    dbos.shutdown().await;
}

/// A scheduled run of a workflow registered portable is written portable, as a start through its
/// reference would be. The schedule starts it by name, so the registration's choice has to come
/// from the registry rather than from a reference the scheduler does not hold.
#[tokio::test]
async fn a_scheduled_run_takes_its_workflows_registered_format() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-schedule", &db));
    let ticks = dbos
        .register_workflow_with(
            "ticks",
            portable(),
            |input: ScheduledWorkflowInput<String>| async move { Ok::<_, Error>(input.context) },
        )
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // Far from now, so the loop fires nothing on its own while the test runs.
    let hour = (jiff::Timestamp::now().as_second() / 3600 + 12) % 24;
    let spec = ScheduleSpec::new(
        "portable-ticks",
        &ticks,
        format!("0 0 {hour} * * *"),
        &"emea".to_owned(),
    )
    .unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    let handle = dbos
        .trigger_schedule::<String, EngineOnly>("portable-ticks")
        .await
        .unwrap();
    let id = handle.workflow_id().to_owned();
    assert_eq!(handle.result().await.unwrap(), "emea");

    let serialization = scalar(
        &db,
        r#"SELECT serialization FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#,
        &id,
    )
    .await;
    assert_eq!(serialization.as_deref(), Some("portable_json"));
    let input: Value = serde_json::from_str(
        &scalar(
            &db,
            r#"SELECT inputs FROM "dbos"."workflow_input" WHERE workflow_uuid = $1"#,
            &id,
        )
        .await
        .expect("the run has an input"),
    )
    .unwrap();
    assert_eq!(
        input["positionalArgs"][0]["context"], "emea",
        "the input is the envelope: {input}"
    );
    dbos.shutdown().await;
}

/// A write takes its workflow's format unless the call names one, so a portable workflow's events
/// and streams are portable and a native one's are not.
#[tokio::test]
async fn a_write_takes_its_workflows_format_unless_it_names_one() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-writes", &db));
    let native = dbos
        .register_workflow("native", |(): ()| async {
            dbos::set_event("inherited", &1).await?;
            dbos::set_event_with(
                "chosen",
                &2,
                SetEventOptions {
                    serialization: SerializationFormat::Portable,
                },
            )
            .await?;
            Ok::<_, Error>(())
        })
        .unwrap();
    let portable_writer = dbos
        .register_workflow_with("portable", portable(), |(): ()| async {
            dbos::set_event("inherited", &3).await?;
            dbos::write_stream("inherited", &4).await?;
            dbos::write_stream_with(
                "chosen",
                &5,
                WriteStreamOptions {
                    serialization: SerializationFormat::Native,
                },
            )
            .await?;
            Ok::<_, Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let native_id = native.start(()).await.unwrap();
    let native_id = {
        let id = native_id.workflow_id().to_owned();
        native_id.result().await.unwrap();
        id
    };
    let portable_handle = portable_writer.start(()).await.unwrap();
    let portable_id = portable_handle.workflow_id().to_owned();
    portable_handle.result().await.unwrap();

    let tag = |table: &'static str, workflow_id: String, key: &'static str| {
        let db = &db;
        async move {
            sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
                r#"SELECT serialization FROM "dbos"."{table}" WHERE workflow_uuid = $1 AND key = $2"#
            )))
            .bind(workflow_id)
            .bind(key)
            .fetch_one(&db.pool().await)
            .await
            .unwrap()
        }
    };
    assert_eq!(
        tag("workflow_events", native_id.clone(), "inherited").await,
        "rust_serde"
    );
    assert_eq!(
        tag("workflow_events", native_id, "chosen").await,
        "portable_json"
    );
    assert_eq!(
        tag("workflow_events", portable_id.clone(), "inherited").await,
        "portable_json"
    );
    assert_eq!(
        tag("streams", portable_id.clone(), "inherited").await,
        "portable_json"
    );
    assert_eq!(tag("streams", portable_id, "chosen").await, "rust_serde");
    dbos.shutdown().await;
}

/// A client calling a workflow in another language: the raw envelope as given, one input as the
/// envelope's single positional argument, or a tuple's elements as several.
#[tokio::test]
async fn a_client_enqueues_another_languages_workflow() {
    let db = test_database().await;
    let client = client(&db).await;
    // An application no executor here runs, so the rows stay as the client wrote them.
    let elsewhere = |queue: Enqueue<'static>| EnqueueOptions {
        app_name: Some("python-app"),
        ..EnqueueOptions::on(queue)
    };

    let raw = client
        .enqueue_portable::<Value, EngineOnly>(
            "pythonWorkflow",
            PortableWorkflowArgs {
                positional_args: vec![json!(1), json!("two")],
                named_args: Map::from_iter([("three".to_owned(), json!(3))]),
            },
            elsewhere(Enqueue::new(QUEUE)),
        )
        .await
        .unwrap();
    let one = client
        .enqueue_with::<_, Value, EngineOnly>(
            "pythonWorkflow",
            Order {
                sku: "tea".to_owned(),
                quantity: 2,
            },
            elsewhere(Enqueue {
                serialization: SerializationFormat::Portable,
                ..Enqueue::new(QUEUE)
            }),
        )
        .await
        .unwrap();

    let portable = || {
        elsewhere(Enqueue {
            serialization: SerializationFormat::Portable,
            ..Enqueue::new(QUEUE)
        })
    };
    let two = client
        .enqueue_with::<_, Value, EngineOnly>("pythonWorkflow", ("tea", 2), portable())
        .await
        .unwrap();
    let none = client
        .enqueue_with::<_, Value, EngineOnly>("pythonWorkflow", (), portable())
        .await
        .unwrap();

    for (handle, expected) in [
        (
            &raw,
            r#"{"positionalArgs":[1,"two"],"namedArgs":{"three":3}}"#,
        ),
        (
            &one,
            r#"{"positionalArgs":[{"sku":"tea","quantity":2}],"namedArgs":{}}"#,
        ),
        // A tuple is that many arguments, and the unit none, as a call in another language
        // passes them.
        (&two, r#"{"positionalArgs":["tea",2],"namedArgs":{}}"#),
        (&none, r#"{"positionalArgs":[],"namedArgs":{}}"#),
    ] {
        let id = handle.workflow_id();
        assert_eq!(
            scalar(
                &db,
                r#"SELECT inputs FROM "dbos"."workflow_input" WHERE workflow_uuid = $1"#,
                id
            )
            .await
            .as_deref(),
            Some(expected)
        );
        assert_eq!(
            scalar(
                &db,
                r#"SELECT serialization FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#,
                id
            )
            .await
            .as_deref(),
            Some("portable_json")
        );
    }
}

/// A workflow that takes the envelope gets what it was started with, not one argument holding it.
#[tokio::test]
async fn a_workflow_taking_the_envelope_gets_the_envelope_it_was_started_with() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-envelope", &db));
    let echo = dbos
        .register_workflow_with(
            "echo",
            portable(),
            |args: PortableWorkflowArgs| async move { Ok::<_, Error>(args) },
        )
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let args = PortableWorkflowArgs {
        positional_args: vec![json!(1), json!("two")],
        named_args: Map::from_iter([("three".to_owned(), json!(3))]),
    };
    assert_eq!(echo.run(args.clone()).await.unwrap(), args);
    dbos.shutdown().await;
}

/// Inserts an unowned row the way another SDK writes one: `ENQUEUED` on `queue`, or `PENDING`
/// with no queue.
async fn insert_row(
    db: &TestDatabase,
    id: &str,
    name: &str,
    queue: Option<&str>,
    input: &str,
    tag: &str,
) {
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status"
             (workflow_uuid, name, queue_name, status, inputs, created_at, serialization)
           VALUES ($1, $2, $3, CASE WHEN $3::TEXT IS NULL THEN 'PENDING' ELSE 'ENQUEUED' END,
                   $4, (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, $5)"#,
    )
    .bind(id)
    .bind(name)
    .bind(queue)
    .bind(input)
    .bind(tag)
    .execute(&db.pool().await)
    .await
    .unwrap();
}

/// A row already written in another encoding runs what the row holds, by the row's tag — not the
/// input a later start was called with, which it encoded differently. The row is a crash's
/// leftover, so the launch's recovery runs it; the start finds the row and runs nothing of its own.
#[tokio::test]
async fn a_start_of_an_existing_row_runs_it_in_its_own_encoding() {
    let db = test_database().await;
    let config = config("portable-restart", &db);
    insert_row(
        &db,
        "wf-restart",
        "echo",
        None,
        r#"{"positionalArgs":["stored"],"namedArgs":{}}"#,
        "portable_json",
    )
    .await;
    // Pending under this executor and version, as a run that crashed before its first step.
    sqlx::query(
        r#"UPDATE "dbos"."workflow_status"
             SET executor_id = 'local', application_version = $2
           WHERE workflow_uuid = $1"#,
    )
    .bind("wf-restart")
    .bind(config.app_version.as_deref())
    .execute(&db.pool().await)
    .await
    .unwrap();

    let dbos = DBOS::new(config);
    let echo = dbos
        .register_workflow("echo", |text: String| async move { Ok::<_, Error>(text) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = echo
        .start_with(
            "caller".to_owned(),
            StartOptions {
                workflow_id: Some("wf-restart"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow finishes")
        .unwrap();
    assert_eq!(result, "stored");
    assert_eq!(
        scalar(
            &db,
            r#"SELECT serialization FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#,
            "wf-restart"
        )
        .await
        .as_deref(),
        Some("portable_json")
    );
    dbos.shutdown().await;
}

/// A client called from a workflow body is still a client: its send takes the client's own
/// serializer, not the surrounding workflow's format.
#[tokio::test]
async fn a_client_inside_a_workflow_sends_as_a_client() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-client-inside", &db));
    let client = Arc::new(client(&db).await);
    let relay = {
        let client = Arc::clone(&client);
        dbos.register_workflow_with("relay", portable(), move |(): ()| {
            let client = Arc::clone(&client);
            async move {
                let id = dbos::workflow_id().expect("inside a workflow");
                client.send(&id, &5).await?;
                Ok::<_, Error>(id)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let id = relay.run(()).await.unwrap();
    assert_eq!(
        scalar(
            &db,
            r#"SELECT serialization FROM "dbos"."notifications" WHERE destination_uuid = $1"#,
            &id
        )
        .await
        .as_deref(),
        Some("rust_serde")
    );
    dbos.shutdown().await;
}

/// A value in an encoding this SDK does not read is refused by name, not misread.
#[tokio::test]
async fn a_value_in_an_unknown_encoding_is_refused() {
    let db = test_database().await;
    let pool = db.pool().await;
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_status" (workflow_uuid, name, status, created_at)
           VALUES ('wf-pickled', 'pythonWorkflow', 'SUCCESS',
                   (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT)"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_events" (workflow_uuid, key, value, serialization)
           VALUES ('wf-pickled', 'progress', 'gASVAgAAAAAAAABLKi4=', 'py_pickle')"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = client(&db)
        .await
        .get_event::<Value>("wf-pickled", "progress", Duration::ZERO)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::UnknownSerialization { name } if name == "py_pickle"),
        "{error:?}"
    );
}

/// **A result portable JSON cannot hold is the workflow's error.** A non-finite float has no
/// portable spelling, so the result is refused; the refusal is recorded as the envelope of
/// [`Error::Serialization`], the row ends ERROR, and a replay of the same id reads it back without
/// running the body again.
#[tokio::test]
async fn a_result_portable_json_cannot_hold_is_recorded_as_the_workflows_error() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-unencodable", &db));
    let runs = Arc::new(AtomicU32::new(0));
    let not_a_number = dbos
        .register_workflow_with("not_a_number", portable(), {
            let runs = Arc::clone(&runs);
            move |(): ()| {
                runs.fetch_add(1, Ordering::SeqCst);
                async { Ok::<f64, Error>(f64::NAN) }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    const REFUSAL: &str = "NaN is not a finite number, and portable JSON has no way to store one";
    let refused = |error: &Error| {
        matches!(
            error,
            Error::Serialization { what, message, .. } if what == "result" && message == REFUSAL
        )
    };

    let handle = not_a_number.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    let error = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow finishes")
        .unwrap_err();
    assert!(refused(&error), "{error:?}");
    assert_eq!(
        scalar(
            &db,
            r#"SELECT status FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#,
            &id
        )
        .await
        .as_deref(),
        Some("ERROR")
    );
    let recorded: Value = serde_json::from_str(
        &scalar(
            &db,
            r#"SELECT error FROM "dbos"."workflow_output" WHERE workflow_uuid = $1"#,
            &id,
        )
        .await
        .expect("an error is recorded"),
    )
    .expect("the error is JSON");
    assert_eq!(
        recorded,
        json!({
            "name": "dbos::Serialization",
            "message": format!("could not serialize the workflow result: {REFUSAL}"),
            "code": null,
            "data": {"what": "result", "message": REFUSAL},
        })
    );

    let replayed = not_a_number
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(&id),
                ..Default::default()
            },
        )
        .await
        .expect("the replay's start failed")
        .result()
        .await
        .unwrap_err();
    assert!(refused(&replayed), "{replayed:?}");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the body ran again");
    dbos.shutdown().await;
}

/// An application error whose fields portable JSON cannot write: no JSON object has a non-string
/// key.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
#[error("no keys fit")]
struct BadKeys {
    by_pair: std::collections::BTreeMap<(u8, u8), u8>,
}

/// **An error portable JSON cannot write is recorded as the refusal**, as the envelope of
/// [`Error::Serialization`] carrying what the error displayed, so the row ends ERROR; the caller
/// and a replay of the same id read it back without the body running again.
#[tokio::test]
async fn an_error_portable_json_cannot_write_is_recorded_as_the_refusal() {
    let db = test_database().await;
    let dbos = DBOS::new(config("portable-unencodable-error", &db));
    let runs = Arc::new(AtomicU32::new(0));
    let bad_keys = dbos
        .register_workflow_with("bad_keys", portable(), {
            let runs = Arc::clone(&runs);
            move |(): ()| {
                runs.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<(), _>(Error::Application(BadKeys {
                        by_pair: [((1, 2), 3)].into_iter().collect(),
                    }))
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let refused = |error: &Error<BadKeys>| {
        matches!(
            error,
            Error::Serialization { what, message, .. }
                if what == "error"
                    && message.contains("key must be a string")
                    && message.contains("no keys fit")
        )
    };

    let handle = bad_keys.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    let error = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow finishes")
        .unwrap_err();
    assert!(refused(&error), "{error:?}");
    assert_eq!(
        scalar(
            &db,
            r#"SELECT status FROM "dbos"."workflow_status" WHERE workflow_uuid = $1"#,
            &id
        )
        .await
        .as_deref(),
        Some("ERROR")
    );
    let recorded: Value = serde_json::from_str(
        &scalar(
            &db,
            r#"SELECT error FROM "dbos"."workflow_output" WHERE workflow_uuid = $1"#,
            &id,
        )
        .await
        .expect("an error is recorded"),
    )
    .expect("the error is JSON");
    assert_eq!(recorded["name"], "dbos::Serialization", "{recorded}");
    assert_eq!(recorded["data"]["what"], "error", "{recorded}");

    let replayed = bad_keys
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(&id),
                ..Default::default()
            },
        )
        .await
        .expect("the replay's start failed")
        .result()
        .await
        .unwrap_err();
    assert!(refused(&replayed), "{replayed:?}");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the body ran again");
    dbos.shutdown().await;
}
