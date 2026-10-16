//! A custom serializer, against a real database: what it writes, what reads it back, and what
//! happens to its rows when it is added or taken away.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

use dbos::{
    Client, ClientConfig, Config, CustomSerializer, DBOS, EngineOnly, Error, Payload, PayloadSlot,
    QueueConflict, QueueOptions, ReadStreamOptions, ScheduleSpec, ScheduledWorkflowInput,
    SerializationFormat, Serializer, SetEventOptions, WorkflowKey, WorkflowOptions,
};
use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);
const QUEUE: &str = "ledgerq";
const NAME: &str = "backwards_json";

/// JSON, written backwards: text no built-in reader can make sense of, which is what an encrypting
/// serializer's would look like.
#[derive(Debug)]
struct Backwards;

impl CustomSerializer for Backwards {
    fn name(&self) -> &str {
        NAME
    }

    fn serialize(
        &self,
        value: Payload<'_>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::to_string(&value)?.chars().rev().collect())
    }

    fn deserialize(
        &self,
        text: &str,
        into: PayloadSlot<'_>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json: String = text.chars().rev().collect();
        let mut deserializer = serde_json::Deserializer::from_str(&json);
        into.fill(&mut deserializer)?;
        Ok(deserializer.end()?)
    }
}

/// Reads a row [`Backwards`] wrote.
fn forwards<T: serde::de::DeserializeOwned>(text: &str) -> T {
    serde_json::from_str(&text.chars().rev().collect::<String>()).unwrap()
}

/// `u128::MAX` is past what a `serde_json::Value` holds, so it arrives whole only if the
/// serializer sees the real type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Ledger {
    account: String,
    cents: u128,
}

fn ledger() -> Ledger {
    Ledger {
        account: "acct-7".to_owned(),
        cents: u128::MAX,
    }
}

#[derive(Debug, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("the card was declined after {attempts} attempts")]
struct CardDeclined {
    attempts: u32,
}

fn config(app_name: &str, db: &TestDatabase, serializer: Serializer) -> Config {
    Config {
        migrate: false,
        app_version: Some(format!("{app_name}-1.0.0")),
        serializer,
        ..Config::new(app_name, db.url())
    }
}

async fn client(db: &TestDatabase, serializer: Serializer) -> Client {
    Client::connect(ClientConfig {
        serializer,
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed")
}

async fn row(db: &TestDatabase, sql: &str, workflow_id: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as(sqlx::AssertSqlSafe(sql.to_owned()))
        .bind(workflow_id)
        .fetch_one(&db.pool().await)
        .await
        .unwrap()
}

/// Every payload a workflow writes goes through the serializer and comes back as it went in: the
/// input a client enqueued, a step's result, an event, a stream value, a message, the result.
#[tokio::test]
async fn every_payload_goes_through_the_custom_serializer() {
    let db = test_database().await;
    let dbos = DBOS::new(config("custom-all", &db, Serializer::custom(Backwards)));
    dbos.register_workflow("ledger", |ledger: Ledger| async move {
        let looked_up = dbos::step("lookup", || {
            let ledger = ledger.clone();
            async move { Ok(ledger) }
        })
        .await?;
        dbos::set_event("ledger", &looked_up).await?;
        dbos::write_stream("ledger", &looked_up).await?;
        dbos::close_stream("ledger").await?;
        let received = dbos::recv::<Ledger, EngineOnly>(None, DEADLINE)
            .await?
            .expect("the message arrives");
        assert_eq!(received, looked_up);
        Ok::<_, Error>(received)
    })
    .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        QUEUE,
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .unwrap();
    let client = client(&db, Serializer::custom(Backwards)).await;

    let handle = client
        .enqueue::<_, Ledger, EngineOnly>("ledger", QUEUE, ledger())
        .await
        .unwrap();
    let id = handle.workflow_id().to_owned();
    client.send(&id, &ledger()).await.unwrap();
    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow finishes")
        .unwrap();
    assert_eq!(result, ledger());

    // What the rows hold is the serializer's text, under its name.
    let (input, tag) = row(
        &db,
        r#"SELECT i.inputs, s.serialization FROM "dbos"."workflow_input" i
           JOIN "dbos"."workflow_status" s USING (workflow_uuid) WHERE workflow_uuid = $1"#,
        &id,
    )
    .await;
    assert_eq!(tag.as_deref(), Some(NAME));
    assert_eq!(forwards::<Ledger>(&input.unwrap()), ledger());
    let (output, tag) = row(
        &db,
        r#"SELECT o.output, s.serialization FROM "dbos"."workflow_output" o
           JOIN "dbos"."workflow_status" s USING (workflow_uuid) WHERE workflow_uuid = $1"#,
        &id,
    )
    .await;
    assert_eq!(tag.as_deref(), Some(NAME));
    assert_eq!(forwards::<Ledger>(&output.unwrap()), ledger());
    for sql in [
        r#"SELECT output, serialization FROM "dbos"."operation_outputs"
           WHERE workflow_uuid = $1 AND function_name = 'lookup'"#,
        r#"SELECT value, serialization FROM "dbos"."workflow_events"
           WHERE workflow_uuid = $1 AND key = 'ledger'"#,
        r#"SELECT value, serialization FROM "dbos"."streams"
           WHERE workflow_uuid = $1 AND key = 'ledger' AND "offset" = 0"#,
        r#"SELECT output, serialization FROM "dbos"."operation_outputs"
           WHERE workflow_uuid = $1 AND function_name = 'DBOS.recv'"#,
    ] {
        let (value, tag) = row(&db, sql, &id).await;
        assert_eq!(tag.as_deref(), Some(NAME), "{sql}");
        assert_eq!(forwards::<Ledger>(&value.unwrap()), ledger(), "{sql}");
    }

    // And the reads outside the workflow decode it.
    assert_eq!(
        client
            .get_event::<Ledger>(&id, "ledger", Duration::ZERO)
            .await
            .unwrap(),
        Some(ledger())
    );
    let mut values = client.read_stream::<Ledger>(&id, "ledger", ReadStreamOptions::default());
    assert_eq!(values.next().await.unwrap(), Some(ledger()));
    assert_eq!(values.next().await.unwrap(), None);
    dbos.shutdown().await;
}

/// A workflow's error, and a step's, are encoded whole and come back as the application's own type.
#[tokio::test]
async fn an_error_round_trips_through_the_custom_serializer() {
    let db = test_database().await;
    let dbos = DBOS::new(config("custom-error", &db, Serializer::custom(Backwards)));
    let declines = dbos
        .register_workflow("declines", |(): ()| async {
            dbos::step("charge", || async {
                Err::<(), _>(Error::Application(CardDeclined { attempts: 3 }))
            })
            .await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = declines.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    let error = handle.result().await.unwrap_err();
    assert!(
        matches!(&error, Error::Application(CardDeclined { attempts: 3 })),
        "{error:?}"
    );

    let (error, tag) = row(
        &db,
        r#"SELECT o.error, s.serialization FROM "dbos"."workflow_output" o
           JOIN "dbos"."workflow_status" s USING (workflow_uuid) WHERE workflow_uuid = $1"#,
        &id,
    )
    .await;
    assert_eq!(tag.as_deref(), Some(NAME));
    let error: serde_json::Value = forwards(&error.unwrap());
    assert_eq!(error["Application"]["attempts"], 3, "{error}");
    let (error, tag) = row(
        &db,
        r#"SELECT error, serialization FROM "dbos"."operation_outputs"
           WHERE workflow_uuid = $1 AND function_name = 'charge'"#,
        &id,
    )
    .await;
    assert_eq!(tag.as_deref(), Some(NAME));
    assert!(error.is_some());
    dbos.shutdown().await;
}

/// Asking for a built-in encoding by name still gets it, whatever is configured.
#[tokio::test]
async fn native_and_portable_still_write_the_built_ins() {
    let db = test_database().await;
    let dbos = DBOS::new(config(
        "custom-builtins",
        &db,
        Serializer::custom(Backwards),
    ));
    let native = dbos
        .register_workflow_with(
            WorkflowKey::new("native"),
            WorkflowOptions {
                serialization: SerializationFormat::Native,
            },
            |(): ()| async {
                dbos::set_event("inherited", &1).await?;
                dbos::set_event_with(
                    "portable",
                    &2,
                    SetEventOptions {
                        serialization: SerializationFormat::Portable,
                    },
                )
                .await?;
                Ok::<_, Error>(())
            },
        )
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = native.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    handle.result().await.unwrap();

    let tag = |sql: &'static str| {
        let db = &db;
        let id = id.clone();
        async move { row(db, sql, &id).await.1 }
    };
    assert_eq!(
        tag(
            r#"SELECT i.inputs, s.serialization FROM "dbos"."workflow_input" i
           JOIN "dbos"."workflow_status" s USING (workflow_uuid) WHERE workflow_uuid = $1"#
        )
        .await
        .as_deref(),
        Some("rust_serde")
    );
    assert_eq!(
        tag(r#"SELECT value, serialization FROM "dbos"."workflow_events"
               WHERE workflow_uuid = $1 AND key = 'inherited'"#)
        .await
        .as_deref(),
        Some("rust_serde")
    );
    assert_eq!(
        tag(r#"SELECT value, serialization FROM "dbos"."workflow_events"
               WHERE workflow_uuid = $1 AND key = 'portable'"#)
        .await
        .as_deref(),
        Some("portable_json")
    );
    dbos.shutdown().await;
}

/// Adding a serializer leaves the rows written before it readable; taking it away leaves its own
/// rows unreadable, by name, rather than misread.
#[tokio::test]
async fn rows_outlive_a_change_of_serializer() {
    let db = test_database().await;
    let dbos = DBOS::new(config("custom-change", &db, Serializer::RustSerde));
    let echo = dbos
        .register_workflow("echo", |ledger: Ledger| async move {
            dbos::set_event("ledger", &ledger).await?;
            Ok::<_, Error>(ledger)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let handle = echo.start(ledger()).await.unwrap();
    let before = handle.workflow_id().to_owned();
    handle.result().await.unwrap();
    dbos.shutdown().await;

    let dbos = DBOS::new(config("custom-change", &db, Serializer::custom(Backwards)));
    let echo = dbos
        .register_workflow("echo", |ledger: Ledger| async move {
            dbos::set_event("ledger", &ledger).await?;
            Ok::<_, Error>(ledger)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let handle = echo.start(ledger()).await.unwrap();
    let after = handle.workflow_id().to_owned();
    handle.result().await.unwrap();
    // The row written before the serializer was added reads as it did.
    assert_eq!(
        dbos.retrieve_workflow::<Ledger, EngineOnly>(&before)
            .unwrap()
            .result()
            .await
            .unwrap(),
        ledger()
    );
    dbos.shutdown().await;

    let without = client(&db, Serializer::RustSerde).await;
    assert_eq!(
        without
            .retrieve_workflow::<Ledger, EngineOnly>(&before)
            .result()
            .await
            .unwrap(),
        ledger()
    );
    let error = without
        .retrieve_workflow::<Ledger, EngineOnly>(&after)
        .result()
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::UnknownSerialization { name } if name == NAME),
        "{error:?}"
    );
    let error = without
        .get_event::<Ledger>(&after, "ledger", Duration::ZERO)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::UnknownSerialization { name } if name == NAME),
        "{error:?}"
    );
}

/// A name a reader could not tell from a built-in one, or from no name, is refused before anything
/// is written.
#[tokio::test]
async fn a_built_in_or_empty_name_is_refused() {
    #[derive(Debug)]
    struct Named(&'static str);

    impl CustomSerializer for Named {
        fn name(&self) -> &str {
            self.0
        }

        fn serialize(
            &self,
            _: Payload<'_>,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            unreachable!("never configured")
        }

        fn deserialize(
            &self,
            _: &str,
            _: PayloadSlot<'_>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            unreachable!("never configured")
        }
    }

    let db = test_database().await;
    for name in ["rust_serde", "portable_json", ""] {
        let dbos = DBOS::new(config(
            "custom-refused",
            &db,
            Serializer::custom(Named(name)),
        ));
        let error = dbos.launch().await.unwrap_err();
        assert!(matches!(&error, Error::Config(_)), "{name}: {error:?}");
        let error = Client::connect(ClientConfig {
            serializer: Serializer::custom(Named(name)),
            ..ClientConfig::new(db.url())
        })
        .await
        .unwrap_err();
        assert!(matches!(&error, Error::Config(_)), "{name}: {error:?}");
    }
}

/// A workflow recovered after a crash replays every record it wrote under the serializer — a step,
/// a message, an event read, a stream read, a child's result, and a portable child's error with
/// its code — without running anything a second time.
#[tokio::test]
async fn a_recovered_workflow_replays_what_it_recorded() {
    let db = test_database().await;
    let dbos = DBOS::new(config(
        "custom-recovery",
        &db,
        Serializer::custom(Backwards),
    ));
    let runs = Arc::new(AtomicU32::new(0));
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());

    let sealed_child = {
        let runs = Arc::clone(&runs);
        dbos.register_workflow("sealed_child", move |(): ()| {
            runs.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Error>(ledger()) }
        })
        .unwrap()
    };
    let portable_child = {
        let runs = Arc::clone(&runs);
        dbos.register_workflow_with(
            WorkflowKey::new("portable_child"),
            WorkflowOptions {
                serialization: SerializationFormat::Portable,
            },
            move |(): ()| {
                runs.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<(), Error>(Error::Portable {
                        name: "CardDeclined".to_owned(),
                        message: "declined".to_owned(),
                        code: Some(json!(7)),
                        data: None,
                    })
                }
            },
        )
        .unwrap()
    };
    let parent = {
        let (runs, reached, release) = (
            Arc::clone(&runs),
            Arc::clone(&reached),
            Arc::clone(&release),
        );
        dbos.register_workflow("parent", move |(): ()| {
            let (runs, reached, release) = (
                Arc::clone(&runs),
                Arc::clone(&reached),
                Arc::clone(&release),
            );
            let (sealed_child, portable_child) = (sealed_child.clone(), portable_child.clone());
            async move {
                let id = dbos::workflow_id().expect("inside a workflow");
                let looked_up = dbos::step("lookup", || {
                    let runs = Arc::clone(&runs);
                    async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        Ok(ledger())
                    }
                })
                .await?;
                let child = sealed_child.run(()).await?;
                let code = match portable_child.run(()).await {
                    Err(Error::Portable { code, .. }) => code,
                    other => panic!("the portable child's error should arrive portable: {other:?}"),
                };
                let received = dbos::recv::<Ledger, EngineOnly>(None, DEADLINE)
                    .await?
                    .expect("the message arrives");
                dbos::set_event("ledger", &looked_up).await?;
                let event = dbos::get_event::<Ledger, EngineOnly>(&id, "ledger", Duration::ZERO)
                    .await?
                    .expect("the event is set");
                dbos::write_stream("ledger", &looked_up).await?;
                let streamed = dbos::read_stream_value::<Ledger, EngineOnly>(
                    &id,
                    "ledger",
                    0,
                    ReadStreamOptions::default(),
                )
                .await?;
                reached.notify_one();
                release.notified().await;
                assert_eq!([&child, &received, &event, &streamed], [&looked_up; 4]);
                Ok::<_, Error>((looked_up, code))
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.unwrap();
    let id = handle.workflow_id().to_owned();
    client(&db, Serializer::custom(Backwards))
        .await
        .send(&id, &ledger())
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the workflow never reached its gate");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        3,
        "the step and both children ran once"
    );

    // Abandoned at the gate, and recovered by the relaunch: everything before the gate replays.
    dbos.shutdown().await;
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the recovered workflow never reached its gate");
    release.notify_one();

    let (result, code) = tokio::time::timeout(
        DEADLINE,
        dbos.retrieve_workflow::<(Ledger, Option<serde_json::Value>), EngineOnly>(&id)
            .unwrap()
            .result(),
    )
    .await
    .expect("the workflow finishes")
    .unwrap();
    assert_eq!(result, ledger());
    assert_eq!(code, Some(json!(7)));
    assert_eq!(runs.load(Ordering::SeqCst), 3, "nothing ran a second time");
    dbos.shutdown().await;
}

/// A schedule's context is written by the configured serializer, and a scheduled run is called
/// with it. The column carries no tag, so a context written before the serializer was configured
/// is read with it too, and does not decode.
#[tokio::test]
async fn a_schedules_context_goes_through_the_serializer() {
    let db = test_database().await;
    let dbos = DBOS::new(config(
        "custom-schedule",
        &db,
        Serializer::custom(Backwards),
    ));
    let report = dbos
        .register_workflow("report", |input: ScheduledWorkflowInput<Vec<String>>| async move {
            Ok::<_, Error>(input.context)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let regions = vec!["emea".to_owned(), "apac".to_owned()];
    let far = "0 0 1 1 *";

    dbos.create_schedule(&ScheduleSpec::new("sealed", &report, far, &regions).unwrap())
        .await
        .unwrap();
    // Written by a process without the serializer, as a schedule from before it was configured.
    client(&db, Serializer::RustSerde)
        .await
        .create_schedule(&ScheduleSpec::new("plain", &report, far, &regions).unwrap())
        .await
        .unwrap();

    let stored = |name: &'static str| {
        let db = &db;
        async move {
            sqlx::query_scalar::<_, String>(
                r#"SELECT context FROM "dbos"."workflow_schedules" WHERE schedule_name = $1"#,
            )
            .bind(name)
            .fetch_one(&db.pool().await)
            .await
            .unwrap()
        }
    };
    assert_eq!(forwards::<Vec<String>>(&stored("sealed").await), regions);
    assert_eq!(stored("plain").await, r#"["emea","apac"]"#);

    let schedule = dbos
        .get_schedule("sealed")
        .await
        .unwrap()
        .expect("it exists");
    assert_eq!(schedule.decode_context::<Vec<String>>().unwrap(), regions);
    let run = dbos
        .trigger_schedule::<Vec<String>, EngineOnly>("sealed")
        .await
        .unwrap();
    let ran = tokio::time::timeout(DEADLINE, run.result())
        .await
        .expect("the run finishes")
        .unwrap();
    assert_eq!(ran, regions);

    // The plain context is read with the custom serializer, which cannot make sense of it.
    let plain = dbos
        .get_schedule("plain")
        .await
        .unwrap()
        .expect("it exists");
    let err = plain.decode_context::<Vec<String>>().unwrap_err();
    assert!(matches!(&err, Error::Deserialization { .. }), "{err}");
    let err = dbos
        .trigger_schedule::<Vec<String>, EngineOnly>("plain")
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::Deserialization { .. }), "{err}");

    // Without the serializer, its own context cannot be read.
    let without = client(&db, Serializer::RustSerde).await;
    let schedule = without
        .get_schedule("sealed")
        .await
        .unwrap()
        .expect("it exists");
    assert!(schedule.decode_context::<Vec<String>>().is_err());
    dbos.shutdown().await;
}

/// MessagePack, as hex: a format that is not human-readable, in which serde types such as
/// `Ipv4Addr` take a different shape than in JSON.
#[derive(Debug)]
struct MessagePack;

impl CustomSerializer for MessagePack {
    fn name(&self) -> &str {
        "msgpack_hex"
    }

    fn serialize(
        &self,
        value: Payload<'_>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = rmp_serde::to_vec(&value)?;
        Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn deserialize(
        &self,
        text: &str,
        into: PayloadSlot<'_>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bytes = (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16))
            .collect::<Result<Vec<u8>, _>>()?;
        into.fill(&mut rmp_serde::Deserializer::new(bytes.as_slice()))
    }
}

/// A context whose type serializes differently in a binary format reads back as itself, both
/// listed and in the run a tick starts.
#[tokio::test]
async fn a_schedules_context_survives_a_format_that_is_not_human_readable() {
    let db = test_database().await;
    let dbos = DBOS::new(config(
        "binary-schedule",
        &db,
        Serializer::custom(MessagePack),
    ));
    let ping = dbos
        .register_workflow(
            "ping",
            |input: ScheduledWorkflowInput<std::net::Ipv4Addr>| async move {
                Ok::<_, Error>(input.context)
            },
        )
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let address = std::net::Ipv4Addr::new(10, 0, 0, 1);

    dbos.create_schedule(&ScheduleSpec::new("ping", &ping, "0 0 1 1 *", &address).unwrap())
        .await
        .unwrap();
    let schedule = dbos.get_schedule("ping").await.unwrap().expect("it exists");
    assert_eq!(
        schedule.decode_context::<std::net::Ipv4Addr>().unwrap(),
        address
    );

    let run = dbos
        .trigger_schedule::<std::net::Ipv4Addr, EngineOnly>("ping")
        .await
        .unwrap();
    let ran = tokio::time::timeout(DEADLINE, run.result())
        .await
        .expect("the run finishes")
        .unwrap();
    assert_eq!(ran, address);
    dbos.shutdown().await;
}
