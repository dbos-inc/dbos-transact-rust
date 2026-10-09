//! Durable streams, against real databases.

use std::sync::Arc;
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowStatus, WrittenBy};
use dbos::{
    Client, ClientConfig, Config, DBOS, EngineOnly, Error, ReadStreamOptions, StartOptions,
};
use tokio::sync::Notify;

use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);

/// Stable per application name; see `events::app_version` for why it cannot be one constant.
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

/// A handle for reading rows behind the instance's back.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

async fn client(db: &TestDatabase) -> Client {
    Client::connect(ClientConfig::new(db.url()))
        .await
        .expect("client connect failed")
}

fn start(id: &str) -> StartOptions<'_> {
    StartOptions {
        workflow_id: Some(id),
        ..StartOptions::default()
    }
}

/// Every value a reader delivers until the stream ends.
async fn drain<T: serde::de::DeserializeOwned>(
    mut values: dbos::StreamReader<T>,
) -> dbos::Result<Vec<T>> {
    let mut read = Vec::new();
    while let Some(value) = values.next().await? {
        read.push(value);
    }
    Ok(read)
}

/// Waits until the workflow's row reaches `status`.
async fn until_status(reader: &PostgresSystemDatabase, workflow_id: &str, status: WorkflowStatus) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader.get_workflow(workflow_id).await.expect("read failed");
            if row.is_some_and(|row| row.status == status) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{workflow_id} never reached {status}"));
}

/// A producer that writes its input to `out`, one step each, and closes it if asked.
fn register_writer(dbos: &DBOS) -> dbos::WorkflowRef<(Vec<String>, bool), ()> {
    dbos.register_workflow(
        "writer",
        |(values, close): (Vec<String>, bool)| async move {
            for value in &values {
                dbos::write_stream("out", value).await?;
            }
            if close {
                dbos::close_stream("out").await?;
            }
            Ok::<_, Error>(())
        },
    )
    .unwrap()
}

/// A producer that writes `before`, waits at a gate, then writes `after` and closes.
fn register_gated_writer(
    dbos: &DBOS,
    reached: &Arc<Notify>,
    release: &Arc<Notify>,
) -> dbos::WorkflowRef<(Vec<String>, Vec<String>), ()> {
    let (reached, release) = (Arc::clone(reached), Arc::clone(release));
    dbos.register_workflow(
        "gated-writer",
        move |(before, after): (Vec<String>, Vec<String>)| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                for value in &before {
                    dbos::write_stream("out", value).await?;
                }
                reached.notify_one();
                release.notified().await;
                for value in &after {
                    dbos::write_stream("out", value).await?;
                }
                dbos::close_stream("out").await?;
                Ok::<_, Error>(())
            }
        },
    )
    .unwrap()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_owned()).collect()
}

/// A workflow writes and closes; readers outside it, on the instance and on a client, read the
/// values in order and then end — from the start, and from an offset.
#[tokio::test]
async fn a_written_stream_reads_back_in_order_and_ends_at_its_close() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-basic", &db));
    let writer = register_writer(&dbos);
    dbos.launch().await.expect("launch failed");

    writer
        .start_with((strings(&["a", "b", "c"]), true), start("wf-basic"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let read = drain(dbos.read_stream::<String>("wf-basic", "out", ReadStreamOptions::default()))
        .await
        .expect("read failed");
    assert_eq!(read, ["a", "b", "c"]);

    let from_one =
        drain(dbos.read_stream_from::<String>("wf-basic", "out", 1, ReadStreamOptions::default()))
            .await
            .expect("read failed");
    assert_eq!(from_one, ["b", "c"]);

    let client = client(&db).await;
    let by_client =
        drain(client.read_stream::<String>("wf-basic", "out", ReadStreamOptions::default()))
            .await
            .expect("read failed");
    assert_eq!(by_client, ["a", "b", "c"]);

    // A key nothing was written to is a stream that ends at once: its workflow has finished.
    let other =
        drain(dbos.read_stream::<String>("wf-basic", "other", ReadStreamOptions::default()))
            .await
            .expect("read failed");
    assert!(other.is_empty());

    // An ended reader stays ended, and takes nothing more from the database to say so.
    let mut values = dbos.read_stream::<String>("wf-basic", "out", ReadStreamOptions::default());
    while values.next().await.expect("read failed").is_some() {}
    assert_eq!(values.next().await.expect("read failed"), None);
    assert_eq!(values.offset(), 3);

    dbos.shutdown().await;
}

/// A stream nobody closes ends when its workflow finishes, after its last value.
#[tokio::test]
async fn an_unclosed_stream_ends_when_its_workflow_finishes() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-unclosed", &db));
    let writer = register_writer(&dbos);
    dbos.launch().await.expect("launch failed");

    writer
        .start_with((strings(&["a", "b"]), false), start("wf-unclosed"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let read = tokio::time::timeout(
        DEADLINE,
        drain(dbos.read_stream::<String>("wf-unclosed", "out", ReadStreamOptions::default())),
    )
    .await
    .expect("the reader never ended")
    .expect("read failed");
    assert_eq!(read, ["a", "b"]);

    dbos.shutdown().await;
}

/// A close ends the stream even with values written after it.
#[tokio::test]
async fn a_read_stops_at_the_close() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-close", &db));
    let workflow = dbos
        .register_workflow("closes-early", |()| async move {
            dbos::write_stream("out", &"a").await?;
            dbos::close_stream("out").await?;
            dbos::write_stream("out", &"after").await?;
            Ok::<_, Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    workflow
        .start_with((), start("wf-close"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let read = drain(dbos.read_stream::<String>("wf-close", "out", ReadStreamOptions::default()))
        .await
        .expect("read failed");
    assert_eq!(read, ["a"]);

    dbos.shutdown().await;
}

/// More values than one page, read both after they are written and while they are being written.
#[tokio::test]
async fn reads_cross_pages_whether_the_values_are_there_or_still_coming() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-pages", &db));
    let writer = register_gated_writer(&dbos, &reached, &release);
    dbos.launch().await.expect("launch failed");

    let many: Vec<String> = (0..250).map(|i| i.to_string()).collect();
    let handle = writer
        .start_with((Vec::new(), many.clone()), start("wf-pages"))
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the writer never reached its gate");

    // A live reader, waiting before anything is written.
    let client = client(&db).await;
    let live = tokio::spawn({
        let values = client.read_stream::<String>("wf-pages", "out", ReadStreamOptions::default());
        async move { drain(values).await }
    });
    release.notify_one();
    handle.result().await.expect("the writer failed");

    let read_live = tokio::time::timeout(DEADLINE, live)
        .await
        .expect("the live reader never ended")
        .expect("the live reader panicked")
        .expect("live read failed");
    assert_eq!(read_live, many);

    // And a reader after the fact, from part-way into the second page.
    let read_after = drain(dbos.read_stream_from::<String>(
        "wf-pages",
        "out",
        150,
        ReadStreamOptions::default(),
    ))
    .await
    .expect("read failed");
    assert_eq!(read_after, many[150..]);

    dbos.shutdown().await;
}

/// The single-value read: a value already there, one still to come, and one that never will be.
#[tokio::test]
async fn the_single_value_read_waits_for_its_value_or_reports_the_stream_ended() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-offset", &db));
    let writer = register_gated_writer(&dbos, &reached, &release);
    dbos.launch().await.expect("launch failed");

    let handle = writer
        .start_with((strings(&["a"]), strings(&["b"])), start("wf-offset"))
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the writer never reached its gate");

    let client = client(&db).await;
    let first: String = client
        .read_stream_value("wf-offset", "out", 0, ReadStreamOptions::default())
        .await
        .expect("read failed");
    assert_eq!(first, "a");

    let waiting = tokio::spawn({
        let dbos = dbos.clone();
        async move {
            dbos.read_stream_value::<String>("wf-offset", "out", 1, ReadStreamOptions::default())
                .await
        }
    });
    release.notify_one();
    handle.result().await.expect("the writer failed");
    let second = tokio::time::timeout(DEADLINE, waiting)
        .await
        .expect("the value read never returned")
        .expect("the value read panicked")
        .expect("read failed");
    assert_eq!(second, "b");

    // Offset 2 is the close, and 5 is past it: neither will ever hold a value.
    for offset in [2, 5] {
        let ended = client
            .read_stream_value::<String>("wf-offset", "out", offset, ReadStreamOptions::default())
            .await
            .expect_err("no value will arrive");
        assert!(
            matches!(&ended, Error::StreamTimeout { timeout: None, .. }),
            "offset {offset}: {ended:?}"
        );
    }

    dbos.shutdown().await;
}

/// The timeout is per value: a reader that has its first value still times out on the second.
#[tokio::test]
async fn a_read_times_out_waiting_for_its_next_value() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-timeout", &db));
    let writer = register_gated_writer(&dbos, &reached, &release);
    dbos.launch().await.expect("launch failed");

    writer
        .start_with((strings(&["a"]), Vec::new()), start("wf-timeout"))
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the writer never reached its gate");

    let timeout = Duration::from_millis(300);
    let client = client(&db).await;
    let mut values = client.read_stream::<String>(
        "wf-timeout",
        "out",
        ReadStreamOptions {
            timeout: Some(timeout),
            polling_interval: Some(Duration::from_millis(50)),
        },
    );
    assert_eq!(
        values.next().await.expect("read failed").as_deref(),
        Some("a")
    );
    let timed_out = values.next().await.expect_err("nothing more was written");
    assert!(
        matches!(&timed_out, Error::StreamTimeout { workflow_id, key, timeout: Some(t) }
            if workflow_id == "wf-timeout" && key == "out" && *t == timeout),
        "{timed_out:?}"
    );
    assert_eq!(values.next().await.expect("a spent reader"), None);

    release.notify_one();
    dbos.shutdown().await;
}

/// A value that does not decode fails its own read, and not the ones before it.
#[tokio::test]
async fn a_value_that_does_not_decode_fails_only_its_own_read() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-decode", &db));
    let workflow = dbos
        .register_workflow("mixed", |()| async move {
            dbos::write_stream("out", &1u32).await?;
            dbos::write_stream("out", &2u32).await?;
            dbos::write_stream("out", &"three").await?;
            dbos::close_stream("out").await?;
            Ok::<_, Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    workflow
        .start_with((), start("wf-mixed"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let mut values = dbos.read_stream::<u32>("wf-mixed", "out", ReadStreamOptions::default());
    assert_eq!(values.next().await.expect("read failed"), Some(1));
    assert_eq!(values.next().await.expect("read failed"), Some(2));
    let failed = values.next().await.expect_err("a string is not a u32");
    assert!(
        matches!(failed, Error::Deserialization { .. }),
        "{failed:?}"
    );

    dbos.shutdown().await;
}

/// A producer cancelled mid-stream: its reader delivers everything written, then ends rather than
/// waiting for values that will never come.
#[tokio::test]
async fn a_cancelled_producers_reader_drains_and_ends() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-cancel", &db));
    let writer = register_gated_writer(&dbos, &reached, &release);
    dbos.launch().await.expect("launch failed");

    writer
        .start_with(
            (strings(&["a", "b"]), strings(&["never"])),
            start("wf-cancelled"),
        )
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the writer never reached its gate");

    let client = client(&db).await;
    let mut values =
        client.read_stream::<String>("wf-cancelled", "out", ReadStreamOptions::default());
    assert_eq!(
        values.next().await.expect("read failed").as_deref(),
        Some("a")
    );
    assert_eq!(
        values.next().await.expect("read failed").as_deref(),
        Some("b")
    );

    let blocked = tokio::spawn(async move { values.next().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    dbos.cancel("wf-cancelled").await.expect("cancel failed");
    let ended = tokio::time::timeout(DEADLINE, blocked)
        .await
        .expect("the reader never noticed the producer stop")
        .expect("the reader panicked")
        .expect("read failed");
    assert_eq!(ended, None);

    dbos.shutdown().await;
}

/// Writes and a close from inside a step are plain: the reader sees them, and the only step the
/// workflow records is its own.
#[tokio::test]
async fn a_step_may_write_and_close_a_stream() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-step", &db));
    let workflow = dbos
        .register_workflow("writes-in-a-step", |()| async move {
            dbos::step("produce", || async {
                dbos::write_stream("out", &"a").await?;
                dbos::write_stream("out", &"b").await?;
                dbos::close_stream("out").await?;
                Ok::<_, Error>(())
            })
            .await?;
            Ok::<_, Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    workflow
        .start_with((), start("wf-step"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the workflow failed");

    let read = drain(dbos.read_stream::<String>("wf-step", "out", ReadStreamOptions::default()))
        .await
        .expect("read failed");
    assert_eq!(read, ["a", "b"]);

    let steps = reader(&db)
        .await
        .list_workflow_steps("wf-step", false, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(
        steps
            .iter()
            .map(|s| s.step_name.as_str())
            .collect::<Vec<_>>(),
        ["produce"],
        "a write inside a step records nothing of its own"
    );

    dbos.shutdown().await;
}

/// **A workflow's read is checkpointed value by value**, so a recovered reader is handed what it
/// read the first time — here even though the stream it read no longer exists at all. The end is
/// recorded too: the replayed reader ends where it ended, and a single-value read past the end
/// replays its timeout, where a live read would now find no workflow.
#[tokio::test]
async fn a_workflows_read_replays_its_recorded_values() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-replay", &db));
    let writer = register_writer(&dbos);
    let consumer = {
        let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
        dbos.register_workflow("consumer", move |producer: String| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                let mut values =
                    dbos::read_stream::<String, EngineOnly>(&producer, "out", Default::default());
                let mut read = Vec::new();
                while let Some(value) = values.next().await? {
                    read.push(value);
                }
                let past_the_end = dbos::read_stream_value::<String, EngineOnly>(
                    &producer,
                    "out",
                    10,
                    Default::default(),
                )
                .await;
                let ended = matches!(
                    past_the_end,
                    Err(Error::StreamTimeout { timeout: None, .. })
                );
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>((read, ended))
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    writer
        .start_with((strings(&["a", "b", "c"]), true), start("wf-source"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");
    let running = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .start_with("wf-source".to_owned(), start("wf-consumer"))
                .await?
                .result()
                .await
        }
    });
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the consumer never reached its gate");

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps("wf-consumer", true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect::<Vec<_>>(),
        [
            (0, "DBOS.read_stream"),
            (1, "DBOS.read_stream"),
            (2, "DBOS.read_stream"),
            (3, "DBOS.read_stream"),
            (4, "DBOS.read_stream_value"),
        ]
    );

    // Abandon the consumer, and delete what it read: a live read would now find no workflow.
    dbos.shutdown().await;
    assert!(matches!(
        running.await.expect("the task panicked"),
        Err(Error::Interrupted { .. })
    ));
    let client = client(&db).await;
    client.delete("wf-source").await.expect("delete failed");

    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the recovered consumer never reached its gate");
    release.notify_one();
    until_status(&reader, "wf-consumer", WorkflowStatus::Success).await;
    let (read, ended): (Vec<String>, bool) = client
        .retrieve_workflow::<_, EngineOnly>("wf-consumer")
        .result()
        .await
        .expect("the consumer failed");
    assert_eq!(
        read,
        ["a", "b", "c"],
        "the recorded values, then the recorded end"
    );
    assert!(ended, "the read past the end replayed its recorded end");

    dbos.shutdown().await;
}

/// **A workflow's timed-out read is recorded**, so its replay times out again without waiting —
/// even though, by then, a value is there to be read.
#[tokio::test]
async fn a_workflows_timed_out_read_replays_as_a_timeout() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-timeout-replay", &db));
    let silent = dbos
        .register_workflow("silent", |()| async move {
            // Runs, and writes nothing, until it is abandoned.
            std::future::pending::<()>().await;
            Ok::<_, Error>(())
        })
        .unwrap();
    let consumer = {
        let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
        dbos.register_workflow("impatient", move |producer: String| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                let read = dbos::read_stream_value::<String, EngineOnly>(
                    &producer,
                    "out",
                    0,
                    ReadStreamOptions {
                        timeout: Some(Duration::from_millis(200)),
                        polling_interval: Some(Duration::from_millis(50)),
                    },
                )
                .await;
                let timed_out = matches!(read, Err(Error::StreamTimeout { .. }));
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>(timed_out)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    silent
        .start_with((), start("wf-silent"))
        .await
        .expect("start failed");
    let running = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .start_with("wf-silent".to_owned(), start("wf-impatient"))
                .await?
                .result()
                .await
        }
    });
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the consumer never reached its gate");

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps("wf-impatient", true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "DBOS.read_stream_value");
    assert!(steps[0].error.is_some(), "the timeout is the step's error");

    dbos.shutdown().await;
    let _ = running.await.expect("the task panicked");
    // The value a live read would now find at once.
    reader
        .write_stream(
            "wf-silent",
            99,
            "out",
            "\"late\"",
            Some("rust_serde"),
            WrittenBy::Step,
        )
        .await
        .expect("write failed");

    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the recovered consumer never reached its gate");
    release.notify_one();
    until_status(&reader, "wf-impatient", WorkflowStatus::Success).await;
    let client = client(&db).await;
    let timed_out: bool = client
        .retrieve_workflow::<_, EngineOnly>("wf-impatient")
        .result()
        .await
        .expect("the consumer failed");
    assert!(
        timed_out,
        "the replay read the late value instead of its record"
    );

    dbos.shutdown().await;
}

/// Two reads driven together in one workflow body: each took its step id where it was written, so
/// both are allowed and both read.
#[tokio::test]
async fn reads_joined_in_one_workflow_both_read() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-joined", &db));
    let writer = register_writer(&dbos);
    let reads_two = dbos
        .register_workflow("reads-two", |()| async move {
            let mut one =
                dbos::read_stream::<String, EngineOnly>("wf-two", "out", Default::default());
            let mut two =
                dbos::read_stream::<String, EngineOnly>("wf-two", "out", Default::default());
            let (first, second) = tokio::join!(one.next(), two.next());
            Ok::<_, Error>((first?, second?))
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    writer
        .start_with((strings(&["a"]), true), start("wf-two"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let read = reads_two
        .start_with((), start("wf-reads-two"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the workflow failed");
    assert_eq!(read, (Some("a".to_owned()), Some("a".to_owned())));

    dbos.shutdown().await;
}

/// **A read dropped while it records its value loses nothing.** The losing branch of a
/// `select_step!` is dropped wherever it stands; here a zero timeout stands in for it, polling a
/// read once — into its record — and dropping it. The next read delivers the value the dropped one
/// was recording, and the stream comes out whole: nothing skipped, nothing twice.
#[tokio::test]
async fn a_dropped_read_loses_no_value() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-dropped", &db));
    let writer = register_writer(&dbos);
    let drops_one = dbos
        .register_workflow("drops-one", |()| async move {
            let mut values =
                dbos::read_stream::<String, EngineOnly>("wf-dropped", "out", Default::default());
            let mut read = Vec::new();
            // Fills the read-ahead buffer with the rest of the stream.
            read.extend(values.next().await?);
            if let Ok(finished) = tokio::time::timeout(Duration::ZERO, values.next()).await {
                read.extend(finished?);
            }
            while let Some(value) = values.next().await? {
                read.push(value);
            }
            Ok::<_, Error>(read)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let many = strings(&["0", "1", "2", "3", "4"]);
    writer
        .start_with((many.clone(), true), start("wf-dropped"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let read = drops_one
        .start_with((), start("wf-drops-one"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the workflow failed");
    assert_eq!(read, many);

    dbos.shutdown().await;
}

/// **A read of a workflow that does not exist is recorded**, so a workflow that caught the refusal
/// replays it — even once the workflow exists and a live read would find its value.
#[tokio::test]
async fn a_read_of_a_missing_workflow_replays_as_missing() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-missing", &db));
    let writer = register_writer(&dbos);
    let consumer = {
        let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
        dbos.register_workflow("too-early", move |producer: String| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                let read = dbos::read_stream_value::<String, EngineOnly>(
                    &producer,
                    "out",
                    0,
                    Default::default(),
                )
                .await;
                let missing = matches!(
                    read,
                    Err(Error::SystemDatabase(
                        dbos::sysdb::Error::NonExistentWorkflow { .. }
                    ))
                );
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>(missing)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let running = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .start_with("wf-later".to_owned(), start("wf-too-early"))
                .await?
                .result()
                .await
        }
    });
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the consumer never reached its gate");
    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps("wf-too-early", true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "DBOS.read_stream_value");
    assert!(steps[0].error.is_some(), "the refusal is the step's error");

    // Now the workflow exists, with a value where the read looked.
    writer
        .start_with((strings(&["x"]), true), start("wf-later"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    dbos.shutdown().await;
    let _ = running.await.expect("the task panicked");
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the recovered consumer never reached its gate");
    release.notify_one();
    until_status(&reader, "wf-too-early", WorkflowStatus::Success).await;
    let missing: bool = client(&db)
        .await
        .retrieve_workflow::<_, EngineOnly>("wf-too-early")
        .result()
        .await
        .expect("the consumer failed");
    assert!(missing, "the replay read live instead of its record");

    dbos.shutdown().await;
}

/// The calls that cannot stand where they were made, and the arguments a reader refuses.
#[tokio::test]
async fn stream_calls_refuse_what_they_cannot_do() {
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-refusals", &db));
    let writer = register_writer(&dbos);
    dbos.launch().await.expect("launch failed");
    writer
        .start_with((strings(&["a"]), true), start("wf-refusals"))
        .await
        .expect("start failed")
        .result()
        .await
        .expect("the writer failed");

    let outside = dbos::write_stream::<_, EngineOnly>("out", &"x")
        .await
        .expect_err("no workflow to write to");
    assert!(
        matches!(outside, Error::NotInWorkflow { .. }),
        "{outside:?}"
    );
    let outside = dbos::close_stream::<EngineOnly>("out")
        .await
        .expect_err("no workflow to close");
    assert!(
        matches!(outside, Error::NotInWorkflow { .. }),
        "{outside:?}"
    );
    let outside = dbos::read_stream::<String, EngineOnly>("wf-refusals", "out", Default::default())
        .next()
        .await
        .expect_err("the free reader is for workflows");
    assert!(
        matches!(outside, Error::NotInWorkflow { .. }),
        "{outside:?}"
    );

    let missing = dbos
        .read_stream::<String>("wf-nobody", "out", ReadStreamOptions::default())
        .next()
        .await
        .expect_err("no such workflow");
    assert!(
        matches!(
            missing,
            Error::SystemDatabase(dbos::sysdb::Error::NonExistentWorkflow { .. })
        ),
        "{missing:?}"
    );

    for (offset, options) in [
        (
            0,
            ReadStreamOptions {
                polling_interval: Some(Duration::ZERO),
                ..ReadStreamOptions::default()
            },
        ),
        (-1, ReadStreamOptions::default()),
    ] {
        let mut values = dbos.read_stream_from::<String>("wf-refusals", "out", offset, options);
        let refused = values.next().await.expect_err("an invalid option");
        assert!(
            matches!(refused, Error::InvalidArgument { .. }),
            "{refused:?}"
        );
        assert_eq!(values.next().await.expect("a spent reader"), None);
    }

    dbos.shutdown().await;
}

/// A workflow waiting on a stream stops when the workflow itself is cancelled, rather than waiting
/// on a producer that may never write.
///
/// Cancelling only marks the row, and awaiting the handle reads the row, so the handle alone would
/// report the cancellation with the read still waiting. The workflow signals when its read returns,
/// which is what shows the read itself stopped.
#[tokio::test]
async fn a_waiting_workflow_read_stops_when_its_workflow_is_cancelled() {
    let returned = Arc::new(Notify::new());
    let cancelled = Arc::new(std::sync::Mutex::new(None));
    let db = test_database().await;
    let dbos = DBOS::new(config("streams-reader-cancel", &db));
    let silent = dbos
        .register_workflow("silent", |()| async move {
            std::future::pending::<()>().await;
            Ok::<_, Error>(())
        })
        .unwrap();
    let waits = {
        let (returned, cancelled) = (Arc::clone(&returned), Arc::clone(&cancelled));
        dbos.register_workflow("waits", move |producer: String| {
            let (returned, cancelled) = (Arc::clone(&returned), Arc::clone(&cancelled));
            async move {
                let mut values =
                    dbos::read_stream::<String, EngineOnly>(&producer, "out", Default::default());
                let read = values.next().await;
                *cancelled.lock().unwrap() =
                    Some(matches!(read, Err(Error::WorkflowCancelled { .. })));
                returned.notify_one();
                read
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    silent
        .start_with((), start("wf-quiet"))
        .await
        .expect("start failed");
    waits
        .start_with("wf-quiet".to_owned(), start("wf-waits"))
        .await
        .expect("start failed");
    tokio::time::sleep(Duration::from_millis(300)).await;

    dbos.cancel("wf-waits").await.expect("cancel failed");
    tokio::time::timeout(DEADLINE, returned.notified())
        .await
        .expect("the read never noticed its workflow was cancelled");
    assert_eq!(
        *cancelled.lock().unwrap(),
        Some(true),
        "the read stopped with the workflow's cancellation"
    );

    dbos.shutdown().await;
}
