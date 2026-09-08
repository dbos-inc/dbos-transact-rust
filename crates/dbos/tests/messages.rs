//! Workflow messages, against real databases.

use std::sync::Arc;
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error, Forks, Message, SendOptions};

use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);

/// The version each instance in this file launches with, derived from its application name — see
/// the note on `events::app_version`, which this mirrors for the same reason.
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

/// The step names a workflow recorded, in step-id order.
async fn steps(reader: &PostgresSystemDatabase, workflow_id: &str) -> Vec<(i32, String)> {
    reader
        .list_workflow_steps(workflow_id, true, None, None, None)
        .await
        .expect("read failed")
        .into_iter()
        .map(|step| (step.step_id, step.step_name))
        .collect()
}

/// Waits for a workflow's row to reach `SUCCESS`.
async fn await_success(reader: &PostgresSystemDatabase, workflow_id: &str) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(workflow_id)
                .await
                .expect("read failed")
                .expect("the row exists");
            if row.status == WorkflowStatus::Success {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the workflow never finished");
}

/// The approval pattern: a workflow waits, something outside it sends, the workflow proceeds.
///
/// Also pins the two checkpoints a receive writes and the order of their ids — the receive first,
/// its deadline second — which is what a replay looks up and what another SDK replaying this
/// workflow would expect to find.
#[tokio::test]
async fn a_message_from_outside_reaches_a_waiting_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("recv-app", &db));
    let waits = dbos
        .register_workflow("waits", |()| async {
            let approval: Option<String> = dbos::recv(None, DEADLINE).await?;
            Ok::<_, dbos::Error>(approval)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = waits.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();

    // The send races the workflow reaching its receive, deliberately: a message that arrives first
    // waits in the database, so both orderings must deliver.
    dbos.send(&workflow_id, &"approved")
        .await
        .expect("send failed");

    let approval = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow never finished")
        .expect("the workflow failed");
    assert_eq!(approval, Some("approved".to_owned()));

    let reader = reader(&db).await;
    assert_eq!(
        steps(&reader, &workflow_id).await,
        [(0, "DBOS.recv".to_owned()), (1, "DBOS.sleep".to_owned())],
        "the receive is recorded first and its deadline second",
    );

    dbos.shutdown().await;
}

/// **Messages take their step ids where they are built, not where they are first polled.**
///
/// The counterpart in `events.rs` makes the argument for the shape: `join!` builds every branch
/// before polling any and then first-polls them in source order, so a test that builds and drives
/// in the same order passes against poll-time ids too. These three are built `a, b, c` and handed
/// to `join!` as `c, b, a`.
///
/// The receive is what this adds to that test: it is **two** steps, and its deadline's id has to
/// come from the counter directly behind its own however the two are driven — a pair split by the
/// poll order would put the sleep somewhere a replay does not expect it.
#[tokio::test]
async fn messages_driven_out_of_build_order_keep_the_ids_they_were_built_with() {
    let db = test_database().await;
    let dbos = DBOS::new(config("message-join-app", &db));
    let talks_to_itself = dbos
        .register_workflow("talks_to_itself", |id: String| async move {
            // Built a, b, c — the order their ids come from the counter in, and the order a
            // replay will build them in again.
            let a = dbos::send(&id, &"hello");
            let b = dbos::recv::<String, _>(None, DEADLINE);
            let c = dbos::step("after", || async { Ok::<u32, Error>(1) });
            assert_eq!(
                (a.step_id(), b.step_id(), c.step_id()),
                // A receive is two steps, so the one after it is two on: the deadline holds id 2.
                (Some(0), Some(1), Some(3)),
                "the ids were taken at the call, in source order, before anything was polled"
            );
            // ...and driven c, b, a. The send is polled last and the receive is waiting on it,
            // which `join!` resolves by polling both until they are done.
            let (c, b, a) = tokio::join!(c, b, a);
            a?;
            assert_eq!(b?.as_deref(), Some("hello"), "the message never arrived");
            c?;
            Ok::<u32, Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let workflow_id = "talks-to-itself";
    talks_to_itself
        .run_with(
            workflow_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(workflow_id),
                ..Default::default()
            },
        )
        .await
        .expect("the workflow failed");

    let steps = reader(&db)
        .await
        .list_workflow_steps(workflow_id, true, None, None, None)
        .await
        .expect("read failed");
    let recorded: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        recorded,
        [
            (0, "DBOS.send"),
            // The receive and the deadline it took with it, still adjacent.
            (1, "DBOS.recv"),
            (2, "DBOS.sleep"),
            (3, "after"),
        ],
        "the ids follow the order the calls were built in, not the order they were polled in",
    );

    dbos.shutdown().await;
}

/// A receive on one topic never takes a message sent on another, and finding nothing is a value.
#[tokio::test]
async fn topics_do_not_cross_and_absence_is_a_value() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("topics-app", &db));
    let picky = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("picky", move |()| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                // Held until the test has sent, so a `None` below is a topic that did not match
                // rather than a message that had not arrived.
                reached.notify_one();
                release.notified().await;
                let default: Option<String> = dbos::recv(None, Duration::ZERO).await?;
                let approvals: Option<String> =
                    dbos::recv(Some("approvals"), Duration::ZERO).await?;
                Ok::<_, dbos::Error>((default, approvals))
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let handle = picky.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    dbos.send_with(
        &workflow_id,
        &"yes",
        SendOptions {
            topic: Some("approvals"),
            ..Default::default()
        },
    )
    .await
    .expect("send failed");
    release_gate.notify_one();

    let (default, approvals) = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow never finished")
        .expect("the workflow failed");
    assert_eq!(
        default, None,
        "a receive on the default topic must not take a message sent on `approvals`",
    );
    assert_eq!(approvals, Some("yes".to_owned()));

    dbos.shutdown().await;
}

/// A replay returns the message the first run took and does not consume a second one.
///
/// The property the whole checkpoint exists for: a message taken but forgotten would be gone with
/// nothing recording it, which is the one failure a durable receive may not have.
#[tokio::test]
async fn a_replay_returns_the_message_it_took_rather_than_taking_another() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("replay-app", &db));
    let receives = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("receives", move |()| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                let first: Option<String> = dbos::recv(None, DEADLINE).await?;
                reached.notify_one();
                release.notified().await;
                Ok::<_, dbos::Error>(first)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let handle = receives.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    // Two messages, so a replay that took another would visibly take the second.
    dbos.send(&workflow_id, &"first")
        .await
        .expect("send failed");
    dbos.send(&workflow_id, &"second")
        .await
        .expect("send failed");

    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    // Abandon at the gate and let recovery replay the receive.
    dbos.shutdown().await;
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered workflow never reached its gate again");
    release_gate.notify_one();

    let reader = reader(&db).await;
    await_success(&reader, &workflow_id).await;

    assert_eq!(
        steps(&reader, &workflow_id).await,
        [(0, "DBOS.recv".to_owned()), (1, "DBOS.sleep".to_owned())],
        "the replay added no second receive",
    );

    // What the replay returned, which is the half the step list cannot show: a replay that
    // consumed nothing but also remembered nothing would pass every assertion below.
    let recovered = dbos
        .retrieve_workflow::<Option<String>, dbos::Error>(&workflow_id)
        .expect("retrieve failed");
    let taken = tokio::time::timeout(DEADLINE, recovered.result())
        .await
        .expect("the recovered workflow never finished")
        .expect("the workflow failed");
    assert_eq!(
        taken,
        Some("first".to_owned()),
        "the replay returned the message the first run took",
    );

    let notifications = reader
        .get_all_notifications(&workflow_id)
        .await
        .expect("read failed");
    // A count rather than a positional `[true, false]`: `get_all_notifications` orders by
    // `created_at_epoch_ms` alone, and two back-to-back sends can share a millisecond. Which
    // message was taken is pinned by the returned value above, so the rows need only say how
    // many were taken.
    assert_eq!(notifications.len(), 2, "both messages were delivered");
    assert_eq!(
        notifications.iter().filter(|n| n.consumed).count(),
        1,
        "the replay must not have consumed the second message",
    );

    dbos.shutdown().await;
}

/// A workflow's send is a checkpointed step, so a replay delivers once rather than twice.
#[tokio::test]
async fn a_workflows_send_is_checkpointed_and_a_replay_does_not_send_twice() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("send-app", &db));
    // The destination: a workflow that exists to be sent to, and that outlives the sender's
    // abandonment so the foreign key always has something to point at.
    let sleeps = dbos
        .register_workflow("sleeps", |()| async {
            dbos::sleep(Duration::from_secs(5)).await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    let sends = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("sends", move |destination: String| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::send(&destination, &"hello").await?;
                reached.notify_one();
                release.notified().await;
                Ok::<_, dbos::Error>(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let destination = sleeps.start(()).await.expect("start failed");
    let destination_id = destination.workflow_id().to_owned();
    let sender = sends
        .start(destination_id.clone())
        .await
        .expect("start failed");
    let sender_id = sender.workflow_id().to_owned();

    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the sender never reached its gate");

    // Abandon after the send and let recovery replay it.
    dbos.shutdown().await;
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered sender never reached its gate again");
    release_gate.notify_one();

    let reader = reader(&db).await;
    await_success(&reader, &sender_id).await;

    assert_eq!(
        steps(&reader, &sender_id).await,
        [(0, "DBOS.send".to_owned())],
        "the send is recorded under the cross-SDK name, once",
    );
    let notifications = reader
        .get_all_notifications(&destination_id)
        .await
        .expect("read failed");
    assert_eq!(
        notifications.len(),
        1,
        "the replay found the recorded step and delivered nothing",
    );

    dbos.shutdown().await;
}

/// A receive may not stand inside a step; a send may, and is then plain.
///
/// The asymmetry is the references': a send that runs twice delivers twice, where a receive that
/// runs twice loses a message. Python, TypeScript and Java send plainly from inside a step and all
/// four block a receive there.
#[tokio::test]
async fn a_step_may_send_but_may_not_receive() {
    let db = test_database().await;
    let dbos = DBOS::new(config("guard-app", &db));
    let receives_in_step = dbos
        .register_workflow("receives_in_step", |()| async {
            dbos::step("read", || async {
                let _: Option<String> = dbos::recv(None, Duration::ZERO).await?;
                Ok(())
            })
            .await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    // The destination outlives the sender, so the foreign key always has something to point at.
    let sleeps = dbos
        .register_workflow("sleeps", |()| async {
            dbos::sleep(Duration::from_secs(5)).await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    let sends_in_step = dbos
        .register_workflow("sends_in_step", |destination: String| async move {
            dbos::step("write", || {
                let destination = destination.clone();
                async move {
                    dbos::send(&destination, &"hello").await?;
                    Ok(())
                }
            })
            .await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let err = receives_in_step
        .run(())
        .await
        .expect_err("a receive inside a step should be refused");
    assert!(
        matches!(&err, Error::InsideStep { operation } if operation == "recv"),
        "{err}"
    );

    let destination = sleeps.start(()).await.expect("start failed");
    let destination_id = destination.workflow_id().to_owned();
    let handle = sends_in_step
        .start(destination_id.clone())
        .await
        .expect("start failed");
    let sender_id = handle.workflow_id().to_owned();
    tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the sender never finished")
        .expect("the send inside a step should have been allowed");

    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_all_notifications(&destination_id)
            .await
            .expect("read failed")
            .len(),
        1,
        "the message was delivered",
    );
    assert_eq!(
        steps(&reader, &sender_id).await,
        [(0, "write".to_owned())],
        "the step is the only checkpoint: the send inside it recorded nothing of its own",
    );

    dbos.shutdown().await;
}

/// `SendOptions` reaches the insert: with `Forks::Include` a message also lands on the workflows
/// forked from its destination, and with the default it does not.
///
/// The fan-out itself is `sysdb`'s and is tested there. What this pins is the engine mapping —
/// `SendOptions::forks` becoming the flag `sysdb` takes — which `Connection::send_message` does
/// identically for all three single-send surfaces, so exercising it through one covers them all.
/// `Connection::send_messages` maps it the same way for the batch, which
/// [`a_workflows_batch_is_one_checkpoint`] reaches.
#[tokio::test]
async fn a_send_may_fan_out_to_the_destinations_forks() {
    let db = test_database().await;
    let dbos = DBOS::new(config("forks-app", &db));
    let noop = dbos
        .register_workflow("noop", |()| async { Ok::<_, dbos::Error>(()) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let original = noop.start(()).await.expect("start failed");
    let original_id = original.workflow_id().to_owned();
    original.result().await.expect("the workflow failed");

    let fork = dbos
        .fork::<(), dbos::EngineOnly>(&original_id, dbos::ForkFrom::Beginning)
        .await
        .expect("fork failed");
    let fork_id = fork.workflow_id().to_owned();

    let reader = reader(&db).await;
    let delivered = async |id: &str| {
        reader
            .get_all_notifications(id)
            .await
            .expect("read failed")
            .len()
    };

    // The default addresses the destination alone.
    dbos.send(&original_id, &"skipped")
        .await
        .expect("send failed");
    assert_eq!(delivered(&original_id).await, 1);
    assert_eq!(
        delivered(&fork_id).await,
        0,
        "the default is Forks::Skip: a fork is not a destination"
    );

    // Asking for the fan-out reaches both.
    dbos.send_with(
        &original_id,
        &"included",
        SendOptions {
            forks: Forks::Include,
            ..Default::default()
        },
    )
    .await
    .expect("send failed");
    assert_eq!(delivered(&original_id).await, 2);
    assert_eq!(
        delivered(&fork_id).await,
        1,
        "Forks::Include reaches the workflows forked from the destination"
    );

    dbos.shutdown().await;
}

/// A workflow's batch is one checkpoint, so a replay delivers none of it again.
///
/// Also pins the name the *surface* chooses: a batch records `DBOS.sendBulk` whatever its length,
/// a batch of one included. The name distinguishes the two API surfaces, so reaching for a
/// different one on a replay is caught as a determinism error — while a batch that merely changed
/// size is not, being no change of operation.
#[tokio::test]
async fn a_workflows_batch_is_one_checkpoint() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("bulk-app", &db));
    let sleeps = dbos
        .register_workflow("sleeps", |()| async {
            dbos::sleep(Duration::from_secs(5)).await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    let broadcasts = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("broadcasts", move |ids: Vec<String>| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::send_bulk(&[Message::new(&ids[0], &"one"), Message::new(&ids[1], &"two")])
                    .await?;
                // A batch of one: still `DBOS.sendBulk`, because that is the surface reached for.
                dbos::send_bulk(&[Message::new(&ids[0], &"alone")]).await?;
                reached.notify_one();
                release.notified().await;
                Ok::<_, dbos::Error>(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let first = sleeps.start(()).await.expect("start failed");
    let second = sleeps.start(()).await.expect("start failed");
    let (first_id, second_id) = (
        first.workflow_id().to_owned(),
        second.workflow_id().to_owned(),
    );
    let sender = broadcasts
        .start(vec![first_id.clone(), second_id.clone()])
        .await
        .expect("start failed");
    let sender_id = sender.workflow_id().to_owned();

    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the sender never reached its gate");

    // Abandon after both sends and let recovery replay them.
    dbos.shutdown().await;
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered sender never reached its gate again");
    release_gate.notify_one();

    let reader = reader(&db).await;
    await_success(&reader, &sender_id).await;

    assert_eq!(
        steps(&reader, &sender_id).await,
        [
            (0, "DBOS.sendBulk".to_owned()),
            (1, "DBOS.sendBulk".to_owned())
        ],
        "one step per batch, named by the surface reached for — a batch of one is still a batch",
    );
    assert_eq!(
        reader
            .get_all_notifications(&first_id)
            .await
            .expect("read failed")
            .len(),
        2,
        "the replay delivered nothing a second time",
    );
    assert_eq!(
        reader
            .get_all_notifications(&second_id)
            .await
            .expect("read failed")
            .len(),
        1
    );

    dbos.shutdown().await;
}
