//! Workflow events, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, Error};

use dbos_test_support::{TestDatabase, test_database};

const DEADLINE: Duration = Duration::from_secs(60);

/// The version each instance in this file launches with, derived from its application name.
///
/// DBOS computes none, so a launch without one fails, and it has to be stable across a relaunch of
/// the same application or the relaunch would recover nothing. It cannot simply be a shared
/// constant, though: `application_versions` still carries a global `UNIQUE (version_name)`, so two
/// differently-named applications sharing one database cannot both register `1.0.0` — see the
/// UPSTREAM notes on `resolve_owning_application`, and
/// `lifecycle::a_launch_that_fails_after_connecting_closes_the_database`, which contests a version
/// on purpose. Keying on the name gives both properties at once.
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

/// The Workflows tab in miniature: progress published after each step, polled from outside, and
/// surviving an abandonment without being published twice.
#[tokio::test]
async fn progress_events_survive_recovery_without_republishing() {
    let one = Arc::new(AtomicU32::new(0));
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("events-app", &db));
    let publishes = {
        let one = Arc::clone(&one);
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("publishes", move |()| {
            let one = Arc::clone(&one);
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::step("one", || {
                    let one = Arc::clone(&one);
                    async move {
                        one.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await?;
                dbos::set_event("progress", &1u32).await?;
                reached.notify_one();
                release.notified().await;
                dbos::set_event("progress", &2u32).await?;
                Ok::<_, dbos::Error>(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let running = tokio::spawn(async move { publishes.run(()).await });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    // The demo's `GET /last_step` handler: an outside caller polling with a zero timeout.
    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed");
    let workflow_id = rows[0].workflow_id.clone();
    let progress: Option<u32> = dbos
        .get_event(&workflow_id, "progress", Duration::ZERO)
        .await
        .expect("get_event failed");
    assert_eq!(progress, Some(1), "the value published so far");
    let missing: Option<u32> = dbos
        .get_event(&workflow_id, "no_such_key", Duration::ZERO)
        .await
        .expect("get_event failed");
    assert_eq!(missing, None, "absence is a value, not an error");

    // Abandon at the gate, relaunch, and let recovery replay the step and the first publish.
    dbos.shutdown().await;
    let err = running.await.expect("the task panicked").unwrap_err();
    assert!(matches!(err, Error::Interrupted { .. }), "{err}");

    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered workflow never reached its gate again");
    let progress: Option<u32> = dbos
        .get_event(&workflow_id, "progress", Duration::ZERO)
        .await
        .expect("get_event failed");
    assert_eq!(
        progress,
        Some(1),
        "replay did not republish or lose the value"
    );
    release_gate.notify_one();

    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(&workflow_id)
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

    let progress: Option<u32> = dbos
        .get_event(&workflow_id, "progress", Duration::ZERO)
        .await
        .expect("get_event failed");
    assert_eq!(progress, Some(2));
    assert_eq!(one.load(Ordering::SeqCst), 1, "the step ran exactly once");

    // The publishes are checkpoints, under the cross-SDK step name, and the replay added none.
    let steps = reader
        .list_workflow_steps(&workflow_id, true, None, None, None)
        .await
        .expect("read failed");
    let seen: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        seen,
        [(0, "one"), (1, "DBOS.setEvent"), (2, "DBOS.setEvent")]
    );

    dbos.shutdown().await;
}

/// A workflow that reads an event is checkpointed — two steps — and one that reads from inside a
/// step is not, the step's own checkpoint standing for everything its body did.
#[tokio::test]
async fn a_reading_workflow_is_checkpointed_and_a_reading_step_is_not() {
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("the reader failed")]
    struct ReaderError;

    let db = test_database().await;
    let dbos = DBOS::new(config("read-app", &db));
    let publisher = dbos
        .register_workflow("publisher", |()| async {
            dbos::set_event("answer", &42u32).await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    // Reads through the ambient context, with its own error type — and captures nothing, which is
    // the point: a closure holding a `DBOS` would be stored inside the very instance it holds.
    let in_workflow = dbos
        .register_workflow("in_workflow", |publisher_id: String| async move {
            let answer: Option<u32> =
                dbos::get_event(&publisher_id, "answer", Duration::ZERO).await?;
            Ok::<_, Error<ReaderError>>(answer)
        })
        .unwrap();
    // Reads from inside a step: a leaf, so no ids are allocated for the read.
    let in_step = dbos
        .register_workflow("in_step", |publisher_id: String| async move {
            let answer = dbos::step("read", || {
                let publisher_id = publisher_id.clone();
                async move {
                    let answer: Option<u32> =
                        dbos::get_event(&publisher_id, "answer", Duration::ZERO).await?;
                    Ok(answer)
                }
            })
            .await?;
            Ok::<_, dbos::Error>(answer)
        })
        .unwrap();
    // The instance method called from inside a workflow is still supported, and still
    // checkpointed the same way; it needs `Error::lift` because it reports in the engine's own
    // channel, and it needs a captured handle, which is why the free function is the one to reach
    // for.
    let via_instance = {
        let dbos = dbos.clone();
        dbos.clone()
            .register_workflow("via_instance", move |publisher_id: String| {
                let dbos = dbos.clone();
                async move {
                    let answer: Option<u32> = dbos
                        .get_event(&publisher_id, "answer", Duration::ZERO)
                        .await
                        .map_err(Error::lift)?;
                    Ok::<_, Error<ReaderError>>(answer)
                }
            })
            .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    publisher.run(()).await.expect("the publisher failed");
    let reader = reader(&db).await;
    let publisher_id = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed")
        .iter()
        .find(|r| r.name.as_deref() == Some("publisher"))
        .expect("the publisher ran")
        .workflow_id
        .clone();

    assert_eq!(
        in_workflow
            .run(publisher_id.clone())
            .await
            .expect("the reader failed"),
        Some(42)
    );
    assert_eq!(
        in_step
            .run(publisher_id.clone())
            .await
            .expect("the reader failed"),
        Some(42)
    );
    assert_eq!(
        via_instance
            .run(publisher_id.clone())
            .await
            .expect("the reader failed"),
        Some(42)
    );

    let steps_of = |name: &'static str| {
        let reader = &reader;
        async move {
            let rows = reader
                .list_workflows(&Default::default(), None)
                .await
                .expect("read failed");
            let id = &rows
                .iter()
                .find(|r| r.name.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("no row for {name}"))
                .workflow_id;
            reader
                .list_workflow_steps(id, true, None, None, None)
                .await
                .expect("read failed")
                .iter()
                .map(|s| (s.step_id, s.step_name.clone()))
                .collect::<Vec<_>>()
        }
    };

    assert_eq!(
        steps_of("in_workflow").await,
        [
            (0, "DBOS.getEvent".to_owned()),
            (1, "DBOS.sleep".to_owned())
        ],
        "the read and its deadline are checkpointed"
    );
    assert_eq!(
        steps_of("in_step").await,
        [(0, "read".to_owned())],
        "the step is the only checkpoint; the read inside it left no ids"
    );
    assert_eq!(
        steps_of("via_instance").await,
        [
            (0, "DBOS.getEvent".to_owned()),
            (1, "DBOS.sleep".to_owned())
        ],
        "the instance method checkpoints a workflow's read the same way"
    );

    dbos.shutdown().await;
}

/// The instance method refuses a handle that is not the instance running the workflow.
///
/// It takes its executor from the handle and its step ids from the ambient context. Normally those
/// are the same instance; when they are not, the checkpoint would be written through one database
/// against ids issued by another, so it would land where the workflow that allocated them cannot
/// see it. Refused rather than silently split.
#[tokio::test]
async fn reading_through_another_instance_from_inside_a_workflow_is_refused() {
    let db = test_database().await;
    let other = DBOS::new(config("other-app", &db));
    let owner = DBOS::new(config("owner-app", &db));

    let reads_through_other = {
        let other = other.clone();
        owner
            .register_workflow("reads_through_other", move |()| {
                let other = other.clone();
                async move {
                    let answer: Option<u32> =
                        other.get_event("wf-1", "answer", Duration::ZERO).await?;
                    Ok::<_, dbos::Error>(answer)
                }
            })
            .unwrap()
    };
    // From inside a *step* the read is plain — nothing is checkpointed, so the two halves are
    // never combined and there is nothing to refuse.
    let in_step = {
        let other = other.clone();
        owner
            .register_workflow("reads_in_step", move |()| {
                let other = other.clone();
                async move {
                    let answer = dbos::step("read", || {
                        let other = other.clone();
                        async move {
                            let answer: Option<u32> =
                                other.get_event("wf-1", "answer", Duration::ZERO).await?;
                            Ok(answer)
                        }
                    })
                    .await?;
                    Ok::<_, dbos::Error>(answer)
                }
            })
            .unwrap()
    };
    other.launch().await.expect("launch failed");
    owner.launch().await.expect("launch failed");

    let err = reads_through_other.run(()).await.unwrap_err();
    assert!(matches!(err, Error::WrongInstance { .. }), "{err}");

    assert_eq!(
        in_step.run(()).await.expect("a plain read is allowed"),
        None,
        "no such workflow, so no such event — but not a refusal"
    );

    owner.shutdown().await;
    other.shutdown().await;
}

/// `set_event` needs a workflow around it, and refuses a step — there is no plain version of a
/// durable write to degrade to.
#[tokio::test]
async fn set_event_refuses_to_run_outside_a_workflow_or_inside_a_step() {
    let outside: dbos::Result<()> = dbos::set_event("key", &1u32).await;
    assert!(
        matches!(outside.unwrap_err(), Error::NotInWorkflow { .. }),
        "no workflow, no step sequence to checkpoint against"
    );

    // The free reader is the same: it takes its executor from the context, and outside a workflow
    // there is none. That is where `DBOS::get_event` is the call.
    let outside: dbos::Result<Option<u32>> = dbos::get_event("wf-1", "key", Duration::ZERO).await;
    assert!(
        matches!(outside.unwrap_err(), Error::NotInWorkflow { .. }),
        "no workflow, no executor to read through"
    );

    let db = test_database().await;
    let dbos = DBOS::new(config("guard-app", &db));
    let in_step = dbos
        .register_workflow("in_step", |()| async {
            dbos::step("publishes", || async {
                dbos::set_event("key", &1u32).await
            })
            .await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let err = in_step.run(()).await.unwrap_err();
    assert!(matches!(err, Error::InsideStep { .. }), "{err}");

    dbos.shutdown().await;
}

/// **A read, a wait, a management call, a publish and a sleep take their ids when they are built**, like a step
/// does, so a set of them driven together with `join!` is numbered in source order however the
/// futures interleave. The read is two ids (the read and its deadline), and each of the others is
/// one; a step built after all of them lands after all of them.
#[tokio::test]
async fn non_step_calls_driven_together_take_ids_in_build_order() {
    let db = test_database().await;
    let dbos = DBOS::new(config("built-together-app", &db));
    let publisher = dbos
        .register_workflow("publisher", |()| async {
            dbos::set_event("answer", &42u32).await?;
            Ok::<_, dbos::Error>(())
        })
        .unwrap();
    let reader_wf = {
        let dbos = dbos.clone();
        dbos.clone()
            .register_workflow("reader", move |publisher_id: String| {
                let dbos = dbos.clone();
                async move {
                    let ids = [publisher_id.as_str()];
                    let read = dbos::get_event::<u32, dbos::EngineOnly>(
                        &publisher_id,
                        "answer",
                        Duration::from_secs(5),
                    );
                    let joined = dbos::join_workflows::<dbos::EngineOnly>(&ids);
                    let listed = dbos.list_workflow_steps(&publisher_id);
                    let published = dbos::set_event::<_, dbos::EngineOnly>("progress", &1u32);
                    let slept = dbos::sleep::<dbos::EngineOnly>(Duration::from_millis(10));
                    let stepped = dbos::step("after", || async { Ok::<_, dbos::Error>(7u32) });
                    assert_eq!(
                        (
                            read.step_id(),
                            joined.step_id(),
                            listed.step_id(),
                            published.step_id(),
                            slept.step_id(),
                            stepped.step_id()
                        ),
                        (Some(0), Some(2), Some(3), Some(4), Some(5), Some(6)),
                        "ids are taken at the call, in source order, the read taking two"
                    );
                    // Driven out of build order, so a poll-time allocation would number them
                    // differently.
                    let (seven, slept, published, rows, joined, answer) =
                        tokio::join!(stepped, slept, published, listed, joined, read);
                    let (seven, (), (), rows, (), answer) = (
                        seven?,
                        slept?,
                        published?,
                        rows.map_err(dbos::Error::lift)?,
                        joined?,
                        answer?,
                    );
                    assert_eq!(seven, 7);
                    // Polled before or after the publisher's own write lands, so either count
                    // is right; what the test pins is the id the listing recorded under.
                    assert!(
                        rows.len() <= 1,
                        "the publisher records one setEvent at most"
                    );
                    Ok::<_, dbos::Error>(answer)
                }
            })
            .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let publisher_id = publisher
        .start(())
        .await
        .expect("the publisher failed to start")
        .workflow_id()
        .to_owned();
    assert_eq!(
        reader_wf
            .run(publisher_id.clone())
            .await
            .expect("the reader failed"),
        Some(42)
    );

    let reader = reader(&db).await;
    let reader_id = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed")
        .iter()
        .find(|r| r.name.as_deref() == Some("reader"))
        .expect("the reader ran")
        .workflow_id
        .clone();
    let steps = reader
        .list_workflow_steps(&reader_id, true, None, None, None)
        .await
        .expect("read failed")
        .iter()
        .map(|s| (s.step_id, s.step_name.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        steps,
        [
            (0, "DBOS.getEvent".to_owned()),
            (1, "DBOS.sleep".to_owned()),
            (2, "DBOS.joinWorkflows".to_owned()),
            (3, "DBOS.listWorkflowSteps".to_owned()),
            (4, "DBOS.setEvent".to_owned()),
            (5, "DBOS.sleep".to_owned()),
            (6, "after".to_owned()),
        ],
        "every call recorded under the id it was built with"
    );

    dbos.shutdown().await;
}
