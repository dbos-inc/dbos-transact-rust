//! Waiting on several workflows at once, against real databases.
//!
//! Two questions, and they are answered by two different shapes of row read. `select_workflow`
//! reports *which* member settled and has to pin that choice into the caller's replay;
//! `join_workflows` reports only that every member has, and has nothing to pin. The tests are
//! grouped that way.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Client, ClientConfig, Config, DBOS, Error, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

/// Counts workflows that have reached their gate.
///
/// A [`Semaphore`](tokio::sync::Semaphore) rather than a `Notify`, because arrivals have to
/// *accumulate*: `notify_one` with nobody waiting stores a single permit however many times it is
/// called, so three workflows arriving before the test looks would be counted once and the test
/// would hang. Permits added are permits taken.
fn arrivals() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(0))
}

/// Waits for `n` workflows to have reached their gate.
async fn reached_all(gate: &tokio::sync::Semaphore, n: u32) {
    tokio::time::timeout(DEADLINE, gate.acquire_many(n))
        .await
        .expect("a workflow never reached its gate")
        .expect("the gate was closed")
        .forget();
}

const DEADLINE: Duration = Duration::from_secs(60);

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        // Short, because every test here is waiting on a poll loop.
        outcome_poll_interval: Some(Duration::from_millis(50)),
        ..Config::new(app_name, db.url())
    }
}

/// A second handle on the database, for reading what a call actually wrote.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// A gate a workflow waits at, so a test decides the order things finish in.
struct Gates {
    reached: Arc<tokio::sync::Semaphore>,
    release: Vec<Arc<tokio::sync::Notify>>,
}

/// `select_workflow` reports the position of whichever member settles first, and the test picks
/// which that is.
///
/// Three workflows blocked at their own gates; the middle one is released. The answer has to name
/// that one — which is the whole contract: not the first id passed, not the first started, but the
/// first to *finish*.
#[tokio::test]
async fn select_workflow_reports_the_first_to_settle() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-first-app", &db));

    let gates = Gates {
        reached: arrivals(),
        release: (0..3)
            .map(|_| Arc::new(tokio::sync::Notify::new()))
            .collect(),
    };
    let release = gates.release.clone();
    let reached = Arc::clone(&gates.reached);
    let blocked = dbos
        .register_workflow("blocked", move |which: u32| {
            let (release, reached) = (release.clone(), Arc::clone(&reached));
            async move {
                reached.add_permits(1);
                release[which as usize].notified().await;
                Ok::<_, Error>(which)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let mut handles = Vec::new();
    for which in 0..3u32 {
        handles.push(
            blocked
                .start_with(
                    which,
                    StartOptions {
                        workflow_id: Some(&format!("blocked-{which}")),
                        ..Default::default()
                    },
                )
                .await
                .expect("start failed"),
        );
    }
    // All three are at their gates, so nothing has settled and none of them can win by accident.
    reached_all(&gates.reached, 3).await;

    let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
    // Released *after* the wait is in flight, so the wait genuinely waits rather than reading an
    // already-settled row.
    let waiting = tokio::spawn({
        let dbos = dbos.clone();
        let ids: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
        async move {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            dbos.select_workflow(&ids).await
        }
    });
    gates.release[1].notify_one();

    let first = tokio::time::timeout(DEADLINE, waiting)
        .await
        .expect("the wait never resolved")
        .expect("the waiting task panicked")
        .expect("select_workflow failed");
    assert_eq!(
        first, "blocked-1",
        "the released workflow is the one that finished"
    );

    // The others are still going, which is what makes the answer meaningful.
    for other in [0, 2] {
        assert_eq!(
            handles[other].status().await.expect("status failed"),
            WorkflowStatus::Pending,
            "workflow {other} settled when only 1 was released"
        );
    }

    for gate in &gates.release {
        gate.notify_one();
    }
    dbos.shutdown().await;
}

/// A cancelled workflow ends the wait, exactly as a successful one does.
///
/// The definition of "settled" is the status leaving `PENDING`/`ENQUEUED`/`DELAYED`, which Python
/// and TypeScript share — so this is a cross-SDK contract rather than a local choice. Reporting
/// only successes would leave a fan-out waiting out a member nobody is running any more.
#[tokio::test]
async fn a_cancelled_workflow_counts_as_settled() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-cancelled-app", &db));

    let release = Arc::new(tokio::sync::Notify::new());
    let reached = arrivals();
    let blocked = dbos
        .register_workflow("blocked", {
            let (release, reached) = (Arc::clone(&release), Arc::clone(&reached));
            move |()| {
                let (release, reached) = (Arc::clone(&release), Arc::clone(&reached));
                async move {
                    reached.add_permits(1);
                    release.notified().await;
                    Ok::<_, Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let doomed = blocked
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("doomed"),
                ..Default::default()
            },
        )
        .await
        .expect("start failed");
    let survivor = blocked
        .start_with(
            (),
            StartOptions {
                workflow_id: Some("survivor"),
                ..Default::default()
            },
        )
        .await
        .expect("start failed");
    reached_all(&reached, 2).await;

    dbos.cancel(doomed.workflow_id())
        .await
        .expect("cancel failed");

    let first = tokio::time::timeout(
        DEADLINE,
        dbos.select_workflow(&[doomed.workflow_id(), survivor.workflow_id()]),
    )
    .await
    .expect("the wait never resolved")
    .expect("select_workflow failed");
    assert_eq!(
        first, "doomed",
        "the cancelled workflow is the one that settled"
    );

    release.notify_one();
    dbos.shutdown().await;
}

/// `join_workflows` returns only once every member has settled — and then every handle resolves
/// without waiting.
#[tokio::test]
async fn join_workflows_returns_when_the_last_one_settles() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-all-app", &db));

    let release = Arc::new(tokio::sync::Notify::new());
    let reached = arrivals();
    let blocked = dbos
        .register_workflow("blocked", {
            let (release, reached) = (Arc::clone(&release), Arc::clone(&reached));
            move |which: u32| {
                let (release, reached) = (Arc::clone(&release), Arc::clone(&reached));
                async move {
                    reached.add_permits(1);
                    release.notified().await;
                    Ok::<_, Error>(which * 2)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let mut handles = Vec::new();
    for which in 0..3u32 {
        handles.push(blocked.start(which).await.expect("start failed"));
    }
    reached_all(&reached, 3).await;

    let ids: Vec<String> = handles.iter().map(|h| h.workflow_id().to_owned()).collect();
    let waiting = tokio::spawn({
        let dbos = dbos.clone();
        let ids = ids.clone();
        async move {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            dbos.join_workflows(&ids).await
        }
    });

    // Releasing two of the three must not end the wait. There is no event to synchronise on here
    // — the assertion is that nothing happens — so this gives the wait a generous window to
    // wrongly resolve in.
    release.notify_one();
    release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(750), std::future::pending::<()>())
            .await
            .is_err(),
    );
    assert!(
        !waiting.is_finished(),
        "join_workflows returned before the last member settled"
    );

    release.notify_one();
    tokio::time::timeout(DEADLINE, waiting)
        .await
        .expect("the wait never resolved")
        .expect("the waiting task panicked")
        .expect("join_workflows failed");

    // What the wait bought: every handle now has its answer in hand.
    for (which, handle) in handles.into_iter().enumerate() {
        assert_eq!(
            handle.result().await.expect("the workflow failed"),
            which as u32 * 2
        );
    }
    dbos.shutdown().await;
}

/// The two empty cases differ, and the difference is not an accident.
///
/// An all-wait over nothing is satisfied — there is nothing outstanding. A first-wait over nothing
/// has no answer it could ever give, so it is refused rather than parked forever. Python raises at
/// the same point.
#[tokio::test]
async fn an_empty_wait_is_satisfied_for_all_and_refused_for_first() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-empty-app", &db));
    dbos.launch().await.expect("launch failed");

    dbos.join_workflows(&[])
        .await
        .expect("an empty join_workflows should be satisfied");

    let err = dbos
        .select_workflow(&[])
        .await
        .expect_err("an empty select_workflow should be refused");
    assert!(matches!(err, Error::InvalidArgument { .. }), "{err}");
    assert!(err.to_string().contains("no workflow ids"), "{err}");

    dbos.shutdown().await;
}

/// An empty wait inside a workflow still occupies its step id.
///
/// **Which slot a call takes must depend on where it was written, never on what it was passed.** A
/// set computed from state can be empty on one execution and not on the next, so a wait that took
/// an id in one and none in the other would shift every later step of that workflow onto a slot it
/// did not record. So the empty case is placed and recorded like any other: `join_workflows` waits
/// for nothing and records that it happened, and the step after it keeps the id it would have had
/// with a full set.
#[tokio::test]
async fn an_empty_wait_inside_a_workflow_still_takes_its_step_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-empty-id-app", &db));
    let wf = dbos
        .register_workflow("wf", |()| async {
            let empty: Vec<&str> = Vec::new();
            dbos::join_workflows(&empty).await?;
            dbos::step("after", || async { Ok::<u32, Error>(1) }).await?;
            Ok::<u32, Error>(1)
        })
        .expect("registration failed");
    dbos.launch().await.expect("launch failed");

    let handle = wf.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    assert_eq!(handle.result().await.expect("the workflow failed"), 1);

    let steps = dbos
        .list_workflow_steps(&id)
        .await
        .expect("could not read the steps");
    let seen: Vec<(i32, &str)> = steps
        .iter()
        .map(|step| (step.step_id, step.step_name.as_str()))
        .collect();
    assert_eq!(
        seen,
        [(0, "DBOS.joinWorkflows"), (1, "after")],
        "the empty wait recorded its own row and left `after` on the slot it would have had anyway"
    );

    dbos.shutdown().await;
}

/// An empty first-wait inside a workflow records its refusal as the step's outcome.
///
/// It took a step id, and a step that took one owes its slot an outcome — so the refusal is
/// written to the row rather than happening instead of a step. A replay then reads it back
/// instead of deciding it again, which is what makes it safe for the set to have changed since:
/// step arguments are not checkpointed anywhere in DBOS.
#[tokio::test]
async fn an_empty_first_wait_records_its_refusal() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-empty-refusal-app", &db));
    let wf = dbos
        .register_workflow("wf", |()| async {
            let none: [&str; 0] = [];
            let refused: dbos::Result<String> = dbos::select_workflow(&none).await;
            let message = match refused {
                Err(error) => format!("{error}"),
                Ok(id) => panic!("an empty first-wait answered with {id}"),
            };
            dbos::step("after", || async { Ok::<u32, Error>(1) }).await?;
            Ok::<String, Error>(message)
        })
        .expect("registration failed");
    dbos.launch().await.expect("launch failed");

    let handle = wf.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    let message = handle.result().await.expect("the workflow failed");
    assert!(
        message.contains("no workflow ids") && message.contains("invalid argument"),
        "{message}"
    );

    let steps = dbos
        .list_workflow_steps(&id)
        .await
        .expect("could not read the steps");
    let seen: Vec<(i32, &str)> = steps
        .iter()
        .map(|step| (step.step_id, step.step_name.as_str()))
        .collect();
    assert_eq!(
        seen,
        [(0, "DBOS.selectWorkflow"), (1, "after")],
        "the refusal occupies its own slot, leaving `after` where it would have been anyway"
    );
    let refusal = &steps[0];
    assert!(
        refusal.error.is_some() && refusal.output.is_none(),
        "the refusal is the step's recorded outcome: {refusal:?}"
    );

    dbos.shutdown().await;
}

/// A repeated id is accepted by both waits, and answers with the id it names.
///
/// Python and TypeScript refuse it in `waitFirst`, but only because they return the winning
/// *handle* and key a map by id to find it. An id has no such collision: a set holding `a` twice
/// answers `a`, which names one workflow however many entries pointed at it. The set arises
/// legitimately — a caller-supplied id that `start` joined to a run already going hands back a
/// second handle on one workflow.
#[tokio::test]
async fn a_repeated_id_is_accepted_by_both_waits() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-dup-app", &db));
    let quick = dbos
        .register_workflow("quick", |()| async { Ok::<_, Error>(1u32) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = quick.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    assert_eq!(handle.result().await.expect("the workflow failed"), 1);

    let first = tokio::time::timeout(DEADLINE, dbos.select_workflow(&[&id, &id]))
        .await
        .expect("the wait never resolved")
        .expect("a duplicate should be accepted");
    assert_eq!(first, id, "the answer names the one workflow in the set");

    tokio::time::timeout(DEADLINE, dbos.join_workflows(&[&id, &id]))
        .await
        .expect("the wait never resolved")
        .expect("join_workflows failed");

    dbos.shutdown().await;
}

/// Called from inside a workflow, both waits are checkpointed under their cross-SDK step names —
/// and `select_workflow` records the winner while `join_workflows` records no payload.
///
/// The recorded winner is what makes the choice survive a replay: a second execution reads the id
/// back rather than racing again, so a workflow that branches on which member won cannot take a
/// different branch the second time.
#[tokio::test]
async fn a_wait_inside_a_workflow_is_a_checkpointed_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-step-app", &db));
    let reader = reader(&db).await;

    let quick = dbos
        .register_workflow("quick", |which: u32| async move { Ok::<_, Error>(which) })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let quick = quick.clone();
            move |()| {
                let quick = quick.clone();
                async move {
                    // Launched one at a time, as a parent must: every call takes a step id from
                    // the parent's counter.
                    let mut children = Vec::new();
                    for which in 0..2u32 {
                        children.push(quick.start(which).await?);
                    }
                    let ids: Vec<&str> = children.iter().map(|c| c.workflow_id()).collect();
                    // The free functions, not the instance methods: a workflow body takes its
                    // executor from the ambient context rather than capturing a `DBOS`.
                    let first = dbos::select_workflow(&ids).await?;
                    dbos::join_workflows(&ids).await?;
                    Ok::<_, Error>(first)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = parent.start(()).await.expect("start failed");
    let workflow_id = handle.workflow_id().to_owned();
    let first = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the parent never finished")
        .expect("the parent failed");

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
        [
            (0, "quick"),
            (1, "quick"),
            (2, "DBOS.selectWorkflow"),
            (3, "DBOS.joinWorkflows"),
        ],
        "the two launches and the two waits, in order"
    );

    // The winner is the payload, and it is exactly what the parent returned — no projection on
    // the way out, which is the point of answering with the id rather than a position.
    let recorded = steps[2]
        .output
        .as_deref()
        .expect("selectWorkflow recorded no winner");
    let winner: String = serde_json::from_str(recorded).expect("the winner is not a string");
    assert_eq!(
        winner, first,
        "the recorded winner is what the parent returned"
    );
    let children = reader
        .get_workflow_children(&workflow_id)
        .await
        .expect("read failed");
    assert!(
        children.contains(&winner),
        "the winner {winner} is not one of the children {children:?}"
    );

    // An all-wait decides nothing, so there is nothing for a replay to branch on.
    assert_eq!(steps[3].output, None, "joinWorkflows recorded a payload");
    assert_eq!(steps[3].error, None);

    dbos.shutdown().await;
}

/// The same two waits from a client, which has no workflow to checkpoint against.
///
/// This is the surface an operator's tool reaches for: the client names workflows it did not start
/// and has no code for.
#[tokio::test]
async fn a_client_waits_on_workflows_it_did_not_start() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-client-app", &db));

    let ran = Arc::new(AtomicU32::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let reached = arrivals();
    let blocked = dbos
        .register_workflow("blocked", {
            let (ran, release, reached) =
                (Arc::clone(&ran), Arc::clone(&release), Arc::clone(&reached));
            move |()| {
                let (ran, release, reached) =
                    (Arc::clone(&ran), Arc::clone(&release), Arc::clone(&reached));
                async move {
                    reached.add_permits(1);
                    release.notified().await;
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let client = Client::connect(ClientConfig {
        app_name: Some("wait-client-app".to_owned()),
        outcome_poll_interval: Some(Duration::from_millis(50)),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");

    let mut ids = Vec::new();
    for _ in 0..2 {
        let handle = blocked.start(()).await.expect("start failed");
        ids.push(handle.workflow_id().to_owned());
    }
    reached_all(&reached, 2).await;

    let borrowed: Vec<&str> = ids.iter().map(String::as_str).collect();
    let waiting = tokio::spawn({
        let client = client.clone();
        let ids = ids.clone();
        async move {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            client.join_workflows(&ids).await
        }
    });

    release.notify_one();
    release.notify_one();
    tokio::time::timeout(DEADLINE, waiting)
        .await
        .expect("the client's wait never resolved")
        .expect("the waiting task panicked")
        .expect("join_workflows failed");
    assert_eq!(ran.load(Ordering::SeqCst), 2);

    // Both are settled, so a first-wait answers at once with an id from the set it was given.
    let first = tokio::time::timeout(DEADLINE, client.select_workflow(&borrowed))
        .await
        .expect("the wait never resolved")
        .expect("select_workflow failed");
    assert!(
        borrowed.contains(&first.as_str()),
        "the answer {first} is not one of the ids waited on"
    );

    dbos.shutdown().await;
}
