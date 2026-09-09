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
use dbos::{Client, ClientConfig, Config, DBOS, Error, ForkFrom, StartOptions, WorkflowHandle};

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

    // Every handle has its answer in hand, without waiting.
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

/// **A wait takes its step id where it is built, not where it is first polled.**
///
/// The counterpart in `events.rs` makes the argument for the shape: `join!` builds every branch
/// before polling any and then first-polls them in source order, so a test that builds and drives
/// in the same order passes against poll-time ids too. These three are built `a, b, c` and handed
/// to `join!` as `c, b, a`.
///
/// **Only two of the three have an id to keep.** `join_workflows` is a plain wait that takes none,
/// which is why it can sit anywhere in a `join!` without moving what the others got — the
/// strongest form of the property this test is about. The first-wait is over a workflow that has
/// already settled, so nothing here blocks and the only thing separating a build-time id from a
/// poll-time one is the order they were written in.
#[tokio::test]
async fn waits_driven_out_of_build_order_keep_the_ids_they_were_built_with() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-join-app", &db));
    let target = dbos
        .register_workflow("target", |()| async { Ok::<u32, Error>(1) })
        .unwrap();
    let waiter = dbos
        .register_workflow("waiter", |settled: String| async move {
            // Built a, b, c — the order their ids come from the counter in, and the order a
            // replay will build them in again.
            let ids = [settled.as_str()];
            let a = dbos::join_workflows(&ids);
            let b = dbos::select_workflow(&ids);
            let c = dbos::step("after", || async { Ok::<u32, Error>(1) });
            assert_eq!(
                (b.step_id(), c.step_id()),
                (Some(0), Some(1)),
                "the ids were taken at the call, in source order, before anything was polled — \
                 and the all-wait between them took none"
            );
            // ...and driven c, b, a.
            let (c, b, a) = tokio::join!(c, b, a);
            a?;
            assert_eq!(b?, settled, "the only member of the set did not win it");
            c?;
            Ok::<u32, Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let settled = target.start(()).await.expect("start failed");
    let settled_id = settled.workflow_id().to_owned();
    assert_eq!(settled.result().await.expect("the target failed"), 1);

    let workflow_id = "waits-out-of-order";
    waiter
        .run_with(
            settled_id,
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
        [(0, "DBOS.selectWorkflow"), (1, "after")],
        "the ids follow the order the calls were built in, not the order they were polled in",
    );

    dbos.shutdown().await;
}

/// An all-wait inside a workflow takes no step id, whatever it was passed.
///
/// **Which slot a call takes must depend on where it was written, never on what it was passed** —
/// a set computed from state can be empty on one execution and not on the next, so a wait that
/// took an id in one and none in the other would shift every later step of that workflow onto a
/// slot it did not record. `join_workflows` satisfies that the other way round: it takes no id on
/// any execution, so an empty set and a full one leave the following step on the same slot for the
/// same reason. The empty case is the one worth a test, because it is where a wait that *did*
/// place itself would have been tempted to return early.
#[tokio::test]
async fn an_empty_wait_inside_a_workflow_takes_no_step_id() {
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
        [(0, "after")],
        "the all-wait recorded nothing, so `after` holds the workflow's first step id"
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

/// Called from inside a workflow, `select_workflow` is a checkpointed step and `join_workflows` is
/// not.
///
/// The recorded winner is what makes the choice survive a replay: a second execution reads the id
/// back rather than racing again, so a workflow that branches on which member won cannot take a
/// different branch the second time. The all-wait after it decides nothing, so it writes no row at
/// all and leaves the step ids to the calls that do.
#[tokio::test]
async fn a_first_wait_inside_a_workflow_is_a_checkpointed_step_and_an_all_wait_is_not() {
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
                    // A loop rather than a `join!` because that is the plainest way to write it
                    // — each start takes its step id where it is written, so driving them
                    // together would number them the same way.
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
        [(0, "quick"), (1, "quick"), (2, "DBOS.selectWorkflow")],
        "the two launches and the one wait that records, in order"
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

    dbos.shutdown().await;
}

/// A recorded refusal replays as that refusal, against whatever set the second execution has.
///
/// **Step arguments are not checkpointed anywhere in DBOS**, so a replay answers the question the
/// first run asked rather than the one it is holding now. That is what makes recording the empty
/// first-wait's refusal the right thing rather than merely a tidy one: the second execution here
/// carries a set with a settled workflow in it, which would have a winner to report — and reports
/// the refusal instead, because that is what this position of the code decided.
///
/// The second execution is a fork from step 1, which carries step 0's row across and re-runs from
/// there, so the wait at position zero meets its own recorded outcome.
#[tokio::test]
async fn a_recorded_refusal_replays_rather_than_being_decided_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("wait-replay-app", &db));

    let quick = dbos
        .register_workflow("quick", |()| async { Ok::<u32, Error>(1) })
        .unwrap();
    // Empty on the first execution and not on the second, which is the whole point: a set built
    // from state is exactly what changes between a run and its replay.
    let settled: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let executions = Arc::new(AtomicU32::new(0));
    let parent = dbos
        .register_workflow("parent", {
            let (settled, executions) = (Arc::clone(&settled), Arc::clone(&executions));
            move |()| {
                let (settled, executions) = (Arc::clone(&settled), Arc::clone(&executions));
                async move {
                    let ids: Vec<String> = if executions.fetch_add(1, Ordering::SeqCst) == 0 {
                        Vec::new()
                    } else {
                        settled.lock().unwrap().clone()
                    };
                    let borrowed: Vec<&str> = ids.iter().map(String::as_str).collect();
                    let outcome: dbos::Result<String> = dbos::select_workflow(&borrowed).await;
                    let described = match outcome {
                        Err(error) => format!("{error}"),
                        Ok(winner) => format!("won by {winner}"),
                    };
                    dbos::step("after", || async { Ok::<u32, Error>(1) }).await?;
                    Ok::<String, Error>(described)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // A workflow that has already settled, so a second execution that *did* race would have a
    // winner to report rather than hanging — which is what makes the assertion below sharp.
    let done = quick.start(()).await.expect("start failed");
    let done_id = done.workflow_id().to_owned();
    done.result().await.expect("quick failed");
    settled.lock().unwrap().push(done_id.clone());

    let first = parent.start(()).await.expect("start failed");
    let parent_id = first.workflow_id().to_owned();
    let described = tokio::time::timeout(DEADLINE, first.result())
        .await
        .expect("the parent never finished")
        .expect("the parent failed");
    assert!(described.contains("no workflow ids"), "{described}");

    let forked: WorkflowHandle<String, Error> = dbos
        .fork(&parent_id, ForkFrom::Step(1))
        .await
        .expect("fork failed");
    let replayed = tokio::time::timeout(DEADLINE, forked.result())
        .await
        .expect("the fork never finished")
        .expect("the fork failed");
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the fork should have run the body a second time"
    );
    assert_eq!(
        replayed, described,
        "the replay reported {replayed} against a set holding {done_id}, instead of reading back \
         the refusal it recorded"
    );

    dbos.shutdown().await;
}

/// A client's waits answer an empty set the same way, having no workflow to record against.
///
/// The refusal and the satisfaction are properties of the call rather than of the checkpoint, so
/// they read the same from a surface that never takes a step id.
#[tokio::test]
async fn a_clients_empty_waits_answer_without_a_slot() {
    let db = test_database().await;
    let client = Client::connect(ClientConfig {
        app_name: Some("wait-client-empty-app".to_owned()),
        outcome_poll_interval: Some(Duration::from_millis(50)),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");

    client
        .join_workflows(&[])
        .await
        .expect("an empty all-wait is satisfied for a client too");

    let error = client
        .select_workflow(&[])
        .await
        .expect_err("an empty first-wait is refused for a client too");
    assert!(matches!(error, Error::InvalidArgument { .. }), "{error}");
    assert!(error.to_string().contains("no workflow ids"), "{error}");

    client.close().await;
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
