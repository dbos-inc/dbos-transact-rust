//! Patching a workflow's code while executions of the old code are in flight, against real
//! databases.
//!
//! Each "deployment" is an instance on one database, shut down before the next is launched with
//! changed code registered under the same name and the same application version — which is what
//! lets the new code fork and recover the old code's executions. Forks stand in for recoveries
//! where the point is only which path an execution with a given history takes: a fork copies the
//! history below its start step and runs the rest under the code now registered.
//!
//! Step ids are zero-based, so a fork at the last step of a two-step workflow is a fork at 1.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{Outcome, WorkflowStatus};
use dbos::sysdb::{Error as SysdbError, SystemDatabase};
use dbos::{Config, DBOS, EngineOnly, Error, RunOptions, StartOptions, WorkflowRef};
use dbos_test_support::{TestDatabase, owner_token, test_database};
use tokio::sync::Notify;

/// Nothing legitimate takes this long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

/// The version every deployment here shares: patched code recovers only what ran under its own
/// version.
const APP_VERSION: &str = "1.0.0";

const V2: &str = "DBOS.patch-v2";
const V3: &str = "DBOS.patch-v3";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

async fn step_one() -> dbos::Result<i32> {
    dbos::step("step_one", || async { Ok(1) }).await
}

async fn step_two() -> dbos::Result<i32> {
    dbos::step("step_two", || async { Ok(2) }).await
}

async fn step_three() -> dbos::Result<i32> {
    dbos::step("step_three", || async { Ok(3) }).await
}

/// The code as first deployed.
async fn v1(_: ()) -> dbos::Result<i32> {
    let a = step_one().await?;
    let b = step_two().await?;
    Ok(a + b)
}

/// The first step changed, behind a patch.
async fn v2(_: ()) -> dbos::Result<i32> {
    let a = if dbos::patch("v2").await? {
        step_three().await?
    } else {
        step_one().await?
    };
    let b = step_two().await?;
    Ok(a + b)
}

/// The first step changed again, behind a second patch tested before the first.
async fn v3(_: ()) -> dbos::Result<i32> {
    let a = if dbos::patch("v3").await? {
        step_two().await?
    } else if dbos::patch("v2").await? {
        step_three().await?
    } else {
        step_one().await?
    };
    let b = step_two().await?;
    Ok(a + b)
}

/// Only `v3`'s code is left, and its patch is deprecated.
async fn v4(_: ()) -> dbos::Result<i32> {
    assert!(dbos::deprecate_patch("v3").await?, "always true");
    let a = step_two().await?;
    let b = step_two().await?;
    Ok(a + b)
}

/// The deprecated patch is gone.
async fn v5(_: ()) -> dbos::Result<i32> {
    let a = step_two().await?;
    let b = step_two().await?;
    Ok(a + b)
}

/// Launches a deployment whose one workflow, `workflow`, is `body`.
async fn deploy<F, Fut>(
    app_name: &str,
    db: &TestDatabase,
    body: F,
) -> (DBOS, WorkflowRef<(), i32, EngineOnly>)
where
    F: Fn(()) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = dbos::Result<i32>> + Send + 'static,
{
    let dbos = DBOS::new(config(app_name, db));
    let workflow = dbos.register_workflow("workflow", body).unwrap();
    dbos.launch().await.expect("launch failed");
    (dbos, workflow)
}

/// Runs the deployed workflow to completion under `id`.
async fn run(workflow: &WorkflowRef<(), i32, EngineOnly>, id: &str) -> i32 {
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{id} failed: {error}"))
}

/// Forks `id` at `start_step` under the deployed code and waits for the fork's result.
async fn fork(dbos: &DBOS, id: &str, start_step: i32) -> (String, i32) {
    let handle = dbos
        .fork::<i32, EngineOnly>(id, start_step)
        .await
        .expect("fork failed");
    let fork_id = handle.workflow_id().to_owned();
    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .unwrap_or_else(|_| panic!("the fork of {id} never finished"))
        .unwrap_or_else(|error| panic!("the fork of {id} failed: {error}"));
    (fork_id, result)
}

/// The names a workflow's steps are recorded under, in order.
async fn names(dbos: &DBOS, id: &str) -> Vec<String> {
    dbos.list_workflow_steps(id)
        .await
        .expect("listing failed")
        .into_iter()
        .map(|step| step.step_name)
        .collect()
}

/// **The whole lifecycle**: a patch added, a second one nested over it, the newer one deprecated,
/// and then removed — with executions of every earlier generation forked under each.
///
/// Every deployment names the same version explicitly: a shared version is all a patch needs.
#[tokio::test]
async fn a_patch_is_added_nested_deprecated_and_removed() {
    let db = test_database().await;
    let app = "patch-lifecycle-app";

    // The code before any patch.
    let (dbos, workflow) = deploy(app, &db, v1).await;
    assert_eq!(run(&workflow, "v1-run").await, 3);
    assert_eq!(names(&dbos, "v1-run").await, ["step_one", "step_two"]);
    dbos.shutdown().await;

    // A patch. A new execution takes the new path and records the marker first.
    let (dbos, workflow) = deploy(app, &db, v2).await;
    assert_eq!(run(&workflow, "v2-run").await, 5);
    assert_eq!(names(&dbos, "v2-run").await, [V2, "step_three", "step_two"]);
    // The marker is a name and nothing else.
    let steps = dbos
        .list_workflow_steps("v2-run")
        .await
        .expect("listing failed");
    let marker = &steps[0];
    assert_eq!(marker.step_id, 0);
    assert_eq!(marker.output, None, "a marker records no output");
    assert_eq!(marker.error, None, "a marker records no error");
    assert_eq!(marker.serialization, None, "and so names no encoding");
    assert_eq!(marker.child_workflow_id, None);
    assert!(marker.completed_at.is_some(), "stamped like any checkpoint");
    assert_eq!(steps[1].step_id, 1, "the patched step follows the marker");

    // An execution holding the marker replays past it, onto the new path.
    let (forked, result) = fork(&dbos, "v2-run", 2).await;
    assert_eq!(result, 5);
    assert_eq!(names(&dbos, &forked).await, [V2, "step_three", "step_two"]);

    // An execution of the old code takes the old path and records no marker.
    let (forked, result) = fork(&dbos, "v1-run", 1).await;
    assert_eq!(result, 3);
    assert_eq!(names(&dbos, &forked).await, ["step_one", "step_two"]);
    dbos.shutdown().await;

    // A second patch, nested over the first.
    let (dbos, workflow) = deploy(app, &db, v3).await;
    assert_eq!(run(&workflow, "v3-run").await, 4);
    assert_eq!(names(&dbos, "v3-run").await, [V3, "step_two", "step_two"]);

    let (forked, result) = fork(&dbos, "v3-run", 2).await;
    assert_eq!(result, 4, "the v3 marker replays onto v3's path");
    assert_eq!(names(&dbos, &forked).await, [V3, "step_two", "step_two"]);

    // `patch("v3")` finds v2's marker, declines it and leaves the position for `patch("v2")`.
    let (forked, result) = fork(&dbos, "v2-run", 2).await;
    assert_eq!(result, 5, "the v2 marker replays onto v2's path");
    assert_eq!(names(&dbos, &forked).await, [V2, "step_three", "step_two"]);

    let (forked, result) = fork(&dbos, "v1-run", 1).await;
    assert_eq!(
        result, 3,
        "an execution from before both patches takes neither"
    );
    assert_eq!(names(&dbos, &forked).await, ["step_one", "step_two"]);
    dbos.shutdown().await;

    // The v3 patch deprecated: honoured where recorded, never written.
    let (dbos, workflow) = deploy(app, &db, v4).await;
    assert_eq!(run(&workflow, "v4-run").await, 4);
    assert_eq!(
        names(&dbos, "v4-run").await,
        ["step_two", "step_two"],
        "a new execution records no marker"
    );

    let (forked, result) = fork(&dbos, "v4-run", 1).await;
    assert_eq!(result, 4);
    assert_eq!(names(&dbos, &forked).await, ["step_two", "step_two"]);

    let (forked, result) = fork(&dbos, "v3-run", 2).await;
    assert_eq!(result, 4, "an execution holding the marker moves past it");
    assert_eq!(names(&dbos, &forked).await, [V3, "step_two", "step_two"]);
    dbos.shutdown().await;

    // The deprecated patch removed: what ran under the deprecation replays unchanged.
    let (dbos, _workflow) = deploy(app, &db, v5).await;
    let (forked, result) = fork(&dbos, "v4-run", 1).await;
    assert_eq!(result, 4);
    assert_eq!(names(&dbos, &forked).await, ["step_two", "step_two"]);
    dbos.shutdown().await;
}

/// **The case patching exists for**: an execution of the old code, abandoned mid-flight and
/// recovered by a deployment that has since added a patch, takes the old path.
#[tokio::test]
async fn an_execution_recovered_under_patched_code_takes_the_old_path() {
    let db = test_database().await;
    let app = "patch-recovery-app";
    let id = "old-run";

    // The old code, stopped after its first step.
    let reached = Arc::new(Notify::new());
    let dbos = DBOS::new(config(app, &db));
    let workflow = dbos
        .register_workflow("workflow", {
            let reached = Arc::clone(&reached);
            move |()| {
                let reached = Arc::clone(&reached);
                async move {
                    let a = step_one().await?;
                    reached.notify_one();
                    std::future::pending::<()>().await;
                    let b = step_two().await?;
                    Ok::<_, Error>(a + b)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let _abandoned = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached.notified())
        .await
        .expect("the old code never reached its first step");
    dbos.shutdown().await;

    // The patched code recovers it on launch.
    let (dbos, _workflow) = deploy(app, &db, v2).await;
    let reader = reader(&db).await;
    let row = tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(id)
                .await
                .expect("read failed")
                .expect("the row exists");
            if row.status == WorkflowStatus::Success {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the recovered execution never finished");
    assert_eq!(row.output.as_deref(), Some("3"), "the old path: 1 + 2");
    assert_eq!(
        names(&dbos, id).await,
        ["step_one", "step_two"],
        "no marker: the position already held the old code's step"
    );
    dbos.shutdown().await;
}

/// Launches `body` under `id`, gated before its first statement, plants `planted` as the step at
/// position 0, and returns what the workflow then answers.
///
/// How an execution whose history the code no longer knows is reached in-process: a fork of one
/// would fail by a control signal, which records no outcome and leaves the fork `PENDING` with no
/// one to report to.
async fn with_planted_history<F, Fut>(
    app_name: &str,
    id: &str,
    planted: &str,
    body: F,
) -> dbos::Result<i32>
where
    F: Fn(()) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = dbos::Result<i32>> + Send + 'static,
{
    let db = test_database().await;
    let gate = Arc::new(Notify::new());
    let body = Arc::new(body);
    let dbos = DBOS::new(config(app_name, &db));
    let workflow = dbos
        .register_workflow("workflow", {
            let gate = Arc::clone(&gate);
            move |()| {
                let (gate, body) = (Arc::clone(&gate), Arc::clone(&body));
                async move {
                    gate.notified().await;
                    body(()).await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
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

    let reader = reader(&db).await;
    reader
        .record_step(
            id,
            0,
            planted,
            Outcome::Output(None),
            None,
            None,
            &owner_token(&reader, id).await,
        )
        .await
        .expect("planting the step failed");
    gate.notify_one();

    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the workflow never finished");
    dbos.shutdown().await;
    result
}

/// **Code that no longer knows a marker meets it as any changed code meets an old history**: as an
/// unexpected step, with nothing recorded over it.
#[tokio::test]
async fn code_that_no_longer_knows_a_marker_finds_an_unexpected_step() {
    // The v3 patch deprecated, meeting an execution that took the v2 patch.
    let result = with_planted_history("patch-deprecated-app", "holds-v2", V2, v4).await;
    match result {
        Err(Error::SystemDatabase(SysdbError::UnexpectedStep {
            step_id,
            expected,
            recorded,
            ..
        })) => {
            assert_eq!(step_id, 0);
            assert_eq!(expected, "step_two");
            assert_eq!(recorded, V2);
        }
        other => panic!("expected an unexpected step, got {other:?}"),
    }

    // The deprecated patch removed, meeting an execution that took it.
    let result = with_planted_history("patch-removed-app", "holds-v3", V3, v5).await;
    match result {
        Err(Error::SystemDatabase(SysdbError::UnexpectedStep {
            step_id, recorded, ..
        })) => {
            assert_eq!(step_id, 0);
            assert_eq!(recorded, V3);
        }
        other => panic!("expected an unexpected step, got {other:?}"),
    }
}

/// A patch is a position in the workflow's own history, and a step body has none to record at.
#[tokio::test]
async fn a_patch_inside_a_step_is_refused() {
    async fn patches_in_a_step(_: ()) -> dbos::Result<bool> {
        dbos::step("probe", || async {
            let patched = dbos::patch::<EngineOnly>("v2").await;
            let deprecated = dbos::deprecate_patch::<EngineOnly>("v2").await;
            Ok(
                matches!(&patched, Err(Error::InsideStep { operation }) if operation == "patch")
                    && matches!(
                        &deprecated,
                        Err(Error::InsideStep { operation }) if operation == "deprecate_patch"
                    ),
            )
        })
        .await
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("patch-in-step-app", &db));
    let workflow = dbos
        .register_workflow("patches-in-a-step", patches_in_a_step)
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "patches-in-a-step-run";
    let refused = workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");
    assert!(refused, "both calls are refused inside the step body");
    assert_eq!(names(&dbos, id).await, ["probe"], "and neither recorded");

    dbos.shutdown().await;
}

/// Runs `body` as a fresh workflow under `id` and returns what it answers, without unwrapping.
async fn run_raw<F, Fut, T>(
    app_name: &str,
    db: &TestDatabase,
    id: &str,
    body: F,
) -> (DBOS, dbos::Result<T>)
where
    F: Fn(()) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = dbos::Result<T>> + Send + 'static,
    T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    let dbos = DBOS::new(config(app_name, db));
    let workflow = dbos.register_workflow("workflow", body).unwrap();
    dbos.launch().await.expect("launch failed");
    let result = workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await;
    (dbos, result)
}

/// The refusal a patch built at position 0 gets when a step built after it took that position.
fn assert_lost_position_zero(result: dbos::Result<bool>, id: &str, patch: &str) {
    match result {
        Err(Error::PatchNondeterminism {
            workflow_id,
            patch: refused,
            step_id,
        }) => {
            assert_eq!(workflow_id, id);
            assert_eq!(refused, patch);
            assert_eq!(step_id, 0, "the position the step took");
        }
        other => panic!("expected the patch to be refused, got {other:?}"),
    }
}

/// **A patch whose position another call took fails loudly**, on every execution: the position is
/// fixed where the patch is built, and a step built after it and awaited first has it.
#[tokio::test]
async fn a_patch_whose_position_another_call_took_is_refused() {
    async fn out_of_order(_: ()) -> dbos::Result<bool> {
        let patched = dbos::patch::<EngineOnly>("v2");
        let stepped = dbos::step("step_two", || async { Ok::<_, Error>(2) });
        stepped.await?;
        patched.await
    }

    let db = test_database().await;
    let id = "out-of-order-run";
    let (dbos, result) = run_raw("patch-race-app", &db, id, out_of_order).await;
    assert_lost_position_zero(result, id, "v2");
    assert_eq!(
        names(&dbos, id).await,
        ["step_two"],
        "the marker was never written over the step's position"
    );
    dbos.shutdown().await;
}

/// **The same holds for a deprecated patch**, which checks that nothing took its position even
/// though it records nothing there.
#[tokio::test]
async fn a_deprecated_patch_whose_position_another_call_took_is_refused() {
    async fn out_of_order(_: ()) -> dbos::Result<bool> {
        let deprecated = dbos::deprecate_patch::<EngineOnly>("v2");
        let stepped = dbos::step("step_two", || async { Ok::<_, Error>(2) });
        stepped.await?;
        deprecated.await
    }

    let db = test_database().await;
    let id = "deprecated-out-of-order-run";
    let (dbos, result) = run_raw("deprecate-race-app", &db, id, out_of_order).await;
    assert_lost_position_zero(result, id, "v2");
    assert_eq!(names(&dbos, id).await, ["step_two"]);
    dbos.shutdown().await;
}

/// **Two patches in a row each take a position of their own**, and a replay past them reads both
/// markers back without running anything again.
#[tokio::test]
async fn two_patches_in_a_row_record_a_marker_each() {
    async fn two(_: ()) -> dbos::Result<i32> {
        let a = dbos::patch::<EngineOnly>("first").await?;
        let b = dbos::patch::<EngineOnly>("second").await?;
        assert!(a && b, "a new execution takes both new paths");
        step_two().await
    }

    let db = test_database().await;
    let (dbos, workflow) = deploy("two-patches-app", &db, two).await;
    assert_eq!(run(&workflow, "two-patches-run").await, 2);
    assert_eq!(
        names(&dbos, "two-patches-run").await,
        ["DBOS.patch-first", "DBOS.patch-second", "step_two"]
    );

    let (forked, result) = fork(&dbos, "two-patches-run", 2).await;
    assert_eq!(result, 2);
    assert_eq!(
        names(&dbos, &forked).await,
        ["DBOS.patch-first", "DBOS.patch-second", "step_two"],
        "the replay honoured both markers and recorded the step where it was"
    );
    dbos.shutdown().await;
}

/// **A patch as the workflow's last call** records its marker at the end of the history, and a
/// replay up to it reads it back.
#[tokio::test]
async fn a_patch_as_the_last_call_replays() {
    async fn last(_: ()) -> dbos::Result<i32> {
        let a = step_one().await?;
        let patched = dbos::patch::<EngineOnly>("tail").await?;
        Ok(if patched { a + 10 } else { a })
    }

    let db = test_database().await;
    let (dbos, workflow) = deploy("patch-last-app", &db, last).await;
    assert_eq!(run(&workflow, "patch-last-run").await, 11);
    assert_eq!(
        names(&dbos, "patch-last-run").await,
        ["step_one", "DBOS.patch-tail"]
    );

    // A fork past both positions replays the step and the marker.
    let (forked, result) = fork(&dbos, "patch-last-run", 2).await;
    assert_eq!(result, 11);
    assert_eq!(names(&dbos, &forked).await, ["step_one", "DBOS.patch-tail"]);
    dbos.shutdown().await;
}

/// **A deprecated patch meeting an execution of the code from before it** honours nothing, answers
/// `true`, and leaves that execution's step at the position, where the new code then finds it
/// unexpected — which is why a patch is deprecated only once no such execution can run again.
#[tokio::test]
async fn a_deprecated_patch_meeting_the_old_code_finds_an_unexpected_step() {
    async fn deprecated(_: ()) -> dbos::Result<i32> {
        assert!(dbos::deprecate_patch::<EngineOnly>("v2").await?);
        let a = step_three().await?;
        let b = step_two().await?;
        Ok(a + b)
    }

    let result =
        with_planted_history("deprecate-old-app", "holds-v1", "step_one", deprecated).await;
    match result {
        Err(Error::SystemDatabase(SysdbError::UnexpectedStep {
            step_id,
            expected,
            recorded,
            ..
        })) => {
            assert_eq!(step_id, 0);
            assert_eq!(expected, "step_three");
            assert_eq!(recorded, "step_one");
        }
        other => panic!("expected an unexpected step, got {other:?}"),
    }
}

/// **A patch is held to the workflow it was built in**, on every poll: carried into a step body or
/// onto a spawned task, it is refused before it claims a position or writes a marker.
#[tokio::test]
async fn a_patch_polled_away_from_its_workflow_is_refused() {
    async fn carried_away(_: ()) -> dbos::Result<bool> {
        // Into a step body, where a claimed position would be lost when the step replays.
        // A step body may run more than once, so the patch is handed over through a slot.
        let slot = Arc::new(std::sync::Mutex::new(Some(dbos::patch::<EngineOnly>(
            "into-a-step",
        ))));
        let in_step = dbos::step("carrier", move || {
            let patched = slot.lock().unwrap().take();
            async move {
                let patched = patched.expect("the step body ran once");
                Ok::<_, Error>(matches!(
                    patched.await,
                    Err(Error::StepBuiltElsewhere { step, .. }) if step == "DBOS.patch-into-a-step"
                ))
            }
        })
        .await?;

        // Onto a task of its own, outside any workflow.
        let patched = dbos::patch::<EngineOnly>("onto-a-task");
        let on_task = matches!(
            tokio::spawn(patched).await.expect("the task panicked"),
            Err(Error::StepBuiltElsewhere { step, .. }) if step == "DBOS.patch-onto-a-task"
        );
        Ok(in_step && on_task)
    }

    let db = test_database().await;
    let id = "carried-away-run";
    let (dbos, result) = run_raw("patch-carried-app", &db, id, carried_away).await;
    assert!(result.expect("the workflow failed"), "both were refused");
    assert_eq!(
        names(&dbos, id).await,
        ["carrier"],
        "neither refused patch wrote a marker"
    );
    dbos.shutdown().await;
}
