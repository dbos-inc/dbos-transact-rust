//! The DBOS Rust starter: the Workflows and Queues tabs of the starter app.
//!
//! **Workflows** is three steps, five seconds each, a progress event after each — and a crash
//! button. Launch a workflow, crash the process, restart it, and watch execution resume at the
//! step after the last one that finished. Durable execution is the whole demo: the crash-and-resume
//! needs no application code at all, because `launch()` recovers whatever the previous run
//! abandoned.
//!
//! **Queues** is a fan-out under a concurrency limit. Enqueue five workflows that sleep five
//! seconds each against a queue allowing three at a time, and watch three run while two wait. The
//! part worth pressing is the Apply button: `worker_concurrency` changes while the app is running,
//! because a queue's configuration is a row every worker re-reads on every pass rather than a
//! constant captured at startup.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use dbos::{
    Change, Config, DBOS, Enqueue, QueueChange, QueueOptions, StartOptions, WorkflowHandle,
    WorkflowRef,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// The key the workflow publishes its progress under, and the UI polls.
const STEPS_EVENT: &str = "steps_event";
const STEP_DURATION: Duration = Duration::from_secs(5);

/// The queue the fan-out demo enqueues onto.
const QUEUE_NAME: &str = "demo-queue";

/// How many of the queue's workflows one process may run at once, to begin with.
///
/// Three of five, so two visibly wait. The Apply button changes it without a restart.
const DEFAULT_WORKER_CONCURRENCY: i32 = 3;

/// How many the Enqueue button puts on the queue at once.
const ENQUEUE_BATCH: usize = 5;

/// Used when `DBOS_DATABASE_URL` is not set. The database is created if it does not exist.
///
/// Deliberately no username or password: the driver fills in whatever the URL leaves out
/// from the standard libpq variables, such as PGUSER and PGPASSWORD.
const DEFAULT_DATABASE_URL: &str = "postgres://localhost:5432/dbos_rust_starter";

/// Pinning the version matters here more than in most apps: it defaults to a hash of the
/// executable, and recovery only resumes workflows stamped with its own version — so a rebuild
/// between the crash and the restart would look exactly like broken recovery.
const DEFAULT_APP_VERSION: &str = "0.1.0";

/// A durable workflow, resilient to any failure: if the program is crashed, interrupted, or
/// restarted while it runs, it automatically resumes from the last completed step.
///
/// Registration is what makes it durable: `register_workflow` returns a typed `WorkflowRef`, and
/// the durable invocations are its methods — `start` for a handle without waiting (what the
/// `/workflow` endpoint uses), `run` to await the result in place.
async fn example_workflow(_: ()) -> dbos::Result<String> {
    dbos::step("step_one", step_one).await?;
    // Publish progress after each step, for the frontend to display.
    dbos::set_event(STEPS_EVENT, &1u32).await?;
    dbos::step("step_two", step_two).await?;
    dbos::set_event(STEPS_EVENT, &2u32).await?;
    dbos::step("step_three", step_three).await?;
    dbos::set_event(STEPS_EVENT, &3u32).await?;
    Ok("Workflow completed".to_owned())
}

async fn step_one() -> dbos::Result<()> {
    tokio::time::sleep(STEP_DURATION).await;
    println!("Completed step 1!");
    Ok(())
}

async fn step_two() -> dbos::Result<()> {
    tokio::time::sleep(STEP_DURATION).await;
    println!("Completed step 2!");
    Ok(())
}

async fn step_three() -> dbos::Result<()> {
    tokio::time::sleep(STEP_DURATION).await;
    println!("Completed step 3!");
    Ok(())
}

/// A workflow with nothing to it but a wait, which is all the queue demo needs: what it
/// demonstrates is *when* it runs, not what it does.
async fn enqueued_workflow(_: ()) -> dbos::Result<String> {
    println!("Enqueued workflow starting.");
    dbos::sleep(STEP_DURATION).await?;
    println!("Enqueued workflow ending.");
    Ok("Enqueued workflow completed".to_owned())
}

/// `DBOS` is an `Arc` newtype, so it goes into the router's state by `clone()`.
#[derive(Clone)]
struct App {
    dbos: DBOS,
    example: WorkflowRef<(), String>,
    enqueued: WorkflowRef<(), String>,
    /// Handles for what this process has put on the queue, so the tab can report their statuses.
    ///
    /// Kept rather than queried back: listing workflows by name and age belongs to the management
    /// surface, which is a separate piece of work. A restart forgets them, which for a demo is the
    /// right amount of memory — and the queue itself does not forget, because the rows are still
    /// there and still get dequeued.
    ///
    /// A `tokio` mutex because the guard is held across `status().await`.
    queued: Arc<Mutex<Vec<WorkflowHandle<String>>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut config = Config::from_env("dbos-rust-starter");
    if config.database_url.is_empty() {
        config.database_url = DEFAULT_DATABASE_URL.to_owned();
    }
    config
        .app_version
        .get_or_insert_with(|| DEFAULT_APP_VERSION.to_owned());
    let dbos = DBOS::new(config);
    let example = dbos.register_workflow("ExampleWorkflow", example_workflow)?;
    let enqueued = dbos.register_workflow("EnqueuedWorkflow", enqueued_workflow)?;

    // Migrates, connects — and recovers whatever the previous run abandoned, which is the
    // entire crash-and-resume demonstration.
    dbos.launch().await?;

    // **After launch, unlike a workflow.** A queue is a row, so registering one is a write and
    // needs a launched instance. `NeverUpdate` so a restart does not undo whatever the Apply
    // button last set — the row is the source of truth, and the app is only seeding it.
    dbos.register_queue(
        QUEUE_NAME,
        QueueOptions {
            worker_concurrency: Some(DEFAULT_WORKER_CONCURRENCY),
            on_conflict: dbos::QueueConflict::NeverUpdate,
            ..QueueOptions::default()
        },
    )
    .await?;

    let app = App {
        dbos: dbos.clone(),
        example,
        enqueued,
        queued: Arc::default(),
    };
    let router = Router::new()
        .route("/", get(index))
        .route("/workflow/{task_id}", post(start_workflow))
        .route("/last_step/{task_id}", get(last_step))
        .route("/crash", post(crash))
        .route("/queue/status", get(queue_status))
        .route("/queue/enqueue", post(queue_enqueue))
        .route("/queue/concurrency", post(queue_concurrency))
        .with_state(app);

    // Loopback, not 0.0.0.0: this app ships a button that exits the process, which is a fine
    // thing to hand yourself and a poor thing to hand your network.
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server starting on http://localhost:8080");
    // Serving until Ctrl-C rather than forever is what makes the line below reachable — and
    // `shutdown` is worth reaching: it leaves every running workflow PENDING for the next launch
    // to recover, which is the same path the crash button takes the long way round.
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("Shutting down");
        })
        .await?;

    dbos.shutdown().await;
    Ok(())
}

/// Serves the HTML frontend, embedded in the binary.
async fn index() -> Html<&'static str> {
    Html(include_str!("../html/app.html"))
}

/// Starts the workflow under the caller's id and returns at once; the handle is dropped.
///
/// The id is the caller's, so posting the same task twice joins the workflow already running
/// rather than failing — the id is an idempotency key, and a double-click is not an error. A
/// `POST` because it starts something: a `GET` that does is one a prefetch or a back button can
/// fire on the user's behalf.
async fn start_workflow(
    State(app): State<App>,
    Path(task_id): Path<String>,
) -> Result<(), AppError> {
    app.example
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(&task_id),
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

/// How many steps the workflow has completed — from outside any workflow, with a zero timeout.
///
/// Zero until the workflow publishes its first event, which is what shows "executing step 1".
async fn last_step(
    State(app): State<App>,
    Path(task_id): Path<String>,
) -> Result<String, AppError> {
    let step: Option<u32> = app
        .dbos
        .get_event(&task_id, STEPS_EVENT, Duration::ZERO)
        .await?;
    Ok(step.unwrap_or(0).to_string())
}

/// What the Queues tab polls: the limit currently stored, and what this app's enqueued workflows
/// are doing.
#[derive(Serialize)]
struct QueueStatus {
    worker_concurrency: i32,
    /// Counts keyed by status name — `ENQUEUED`, `PENDING`, `SUCCESS`, and so on.
    workflow_counts: std::collections::BTreeMap<String, usize>,
}

/// Reads the limit back from the **row**, not from what this process registered.
///
/// That is the point of the tab: a peer could have changed it, and what this executor's dequeues
/// honour is whatever the row says right now.
async fn queue_status(State(app): State<App>) -> Result<Json<QueueStatus>, AppError> {
    let worker_concurrency = app
        .dbos
        .queue(QUEUE_NAME)
        .await?
        .and_then(|queue| queue.worker_concurrency())
        .unwrap_or(DEFAULT_WORKER_CONCURRENCY);

    let mut workflow_counts = std::collections::BTreeMap::new();
    for handle in app.queued.lock().await.iter() {
        let status = handle.status().await?;
        *workflow_counts
            .entry(format!("{status:?}").to_uppercase())
            .or_default() += 1;
    }

    Ok(Json(QueueStatus {
        worker_concurrency,
        workflow_counts,
    }))
}

/// Puts a batch of workflows on the queue and returns at once.
///
/// None of them runs here just because this process asked: each is recorded `ENQUEUED`, and
/// whichever executor next polls the queue claims it — under the limit the row currently carries.
async fn queue_enqueue(State(app): State<App>) -> Result<(), AppError> {
    for _ in 0..ENQUEUE_BATCH {
        let handle = app
            .enqueued
            .start_with(
                (),
                StartOptions {
                    queue: Some(Enqueue::new(QUEUE_NAME)),
                    ..Default::default()
                },
            )
            .await?;
        app.queued.lock().await.push(handle);
    }
    Ok(())
}

#[derive(Deserialize)]
struct ConcurrencyRequest {
    concurrency: Option<i32>,
}

/// **The Apply button: a limit changed while the app runs.**
///
/// No restart and no redeploy. The write lands in the queue's row, the supervisor publishes it on
/// its next sweep, and every worker in the fleet — not just this process — picks it up on its next
/// pass.
async fn queue_concurrency(
    State(app): State<App>,
    Json(request): Json<ConcurrencyRequest>,
) -> Result<(), AppError> {
    let concurrency = request
        .concurrency
        .filter(|value| *value >= 1)
        .unwrap_or(DEFAULT_WORKER_CONCURRENCY);
    app.dbos
        .update_queue(
            QUEUE_NAME,
            QueueChange {
                worker_concurrency: Change::Set(Some(concurrency)),
                ..QueueChange::default()
            },
        )
        .await?;
    Ok(())
}

/// Crashes the application. For demonstration purposes only :)
async fn crash() {
    println!("Simulating application crash");
    std::process::exit(1);
}

/// A `dbos::Error` carried out of a handler as a 500.
struct AppError(dbos::Error);

impl From<dbos::Error> for AppError {
    fn from(error: dbos::Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
