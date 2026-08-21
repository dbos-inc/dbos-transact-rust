//! The DBOS Rust starter: the Workflows tab of the starter app.
//!
//! Three steps, five seconds each, a progress event after each — and a crash button. Launch a
//! workflow, crash the process, restart it, and watch execution resume at the step after the last
//! one that finished. Durable execution is the whole demo: the crash-and-resume needs no
//! application code at all, because `launch()` recovers whatever the previous run abandoned.

use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use dbos::{Config, DBOS, StartOptions, WorkflowRef};
use tokio::net::TcpListener;

/// The key the workflow publishes its progress under, and the UI polls.
const STEPS_EVENT: &str = "steps_event";
const STEP_DURATION: Duration = Duration::from_secs(5);

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

/// `DBOS` is an `Arc` newtype, so it goes into the router's state by `clone()`.
#[derive(Clone)]
struct App {
    dbos: DBOS,
    example: WorkflowRef<(), String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut config = Config::from_env("dbos-rust-starter");
    if config.database_url.is_empty() {
        config.database_url = DEFAULT_DATABASE_URL.to_owned();
    }
    config
        .application_version
        .get_or_insert_with(|| DEFAULT_APP_VERSION.to_owned());
    let dbos = DBOS::new(config);
    let example = dbos.register_workflow("ExampleWorkflow", example_workflow)?;

    // Migrates, connects — and recovers whatever the previous run abandoned, which is the
    // entire crash-and-resume demonstration.
    dbos.launch().await?;

    let app = App {
        dbos: dbos.clone(),
        example,
    };
    let router = Router::new()
        .route("/", get(index))
        .route("/workflow/{task_id}", get(start_workflow))
        .route("/last_step/{task_id}", get(last_step))
        .route("/crash", post(crash))
        .with_state(app);

    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("Server starting on http://localhost:8080");
    axum::serve(listener, router).await?;

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
/// rather than failing — the id is an idempotency key, and a double-click is not an error.
async fn start_workflow(
    State(app): State<App>,
    Path(task_id): Path<String>,
) -> Result<(), AppError> {
    app.example
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(&task_id),
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
