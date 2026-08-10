//! The PostgreSQL implementation of [`SystemDatabase`], which also serves CockroachDB.
//!
//! The pool lives here and is never exposed: the whole point of the trait is that callers
//! cannot depend on which driver is underneath.
//!
//! # Logging
//!
//! Events go through `tracing` with structured fields rather than interpolated messages, and the
//! crate installs no subscriber — a host chooses what to do with them.
//!
//! What gets logged is taken from the four references, which agree on the shape:
//!
//! - **`debug!` on a replay-or-run decision**, for any operation gated by a recorded step. Java
//!   logs `"Replaying setEvent, workflow: {}, step: {}, key: {}"` against `"Running setEvent…"`,
//!   Python the same pair, on `set_event`, `get_event`, `recv`, `sleep`, `send_bulk`, and
//!   `write_stream`. It is the single most useful line when a workflow behaves oddly on recovery,
//!   because it says which side of the replay the caller is on.
//! - **`debug!` with a count** on anything that sweeps or acts in bulk — Go, Python and
//!   TypeScript all log dequeue counts this way. A caller that asked for five and moved three
//!   wants to know without a second query.
//! - **`debug!` on a non-error outcome that changes what the caller should do**: a workflow
//!   recorded but not claimed, an outcome another run already wrote.
//! - **`warn!` before returning a conflict.** Java and TypeScript both warn that a step "was
//!   already recorded" before throwing, because the error alone does not say which execution
//!   lost.
//!
//! Two things deliberately absent. **No `error!`** — errors are returned, and Go and TypeScript
//! logging them before returning double-reports every failure the caller then handles. And **no
//! per-method entry logging**, which Java has (`debug("initWorkflowStatus workflowId {}")`) and
//! nothing else does; a span belongs to the caller, not to every statement.

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};

use super::migrations::{self, quote_identifier};
use super::retry::{RetryPolicy, with_retry};
use std::time::Duration;

use super::types::{
    EventRecord, Fork, ForkOptions, NewWorkflow, NotificationRecord, Outcome, StepRecord,
    StepTiming, StreamRecord, Submission, Timestamp, VersionInfo, WorkflowDelay, WorkflowFilter,
    WorkflowRecord, WorkflowStatus, duration_from_ms, validate_attributes,
};
use super::{
    BackendError, BackendErrorKind, DEFAULT_SCHEMA, Error, INTERNAL_QUEUE, OutcomeWrite,
    SystemDatabase, WorkflowInitResult,
};

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        let sqlstate = e
            .as_database_error()
            .and_then(|d| d.code())
            .map(|c| c.into_owned());
        Error::Backend(BackendError {
            kind: classify(&e, sqlstate.as_deref()),
            message: e.to_string(),
            sqlstate,
        })
    }
}

/// Treats an empty string as an absent value.
///
/// Only for the fields where another SDK legitimately writes `""`; everywhere else an empty
/// string is a caller error and [`NewWorkflow::validate`] rejects it.
fn empty_to_none(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.is_empty())
}

/// Whether a driver failure is worth asking again about.
///
/// The classes are the union of the other implementations', which agree on the SQLSTATE prefixes
/// and differ only in how they group them:
///
/// | Prefix | Meaning | Java | Python | Go |
/// |---|---|---|---|---|
/// | `08` | connection exception | connection | connection | retryable |
/// | `57` | operator intervention — shutdown, cannot connect now | connection | connection | retryable |
/// | `53` | insufficient resources — out of memory, too many connections | transient | connection | — |
/// | `40` | serialization failure, deadlock detected | transient | — | transaction-retryable |
///
/// `53` is grouped with connections here, following Python: `53300 too_many_connections` is a
/// failure to obtain a connection, and the difference only decides whether
/// [`RetryPolicy::retry_connection_errors`] can opt out of it.
///
/// Prefixes rather than exact codes, deliberately. The first version of the migration runner's
/// classifier listed codes and was wrong twice; matching the class the standard defines is what
/// stopped that.
fn classify(error: &sqlx::Error, sqlstate: Option<&str>) -> BackendErrorKind {
    if let Some(code) = sqlstate {
        return match &code[..2.min(code.len())] {
            "40" => BackendErrorKind::Transient,
            "08" | "53" | "57" => BackendErrorKind::Connection,
            _ => BackendErrorKind::Permanent,
        };
    }
    // No SQLSTATE means the database never answered, so the request never reached it. Java
    // matches driver message text for this case; sqlx gives the variants directly.
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::WorkerCrashed => BackendErrorKind::Connection,
        // Terminal, despite looking like every other connection failure. A closed pool is closed
        // for good — sqlx has no way to reopen one — so waiting for it to come back never ends.
        // Calling it a connection error makes any operation issued after shutdown hang instead of
        // returning, which is the one outcome the retry layer must never produce.
        sqlx::Error::PoolClosed => BackendErrorKind::Permanent,
        _ => BackendErrorKind::Permanent,
    }
}

/// How a handle behaves against a database it is already connected to.
///
/// Split from [`Config`] because the two constructors need different halves of it.
/// [`PostgresSystemDatabase::connect`] has to be told how to *reach* the database;
/// [`from_pool`](PostgresSystemDatabase::from_pool) is handed a live pool and only needs this.
/// Sharing one type would leave `from_pool` ignoring a URL, and giving it setters instead would
/// mean two ways to say the same thing that drift apart as fields are added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings<'a> {
    /// Schema holding the DBOS tables. Defaults to [`DEFAULT_SCHEMA`].
    pub schema: &'a str,
    /// How failures that may pass are waited out.
    pub retry: RetryPolicy,
    /// Identifies this process among the executors sharing the database.
    ///
    /// `None` leaves the executor column alone entirely, which is Python's behaviour when it has
    /// no id to stamp.
    pub executor_id: Option<&'a str>,
}

impl Default for Settings<'_> {
    /// The defaults every implementation shares.
    ///
    /// Hand-written rather than derived because `<&str as Default>` is the empty string, and a
    /// handle addressing schema `""` would fail on its first query rather than at construction.
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA,
            retry: RetryPolicy::default(),
            executor_id: None,
        }
    }
}

/// How to reach and set up the system database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config<'a> {
    /// Connection URL for the system database.
    pub url: &'a str,
    /// Maximum pooled connections.
    pub max_connections: u32,
    /// Whether the schema uses LISTEN/NOTIFY triggers.
    ///
    /// Here rather than on [`Settings`] because it is a *migration* input: it decides which
    /// variant of the schema is applied, and `from_pool` never migrates.
    pub use_listen_notify: bool,
    /// Whether [`PostgresSystemDatabase::connect`] brings the schema up to date.
    ///
    /// On by default. Turning it off suits an application whose database is migrated by something
    /// else — a deployment step, or another executor that got there first — and it then connects
    /// to whatever is already there. Java gates the same step on `DBOSConfig.migrate`.
    pub migrate: bool,
    /// How the resulting handle behaves.
    pub settings: Settings<'a>,
}

impl<'a> Config<'a> {
    /// A configuration with the defaults every implementation shares.
    pub fn new(url: &'a str) -> Self {
        Self {
            url,
            max_connections: 10,
            use_listen_notify: true,
            migrate: true,
            settings: Settings::default(),
        }
    }
}

/// A system database backed by PostgreSQL or CockroachDB.
pub struct PostgresSystemDatabase {
    pool: PgPool,
    /// Schema-qualified, quoted table names, built once.
    ///
    /// The schema is fixed at construction, so rendering these per query would allocate three
    /// strings on every database operation to produce a value that never changes.
    tables: Tables,
    retry: RetryPolicy,
    executor_id: Option<String>,
}

impl PostgresSystemDatabase {
    /// Connects, creating the database if it does not exist, and migrates it.
    ///
    /// Creating the database is part of connecting rather than part of migrating: you cannot
    /// migrate a database you cannot connect to. Every other DBOS implementation does the same,
    /// so pointing a fresh application at an empty server is expected to work.
    pub async fn connect(config: &Config<'_>) -> Result<Self, Error> {
        // Only when migrating. A caller that has opted out is saying the database is someone
        // else's to set up, and creating one here would hide the fact that it is missing.
        if config.migrate {
            ensure_database_exists(config.url).await?;
        }

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(config.url)
            .await?;

        // Either bring the schema up, or check that whoever was supposed to has. Connecting to a
        // schema this build's queries do not fit is a failure either way; the only question is
        // whether it is reported now or as a missing column on some later statement.
        let prepared = if config.migrate {
            migrations::runner::run(&pool, config.settings.schema, config.use_listen_notify)
                .await
                .map(|_| ())
        } else {
            migrations::runner::verify(&pool, config.settings.schema).await
        };
        // The runner has already retried what it could; whatever reaches here is settled.
        prepared.map_err(|e| {
            Error::Backend(BackendError {
                message: e.to_string(),
                sqlstate: None,
                kind: BackendErrorKind::Permanent,
            })
        })?;

        Ok(Self {
            pool,
            tables: Tables::new(config.settings.schema),
            retry: config.settings.retry,
            // Copied out: the handle outlives the borrowed configuration.
            executor_id: config.settings.executor_id.map(str::to_owned),
        })
    }

    /// Wraps an existing pool, assuming the schema is already migrated.
    ///
    /// For callers that manage their own connections — and for tests, which migrate through the
    /// harness. Takes the same [`Settings`] as [`connect`](Self::connect), so there is one way to
    /// describe a handle rather than a constructor plus a set of chained overrides.
    pub fn from_pool(pool: PgPool, settings: &Settings<'_>) -> Self {
        Self {
            pool,
            tables: Tables::new(settings.schema),
            retry: settings.retry,
            // Copied out: the handle outlives the borrowed settings.
            executor_id: settings.executor_id.map(str::to_owned),
        }
    }
}

/// The tables this backend addresses, quoted and schema-qualified.
///
/// One field per table rather than a map: every lookup is a literal in this file, so a missing
/// table should be a compile error rather than a runtime `None`.
struct Tables {
    workflow_status: String,
    operation_outputs: String,
    application_versions: String,
    notifications: String,
    workflow_events: String,
    workflow_events_history: String,
    streams: String,
}

impl Tables {
    fn new(schema: &str) -> Self {
        let schema = quote_identifier(schema);
        Self {
            workflow_status: format!("{schema}.{}", quote_identifier("workflow_status")),
            operation_outputs: format!("{schema}.{}", quote_identifier("operation_outputs")),
            application_versions: format!("{schema}.{}", quote_identifier("application_versions")),
            notifications: format!("{schema}.{}", quote_identifier("notifications")),
            workflow_events: format!("{schema}.{}", quote_identifier("workflow_events")),
            workflow_events_history: format!(
                "{schema}.{}",
                quote_identifier("workflow_events_history")
            ),
            streams: format!("{schema}.{}", quote_identifier("streams")),
        }
    }
}

/// Creates the target database if the server does not already have it.
///
/// Connects to the maintenance database to ask, because you cannot create a database from
/// inside itself. A concurrent creator is not an error — the postcondition is that the database
/// exists, and it does.
async fn ensure_database_exists(url: &str) -> Result<(), Error> {
    let Some((maintenance_url, database)) = split_database(url) else {
        // No database in the URL means the server's default, which necessarily exists.
        return Ok(());
    };

    let admin = match PgPoolOptions::new()
        .max_connections(1)
        .connect(&maintenance_url)
        .await
    {
        Ok(pool) => pool,
        // Reaching the maintenance database is a convenience, not a requirement. If it is not
        // permitted, the target may still exist and be reachable.
        Err(_) => return Ok(()),
    };

    let exists = sqlx::query("SELECT 1 FROM pg_database WHERE datname = $1")
        .bind(&database)
        .fetch_optional(&admin)
        .await?
        .is_some();

    if !exists {
        let quoted = quote_identifier(&database);
        match sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {quoted}")))
            .execute(&admin)
            .await
        {
            Ok(_) => tracing::info!(database = %database, "created the system database"),
            // 42P04 duplicate_database: a peer created it first, which is the outcome wanted.
            Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("42P04") => {}
            Err(e) => return Err(e.into()),
        }
    }
    admin.close().await;
    Ok(())
}

/// Splits a connection URL into a maintenance URL and the database it names.
///
/// Returns `None` when the URL names no database, in which case there is nothing to create.
fn split_database(url: &str) -> Option<(String, String)> {
    let (prefix, tail) = url.split_once("://")?;
    let (authority, rest) = tail.split_once('/')?;
    let (database, query) = match rest.split_once('?') {
        Some((db, q)) => (db, Some(q)),
        None => (rest, None),
    };
    if database.is_empty() {
        return None;
    }
    let mut maintenance = format!("{prefix}://{authority}/postgres");
    if let Some(q) = query {
        maintenance.push('?');
        maintenance.push_str(q);
    }
    Some((maintenance, database.to_owned()))
}

/// Every column `workflow_from_row` reads, in one place so the queries cannot drift from it.
const WORKFLOW_COLUMNS: &str = "workflow_uuid, status, name, class_name, config_name, \
     serialization, executor_id, application_version, recovery_attempts, \
     queue_name, created_at, updated_at, started_at_epoch_ms, completed_at, forked_from, \
     parent_workflow_id, was_forked_from, owner_xid, application_id, authenticated_user, \
     authenticated_roles, assumed_role, request, deduplication_id, priority, \
     queue_partition_key, rate_limited, schedule_name, workflow_timeout_ms, \
     workflow_deadline_epoch_ms, delay_until_epoch_ms, debounce_deadline_epoch_ms, \
     is_debounced, attributes::text AS attributes";

/// The payload columns, as typed `NULL`s when the caller declines them.
///
/// Held apart from [`WORKFLOW_COLUMNS`] and *appended* rather than substituted into it. Rewriting
/// the list with `str::replace` would match substrings, so a column added later called
/// `last_error` would silently become `last_NULL::text AS error` — valid-looking SQL that
/// selects the wrong thing. The cast is required either way: a bare `NULL` has no type for the
/// driver to decode.
///
/// Order does not matter, because [`workflow_from_row`] reads columns by name.
fn workflow_payloads(load_input: bool, load_output: bool) -> &'static str {
    match (load_input, load_output) {
        (true, true) => "inputs, output, error",
        (true, false) => "inputs, NULL::text AS output, NULL::text AS error",
        (false, true) => "NULL::text AS inputs, output, error",
        (false, false) => "NULL::text AS inputs, NULL::text AS output, NULL::text AS error",
    }
}

/// Encodes the roles list for the column, which is `NULL` when there are none.
///
/// The column is always a JSON array of strings — unlike `input` and the outcome payloads, whose
/// encoding is the caller's. Empty maps to `NULL` so "no roles" has one representation, matching
/// the empty-string normalisation applied to `authenticated_user`.
fn encode_roles(roles: &[&str]) -> Option<String> {
    (!roles.is_empty()).then(|| serde_json::to_string(roles).expect("a list of strings is JSON"))
}

/// Decodes the roles column, treating `NULL` as no roles.
///
/// A value that is not a JSON array of strings is [`Error::Malformed`]: it means another
/// implementation wrote something this one does not understand, which is exactly what that
/// variant is for.
fn decode_roles(stored: Option<String>) -> Result<Vec<String>, Error> {
    let Some(json) = stored else {
        return Ok(Vec::new());
    };
    serde_json::from_str(&json).map_err(|e| {
        Error::Malformed(format!(
            "authenticated_roles is not a JSON array of strings: {e}"
        ))
    })
}

/// The step name `record_sleep` records. A cross-SDK constant, like [`SET_EVENT_STEP_NAME`].
const SLEEP_STEP_NAME: &str = "DBOS.sleep";

/// The encoding this layer uses for values it produces itself.
///
/// Distinct from the workflow's own format: a sleep's wake time is a plain number written and
/// read by the system database, so it is stored legibly rather than in whatever the workflow
/// chose. Python does the same for the same value; Java uses the workflow's serializer.
const PORTABLE_JSON: &str = "portable_json";

/// The step name `set_event` records, which a replay compares against.
///
/// A cross-SDK constant: Java and Python both record exactly `"DBOS.setEvent"`, and a workflow
/// replayed by another implementation must find the name it expects or raise `UnexpectedStep`.
const SET_EVENT_STEP_NAME: &str = "DBOS.setEvent";

/// Every column `version_from_row` reads.
const VERSION_COLUMNS: &str = "version_id, version_name, version_timestamp, created_at";

/// Reads a sleep step's recorded wake time.
///
/// Stored as epoch milliseconds in portable JSON — a bare number, so parsing is the whole of
/// decoding it. A value that is not one means another implementation wrote something this one
/// does not understand, which is what [`Error::Malformed`] is for.
fn decode_wake_time(
    output: Option<&str>,
    workflow_id: &str,
    step_id: i32,
) -> Result<Timestamp, Error> {
    let recorded = output.ok_or_else(|| {
        Error::Malformed(format!(
            "workflow {workflow_id} step {step_id} is a sleep with no recorded wake time"
        ))
    })?;
    recorded
        .trim()
        .parse::<i64>()
        .map(Timestamp::from_epoch_ms)
        .map_err(|e| {
            Error::Malformed(format!(
                "workflow {workflow_id} step {step_id} wake time {recorded:?} is not epoch milliseconds: {e}"
            ))
        })
}

fn version_from_row(row: &sqlx::postgres::PgRow) -> Result<VersionInfo, Error> {
    Ok(VersionInfo {
        version_id: row.try_get("version_id")?,
        version_name: row.try_get("version_name")?,
        version_timestamp: Timestamp::from_epoch_ms(row.try_get("version_timestamp")?),
        created_at: Timestamp::from_epoch_ms(row.try_get("created_at")?),
    })
}

fn workflow_from_row(row: &sqlx::postgres::PgRow) -> Result<WorkflowRecord, Error> {
    let status_text: String = row.try_get("status")?;
    let status = WorkflowStatus::parse(&status_text)
        .ok_or_else(|| Error::Malformed(format!("unknown workflow status {status_text:?}")))?;

    Ok(WorkflowRecord {
        workflow_id: row.try_get("workflow_uuid")?,
        status,
        name: row.try_get("name")?,
        class_name: row.try_get("class_name")?,
        config_name: row.try_get("config_name")?,
        input: row.try_get("inputs")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        serialization: row.try_get("serialization")?,
        executor_id: row.try_get("executor_id")?,
        application_version: row.try_get("application_version")?,
        // `recovery_attempts` is BIGINT; reading it as i64 is exact on both backends.
        recovery_attempts: row
            .try_get::<Option<i64>, _>("recovery_attempts")?
            .unwrap_or(0),
        queue_name: row.try_get("queue_name")?,
        created_at: Timestamp::from_epoch_ms(row.try_get("created_at")?),
        updated_at: Timestamp::from_epoch_ms(row.try_get("updated_at")?),
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at")?
            .map(Timestamp::from_epoch_ms),
        forked_from: row.try_get("forked_from")?,
        parent_workflow_id: row.try_get("parent_workflow_id")?,
        was_forked_from: row
            .try_get::<Option<bool>, _>("was_forked_from")?
            .unwrap_or(false),

        owner_xid: row.try_get("owner_xid")?,
        application_id: row.try_get("application_id")?,
        authenticated_user: row.try_get("authenticated_user")?,
        authenticated_roles: decode_roles(
            row.try_get::<Option<String>, _>("authenticated_roles")?,
        )?,
        assumed_role: row.try_get("assumed_role")?,
        request: row.try_get("request")?,

        deduplication_id: row.try_get("deduplication_id")?,
        priority: row.try_get::<Option<i32>, _>("priority")?.unwrap_or(0),
        queue_partition_key: row.try_get("queue_partition_key")?,
        rate_limited: row
            .try_get::<Option<bool>, _>("rate_limited")?
            .unwrap_or(false),
        schedule_name: row.try_get("schedule_name")?,

        // A duration, where the three below are instants — see `types`.
        timeout: row
            .try_get::<Option<i64>, _>("workflow_timeout_ms")?
            .and_then(duration_from_ms),
        deadline: row
            .try_get::<Option<i64>, _>("workflow_deadline_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        delay_until: row
            .try_get::<Option<i64>, _>("delay_until_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        debounce_deadline: row
            .try_get::<Option<i64>, _>("debounce_deadline_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        is_debounced: row
            .try_get::<Option<bool>, _>("is_debounced")?
            .unwrap_or(false),

        // Cast to text in the query, so no JSON library is needed to move a payload this
        // layer never parses.
        attributes: row.try_get("attributes")?,
    })
}
impl PostgresSystemDatabase {
    /// [`check_step`](SystemDatabase::check_step) against a caller's connection.
    ///
    /// Split out so a transaction can drive it: `set_event` has to check the step and write its
    /// event in one commit, and a step record landing without the write would make a replay skip
    /// the write and lose the event.
    ///
    /// Takes `&mut PgConnection` rather than a generic `PgExecutor` because that trait is
    /// consumed by value — `record_step_on` issues two statements and needs to reborrow. A
    /// transaction derefs to one, and the pool path acquires.
    async fn check_step_on(
        &self,
        conn: &mut sqlx::PgConnection,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let (workflow_table, steps_table) = (workflow_table.as_str(), steps_table.as_str());
        let step_columns = qualified_step_columns("o");
        {
            // One statement, so the status and the step come from a single snapshot. Two queries
            // could straddle a cancellation — read PENDING, get cancelled, then replay a step of
            // a cancelled workflow. Python avoids that by wrapping both in a transaction; the
            // join gets the same consistency, and one round trip instead of two.
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT s.status, {step_columns} \
                 FROM {workflow_table} s \
                 LEFT JOIN {steps_table} o \
                   ON o.workflow_uuid = s.workflow_uuid AND o.function_id = $2 \
                 WHERE s.workflow_uuid = $1"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .fetch_optional(&mut *conn)
            .await?;

            // No row at all means no such workflow — the outer table drives the join.
            let Some(row) = row else {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: vec![workflow_id.to_owned()],
                });
            };
            let status: String = row.try_get("status")?;
            if status == WorkflowStatus::Cancelled.as_str() {
                return Err(Error::WorkflowCancelled {
                    workflow_id: workflow_id.to_owned(),
                });
            }

            // `function_id` is `INT4 NOT NULL` in the table, so a NULL here can only mean the
            // join found nothing: the step has not run.
            if row.try_get::<Option<i32>, _>("function_id")?.is_none() {
                return Ok(None);
            }
            let step = step_from_row(&row, workflow_id)?;

            // The recorded name disagreeing means the workflow's code changed between runs, so
            // this position no longer holds the step that is asking for it.
            if step.step_name != step_name {
                return Err(Error::UnexpectedStep {
                    workflow_id: workflow_id.to_owned(),
                    step_id,
                    expected: step_name.to_owned(),
                    recorded: step.step_name,
                });
            }
            Ok(Some(step))
        }
    }

    /// [`record_step`](SystemDatabase::record_step) against a caller's connection.
    ///
    /// One argument over clippy's threshold, and deliberately: it is the trait method's own
    /// parameter list plus the connection. Grouping them into a struct here would mean either a
    /// type used in one place or changing the public signature to match, and that signature was
    /// chosen so a caller does not allocate.
    #[allow(clippy::too_many_arguments)]
    async fn record_step_on(
        &self,
        conn: &mut sqlx::PgConnection,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error> {
        if workflow_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "workflow_id",
                detail: "must not be empty".to_owned(),
            });
        }
        if step_id < 0 {
            return Err(Error::InvalidInput {
                field: "step_id",
                detail: "must not be negative".to_owned(),
            });
        }
        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let (workflow_table, steps_table) = (workflow_table.as_str(), steps_table.as_str());
        let executor_id = self.executor_id.as_deref();
        // The sum type collapses to the two nullable columns only here, at the edge.
        let (output, error) = outcome.columns();

        {
            // The step goes in first, and the executor claim follows only if this caller won.
            // Order is what makes two statements safe here: a crash between them leaves a
            // recorded step under a stale executor marker, which the next step corrects. The
            // reverse order — claim, then record — leaves a workflow attributed to an executor
            // with no step to show for it, and nothing later fixes that.
            //
            // And it is why this is not a transaction, which the ordering makes unnecessary. The
            // references split on exactly this line: Python claims before inserting and so wraps
            // both in `engine.begin()`; Java and TypeScript claim after winning and use a plain
            // connection, as here. A transaction would only hold a `workflow_status` row lock
            // across two round trips.
            //
            // The gap it would close is already covered. A *retryable* failure on the second
            // statement re-runs this closure, the insert returns our own completion time, and
            // the claim runs again. A *permanent* one returns an error with the step recorded —
            // and a caller retrying with the same `StepTiming` is recognised as its own write.
            //
            // `DO UPDATE` setting a column to itself rather than `DO NOTHING`: the point is to
            // make `RETURNING` fire on conflict, so the stored completion comes back and can be
            // compared. `DO NOTHING` returns no row, which cannot tell a rival execution from
            // this caller's own retry. Python and TypeScript use the same trick; Java uses
            // `DO NOTHING` and so silently accepts a duplicate.
            //
            // `Option<Option<i64>>`: the outer is "was there a row", the inner is the column,
            // which is nullable because a caller may record a step without timings.
            let stored: Option<Option<i64>> = sqlx::query_scalar(AssertSqlSafe(format!(
                "INSERT INTO {steps_table} (workflow_uuid, function_id, function_name, output, \
                 error, serialization, started_at_epoch_ms, completed_at_epoch_ms) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                 ON CONFLICT (workflow_uuid, function_id) DO UPDATE \
                 SET completed_at_epoch_ms = {steps_table}.completed_at_epoch_ms \
                 RETURNING completed_at_epoch_ms"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .bind(step_name)
            .bind(output)
            .bind(error)
            .bind(serialization)
            .bind(timing.map(|t| t.started_at.as_epoch_ms()))
            .bind(timing.map(|t| t.completed_at.as_epoch_ms()))
            .fetch_optional(&mut *conn)
            .await?;

            // The stored completion is ours when it matches, and when neither side has one:
            // without timings we already accept the duplicate as ours, so the claim below has to
            // follow the same reading. A value that differs is another execution's.
            let ours = stored.flatten() == timing.map(|t| t.completed_at.as_epoch_ms());
            if !ours {
                tracing::warn!(
                    workflow_id,
                    step_id,
                    step_name,
                    "step was already recorded by another execution"
                );
                return Err(Error::StepAlreadyRecorded {
                    workflow_id: workflow_id.to_owned(),
                    step_id,
                });
            }

            // Winning the checkpoint is what proves this executor is advancing the workflow, so
            // the claim is conditional on it — an executor that recovered a workflow from a dead
            // peer takes ownership here, and one that lost the race does not. Java guards the
            // same write with `if (won)`; TypeScript's comment reads "Winning the checkpoint
            // proves this executor is advancing the workflow". Python re-stamps unconditionally
            // and beforehand, which claims workflows it has just lost.
            //
            // Skipped when this process has no id, as Python does.
            if let Some(executor_id) = executor_id {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} SET executor_id = $2 \
                     WHERE workflow_uuid = $1 AND executor_id IS DISTINCT FROM $2"
                )))
                .bind(workflow_id)
                .bind(executor_id)
                .execute(&mut *conn)
                .await?;
            }
            Ok(())
        }
    }

    /// Cancels one batch, returning the ids that were still running.
    ///
    /// Not retried in here, and not reading its own clock. Both belong to the trait method that
    /// owns the whole operation: a retry that restarted only this statement would leave the
    /// cascade around it half-walked, and a per-level clock would give one cancellation as many
    /// `completed_at` values as the tree has depth.
    ///
    /// The terminal-status guard is what makes cancellation safe to repeat: a workflow that has
    /// already succeeded keeps its result rather than being overwritten with `CANCELLED`.
    async fn cancel_batch<S>(&self, workflow_ids: &[S], now: i64) -> Result<Vec<String>, Error>
    where
        S: AsRef<str> + Sync,
    {
        let table = &self.tables.workflow_status;
        let ids: Vec<&str> = workflow_ids.iter().map(AsRef::as_ref).collect();
        // TODO: revisit clearing `started_at_epoch_ms` here.
        //
        // All four implementations do it and none of them explain it. The column is read by the
        // rate limiter, which counts rows with `queue_name = <queue>`, `rate_limited = TRUE`, a
        // non-queued status, and `started_at_epoch_ms > now - period`. That justifies the same
        // clearing in `clear_queue_assignment` and `resume_workflows`, which keep or set a queue
        // name and so stay in the limiter's scope. **It does not justify it here**: this
        // statement also sets `queue_name = NULL`, which drops the row from that count on its
        // own.
        //
        // The cost is real — a workflow that was running when cancelled loses its start time, so
        // `started_after`/`started_before` no longer find it and `completed_at` is left without a
        // matching start. Kept for now because diverging from all four on a durable column is a
        // worse trade than the inconsistency; worth raising upstream to find out whether it is
        // intent or inheritance.
        let cancelled: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {table} SET status = 'CANCELLED', queue_name = NULL, \
             deduplication_id = NULL, started_at_epoch_ms = NULL, \
             updated_at = $2, completed_at = $2 \
             WHERE workflow_uuid = ANY($1) \
               AND status NOT IN ('SUCCESS', 'ERROR', 'CANCELLED') \
             RETURNING workflow_uuid"
        )))
        .bind(ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(cancelled)
    }

    /// The workflows whose parent is one of these.
    ///
    /// Generic over the element type so a caller can pass `&[String]` from a frontier or
    /// `&[&str]` from a borrowed seed without cloning either. The only copy is the vector of
    /// pointers the driver needs to encode a `text[]`.
    async fn direct_children<S>(&self, workflow_ids: &[S]) -> Result<Vec<String>, Error>
    where
        S: AsRef<str> + Sync,
    {
        let table = &self.tables.workflow_status;
        let ids: Vec<&str> = workflow_ids.iter().map(AsRef::as_ref).collect();
        let children: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT workflow_uuid FROM {table} WHERE parent_workflow_id = ANY($1)"
        )))
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(children)
    }
}

/// Every column of `operation_outputs` [`step_from_row`] reads, except `workflow_uuid`.
///
/// The workflow id is left out because every caller already has it as a parameter, and because
/// `check_step` joins this table against `workflow_status`, where selecting both tables'
/// `workflow_uuid` would be ambiguous.
const STEP_COLUMNS: &str = "function_id, function_name, \
     child_workflow_id, serialization, started_at_epoch_ms, completed_at_epoch_ms";

/// A step's payload columns, on the same terms as [`workflow_payloads`].
fn step_payloads(load_output: bool) -> &'static str {
    if load_output {
        "output, error"
    } else {
        "NULL::text AS output, NULL::text AS error"
    }
}

/// The same columns qualified for a join, derived from [`STEP_COLUMNS`] so the two cannot drift.
///
/// `output`, `error`, and `started_at_epoch_ms` exist on *both* tables, so the qualification is
/// required rather than tidiness.
fn qualified_step_columns(alias: &str) -> String {
    // Payloads included: the replay check always loads them.
    format!("{STEP_COLUMNS}, {}", step_payloads(true))
        .split(',')
        .map(|c| format!("{alias}.{}", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn step_from_row(row: &sqlx::postgres::PgRow, workflow_id: &str) -> Result<StepRecord, Error> {
    Ok(StepRecord {
        workflow_id: workflow_id.to_owned(),
        step_id: row.try_get("function_id")?,
        step_name: row.try_get("function_name")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        child_workflow_id: row.try_get("child_workflow_id")?,
        serialization: row.try_get("serialization")?,
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
    })
}

#[async_trait]
impl SystemDatabase for PostgresSystemDatabase {
    async fn init_workflow(
        &self,
        workflow: &NewWorkflow,
        max_recovery_attempts: Option<i64>,
        submission: Submission,
    ) -> Result<WorkflowInitResult, Error> {
        workflow.validate()?;
        let table = &self.tables.workflow_status;
        // Derived, never supplied: a caller cannot enqueue a workflow and label it SUCCESS.
        let initial_status = workflow.initial_status();
        // Queued workflows are not running, so neither the attempt counter nor the executor
        // stamp applies to them — both `CASE` expressions below turn on that distinction.
        let queued = matches!(
            initial_status,
            WorkflowStatus::Enqueued | WorkflowStatus::Delayed
        );
        let initial_attempts = i64::from(!queued);
        // A recovery or a dequeue is being told it owns this workflow; a fresh start is not.
        let claiming = submission.claims_ownership();
        let increment = i64::from(claiming && !queued);
        // Everything below is generated **outside** the retry, and that is the whole reason the
        // retry is a wrapper rather than a loop around the statement.
        //
        // The owner identity is per logical attempt, not per statement. If a commit
        // acknowledgement is lost, the retry must present the same identity or it will fail to
        // recognise its own write and conclude someone else owns the row — which would report
        // `should_execute: false` for a workflow this caller does in fact own. Java's comment
        // says the same: "generated outside of the DB retry loop, in case commit acks get lost".
        let owner_xid = uuid::Uuid::new_v4().to_string();
        // One clock reading for every timestamp this row gets, so `delay_until` cannot disagree
        // with `created_at`, and a retry cannot push the delay further out each time. This is
        // also why `delay` crosses the API as a duration.
        let now = Timestamp::now();
        let delay_until = workflow
            .delay
            .map(|d| Timestamp::from_epoch_ms(now.as_epoch_ms() + d.as_millis() as i64));

        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (table, pool, owner_xid) = (table.as_str(), &self.pool, owner_xid.as_str());

        with_retry(&self.retry, "init_workflow", move || async move {
            // The column list is Java's INSERT, in its order, plus Python's two debounce
            // columns. Columns absent from it are absent deliberately: `output`, `error`,
            // `started_at`, `completed_at`, `forked_from`, `was_forked_from`, and `rate_limited`
            // are written by execution, forking, and the rate limiter — never at creation.
            //
            // The executor re-stamp is guarded on ownership, which is a deliberate divergence.
            // On conflict the upsert would otherwise hand `executor_id` to whoever submitted
            // last — including a duplicate `Fresh` submission of a workflow another executor is
            // running. That misdirects recovery: `get_pending_workflows` keys on `executor_id`,
            // so the real owner's sweep stops finding the workflow and the submitter's starts,
            // and the second execution is only caught later, at its first step.
            //
            // Java and TypeScript prevent it by letting the write land and rolling the
            // transaction back for a non-owner; Python and Go let it stand. The `CASE` reaches
            // Java's and TypeScript's outcome without their transaction — the row simply keeps
            // its executor.
            //
            // Its middle arm is the lost-acknowledgement case: `owner_xid` is generated once per
            // logical attempt, so it matches an existing row only when this *is* that attempt
            // asking again. Ownership is compared on `owner_xid` and not `executor_id` for the
            // reason migration 7 exists — `executor_id` defaults to `"local"` and collides
            // between processes on one machine.
            //
            // TODO: raise this divergence with the DBOS team before v1. No implementation does
            // exactly what the `CASE` does, and the 2–2 split is weaker than it looks: Java and
            // TypeScript are the same code (identical `shouldCommit` flag, identical comment),
            // and Go's commit is entangled with its enqueue path, which must commit regardless.
            // Python is the only unambiguous vote for leaving the re-stamp in place, and it may
            // be inheritance rather than intent — the same open question as
            // `started_at_epoch_ms` in `cancel_batch`. Worth confirming, and worth proposing
            // upstream rather than carrying as a Rust-only difference.
            let row = sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {table} (workflow_uuid, status, inputs, \
                 name, class_name, config_name, \
                 queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms, \
                 authenticated_user, assumed_role, authenticated_roles, \
                 executor_id, application_version, application_id, \
                 created_at, updated_at, recovery_attempts, \
                 workflow_timeout_ms, workflow_deadline_epoch_ms, \
                 parent_workflow_id, owner_xid, serialization, attributes, schedule_name, \
                 debounce_deadline_epoch_ms, is_debounced) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                 $17, $18, $19, $20, $21, $22, $23, $24, $25, $26::jsonb, $27, $28, $29) \
                 ON CONFLICT (workflow_uuid) DO UPDATE SET \
                   recovery_attempts = CASE \
                       WHEN {table}.status != 'ENQUEUED' AND {table}.status != 'DELAYED' \
                       THEN {table}.recovery_attempts + $30 \
                       ELSE {table}.recovery_attempts \
                   END, \
                   updated_at = EXCLUDED.updated_at, \
                   executor_id = CASE \
                       WHEN EXCLUDED.status = 'ENQUEUED' OR EXCLUDED.status = 'DELAYED' \
                       THEN {table}.executor_id \
                       WHEN {table}.owner_xid IS NULL \
                         OR {table}.owner_xid = EXCLUDED.owner_xid \
                         OR $31 \
                       THEN EXCLUDED.executor_id \
                       ELSE {table}.executor_id \
                   END \
                 RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, \
                 workflow_deadline_epoch_ms, owner_xid, serialization"
            )))
            .bind(workflow.workflow_id)
            .bind(initial_status.as_str())
            .bind(workflow.input)
            .bind(workflow.name)
            .bind(workflow.class_name)
            .bind(workflow.config_name)
            .bind(workflow.queue_name)
            .bind(workflow.deduplication_id)
            .bind(workflow.priority)
            .bind(workflow.queue_partition_key)
            .bind(delay_until.map(Timestamp::as_epoch_ms))
            // TypeScript and Go send `""` rather than null when there is no auth context, and
            // Java normalises it on the way in for that reason. Storing both spellings would
            // make an `authenticated_user IS NULL` filter miss rows another SDK wrote.
            .bind(empty_to_none(workflow.authenticated_user))
            .bind(empty_to_none(workflow.assumed_role))
            .bind(encode_roles(&workflow.authenticated_roles))
            .bind(workflow.executor_id)
            .bind(workflow.application_version)
            .bind(workflow.application_id)
            .bind(now.as_epoch_ms())
            .bind(now.as_epoch_ms())
            .bind(initial_attempts)
            .bind(workflow.timeout.map(|d| d.as_millis() as i64))
            .bind(workflow.deadline.map(Timestamp::as_epoch_ms))
            .bind(workflow.parent_workflow_id)
            .bind(owner_xid)
            .bind(workflow.serialization)
            .bind(workflow.attributes)
            .bind(workflow.schedule_name)
            .bind(workflow.debounce_deadline.map(Timestamp::as_epoch_ms))
            .bind(workflow.is_debounced)
            .bind(increment)
            .bind(claiming)
            .fetch_one(pool)
            .await
            // A unique violation here can only be the partial index on
            // `(queue_name, deduplication_id)`: the primary-key conflict is absorbed by
            // `ON CONFLICT (workflow_uuid)` above. Guarded on the caller having supplied a key
            // rather than asserting it, as Python does, so an unexpected violation stays a
            // backend error instead of being mislabelled.
            .map_err(|e| match Error::from(e) {
                Error::Backend(b)
                    if b.sqlstate.as_deref() == Some("23505")
                        && let Some(deduplication_id) = workflow.deduplication_id
                        && let Some(queue_name) = workflow.queue_name =>
                {
                    Error::QueueDeduplicated {
                        workflow_id: workflow.workflow_id.to_owned(),
                        queue_name: queue_name.to_owned(),
                        deduplication_id: deduplication_id.to_owned(),
                    }
                }
                other => other,
            })?;

            let recovery_attempts: i64 = row.try_get("recovery_attempts")?;
            let status_text: String = row.try_get("status")?;
            let status = WorkflowStatus::parse(&status_text).ok_or_else(|| {
                Error::Malformed(format!("unknown workflow status {status_text:?}"))
            })?;
            let stored_owner: Option<String> = row.try_get("owner_xid")?;
            let deadline: Option<i64> = row.try_get("workflow_deadline_epoch_ms")?;
            let serialization: Option<String> = row.try_get("serialization")?;

            // Same id, different function: a programming error rather than a race, because the id
            // is how every implementation decides two attempts are the same workflow.
            for (field, stored, offered) in [
                (
                    "function name",
                    row.try_get::<Option<String>, _>("name")?,
                    &workflow.name,
                ),
                ("class name", row.try_get("class_name")?, &workflow.class_name),
                ("config name", row.try_get("config_name")?, &workflow.config_name),
            ] {
                if stored.as_deref() != offered.as_deref() {
                    return Err(Error::ConflictingWorkflow {
                        workflow_id: workflow.workflow_id.to_owned(),
                        detail: format!(
                            "existing {field} is {stored:?}, but {offered:?} was provided"
                        ),
                    });
                }
            }
            // A differing queue is only a warning: requeueing the same workflow elsewhere is
            // legitimate, and the stored queue wins.
            let stored_queue: Option<String> = row.try_get("queue_name")?;
            if stored_queue.as_deref() != workflow.queue_name {
                tracing::warn!(
                    workflow_id = %workflow.workflow_id,
                    stored = ?stored_queue,
                    provided = ?workflow.queue_name,
                    "workflow already exists on a different queue; the stored queue is kept"
                );
            }

            // Parked once it has been recovered more often than allowed — but only if some *other*
            // attempt is responsible, so a caller retrying its own attempt is not punished for it.
            let owner_differs = stored_owner.as_deref() != Some(owner_xid);
            if let Some(limit) = max_recovery_attempts
                && !status.is_terminal()
                && recovery_attempts > limit + 1
                && owner_differs
            {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} \
                     SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', deduplication_id = NULL, \
                         started_at_epoch_ms = NULL, queue_name = NULL \
                     WHERE workflow_uuid = $1 AND status = 'PENDING'"
                )))
                .bind(workflow.workflow_id)
                .execute(pool)
                .await?;

                return Err(Error::MaxRecoveryAttemptsExceeded {
                    workflow_id: workflow.workflow_id.to_owned(),
                    limit,
                });
            }

            // Another owner holds the row and this is not a recovery, so recording it is right
            // but running it would be a second execution.
            let should_execute = !(owner_differs && !claiming && stored_owner.is_some());
            if !should_execute {
                tracing::debug!(
                    workflow_id = workflow.workflow_id,
                    "another owner holds this workflow; recorded but not claimed"
                );
            }

            Ok(WorkflowInitResult {
                status,
                recovery_attempts,
                deadline: deadline.map(Timestamp::from_epoch_ms),
                serialization,
                should_execute,
            })
        })
        .await
    }

    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowRecord>, Error> {
        let table = &self.tables.workflow_status;
        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_workflow", move || async move {
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT {WORKFLOW_COLUMNS}, {} FROM {table} WHERE workflow_uuid = $1",
                workflow_payloads(true, true)
            )))
            .bind(workflow_id)
            .fetch_optional(pool)
            .await?;

            row.as_ref().map(workflow_from_row).transpose()
        })
        .await
    }

    async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowRecord>, Error> {
        let table = &self.tables.workflow_status;
        // `status` is the one filter whose values are not already strings.
        let status: Vec<&str> = filter
            .status
            .iter()
            .copied()
            .map(WorkflowStatus::as_str)
            .collect();
        // `LIKE ANY(...)` needs the wildcard appended to each pattern, and `%` and `_` in a
        // caller's prefix would otherwise be wildcards themselves. Escaped out here because it
        // allocates, and nothing about it depends on the attempt.
        let prefixes: Vec<String> = filter
            .workflow_id_prefixes
            .iter()
            .map(|p| {
                format!(
                    "{}%",
                    p.replace('\\', r"\\")
                        .replace('%', r"\%")
                        .replace('_', r"\_")
                )
            })
            .collect();
        let (table, pool) = (table.as_str(), &self.pool);
        let (status, prefixes) = (status.as_slice(), prefixes.as_slice());

        with_retry(&self.retry, "list_workflows", move || async move {
            // The builder is rebuilt per attempt, and has to be: `build` borrows it mutably, so
            // a hoisted one would make each attempt's future borrow the closure — which
            // `FnMut() -> Fut` cannot express. It costs nothing on the happy path, where there
            // is one attempt either way.
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            // One row reader serves every query, so a declined column is selected as a typed
            // NULL rather than dropped.
            q.push(WORKFLOW_COLUMNS)
                .push(", ")
                .push(workflow_payloads(filter.load_input, filter.load_output));
            q.push(" FROM ").push(table);

            // `separated(" AND ")` writes the separator only between clauses, so neither a
            // leading `WHERE` with no filters nor a trailing `AND` is possible by construction.
            let mut first = true;
            let mut clause = |q: &mut sqlx::QueryBuilder<sqlx::Postgres>, sql: &str| {
                q.push(if first { " WHERE " } else { " AND " });
                first = false;
                q.push(sql);
            };

            // `= ANY($n)` rather than `IN ($1, $2, …)`: one placeholder for the whole list,
            // whatever its length, so the SQL text does not vary with the caller's input.
            macro_rules! any_of {
                ($values:expr, $column:literal) => {
                    if !$values.is_empty() {
                        clause(&mut q, concat!($column, " = ANY("));
                        q.push_bind($values).push(")");
                    }
                };
            }
            any_of!(&filter.workflow_ids[..], "workflow_uuid");
            any_of!(&filter.names[..], "name");
            any_of!(&filter.class_names[..], "class_name");
            any_of!(&filter.config_names[..], "config_name");
            any_of!(status, "status");
            any_of!(&filter.application_versions[..], "application_version");
            any_of!(&filter.executor_ids[..], "executor_id");
            any_of!(&filter.authenticated_users[..], "authenticated_user");
            any_of!(&filter.queue_names[..], "queue_name");
            any_of!(&filter.schedule_names[..], "schedule_name");
            any_of!(&filter.deduplication_ids[..], "deduplication_id");
            any_of!(&filter.parent_workflow_ids[..], "parent_workflow_id");
            any_of!(&filter.forked_from[..], "forked_from");

            if !prefixes.is_empty() {
                clause(&mut q, "workflow_uuid LIKE ANY(");
                q.push_bind(prefixes).push(")");
            }

            macro_rules! compare {
                ($value:expr, $sql:literal) => {
                    if let Some(v) = $value {
                        clause(&mut q, $sql);
                        q.push_bind(v.as_epoch_ms());
                    }
                };
            }
            compare!(filter.created_after, "created_at >= ");
            compare!(filter.created_before, "created_at <= ");
            compare!(filter.completed_after, "completed_at >= ");
            compare!(filter.completed_before, "completed_at <= ");
            compare!(filter.started_after, "started_at_epoch_ms >= ");
            compare!(filter.started_before, "started_at_epoch_ms <= ");

            macro_rules! flag {
                ($value:expr, $sql:literal) => {
                    if let Some(v) = $value {
                        clause(&mut q, $sql);
                        q.push_bind(v);
                    }
                };
            }
            flag!(filter.was_forked_from, "was_forked_from = ");
            flag!(filter.is_debounced, "is_debounced = ");

            if filter.queues_only {
                clause(&mut q, "queue_name IS NOT NULL");
            }
            if let Some(has_parent) = filter.has_parent {
                clause(
                    &mut q,
                    if has_parent {
                        "parent_workflow_id IS NOT NULL"
                    } else {
                        "parent_workflow_id IS NULL"
                    },
                );
            }
            if let Some(attributes) = &filter.attributes {
                // Containment, served by the GIN index. SQLite cannot reproduce `@>` and Go
                // rejects the filter there; both Postgres and CockroachDB support it.
                clause(&mut q, "attributes @> ");
                q.push_bind(attributes).push("::jsonb");
            }

            q.push(if filter.sort_desc {
                " ORDER BY created_at DESC"
            } else {
                " ORDER BY created_at ASC"
            });
            if let Some(limit) = filter.limit {
                q.push(" LIMIT ").push_bind(limit);
            }
            if let Some(offset) = filter.offset {
                q.push(" OFFSET ").push_bind(offset);
            }

            let rows = q.build().fetch_all(pool).await?;
            rows.iter().map(workflow_from_row).collect()
        })
        .await
    }

    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, Error> {
        // The retry wraps the whole walk, and the accumulators sit inside it. Unlike the
        // cascade in `cancel_workflows`, every statement here is a read, so restarting from the
        // root reproduces the same answer — there is nothing a failed attempt did that a later
        // one needs to know about.
        with_retry(&self.retry, "get_workflow_children", move || async move {
            // `descendants` owns every id and the frontier is a *range* into it, because each
            // level is appended in order — so an id is allocated twice, once by the driver and
            // once for the dedup set, rather than three times. The root is excluded by
            // comparison rather than by seeding `seen` with a copy of it, which also states the
            // contract: a workflow is not its own descendant.
            let mut seen = std::collections::HashSet::new();
            let mut descendants: Vec<String> = Vec::new();
            let mut absorb = |into: &mut Vec<String>, children: Vec<String>| {
                for child in children {
                    if child != workflow_id && seen.insert(child.clone()) {
                        into.push(child);
                    }
                }
            };

            absorb(
                &mut descendants,
                self.direct_children(&[workflow_id]).await?,
            );

            // Level by level, as all four do. Terminates because `seen` only grows, so a cycle —
            // which the data should not contain — stops rather than looping.
            let mut start = 0;
            while start < descendants.len() {
                let end = descendants.len();
                let children = self.direct_children(&descendants[start..end]).await?;
                start = end;
                absorb(&mut descendants, children);
            }
            Ok(descendants)
        })
        .await
    }

    async fn record_workflow_outcome(
        &self,
        workflow_id: &str,
        outcome: Outcome<'_>,
    ) -> Result<OutcomeWrite, Error> {
        let table = &self.tables.workflow_status;
        // Stamped once, outside the retry: a retried attempt is recording the outcome it
        // already had, and re-reading the clock would move `completed_at` forward each time.
        let now = Timestamp::now().as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);
        let (output, error) = outcome.columns();

        with_retry(&self.retry, "record_workflow_outcome", move || async move {
            // The `status = 'PENDING'` predicate is the whole mechanism: an executor that has
            // been presumed dead and superseded finds zero rows updated, and learns it lost
            // rather than clobbering the winner's result. It also makes this retry-safe — a
            // retry after a lost acknowledgement finds its own write and reports
            // `AlreadyFinished`, which is wrong only in that it is the caller's own outcome.
            let updated = sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} SET status = $2, output = $3, error = $4, \
                 updated_at = $5, completed_at = $5 \
                 WHERE workflow_uuid = $1 AND status = 'PENDING'"
            )))
            .bind(workflow_id)
            .bind(outcome.status().as_str())
            .bind(output)
            .bind(error)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();

            Ok(if updated > 0 {
                OutcomeWrite::Recorded
            } else {
                tracing::debug!(
                    workflow_id,
                    "outcome not recorded; the workflow is no longer this run's to finish"
                );
                OutcomeWrite::AlreadyFinished
            })
        })
        .await
    }

    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay: WorkflowDelay,
    ) -> Result<(), Error> {
        let table = &self.tables.workflow_status;
        // Resolved once, outside the retry, so a relative delay does not creep further out with
        // each attempt.
        let now = Timestamp::now();
        let delay_until = delay.resolve(now).as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "set_workflow_delay", move || async move {
            // `status = 'DELAYED'` is the guard: a released workflow is running or queued, and
            // pushing its delay out would not recall it.
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} SET delay_until_epoch_ms = $2, updated_at = $3 \
                 WHERE workflow_uuid = $1 AND status = 'DELAYED'"
            )))
            .bind(workflow_id)
            .bind(delay_until)
            .bind(now.as_epoch_ms())
            .execute(pool)
            .await?;
            Ok(())
        })
        .await
    }

    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, Error> {
        let table = &self.tables.workflow_status;
        let now = Timestamp::now().as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "clear_queue_assignment", move || async move {
            // `queue_name IS NOT NULL` is what makes this a *return* rather than an enqueue: a
            // workflow that never came from a queue has none to go back to.
            let updated = sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} SET started_at_epoch_ms = NULL, status = 'ENQUEUED', \
                 updated_at = $2 \
                 WHERE workflow_uuid = $1 AND queue_name IS NOT NULL AND status = 'PENDING'"
            )))
            .bind(workflow_id)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();
            Ok(updated > 0)
        })
        .await
    }

    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
    ) -> Result<(), Error> {
        validate_attributes(attributes)?;
        let table = &self.tables.workflow_status;
        // Read once, outside the retry: a second attempt is the same write, and re-reading the
        // clock would date the row to whenever the connection came back.
        let now = Timestamp::now().as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "update_workflow_attributes",
            move || async move {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET attributes = $2::jsonb, updated_at = $3 \
                     WHERE workflow_uuid = $1"
                )))
                .bind(workflow_id)
                .bind(attributes)
                .bind(now)
                .execute(pool)
                .await?;
                Ok(())
            },
        )
        .await
    }

    async fn get_pending_workflows(
        &self,
        executor_id: &str,
        application_version: &str,
    ) -> Result<Vec<String>, Error> {
        let table = &self.tables.workflow_status;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_pending_workflows", move || async move {
            let ids: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {table} \
                 WHERE status = 'PENDING' AND executor_id = $1 AND application_version = $2"
            )))
            .bind(executor_id)
            .bind(application_version)
            .fetch_all(pool)
            .await?;
            Ok(ids)
        })
        .await
    }

    async fn transition_delayed_workflows(&self) -> Result<u64, Error> {
        let table = &self.tables.workflow_status;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "transition_delayed_workflows",
            move || async move {
                // The clock is read per attempt on purpose: this is a sweep, not a write a caller
                // is holding an identity for, and a retry should release whatever has since come
                // due rather than replay a stale cutoff.
                let now = Timestamp::now().as_epoch_ms();
                // Clearing the debounce key belongs in this statement, not a second one. The id
                // is held only while the workflow is DELAYED; once released the workflow is
                // committed to running, and a later debounce with the same key must start a
                // fresh workflow rather than bounce this one.
                let moved = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET status = 'ENQUEUED', updated_at = $1, \
                     deduplication_id = CASE WHEN is_debounced THEN NULL \
                                             ELSE deduplication_id END \
                     WHERE status = 'DELAYED' AND delay_until_epoch_ms <= $1"
                )))
                .bind(now)
                .execute(pool)
                .await?
                .rows_affected();
                if moved > 0 {
                    tracing::debug!(moved, "released delayed workflows");
                }
                Ok(moved)
            },
        )
        .await
    }

    async fn cancel_workflows(
        &self,
        workflow_ids: &[&str],
        cancel_children: bool,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        // The retry wraps the whole cascade, not each statement inside it. A failure partway
        // through leaves some of the tree cancelled, and restarting from the roots finishes the
        // job — where retrying one statement would return a half-walked tree as a success.
        //
        // `cancelled` survives across attempts, which is only sound because `cancel_batch`
        // excludes rows that are already `CANCELLED`: a workflow can be returned once and never
        // again, so a retry adds what the failed attempt did not reach and cannot double-report
        // what it did. The walk itself restarts from the roots each attempt, since there is no
        // way to know how far the last one got.
        //
        // `now` is read once for the whole cascade, so every workflow cancelled by one call
        // shares a `completed_at` however deep the tree goes.
        let now = Timestamp::now().as_epoch_ms();
        // Not for concurrency — nothing else touches this. It is how an accumulator outlives a
        // retry while the closure stays `FnMut` returning a `Send` future, which is what
        // `with_retry` requires. A plain `&mut Vec` would make the future borrow the closure.
        let cancelled = std::sync::Mutex::new(Vec::new());
        let collected = &cancelled;
        with_retry(&self.retry, "cancel_workflows", move || async move {
            // One statement per *level*, not per workflow: `cancel_batch` takes the whole
            // frontier and matches it with `= ANY($1)`, so the round trips scale with the depth
            // of the tree rather than its size. Cancelling the level before asking for its
            // children is the ordering that stops a parent spawning behind the walk.
            //
            // The roots are used borrowed; only the levels below are owned, because those come
            // back from the database already allocated.
            // Awaited before locking: a guard held across an await would make this future
            // non-`Send`, which `with_retry` requires.
            let roots = self.cancel_batch(workflow_ids, now).await?;
            collected.lock().expect("cancelled ids").extend(roots);
            if !cancel_children {
                return Ok(());
            }
            // Owned, because the levels below arrive owned from the database and the set has to
            // hold both.
            let mut seen: std::collections::HashSet<String> =
                workflow_ids.iter().map(|id| (*id).to_owned()).collect();
            let mut frontier = self.direct_children(workflow_ids).await?;
            frontier.retain(|c| seen.insert(c.clone()));

            // Terminates because `seen` only grows and a workflow enters a frontier at most once.
            while !frontier.is_empty() {
                let level = self.cancel_batch(&frontier, now).await?;
                collected.lock().expect("cancelled ids").extend(level);
                let children = self.direct_children(&frontier).await?;
                frontier = children
                    .into_iter()
                    .filter(|c| seen.insert(c.clone()))
                    .collect();
            }
            Ok(())
        })
        .await?;
        let cancelled = cancelled.into_inner().expect("cancelled ids");
        tracing::debug!(
            requested = workflow_ids.len(),
            cancelled = cancelled.len(),
            cancel_children,
            "cancelled workflows"
        );
        Ok(cancelled)
    }

    async fn resume_workflows(
        &self,
        workflow_ids: &[&str],
        queue_name: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }
        let table = &self.tables.workflow_status;
        let queue = queue_name.unwrap_or(INTERNAL_QUEUE);
        // Read once, outside the retry, so a second attempt writes the same `updated_at`.
        let now = Timestamp::now().as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "resume_workflows", move || async move {
            // Existence is asked separately because a zero-row update conflates "already
            // finished" — which is legal — with "no such workflow", which is not.
            let existing: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(workflow_ids)
            .fetch_all(pool)
            .await?;
            let missing: Vec<String> = workflow_ids
                .iter()
                .filter(|id| !existing.iter().any(|e| e == *id))
                .map(|id| (*id).to_owned())
                .collect();
            if !missing.is_empty() {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: missing,
                });
            }

            // `completed_at` and `started_at_epoch_ms` are cleared as well as the counters: the
            // workflow is going to run again, and leaving them set would date it to its last
            // attempt.
            let resumed: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "UPDATE {table} SET status = 'ENQUEUED', queue_name = $2, \
                 recovery_attempts = 0, workflow_deadline_epoch_ms = NULL, \
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL, \
                 updated_at = $3 \
                 WHERE workflow_uuid = ANY($1) AND status NOT IN ('SUCCESS', 'ERROR') \
                 RETURNING workflow_uuid"
            )))
            .bind(workflow_ids)
            .bind(queue)
            .bind(now)
            .fetch_all(pool)
            .await?;
            tracing::debug!(
                requested = workflow_ids.len(),
                resumed = resumed.len(),
                queue,
                "resumed workflows"
            );
            Ok(resumed)
        })
        .await
    }

    async fn delete_workflows(
        &self,
        workflow_ids: &[&str],
        delete_children: bool,
    ) -> Result<u64, Error> {
        if workflow_ids.is_empty() {
            return Ok(0);
        }
        // Descendants arrive owned from the database and are kept alive here; the roots stay
        // borrowed, so `targets` copies pointers rather than strings.
        let mut children: Vec<String> = Vec::new();
        if delete_children {
            // Collected before the delete rather than interleaved: a deleted parent cannot spawn,
            // so there is no race to close, and one statement is cheaper than one per level.
            for id in workflow_ids {
                children.extend(self.get_workflow_children(id).await?);
            }
        }
        let mut targets: Vec<&str> = workflow_ids.to_vec();
        targets.extend(children.iter().map(String::as_str));
        targets.sort_unstable();
        targets.dedup();
        let table = &self.tables.workflow_status;
        let (table, pool, targets) = (table.as_str(), &self.pool, targets.as_slice());

        with_retry(&self.retry, "delete_workflows", move || async move {
            // Steps, notifications, events, and streams go with the row: every child table
            // declares `ON DELETE CASCADE` on this foreign key, from migration 1 onward.
            let deleted = sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(targets)
            .execute(pool)
            .await?
            .rows_affected();
            tracing::debug!(deleted, targets = targets.len(), "deleted workflows");
            Ok(deleted)
        })
        .await
    }

    async fn fork_workflows(
        &self,
        forks: &[Fork<'_>],
        options: &ForkOptions<'_>,
    ) -> Result<Vec<String>, Error> {
        if forks.is_empty() {
            return Ok(Vec::new());
        }
        options.validate()?;
        for fork in forks {
            fork.validate()?;
        }

        // Generated **outside** the retry, like `init_workflow`'s owner identity and for the same
        // reason: a fresh id on the second attempt would not recognise the first attempt's write,
        // so a lost commit acknowledgement would fork every workflow twice.
        let forked_ids: Vec<String> = forks
            .iter()
            .map(|f| {
                f.forked_id
                    .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned)
            })
            .collect();

        // The batch is passed as three parallel arrays and reassembled server-side by `unnest`,
        // so the SQL text is the same whatever the caller passes: the server can reuse a plan,
        // and a large batch cannot grow a statement without bound.
        //
        // **A deliberate divergence.** None of the references map the batch this way — Python
        // unions a `SELECT literal(...)` per fork, Go does the same with casts, and TypeScript
        // builds a `VALUES` CTE, all three emitting SQL that grows a row per workflow. The
        // construct is not exotic, though: Python and TypeScript both `unnest` a `text[]` to
        // batch `pg_notify`. What is new is unnesting three arrays in parallel, which is why it
        // is checked against CockroachDB as well as PostgreSQL rather than assumed portable.
        //
        // Mismatched lengths would pad with `NULL` rather than fail, so the three are built from
        // one iteration of `forks` and cannot disagree — which is also why [`Fork`] is a struct
        // per workflow and not three slices.
        let source_ids: Vec<&str> = forks.iter().map(|f| f.source_id).collect();
        let start_steps: Vec<i32> = forks.iter().map(|f| f.start_step).collect();
        // Nothing precedes step 0, so a fork from there has nothing to carry. **TypeScript's
        // guard, not Python's**: TypeScript skips `startStep > 0` and Python skips `step > 1`,
        // and with steps_table numbered from zero the latter drops step 0 from every fork that resumes
        // at step 1 — the fork then re-runs a step it was given the result of.
        let copies_anything = start_steps.iter().any(|&step| step > 0);

        let queue_name = options.queue_name.unwrap_or(INTERNAL_QUEUE);
        // Reported rather than saturated, following `Timestamp::checked_add` and the rule stated
        // on `to_system_time`: a value this layer cannot store is the caller's to hear about, not
        // one to quietly replace with a different timeout.
        let timeout_ms = match options.timeout.map(|t| i64::try_from(t.as_millis())) {
            None => None,
            Some(Ok(ms)) => Some(ms),
            Some(Err(_)) => {
                return Err(Error::InvalidInput {
                    field: "timeout",
                    detail: "must fit in milliseconds as a 64-bit integer".to_owned(),
                });
            }
        };
        let (replace_from, replace_to): (Vec<&str>, Vec<&str>) =
            options.replacement_children.iter().copied().unzip();

        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let events_table = &self.tables.workflow_events;
        let history_table = &self.tables.workflow_events_history;
        let streams_table = &self.tables.streams;
        let (workflow_table, steps_table, events_table, history_table, streams_table) = (
            workflow_table.as_str(),
            steps_table.as_str(),
            events_table.as_str(),
            history_table.as_str(),
            streams_table.as_str(),
        );
        let pool = &self.pool;
        let (source_ids, forked_ids, start_steps) = (&source_ids, &forked_ids, &start_steps);
        let (replace_from, replace_to) = (&replace_from, &replace_to);

        with_retry(&self.retry, "fork_workflows", move || async move {
            let mut tx = pool.begin().await?;

            // Every source must exist before anything is written. A batch that forked the
            // workflows it could find would leave a caller holding ids for forks that are not
            // there, with nothing to say which.
            let found: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {workflow_table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(source_ids)
            .fetch_all(&mut *tx)
            .await?;
            let missing: Vec<String> = source_ids
                .iter()
                .filter(|id| !found.iter().any(|f| f == *id))
                .map(|id| (*id).to_owned())
                .collect();
            if !missing.is_empty() {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: missing,
                });
            }

            // The fork inherits its source's identity and starts enqueued. `application_version`
            // falls back to the source's, matching Go and TypeScript: a fork stamped with no
            // version would be invisible to the recovery that scopes by it.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {workflow_table} (workflow_uuid, status, name, class_name, config_name, \
                    application_version, application_id, authenticated_user, authenticated_roles, \
                    assumed_role, inputs, serialization, request, queue_name, \
                    queue_partition_key, forked_from, attributes, workflow_timeout_ms) \
                 SELECT m.fork_id, 'ENQUEUED', w.name, w.class_name, w.config_name, \
                    COALESCE($4, w.application_version), w.application_id, w.authenticated_user, \
                    w.authenticated_roles, w.assumed_role, w.inputs, w.serialization, w.request, \
                    $5, $6, w.workflow_uuid, w.attributes, $7 \
                 FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                 JOIN {workflow_table} w ON w.workflow_uuid = m.source_id"
            )))
            .bind(source_ids)
            .bind(forked_ids)
            .bind(start_steps)
            .bind(options.application_version)
            .bind(queue_name)
            .bind(options.queue_partition_key)
            .bind(timeout_ms)
            .execute(&mut *tx)
            .await?;

            // What makes a source discoverable as a fork point afterwards.
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {workflow_table} SET was_forked_from = TRUE WHERE workflow_uuid = ANY($1)"
            )))
            .bind(source_ids)
            .execute(&mut *tx)
            .await?;

            if copies_anything {
                // The recorded steps_table, which are what the fork replays instead of running. The
                // `CASE` rewrites recorded children when the caller is forking a whole tree;
                // with no replacements it collapses to the original column.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {steps_table} (workflow_uuid, function_id, output, error, \
                        serialization, function_name, child_workflow_id, started_at_epoch_ms, \
                        completed_at_epoch_ms) \
                     SELECT m.fork_id, o.function_id, o.output, o.error, o.serialization, \
                        o.function_name, \
                        COALESCE(r.replacement, o.child_workflow_id), \
                        o.started_at_epoch_ms, o.completed_at_epoch_ms \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {steps_table} o \
                       ON o.workflow_uuid = m.source_id AND o.function_id < m.start_step \
                     LEFT JOIN unnest($4::text[], $5::text[]) AS r(original, replacement) \
                       ON r.original = o.child_workflow_id"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .bind(replace_from)
                .bind(replace_to)
                .execute(&mut *tx)
                .await?;

                // The per-step event history_table, bounded the same way.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {history_table} (workflow_uuid, function_id, key, value, serialization) \
                     SELECT m.fork_id, h.function_id, h.key, h.value, h.serialization \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {history_table} h \
                       ON h.workflow_uuid = m.source_id AND h.function_id < m.start_step"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;

                // The current value of each key, rebuilt from the history_table rather than copied
                // from the source's `workflow_events`. The source's current value may have been
                // set *after* the fork point, and a fork must not see the future.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {events_table} (workflow_uuid, key, value, serialization) \
                     SELECT fork_id, key, value, serialization FROM ( \
                       SELECT m.fork_id, h.key, h.value, h.serialization, \
                              row_number() OVER (PARTITION BY m.fork_id, h.key \
                                                 ORDER BY h.function_id DESC) AS rn \
                       FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                       JOIN {history_table} h \
                         ON h.workflow_uuid = m.source_id AND h.function_id < m.start_step \
                     ) latest WHERE rn = 1"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;

                // Stream entries written before the fork point, so a reader replaying the fork
                // sees the same stream the original had produced by then.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {streams_table} (workflow_uuid, function_id, key, value, \
                        serialization, \"offset\") \
                     SELECT m.fork_id, s.function_id, s.key, s.value, s.serialization, s.\"offset\" \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {streams_table} s \
                       ON s.workflow_uuid = m.source_id AND s.function_id < m.start_step"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            tracing::info!(
                count = forks.len(),
                queue_name,
                "forked workflows onto the queue"
            );
            Ok(forked_ids.clone())
        })
        .await
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    async fn check_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let pool = &self.pool;
        with_retry(&self.retry, "check_step", move || async move {
            let mut conn = pool.acquire().await?;
            self.check_step_on(&mut conn, workflow_id, step_id, step_name)
                .await
        })
        .await
    }

    async fn record_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error> {
        let pool = &self.pool;
        with_retry(&self.retry, "record_step", move || async move {
            let mut conn = pool.acquire().await?;
            self.record_step_on(
                &mut conn,
                workflow_id,
                step_id,
                step_name,
                outcome,
                serialization,
                timing,
            )
            .await
        })
        .await
    }

    async fn list_workflow_steps(
        &self,
        workflow_id: &str,
        load_output: bool,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<StepRecord>, Error> {
        let table = &self.tables.operation_outputs;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "list_workflow_steps", move || async move {
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            q.push(STEP_COLUMNS)
                .push(", ")
                .push(step_payloads(load_output))
                .push(" FROM ")
                .push(table)
                // `function_id` is the step order, so ordering by it replays the workflow.
                .push(" WHERE workflow_uuid = ")
                .push_bind(workflow_id)
                .push(" ORDER BY function_id");
            if let Some(limit) = limit {
                q.push(" LIMIT ").push_bind(limit);
            }
            if let Some(offset) = offset {
                q.push(" OFFSET ").push_bind(offset);
            }
            let rows = q.build().fetch_all(pool).await?;
            rows.iter().map(|r| step_from_row(r, workflow_id)).collect()
        })
        .await
    }

    async fn record_sleep(
        &self,
        workflow_id: &str,
        step_id: i32,
        duration: Duration,
    ) -> Result<Timestamp, Error> {
        let pool = &self.pool;
        // Fixed before the retry: the wake time is what a replay must agree on, and re-reading
        // the clock per attempt would push it further out each time.
        let started_at = Timestamp::now();
        let wake_at =
            Timestamp::from_epoch_ms(started_at.as_epoch_ms() + duration.as_millis() as i64);

        with_retry(&self.retry, "record_sleep", move || async move {
            // No transaction, unlike `set_event`: there is no separate write to orphan here —
            // the step record *is* the write, and the wake time is its output. The check-then-
            // record race is caught by the insert's `ON CONFLICT` below, and the insert and the
            // executor claim inside `record_step_on` are safe by their ordering.
            let mut conn = pool.acquire().await?;

            // A replay wakes at the *original* instant. Starting the clock again would make a
            // workflow that crashed fifty minutes into an hour sleep another full hour.
            if let Some(step) = self
                .check_step_on(&mut conn, workflow_id, step_id, SLEEP_STEP_NAME)
                .await?
            {
                tracing::debug!(workflow_id, step_id, "replaying sleep");
                return decode_wake_time(step.output.as_deref(), workflow_id, step_id);
            }
            tracing::debug!(
                workflow_id,
                step_id,
                duration_ms = duration.as_millis() as u64,
                "running sleep"
            );

            let recorded = wake_at.as_epoch_ms().to_string();
            let timing = StepTiming {
                started_at,
                // The wake time, which is in the future — so the step's recorded duration is the
                // sleep. Java does the same; nothing in execution or recovery reads the column.
                completed_at: wake_at,
            };
            match self
                .record_step_on(
                    &mut conn,
                    workflow_id,
                    step_id,
                    SLEEP_STEP_NAME,
                    Outcome::Output(Some(&recorded)),
                    Some(PORTABLE_JSON),
                    Some(timing),
                )
                .await
            {
                Ok(()) => Ok(wake_at),
                // A rival recorded the sleep between our check and our write. Its wake time is
                // the one every execution must agree on, so adopt it rather than returning ours.
                // Python swallows this and returns its own, which two runs would disagree about.
                Err(Error::StepAlreadyRecorded { .. }) => {
                    let step = self
                        .check_step_on(&mut conn, workflow_id, step_id, SLEEP_STEP_NAME)
                        .await?
                        .ok_or_else(|| {
                            Error::Malformed(
                                "sleep reported as recorded but cannot be read back".to_owned(),
                            )
                        })?;
                    decode_wake_time(step.output.as_deref(), workflow_id, step_id)
                }
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn set_event(
        &self,
        workflow_id: &str,
        step_id: i32,
        key: &str,
        value: &str,
        serialization: Option<&str>,
    ) -> Result<(), Error> {
        let events = &self.tables.workflow_events;
        let history = &self.tables.workflow_events_history;
        let (events, history, pool) = (events.as_str(), history.as_str(), &self.pool);
        // Fixed before the retry, like every other step token: a second attempt is recording the
        // same step, and a fresh clock reading would look like a rival execution.
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "set_event", move || async move {
            let mut tx = pool.begin().await?;

            // A replay that already published this key must not publish it again — and must not
            // be told it failed either.
            if self
                .check_step_on(&mut tx, workflow_id, step_id, SET_EVENT_STEP_NAME)
                .await?
                .is_some()
            {
                tracing::debug!(workflow_id, step_id, key, "replaying set_event");
                return Ok(());
            }
            tracing::debug!(workflow_id, step_id, key, "running set_event");

            // The current value, which is what a reader sees.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {events} (workflow_uuid, key, value, serialization) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (workflow_uuid, key) DO UPDATE \
                 SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
            )))
            .bind(workflow_id)
            .bind(key)
            .bind(value)
            .bind(serialization)
            .execute(&mut *tx)
            .await?;

            // The per-step history, which is what a fork copies forward.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {history} (workflow_uuid, function_id, key, value, serialization) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (workflow_uuid, key, function_id) DO UPDATE \
                 SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .bind(key)
            .bind(value)
            .bind(serialization)
            .execute(&mut *tx)
            .await?;

            self.record_step_on(
                &mut tx,
                workflow_id,
                step_id,
                SET_EVENT_STEP_NAME,
                Outcome::Output(None),
                None,
                Some(timing),
            )
            .await?;

            tx.commit().await?;
            Ok(())
        })
        .await
    }

    async fn get_all_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationRecord>, Error> {
        let table = &self.tables.notifications;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_notifications", move || async move {
            // `consumed` rather than a delete on receive, so this reports everything the workflow
            // was sent and not merely what is still waiting.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT message_uuid, topic, message, serialization, created_at_epoch_ms, \
                 consumed \
                 FROM {table} WHERE destination_uuid = $1 ORDER BY created_at_epoch_ms"
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(NotificationRecord {
                        message_uuid: row.try_get("message_uuid")?,
                        topic: row.try_get("topic")?,
                        message: row.try_get("message")?,
                        serialization: row.try_get("serialization")?,
                        created_at: Timestamp::from_epoch_ms(row.try_get("created_at_epoch_ms")?),
                        consumed: row.try_get("consumed")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn get_all_events(&self, workflow_id: &str) -> Result<Vec<EventRecord>, Error> {
        let table = &self.tables.workflow_events;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_events", move || async move {
            // Ordered by key, which the table does not do for us: its primary key is
            // `(workflow_uuid, key)`, so this is a range scan that happens to be sorted, but
            // saying so keeps the result stable if that ever changes.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT key, value, serialization FROM {table} \
                 WHERE workflow_uuid = $1 ORDER BY key"
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(EventRecord {
                        key: row.try_get("key")?,
                        value: row.try_get("value")?,
                        serialization: row.try_get("serialization")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn get_all_stream_entries(&self, workflow_id: &str) -> Result<Vec<StreamRecord>, Error> {
        let table = &self.tables.streams;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_stream_entries", move || async move {
            // `"offset"` is quoted because it is a reserved word, and ordering by it is what
            // makes the result a stream rather than a bag.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT key, \"offset\", value, serialization, function_id FROM {table} \
                 WHERE workflow_uuid = $1 ORDER BY key, \"offset\""
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(StreamRecord {
                        key: row.try_get("key")?,
                        offset: row.try_get("offset")?,
                        value: row.try_get("value")?,
                        serialization: row.try_get("serialization")?,
                        step_id: row.try_get("function_id")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn create_application_version(&self, version_name: &str) -> Result<(), Error> {
        let table = &self.tables.application_versions;
        // Generated outside the retry, like every other identity here. It matters less than
        // `owner_xid` does — the conflict is on `version_name`, so a retry with a fresh id is
        // still a no-op — but the rule is worth keeping uniform.
        let version_id = uuid::Uuid::new_v4().to_string();
        let (table, pool, version_id) = (table.as_str(), &self.pool, version_id.as_str());

        with_retry(
            &self.retry,
            "create_application_version",
            move || async move {
                // `DO NOTHING` on the name, not the id: launching the same version twice must
                // register it once, and must not disturb the timestamp that decides which
                // version is current.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {table} (version_id, version_name) VALUES ($1, $2) \
                     ON CONFLICT (version_name) DO NOTHING"
                )))
                .bind(version_id)
                .bind(version_name)
                .execute(pool)
                .await?;
                Ok(())
            },
        )
        .await
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>, Error> {
        let table = &self.tables.application_versions;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "list_application_versions",
            move || async move {
                let rows = sqlx::query(AssertSqlSafe(format!(
                    "SELECT {VERSION_COLUMNS} FROM {table} ORDER BY version_timestamp DESC"
                )))
                .fetch_all(pool)
                .await?;
                rows.iter().map(version_from_row).collect()
            },
        )
        .await
    }

    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>, Error> {
        let table = &self.tables.application_versions;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "get_latest_application_version",
            move || async move {
                let row = sqlx::query(AssertSqlSafe(format!(
                    "SELECT {VERSION_COLUMNS} FROM {table} \
                     ORDER BY version_timestamp DESC LIMIT 1"
                )))
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(version_from_row).transpose()
            },
        )
        .await
    }

    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        let table = &self.tables.application_versions;
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "update_application_version_timestamp",
            move || async move {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET version_timestamp = $2 WHERE version_name = $1"
                )))
                .bind(version_name)
                .bind(timestamp.as_epoch_ms())
                .execute(pool)
                .await?;
                Ok(())
            },
        )
        .await
    }

    async fn record_child_workflow(
        &self,
        parent_workflow_id: &str,
        child_workflow_id: &str,
        step_id: i32,
        step_name: &str,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        // Python fails loudly here rather than "silently wedging the parent on recovery": a
        // parent that replays and finds an empty child id has no workflow to attach to.
        if child_workflow_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "child_workflow_id",
                detail: "must not be empty".to_owned(),
            });
        }
        let table = &self.tables.operation_outputs;
        // Spans the launch only — the parent does not wait for the child here, so the step is
        // complete as soon as the child exists. Stamped only when the caller offered a start,
        // since half a pair measures nothing. Java passes both null here.
        let completed_at = started_at.map(|_| Timestamp::now());
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "record_child_workflow", move || async move {
            // Same `DO UPDATE`-to-itself trick as `record_step`, but the returned value
            // compared is the **child id**, not the completion time. A retry stamps a new clock
            // reading and would fail a timestamp comparison, while the child id it is trying to
            // record is by definition the same one. Python states the rule exactly: "Same child
            // means an idempotent db_retry; a different child means nondeterminism."
            let stored: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
                "INSERT INTO {table} (workflow_uuid, function_id, function_name, \
                 child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (workflow_uuid, function_id) DO UPDATE \
                 SET child_workflow_id = {table}.child_workflow_id \
                 RETURNING child_workflow_id"
            )))
            .bind(parent_workflow_id)
            .bind(step_id)
            .bind(step_name)
            .bind(child_workflow_id)
            .bind(started_at.map(Timestamp::as_epoch_ms))
            .bind(completed_at.map(Timestamp::as_epoch_ms))
            .fetch_optional(pool)
            .await?;

            if let Some(stored) = stored
                && stored.as_deref() != Some(child_workflow_id)
            {
                return Err(Error::StepAlreadyRecorded {
                    workflow_id: parent_workflow_id.to_owned(),
                    step_id,
                });
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::split_database;

    #[test]
    fn splits_a_url_into_its_maintenance_form_and_database() {
        assert_eq!(
            split_database("postgresql://u:p@host:5432/dbos_sys"),
            Some((
                "postgresql://u:p@host:5432/postgres".to_owned(),
                "dbos_sys".to_owned()
            )),
        );
    }

    /// Query parameters survive, since they often carry the TLS mode needed to connect at all.
    #[test]
    fn query_parameters_are_kept_on_the_maintenance_url() {
        assert_eq!(
            split_database("postgresql://h/db?sslmode=require"),
            Some((
                "postgresql://h/postgres?sslmode=require".to_owned(),
                "db".to_owned()
            )),
        );
    }

    /// Nothing named means nothing to create.
    #[test]
    fn a_url_without_a_database_has_nothing_to_split() {
        assert_eq!(split_database("postgresql://host:5432/"), None);
        assert_eq!(split_database("postgresql://host:5432"), None);
    }
}
