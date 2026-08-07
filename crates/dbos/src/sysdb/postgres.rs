//! The PostgreSQL implementation of [`SystemDatabase`], which also serves CockroachDB.
//!
//! The pool lives here and is never exposed: the whole point of the trait is that callers
//! cannot depend on which driver is underneath.

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};

use super::migrations::quote_identifier;
use super::retry::{RetryPolicy, with_retry};
use super::types::{Timestamp, WorkflowFilter, WorkflowRecord, WorkflowStatus, duration_from_ms};
use super::{
    BackendError, BackendErrorKind, DEFAULT_SCHEMA, Error, INTERNAL_QUEUE, InitWorkflowStatus,
    OutcomeWrite, SystemDatabase, WorkflowInitResult, runner,
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
fn empty_to_none(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|v| !v.is_empty())
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
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => BackendErrorKind::Connection,
        _ => BackendErrorKind::Permanent,
    }
}

/// How to reach and set up the system database.
#[derive(Debug, Clone)]
pub struct Config {
    /// Connection URL for the system database.
    pub url: String,
    /// Schema holding the DBOS tables. Defaults to [`DEFAULT_SCHEMA`].
    pub schema: String,
    /// Maximum pooled connections.
    pub max_connections: u32,
    /// Whether the schema uses LISTEN/NOTIFY triggers.
    ///
    /// Recorded in the schema itself, so it must match what other applications on the same
    /// database expect.
    pub use_listen_notify: bool,
    /// How failures that may pass are waited out.
    pub retry: RetryPolicy,
}

impl Config {
    /// A configuration with the defaults every implementation shares.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            schema: DEFAULT_SCHEMA.to_owned(),
            max_connections: 10,
            use_listen_notify: true,
            retry: RetryPolicy::default(),
        }
    }
}

/// A system database backed by PostgreSQL or CockroachDB.
pub struct PostgresSystemDatabase {
    pool: PgPool,
    schema: String,
    retry: RetryPolicy,
}

impl PostgresSystemDatabase {
    /// Connects, creating the database if it does not exist, and migrates it.
    ///
    /// Creating the database is part of connecting rather than part of migrating: you cannot
    /// migrate a database you cannot connect to. Every other DBOS implementation does the same,
    /// so pointing a fresh application at an empty server is expected to work.
    pub async fn connect(config: &Config) -> Result<Self, Error> {
        ensure_database_exists(&config.url).await?;

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

        runner::run(&pool, &config.schema, config.use_listen_notify)
            .await
            // The runner has already retried what it could; whatever reaches here is settled.
            .map_err(|e| {
                Error::Backend(BackendError {
                    message: e.to_string(),
                    sqlstate: None,
                    kind: BackendErrorKind::Permanent,
                })
            })?;

        Ok(Self {
            pool,
            schema: config.schema.clone(),
            retry: config.retry,
        })
    }

    /// Wraps an existing pool, assuming the schema is already migrated.
    ///
    /// For callers that manage their own connections — and for tests, which migrate through the
    /// harness.
    pub fn from_pool(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            retry: RetryPolicy::default(),
        }
    }

    /// Replaces the retry policy, which otherwise comes from the configuration.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Closes the pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    fn table(&self, name: &str) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(name)
        )
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

fn record_from_row(row: &sqlx::postgres::PgRow) -> Result<WorkflowRecord, Error> {
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
        authenticated_roles: row.try_get("authenticated_roles")?,
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

/// Every column `record_from_row` reads, in one place so the queries cannot drift from it.
const RECORD_COLUMNS: &str = "workflow_uuid, status, name, class_name, config_name, inputs, \
     output, error, serialization, executor_id, application_version, recovery_attempts, \
     queue_name, created_at, updated_at, started_at_epoch_ms, completed_at, forked_from, \
     parent_workflow_id, was_forked_from, owner_xid, application_id, authenticated_user, \
     authenticated_roles, assumed_role, request, deduplication_id, priority, \
     queue_partition_key, rate_limited, schedule_name, workflow_timeout_ms, \
     workflow_deadline_epoch_ms, delay_until_epoch_ms, debounce_deadline_epoch_ms, \
     is_debounced, attributes::text AS attributes";

impl PostgresSystemDatabase {
    /// Cancels one batch, returning the ids that were still running.
    ///
    /// Not retried in here, and not reading its own clock. Both belong to the trait method that
    /// owns the whole operation: a retry that restarted only this statement would leave the
    /// cascade around it half-walked, and a per-level clock would give one cancellation as many
    /// `completed_at` values as the tree has depth.
    ///
    /// The terminal-status guard is what makes cancellation safe to repeat: a workflow that has
    /// already succeeded keeps its result rather than being overwritten with `CANCELLED`.
    async fn cancel_batch(&self, workflow_ids: &[String], now: i64) -> Result<Vec<String>, Error> {
        let table = self.table("workflow_status");
        let cancelled: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {table} SET status = 'CANCELLED', queue_name = NULL, \
             deduplication_id = NULL, started_at_epoch_ms = NULL, \
             updated_at = $2, completed_at = $2 \
             WHERE workflow_uuid = ANY($1) AND status NOT IN ('SUCCESS', 'ERROR') \
             RETURNING workflow_uuid"
        )))
        .bind(workflow_ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(cancelled)
    }

    /// The workflows whose parent is one of these.
    async fn direct_children(&self, workflow_ids: &[String]) -> Result<Vec<String>, Error> {
        let table = self.table("workflow_status");
        let children: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT workflow_uuid FROM {table} WHERE parent_workflow_id = ANY($1)"
        )))
        .bind(workflow_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(children)
    }
}

#[async_trait]
impl SystemDatabase for PostgresSystemDatabase {
    async fn init_workflow_status(
        &self,
        input: InitWorkflowStatus<'_>,
    ) -> Result<WorkflowInitResult, Error> {
        let wf = input.workflow;
        wf.validate()?;
        let table = self.table("workflow_status");
        // Derived, never supplied: a caller cannot enqueue a workflow and label it SUCCESS.
        let initial_status = wf.initial_status();
        // Queued workflows are not running, so neither the attempt counter nor the executor
        // stamp applies to them — both `CASE` expressions below turn on that distinction.
        let queued = matches!(
            initial_status,
            WorkflowStatus::Enqueued | WorkflowStatus::Delayed
        );
        let initial_attempts = i64::from(!queued);
        // A recovery or a dequeue is being told it owns this workflow; a fresh start is not.
        let claiming = input.is_recovery || input.is_dequeue;
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
        let delay_until = wf
            .delay
            .map(|d| Timestamp::from_epoch_ms(now.as_epoch_ms() + d.as_millis() as i64));

        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (table, pool, owner_xid) = (table.as_str(), &self.pool, owner_xid.as_str());

        with_retry(&self.retry, "init_workflow_status", move || async move {
            // The column list is Java's INSERT, in its order, plus Python's two debounce columns.
            // Columns absent from it are absent deliberately: `output`, `error`, `started_at`,
            // `completed_at`, `forked_from`, `was_forked_from`, and `rate_limited` are written by
            // execution, forking, and the rate limiter — never at creation.
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
                       WHEN EXCLUDED.status != 'ENQUEUED' AND EXCLUDED.status != 'DELAYED' \
                       THEN EXCLUDED.executor_id \
                       ELSE {table}.executor_id \
                   END \
                 RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, \
                 workflow_deadline_epoch_ms, owner_xid, serialization"
            )))
            .bind(&wf.workflow_id)
            .bind(initial_status.as_str())
            .bind(&wf.input)
            .bind(&wf.name)
            .bind(&wf.class_name)
            .bind(&wf.config_name)
            .bind(&wf.queue_name)
            .bind(&wf.deduplication_id)
            .bind(wf.priority)
            .bind(&wf.queue_partition_key)
            .bind(delay_until.map(Timestamp::as_epoch_ms))
            // TypeScript and Go send `""` rather than null when there is no auth context, and
            // Java normalises it on the way in for that reason. Storing both spellings would
            // make an `authenticated_user IS NULL` filter miss rows another SDK wrote.
            .bind(empty_to_none(&wf.authenticated_user))
            .bind(empty_to_none(&wf.assumed_role))
            .bind(&wf.authenticated_roles)
            .bind(&wf.executor_id)
            .bind(&wf.application_version)
            .bind(&wf.application_id)
            .bind(now.as_epoch_ms())
            .bind(now.as_epoch_ms())
            .bind(initial_attempts)
            .bind(wf.timeout.map(|d| d.as_millis() as i64))
            .bind(wf.deadline.map(Timestamp::as_epoch_ms))
            .bind(&wf.parent_workflow_id)
            .bind(owner_xid)
            .bind(&wf.serialization)
            .bind(&wf.attributes)
            .bind(&wf.schedule_name)
            .bind(wf.debounce_deadline.map(Timestamp::as_epoch_ms))
            .bind(wf.is_debounced)
            .bind(increment)
            .fetch_one(pool)
            .await?;

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
                    &wf.name,
                ),
                ("class name", row.try_get("class_name")?, &wf.class_name),
                ("config name", row.try_get("config_name")?, &wf.config_name),
            ] {
                if stored.as_deref() != offered.as_deref() {
                    return Err(Error::ConflictingWorkflow {
                        workflow_id: wf.workflow_id.clone(),
                        detail: format!(
                            "existing {field} is {stored:?}, but {offered:?} was provided"
                        ),
                    });
                }
            }
            // A differing queue is only a warning: requeueing the same workflow elsewhere is
            // legitimate, and the stored queue wins.
            let stored_queue: Option<String> = row.try_get("queue_name")?;
            if stored_queue.as_deref() != wf.queue_name.as_deref() {
                tracing::warn!(
                    workflow_id = %wf.workflow_id,
                    stored = ?stored_queue,
                    provided = ?wf.queue_name,
                    "workflow already exists on a different queue; the stored queue is kept"
                );
            }

            // Parked once it has been recovered more often than allowed — but only if some *other*
            // attempt is responsible, so a caller retrying its own attempt is not punished for it.
            let owner_differs = stored_owner.as_deref() != Some(owner_xid);
            if let Some(limit) = input.max_recovery_attempts
                && !status.is_terminal()
                && recovery_attempts > limit + 1
                && owner_differs
            {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET status = $2, deduplication_id = NULL, \
                     started_at_epoch_ms = NULL, queue_name = NULL \
                     WHERE workflow_uuid = $1 AND status = 'PENDING'"
                )))
                .bind(&wf.workflow_id)
                .bind(WorkflowStatus::MaxRecoveryAttemptsExceeded.as_str())
                .execute(pool)
                .await?;

                return Err(Error::MaxRecoveryAttemptsExceeded {
                    workflow_id: wf.workflow_id.clone(),
                    limit,
                });
            }

            Ok(WorkflowInitResult {
                status,
                recovery_attempts,
                deadline: deadline.map(Timestamp::from_epoch_ms),
                serialization,
                // Another owner holds the row and this is not a recovery, so recording it is right
                // but running it would be a second execution.
                should_execute: !(owner_differs && !claiming && stored_owner.is_some()),
            })
        })
        .await
    }

    async fn get_workflow_status(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowRecord>, Error> {
        let table = self.table("workflow_status");
        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "get_workflow_status", move || async move {
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT {RECORD_COLUMNS} FROM {table} WHERE workflow_uuid = $1"
            )))
            .bind(workflow_id)
            .fetch_optional(pool)
            .await?;

            row.as_ref().map(record_from_row).transpose()
        })
        .await
    }

    async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowRecord>, Error> {
        let table = self.table("workflow_status");
        // `status` is the one filter whose values are not already strings.
        let status: Vec<&str> = filter
            .status
            .iter()
            .copied()
            .map(WorkflowStatus::as_str)
            .collect();
        let (table, pool, status) = (table.as_str(), &self.pool, status.as_slice());

        with_retry(&self.retry, "list_workflows", move || async move {
            // Rebuilt per attempt rather than hoisted: a `QueryBuilder` owns its arguments and
            // is consumed by `build`, and rendering a few hundred bytes of SQL is not the cost
            // worth optimising against a retry that has already waited a second.
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            // One row reader for every query means the column list cannot vary, so the columns
            // the caller declined are selected as NULL rather than dropped. The cast is needed:
            // a bare NULL has no type, and the driver has to be told what it is decoding.
            q.push(match (filter.load_input, filter.load_output) {
                (true, true) => RECORD_COLUMNS.to_owned(),
                (load_input, load_output) => {
                    let mut columns = RECORD_COLUMNS.to_owned();
                    if !load_input {
                        columns = columns.replace("inputs,", "NULL::text AS inputs,");
                    }
                    if !load_output {
                        columns = columns
                            .replace("output,", "NULL::text AS output,")
                            .replace("error,", "NULL::text AS error,");
                    }
                    columns
                }
            });
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

            if !filter.workflow_id_prefixes.is_empty() {
                // `LIKE ANY(...)` needs the wildcard appended to each pattern, and `%` and `_`
                // in a caller's prefix would otherwise be wildcards themselves.
                let patterns: Vec<String> = filter
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
                clause(&mut q, "workflow_uuid LIKE ANY(");
                q.push_bind(patterns).push(")");
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
            rows.iter().map(record_from_row).collect()
        })
        .await
    }

    async fn cancel_workflows(
        &self,
        workflow_ids: &[String],
        cancel_children: bool,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        // The retry wraps the whole cascade, not each statement inside it. A failure partway
        // through leaves some of the tree cancelled, and restarting from the roots finishes the
        // job — where retrying one statement would return a half-walked tree as a success.
        //
        // Every accumulator is therefore declared *inside*, and must be: a retry that appended
        // to a `cancelled` list from the previous attempt would report workflows twice.
        // Re-cancelling is otherwise harmless, since `CANCELLED` is not a terminal status the
        // guard excludes and the row simply stays cancelled.
        //
        // `now` is read once for the whole cascade, so every workflow cancelled by one call
        // shares a `completed_at` however deep the tree goes.
        let now = Timestamp::now().as_epoch_ms();
        with_retry(&self.retry, "cancel_workflows", move || async move {
            let mut cancelled = Vec::new();
            let mut seen: std::collections::HashSet<String> =
                workflow_ids.iter().cloned().collect();
            let mut frontier: Vec<String> = workflow_ids.to_vec();

            // One statement per *level*, not per workflow: `cancel_batch` takes the whole
            // frontier and matches it with `= ANY($1)`, so the round trips scale with the depth
            // of the tree rather than its size. Cancelling the level before asking for its
            // children is the ordering that stops a parent spawning behind the walk. The loop
            // terminates because `seen` only grows and a workflow enters a frontier at most once.
            loop {
                cancelled.extend(self.cancel_batch(&frontier, now).await?);
                if !cancel_children {
                    break;
                }
                let children = self.direct_children(&frontier).await?;
                frontier = children
                    .into_iter()
                    .filter(|c| seen.insert(c.clone()))
                    .collect();
                if frontier.is_empty() {
                    break;
                }
            }
            Ok(cancelled)
        })
        .await
    }

    async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }
        let table = self.table("workflow_status");
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
                .filter(|id| !existing.contains(id))
                .cloned()
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
            Ok(resumed)
        })
        .await
    }

    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
    ) -> Result<(), Error> {
        let table = self.table("workflow_status");
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

    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<OutcomeWrite, Error> {
        let table = self.table("workflow_status");
        // Stamped once, outside the retry: a retried attempt is recording the outcome it
        // already had, and re-reading the clock would move `completed_at` forward each time.
        let now = Timestamp::now().as_epoch_ms();
        let (table, pool) = (table.as_str(), &self.pool);

        with_retry(&self.retry, "update_workflow_outcome", move || async move {
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
            .bind(status.as_str())
            .bind(output)
            .bind(error)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();

            Ok(if updated > 0 {
                OutcomeWrite::Recorded
            } else {
                OutcomeWrite::AlreadyFinished
            })
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
