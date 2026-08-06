//! The PostgreSQL implementation of [`SystemDatabase`], which also serves CockroachDB.
//!
//! The pool lives here and is never exposed: the whole point of the trait is that callers
//! cannot depend on which driver is underneath.

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};

use super::migrations::quote_identifier;
use super::types::{Timestamp, WorkflowRecord, WorkflowStatus, duration_from_ms};
use super::{
    DEFAULT_SCHEMA, Error, InitWorkflowStatus, OutcomeWrite, SystemDatabase, WorkflowInitResult,
    runner,
};

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Backend(e.to_string())
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
}

impl Config {
    /// A configuration with the defaults every implementation shares.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            schema: DEFAULT_SCHEMA.to_owned(),
            max_connections: 10,
            use_listen_notify: true,
        }
    }
}

/// A system database backed by PostgreSQL or CockroachDB.
pub struct PostgresSystemDatabase {
    pool: PgPool,
    schema: String,
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
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            pool,
            schema: config.schema.clone(),
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
        }
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
        was_forked_from: row.try_get::<Option<bool>, _>("was_forked_from")?.unwrap_or(false),

        owner_xid: row.try_get("owner_xid")?,
        application_id: row.try_get("application_id")?,
        authenticated_user: row.try_get("authenticated_user")?,
        authenticated_roles: row.try_get("authenticated_roles")?,
        assumed_role: row.try_get("assumed_role")?,
        request: row.try_get("request")?,

        deduplication_id: row.try_get("deduplication_id")?,
        priority: row.try_get("priority")?,
        queue_partition_key: row.try_get("queue_partition_key")?,
        rate_limited: row.try_get::<Option<bool>, _>("rate_limited")?.unwrap_or(false),
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
        is_debounced: row.try_get::<Option<bool>, _>("is_debounced")?.unwrap_or(false),

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

#[async_trait]
impl SystemDatabase for PostgresSystemDatabase {
    async fn init_workflow_status(
        &self,
        input: InitWorkflowStatus<'_>,
    ) -> Result<WorkflowInitResult, Error> {
        let record = input.record;
        let table = self.table("workflow_status");
        // Queued workflows are not running, so neither the attempt counter nor the executor
        // stamp applies to them — both `CASE` expressions below turn on that distinction.
        let queued = matches!(
            record.status,
            WorkflowStatus::Enqueued | WorkflowStatus::Delayed
        );
        let initial_attempts = i64::from(!queued);
        // A recovery or a dequeue is being told it owns this workflow; a fresh start is not.
        let claiming = input.is_recovery || input.is_dequeue;
        let increment = i64::from(claiming && !queued);
        // Generated here when the caller supplies none, so the row always has an owner to
        // compare against. A caller that retries must pass the same one both times.
        let generated;
        let owner_xid = match input.owner_xid {
            Some(x) => x,
            None => {
                generated = uuid::Uuid::new_v4().to_string();
                &generated
            }
        };

        // A direct port of the other implementations' upsert, including the column order of
        // the RETURNING list, so the three can be compared line by line.
        let row = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} (workflow_uuid, status, name, class_name, config_name, inputs, \
             serialization, executor_id, application_version, queue_name, owner_xid, \
             created_at, updated_at, recovery_attempts) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
             ON CONFLICT (workflow_uuid) DO UPDATE SET \
               recovery_attempts = CASE \
                   WHEN {table}.status != 'ENQUEUED' AND {table}.status != 'DELAYED' \
                   THEN {table}.recovery_attempts + $15 \
                   ELSE {table}.recovery_attempts \
               END, \
               updated_at = EXCLUDED.updated_at, \
               executor_id = CASE \
                   WHEN EXCLUDED.status != 'ENQUEUED' AND EXCLUDED.status != 'DELAYED' \
                   THEN EXCLUDED.executor_id \
                   ELSE {table}.executor_id \
               END \
             RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, \
             owner_xid, serialization"
        )))
        .bind(&record.workflow_id)
        .bind(record.status.as_str())
        .bind(&record.name)
        .bind(&record.class_name)
        .bind(&record.config_name)
        .bind(&record.input)
        .bind(&record.serialization)
        .bind(&record.executor_id)
        .bind(&record.application_version)
        .bind(&record.queue_name)
        .bind(owner_xid)
        .bind(record.created_at.as_epoch_ms())
        .bind(record.updated_at.as_epoch_ms())
        .bind(initial_attempts)
        .bind(increment)
        .fetch_one(&self.pool)
        .await?;

        let recovery_attempts: i64 = row.try_get("recovery_attempts")?;
        let status_text: String = row.try_get("status")?;
        let status = WorkflowStatus::parse(&status_text)
            .ok_or_else(|| Error::Malformed(format!("unknown workflow status {status_text:?}")))?;
        let stored_owner: Option<String> = row.try_get("owner_xid")?;
        let serialization: Option<String> = row.try_get("serialization")?;

        // Same id, different function: a programming error rather than a race, because the id
        // is how every implementation decides two attempts are the same workflow.
        for (field, stored, offered) in [
            (
                "function name",
                row.try_get::<Option<String>, _>("name")?,
                &record.name,
            ),
            ("class name", row.try_get("class_name")?, &record.class_name),
            (
                "config name",
                row.try_get("config_name")?,
                &record.config_name,
            ),
        ] {
            if stored.as_deref() != offered.as_deref() {
                return Err(Error::ConflictingWorkflow {
                    workflow_id: record.workflow_id.clone(),
                    detail: format!("existing {field} is {stored:?}, but {offered:?} was provided"),
                });
            }
        }
        // A differing queue is only a warning: requeueing the same workflow elsewhere is
        // legitimate, and the stored queue wins.
        let stored_queue: Option<String> = row.try_get("queue_name")?;
        if stored_queue.as_deref() != record.queue_name.as_deref() {
            tracing::warn!(
                workflow_id = %record.workflow_id,
                stored = ?stored_queue,
                provided = ?record.queue_name,
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
            .bind(&record.workflow_id)
            .bind(WorkflowStatus::MaxRecoveryAttemptsExceeded.as_str())
            .execute(&self.pool)
            .await?;

            return Err(Error::MaxRecoveryAttemptsExceeded {
                workflow_id: record.workflow_id.clone(),
                limit,
            });
        }

        Ok(WorkflowInitResult {
            status,
            recovery_attempts,
            serialization,
            // Another owner holds the row and this is not a recovery, so recording it is right
            // but running it would be a second execution.
            should_execute: !(owner_differs && !claiming && stored_owner.is_some()),
        })
    }

    async fn get_workflow_status(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowRecord>, Error> {
        let table = self.table("workflow_status");
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {RECORD_COLUMNS} FROM {table} WHERE workflow_uuid = $1"
        )))
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(record_from_row).transpose()
    }

    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<OutcomeWrite, Error> {
        let table = self.table("workflow_status");
        // The `status = 'PENDING'` predicate is the whole mechanism: an executor that has been
        // presumed dead and superseded finds zero rows updated, and learns it lost rather than
        // clobbering the winner's result.
        let updated = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {table} SET status = $2, output = $3, error = $4, \
             updated_at = $5, completed_at = $5 \
             WHERE workflow_uuid = $1 AND status = 'PENDING'"
        )))
        .bind(workflow_id)
        .bind(status.as_str())
        .bind(output)
        .bind(error)
        .bind(Timestamp::now().as_epoch_ms())
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(if updated > 0 {
            OutcomeWrite::Recorded
        } else {
            OutcomeWrite::AlreadyFinished
        })
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
