//! Applies the migration corpus to a database and records how far it got.
//!
//! The recorded version lives in one row of `<schema>.dbos_migrations` and is shared with
//! every other DBOS implementation pointed at the same database, so the rules about what
//! counts as applied are a compatibility contract rather than an internal choice.

use sqlx::{AssertSqlSafe, PgPool, Row};

use super::{Dialect, Migration, RenderError, build_migrations, quote_identifier};

/// How many times a migration is retried before giving up.
///
/// Retrying is what replaces the advisory lock that PostgreSQL has and CockroachDB does not.
/// Concurrent cold starts collide on DDL; the loser re-reads the recorded version and, if a
/// peer has moved past the migration it failed on, carries on from there.
const MAX_ATTEMPTS: u32 = 3;

/// How long to wait for a peer to finish a migration this process collided with.
///
/// Sized for CockroachDB, where schema changes are applied online and one migration can take
/// seconds — migration 1 creates six tables. A short fixed backoff is the wrong shape: the
/// question is not whether a moment has passed but whether the other process has committed.
const PEER_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// How often to re-read the recorded version while waiting for a peer.
const PEER_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// How long to wait when an error does not look like a collision.
///
/// Not zero, because the classification below is a heuristic and has been wrong twice: a brief
/// pause lets an unrecognised collision resolve itself, while keeping a genuinely broken
/// migration to a few seconds rather than the full peer timeout on every attempt.
const UNKNOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Whether an error is PostgreSQL objecting that something already exists.
///
/// **`CREATE ... IF NOT EXISTS` is not atomic.** Two connections can both find the object
/// absent and both try to create it; one then fails on a catalog unique index. The
/// postcondition still holds — the object exists — so these are success, not failure.
/// PostgreSQL's "that object already exists" codes.
///
/// Two migrators racing produce one of these on whichever loses: `CREATE ... IF NOT EXISTS` is
/// not atomic, and `ALTER TABLE ADD COLUMN` has no `IF NOT EXISTS` on older servers at all. The
/// list is the class-42 duplicate family plus the catalogue unique violation a schema race
/// surfaces as.
const ALREADY_EXISTS: &[&str] = &[
    "23505", // unique_violation — the pg_namespace/pg_class catalogue race
    "42701", // duplicate_column
    "42710", // duplicate_object
    "42723", // duplicate_function
    "42P04", // duplicate_database
    "42P06", // duplicate_schema
    "42P07", // duplicate_table
];

/// Transient failures that resolve on their own.
const TRANSIENT: &[&str] = &[
    "40P01", // deadlock_detected — two concurrent CREATE INDEX CONCURRENTLY runs
    "40001", // serialization_failure
    "55P03", // lock_not_available
];

fn has_code(error: &sqlx::Error, codes: &[&str]) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|c| codes.contains(&c.as_ref()))
}

/// Whether an error could plausibly be another process migrating at the same time.
///
/// Waiting only helps if someone else is going to finish the work. Broken SQL never becomes
/// valid, so a syntax error or a missing function fails immediately rather than sitting out the
/// peer timeout on every attempt — which turned a bad migration into a 30-second stall.
///
/// **The cost of being wrong is asymmetric, and not in the obvious direction.** Omitting a code
/// that *is* a collision turns a routine race into a failed start, which is how `42701` was
/// found. Including one that is not merely delays an error that was going to happen anyway. So
/// this errs towards waiting.
fn is_possibly_concurrent(error: &sqlx::Error) -> bool {
    if has_code(error, ALREADY_EXISTS) || has_code(error, TRANSIENT) {
        return true;
    }
    // Not everything concurrent has a code of its own. Two migrators altering the same
    // `pg_proc` row — migration 20 hardening `search_path` — collide as `XX000`, PostgreSQL's
    // catch-all, with the only signal in the text.
    error
        .as_database_error()
        .is_some_and(|e| e.message().contains("concurrently updated"))
}

/// Whether an error is PostgreSQL objecting that something already exists.
///
/// **`CREATE ... IF NOT EXISTS` is not atomic.** Two connections can both find the object
/// absent and both try to create it; one then fails on a catalogue unique index. The
/// postcondition still holds — the object exists — so these are success, not failure.
fn is_already_exists(error: &sqlx::Error) -> bool {
    has_code(error, ALREADY_EXISTS)
}

/// Why migrating failed.
#[derive(Debug)]
pub enum MigrateError {
    /// A query outside any particular migration failed — reading the version, creating the
    /// schema, and so on.
    Database(sqlx::Error),
    /// A migration's placeholders could not be filled in.
    Render(RenderError),
    /// A migration failed, and retrying did not help.
    Migration {
        /// The migration that failed.
        version: u32,
        /// The last error it produced.
        source: sqlx::Error,
    },
}

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrateError::Database(e) => write!(f, "system database error: {e}"),
            MigrateError::Render(e) => write!(f, "could not render a migration: {e}"),
            MigrateError::Migration { version, source } => {
                write!(f, "migration {version} failed: {source}")
            }
        }
    }
}

impl std::error::Error for MigrateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MigrateError::Database(e) | MigrateError::Migration { source: e, .. } => Some(e),
            MigrateError::Render(e) => Some(e),
        }
    }
}

impl From<sqlx::Error> for MigrateError {
    fn from(e: sqlx::Error) -> Self {
        MigrateError::Database(e)
    }
}

impl From<RenderError> for MigrateError {
    fn from(e: RenderError) -> Self {
        MigrateError::Render(e)
    }
}

/// What a call to [`run`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The recorded version on entry; 0 if the database had never been migrated.
    pub from_version: i64,
    /// The recorded version on exit.
    pub to_version: i64,
    /// Migrations that executed statements. Empty ones are not listed, having done nothing,
    /// though their numbers are still consumed by `to_version`.
    pub applied: Vec<u32>,
    /// Whether the database was already at or beyond the latest migration this build knows.
    pub was_current: bool,
}

/// Detects CockroachDB, which reports itself in `version()`.
///
/// Asking the server beats trusting configuration: the dialect changes which statements are
/// even valid, and getting it from a connection string is how you end up running PostgreSQL
/// DDL against CockroachDB.
pub async fn detect_dialect(pool: &PgPool) -> Result<Dialect, MigrateError> {
    let version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;
    Ok(if version.to_lowercase().contains("cockroachdb") {
        Dialect::Cockroach
    } else {
        Dialect::Postgres
    })
}

/// Creates the schema and the version table if they are absent.
async fn ensure_schema(pool: &PgPool, schema: &str) -> Result<(), MigrateError> {
    let quoted = quote_identifier(schema);
    for sql in [
        format!("CREATE SCHEMA IF NOT EXISTS {quoted}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {quoted}.dbos_migrations \
             (version BIGINT NOT NULL PRIMARY KEY)"
        ),
    ] {
        match sqlx::raw_sql(AssertSqlSafe(sql)).execute(pool).await {
            Ok(_) => {}
            // A peer created it between our check and ours — which is the outcome we wanted.
            Err(e) if is_already_exists(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Reads the recorded version, or 0 if nothing has been recorded.
async fn recorded_version(pool: &PgPool, schema: &str) -> Result<i64, MigrateError> {
    let quoted = quote_identifier(schema);
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT version FROM {quoted}.dbos_migrations"
    )))
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.get::<i64, _>(0)).unwrap_or(0))
}

/// Writes the version, inserting the single row on first use.
async fn record_version(
    pool: &PgPool,
    schema: &str,
    version: i64,
    had_row: bool,
) -> Result<(), MigrateError> {
    let quoted = quote_identifier(schema);
    let sql = if had_row {
        format!("UPDATE {quoted}.dbos_migrations SET version = {version}")
    } else {
        format!("INSERT INTO {quoted}.dbos_migrations (version) VALUES ({version})")
    };
    sqlx::raw_sql(AssertSqlSafe(sql)).execute(pool).await?;
    Ok(())
}

/// Drops indexes a previous interrupted `CREATE INDEX CONCURRENTLY` left behind.
///
/// PostgreSQL leaves the index in place and marks it invalid when a concurrent build fails. It
/// then serves no reads while still costing write overhead — and, the reason this runs before
/// every online migration, it holds the name, so re-running `CREATE INDEX IF NOT EXISTS` does
/// not recover. CockroachDB has no such state.
async fn drop_invalid_indexes(pool: &PgPool, schema: &str) -> Result<(), sqlx::Error> {
    // Joined through the *table's* namespace rather than the index's. The two are always the
    // same in PostgreSQL, but this is the form the other implementations use, so a future
    // comparison against them is a plain diff.
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT i.relname FROM pg_index ix \
         JOIN pg_class i ON i.oid = ix.indexrelid \
         JOIN pg_class t ON t.oid = ix.indrelid \
         JOIN pg_namespace n ON n.oid = t.relnamespace \
         WHERE NOT ix.indisvalid AND n.nspname = $1",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    let quoted = quote_identifier(schema);
    for name in names {
        tracing::warn!(
            index = %name,
            "dropping invalid index left by an interrupted migration"
        );
        let index = quote_identifier(&name);
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {quoted}.{index}"
        )))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Applies every migration the database has not yet recorded.
///
/// Safe to call on every start: it returns without touching anything once the database is
/// current, which is the common case.
///
/// Several rules here exist for compatibility rather than convenience:
///
/// - **Empty migrations still consume their version number.** A migration that does nothing on
///   this dialect, or that exists only to pad the numbering, is skipped without a round trip —
///   but the version still advances past it, so a database that has run everything records the
///   full count. Stopping at the last migration with statements would leave the version behind
///   what other implementations record for the same schema.
/// - **A database ahead of this build is left alone.** Another implementation may know
///   migrations this one does not. Proceeding would be wrong and refusing to start would be
///   worse, so it logs and continues.
/// - **Whole files are executed as one statement batch.** Splitting on `;` would shred the
///   `$$`-quoted function bodies.
/// - **A migration and its version write commit together** wherever a transaction is possible,
///   which is everything except the online ones. That is required rather than tidy for
///   migrations in the shared series: every implementation runs those against databases they
///   share, and migration 105 drops and recreates `enqueue_workflow`, so a peer must never
///   find the function missing in between.
pub async fn run(
    pool: &PgPool,
    schema: &str,
    use_listen_notify: bool,
) -> Result<Outcome, MigrateError> {
    let dialect = detect_dialect(pool).await?;
    let migrations = build_migrations(schema, dialect, use_listen_notify);
    apply(pool, schema, &migrations).await
}

/// Applies a prepared migration list, which is what [`run`] does once it has built one.
///
/// Separate so a caller can supply a list [`build_migrations`] would not produce — a shortened
/// one, or one with a deliberately broken entry. Tests use it to prove the runner's failure
/// handling; nothing in production should need it.
pub async fn apply(
    pool: &PgPool,
    schema: &str,
    migrations: &[Migration],
) -> Result<Outcome, MigrateError> {
    ensure_schema(pool, schema).await?;

    let latest = migrations.len() as i64;
    let from_version = recorded_version(pool, schema).await?;
    let mut had_row = from_version > 0;

    if from_version > latest {
        tracing::info!(
            recorded = from_version,
            known = latest,
            "system database is ahead of this build; another implementation has migrated it \
             further. Proceeding without changes."
        );
        return Ok(Outcome {
            from_version,
            to_version: from_version,
            applied: Vec::new(),
            was_current: true,
        });
    }
    if from_version == latest {
        return Ok(Outcome {
            from_version,
            to_version: from_version,
            applied: Vec::new(),
            was_current: true,
        });
    }

    let mut last_applied = from_version;
    let mut applied = Vec::new();

    for migration in migrations {
        let version = i64::from(migration.version);
        if version <= last_applied {
            continue;
        }
        // Nothing to do, but the number is still consumed — the trailing write below records
        // it, so a run of empties costs no round trips at all.
        if migration.sql.trim().is_empty() {
            continue;
        }

        apply_one(pool, schema, migration, &mut last_applied, &mut had_row).await?;
        applied.push(migration.version);
    }

    // Empty migrations at the end still count as applied.
    if latest > last_applied {
        record_version(pool, schema, latest, had_row).await?;
        last_applied = latest;
    }

    Ok(Outcome {
        from_version,
        to_version: last_applied,
        applied,
        was_current: false,
    })
}

/// Applies one migration, retrying if a peer is migrating concurrently.
///
/// Every step of an attempt is inside the retryable region, including the invalid-index sweep:
/// concurrent `CREATE INDEX CONCURRENTLY` runs deadlock against each other, and PostgreSQL
/// resolves that by killing one of them. Letting any of it escape the loop turns a routine
/// collision into a failed start.
async fn apply_one(
    pool: &PgPool,
    schema: &str,
    migration: &Migration,
    last_applied: &mut i64,
    had_row: &mut bool,
) -> Result<(), MigrateError> {
    let version = i64::from(migration.version);

    for attempt in 1..=MAX_ATTEMPTS {
        match try_apply(pool, schema, migration, *had_row).await {
            Ok(()) => {
                *had_row = true;
                *last_applied = version;
                return Ok(());
            }
            Err(source) => {
                // Wait for a peer to finish rather than retrying into the same collision — but
                // only at full length when the error looks like one. Broken SQL never becomes
                // valid, and sitting out three peer timeouts for it turns a clear failure into
                // a half-minute stall.
                let wait = if is_possibly_concurrent(&source) {
                    PEER_WAIT
                } else {
                    UNKNOWN_WAIT
                };
                if let Some(now) = await_peer(pool, schema, version, wait).await {
                    tracing::debug!(
                        version = migration.version,
                        "another process applied this migration concurrently"
                    );
                    *had_row = true;
                    *last_applied = now;
                    return Ok(());
                }
                if attempt == MAX_ATTEMPTS {
                    return Err(MigrateError::Migration {
                        version: migration.version,
                        source,
                    });
                }
                tracing::warn!(
                    version = migration.version,
                    attempt,
                    error = %source,
                    "migration failed; retrying"
                );
            }
        }
    }
    unreachable!("the loop returns or errors on the final attempt")
}

/// Waits for a peer to record `version` or later, returning what it recorded.
///
/// `None` means nothing advanced within [`PEER_WAIT`] — so there was probably no peer, the
/// failure was this process's own, and the caller should retry the migration itself.
async fn await_peer(
    pool: &PgPool,
    schema: &str,
    version: i64,
    wait: std::time::Duration,
) -> Option<i64> {
    let deadline = std::time::Instant::now() + wait;
    loop {
        if let Ok(now) = recorded_version(pool, schema).await
            && now >= version
        {
            return Some(now);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(PEER_POLL).await;
    }
}

/// One attempt at a migration: guard, statements, and the version write.
async fn try_apply(
    pool: &PgPool,
    schema: &str,
    migration: &Migration,
    had_row: bool,
) -> Result<(), sqlx::Error> {
    let version = i64::from(migration.version);
    let quoted = quote_identifier(schema);
    let version_sql = if had_row {
        format!("UPDATE {quoted}.dbos_migrations SET version = {version}")
    } else {
        format!("INSERT INTO {quoted}.dbos_migrations (version) VALUES ({version})")
    };

    // Asked on every attempt: a peer may have satisfied it in the meantime.
    if let Some(guard) = migration.guard
        && sqlx::query(guard)
            .bind(schema)
            .fetch_optional(pool)
            .await?
            .is_some()
    {
        tracing::debug!(version = migration.version, "migration already satisfied");
        sqlx::raw_sql(AssertSqlSafe(version_sql))
            .execute(pool)
            .await?;
        return Ok(());
    }

    if migration.online {
        // Only PostgreSQL reaches here: `build_migrations` never marks a migration online on
        // CockroachDB, where schema changes are online regardless.
        //
        // `CONCURRENTLY` cannot run inside a transaction, so the statements and the version
        // write cannot commit together. A crash between them re-runs the migration, which is
        // why every online migration is `IF NOT EXISTS` or `IF EXISTS`.
        drop_invalid_indexes(pool, schema).await?;
        sqlx::raw_sql(AssertSqlSafe(migration.sql.clone()))
            .execute(pool)
            .await?;
        sqlx::raw_sql(AssertSqlSafe(version_sql))
            .execute(pool)
            .await?;
    } else {
        // Statements and the version write commit together, so an interrupted run always
        // resumes from a clean boundary.
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(AssertSqlSafe(migration.sql.clone()))
            .execute(&mut *tx)
            .await?;
        sqlx::raw_sql(AssertSqlSafe(version_sql))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    /// CockroachDB identifies itself in `version()`; PostgreSQL does not mention it.
    #[test]
    fn dialect_detection_reads_the_server_banner() {
        let crdb = "CockroachDB CCL v26.2.0 (x86_64-pc-linux-gnu)";
        let pg = "PostgreSQL 16.3 on x86_64-pc-linux-gnu";
        assert!(crdb.to_lowercase().contains("cockroachdb"));
        assert!(!pg.to_lowercase().contains("cockroachdb"));
    }
}
