//! The migration runner, against real databases.
//!
//! What is tested here is the bookkeeping rather than the SQL — that a fresh database ends up
//! at the right version, that a second call does nothing, that an interrupted run resumes, and
//! that a database another implementation has taken further is left alone. `migrations.rs`
//! covers whether the statements themselves are valid.

mod support;

use dbos::sysdb::migrations::runner;
use dbos::sysdb::migrations::{Dialect, LOCAL_MIGRATIONS, quote_identifier};
use sqlx::{AssertSqlSafe, PgPool, Row};

use support::raw_database;

use dbos::sysdb::DEFAULT_SCHEMA as SCHEMA;

async fn version(pool: &PgPool) -> i64 {
    sqlx::query(AssertSqlSafe(format!(
        r#"SELECT version FROM {}.dbos_migrations"#,
        quote_identifier(SCHEMA)
    )))
    .fetch_one(pool)
    .await
    .expect("no version row")
    .get::<i64, _>(0)
}

async fn table_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 \
         AND table_name <> 'dbos_migrations'",
    )
    .bind(SCHEMA)
    .fetch_one(pool)
    .await
    .expect("failed to count tables")
}

/// A fresh database is migrated to the latest version this build knows.
///
/// The runner creates the schema and the version table itself, so nothing needs preparing.
#[tokio::test]
async fn migrates_a_fresh_database() {
    let db = raw_database().await;
    let pool = db.pool().await;

    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("migration failed");

    assert_eq!(outcome.from_version, 0, "a fresh database records nothing");
    assert_eq!(outcome.to_version, i64::from(LOCAL_MIGRATIONS));
    assert!(!outcome.was_current);
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
    assert!(table_count(&pool).await >= 10, "the schema should exist");
}

/// Running twice does nothing the second time.
///
/// This is the common case — every process start calls it — so the second call must not
/// re-execute DDL or write to the version row.
#[tokio::test]
async fn a_second_run_is_a_no_op() {
    let db = raw_database().await;
    let pool = db.pool().await;

    runner::run(&pool, SCHEMA, true).await.unwrap();
    let second = runner::run(&pool, SCHEMA, true)
        .await
        .expect("second run failed");

    assert!(second.was_current, "the database was already current");
    assert!(
        second.applied.is_empty(),
        "nothing should have been applied"
    );
    assert_eq!(second.from_version, second.to_version);
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// An interrupted run resumes from where it stopped rather than starting over.
///
/// Simulated by rewinding the recorded version after a complete run: the migrations are all
/// `IF NOT EXISTS`-guarded, so re-applying the tail must succeed and land on the same version.
#[tokio::test]
async fn resumes_from_a_partial_run() {
    let db = raw_database().await;
    let pool = db.pool().await;
    runner::run(&pool, SCHEMA, true).await.unwrap();

    sqlx::raw_sql(AssertSqlSafe(format!(
        "UPDATE {}.dbos_migrations SET version = 30",
        quote_identifier(SCHEMA)
    )))
    .execute(&pool)
    .await
    .unwrap();

    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("resume failed");
    assert_eq!(outcome.from_version, 30);
    assert_eq!(outcome.to_version, i64::from(LOCAL_MIGRATIONS));
    assert!(
        outcome.applied.iter().all(|v| *v > 30),
        "only migrations after 30 should have been applied, got {:?}",
        outcome.applied,
    );
}

/// A database migrated further than this build knows is left untouched.
///
/// Another implementation may define migrations this one does not — the shared series above
/// 100 exists precisely so that can happen. Refusing to start would be worse than proceeding.
#[tokio::test]
async fn tolerates_a_database_ahead_of_this_build() {
    let db = raw_database().await;
    let pool = db.pool().await;
    runner::run(&pool, SCHEMA, true).await.unwrap();

    sqlx::raw_sql(AssertSqlSafe(format!(
        "UPDATE {}.dbos_migrations SET version = 106",
        quote_identifier(SCHEMA)
    )))
    .execute(&pool)
    .await
    .unwrap();

    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("should tolerate");
    assert!(outcome.was_current);
    assert!(outcome.applied.is_empty());
    assert_eq!(
        outcome.to_version, 106,
        "the recorded version is left alone"
    );
    assert_eq!(version(&pool).await, 106);
}

/// Empty migrations consume their version numbers.
///
/// With notifications off, migrations 39, 43, and 44 do nothing — but the final version must
/// still be the full count, or another implementation reading this database would think it
/// had further to go.
#[tokio::test]
async fn empty_migrations_still_advance_the_version() {
    let db = raw_database().await;
    let pool = db.pool().await;

    let outcome = runner::run(&pool, SCHEMA, false)
        .await
        .expect("migration failed");

    assert_eq!(outcome.to_version, i64::from(LOCAL_MIGRATIONS));
    assert!(
        !outcome.applied.contains(&39),
        "39 has no statements with notifications off, so it should not be listed as applied",
    );
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// A non-default schema name works end to end.
#[tokio::test]
async fn honours_a_custom_schema_name() {
    let db = raw_database().await;
    let pool = db.pool().await;

    runner::run(&pool, "custom_dbos", true)
        .await
        .expect("migration failed");

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1",
    )
    .bind("custom_dbos")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(tables > 10, "the schema should hold the DBOS tables");
    assert_eq!(
        table_count(&pool).await,
        0,
        "the default schema is untouched"
    );
}

/// The dialect is read from the server rather than assumed.
#[tokio::test]
async fn detects_the_dialect_from_the_server() {
    let db = raw_database().await;
    let pool = db.pool().await;

    let detected = runner::detect_dialect(&pool)
        .await
        .expect("detection failed");
    let expected = match db.backend() {
        support::Backend::Postgres => Dialect::Postgres,
        support::Backend::Cockroach => Dialect::Cockroach,
    };
    assert_eq!(detected, expected);
}

/// An invalid index left by an interrupted `CREATE INDEX CONCURRENTLY` is cleaned up.
///
/// PostgreSQL leaves a failed concurrent build in place, marked invalid. It serves no reads,
/// costs write overhead, and holds its name — so the next online migration cannot recover by
/// re-running `CREATE INDEX IF NOT EXISTS`. The runner sweeps them before each online
/// migration, and this is the only test that puts one there to be swept.
///
/// An invalid index is produced honestly rather than faked: a unique build over duplicate rows
/// fails, and PostgreSQL keeps the wreckage.
#[tokio::test]
async fn sweeps_invalid_indexes_left_by_an_interrupted_build() {
    let db = raw_database().await;
    if db.backend() == support::Backend::Cockroach {
        return; // No CONCURRENTLY, and no invalid-index state to leave behind.
    }
    let pool = db.pool().await;

    sqlx::raw_sql(AssertSqlSafe(format!(
        r#"CREATE SCHEMA IF NOT EXISTS {0};
           CREATE TABLE {0}.dupes (x INT4);
           INSERT INTO {0}.dupes VALUES (1), (1);"#,
        quote_identifier(SCHEMA)
    )))
    .execute(&pool)
    .await
    .unwrap();

    // Fails on the duplicates, leaving an invalid index behind.
    let failed = sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX CONCURRENTLY wreckage ON {}.dupes (x)",
        quote_identifier(SCHEMA)
    )))
    .execute(&pool)
    .await;
    assert!(failed.is_err(), "the unique build should have failed");

    let invalid_count = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_index ix \
             JOIN pg_class t ON t.oid = ix.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE NOT ix.indisvalid AND n.nspname = $1",
        )
        .bind(SCHEMA)
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    assert_eq!(invalid_count().await, 1, "expected wreckage to sweep");

    runner::run(&pool, SCHEMA, true)
        .await
        .expect("migration failed");

    assert_eq!(
        invalid_count().await,
        0,
        "the runner should have dropped the invalid index before its online migrations",
    );
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// Migration 10 backfills the primary key when a database genuinely lacks it.
///
/// Every other test exercises the *skip* path, because a schema this implementation created
/// already has `message_uuid ... PRIMARY KEY` from migration 1. That leaves the branch
/// migration 10 actually exists for completely uncovered — the case of a database created by a
/// version old enough to have missed it.
///
/// Simulated the way Java's suite does: apply a migration 1 with the primary key stripped out,
/// record version 1, then run everything and check the key arrives.
#[tokio::test]
async fn migration_ten_backfills_a_missing_primary_key() {
    let db = raw_database().await;
    if db.backend() == support::Backend::Cockroach {
        return; // The guard path is covered on PostgreSQL; CockroachDB has the same runner logic.
    }
    let pool = db.pool().await;

    // Migration 1 as it was before the primary key was added.
    let original = dbos::sysdb::migrations::source("1_initial_dbos_schema.sql")
        .unwrap()
        .sql
        .replace(
            "message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY",
            "message_uuid TEXT NOT NULL DEFAULT gen_random_uuid()",
        );
    assert!(
        !original.contains("PRIMARY KEY, -- Built-in function"),
        "the primary key should have been stripped",
    );

    let quoted = quote_identifier(SCHEMA);
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE SCHEMA IF NOT EXISTS {quoted};
         CREATE TABLE {quoted}.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);"
    )))
    .execute(&pool)
    .await
    .unwrap();
    // Both halves, as a real database of that vintage would have had: the base tables and the
    // LISTEN/NOTIFY triggers. Without the second half, migration 20 later tries to harden a
    // trigger function that was never created.
    let values = dbos::sysdb::migrations::Placeholders {
        schema: &quoted,
        concurrently: "CONCURRENTLY",
    };
    let notify_half = dbos::sysdb::migrations::source("1_initial_dbos_schema_listen_notify.sql")
        .unwrap()
        .sql;
    for part in [original.as_str(), notify_half] {
        sqlx::raw_sql(AssertSqlSafe(
            dbos::sysdb::migrations::render(part, values).unwrap(),
        ))
        .execute(&pool)
        .await
        .expect("the original migration 1 should apply");
    }
    sqlx::raw_sql(AssertSqlSafe(format!(
        "INSERT INTO {quoted}.dbos_migrations (version) VALUES (1)"
    )))
    .execute(&pool)
    .await
    .unwrap();

    let has_pk = || async {
        sqlx::query(
            "SELECT 1 FROM pg_constraint c \
             JOIN pg_class cl ON c.conrelid = cl.oid \
             JOIN pg_namespace n ON cl.relnamespace = n.oid \
             WHERE n.nspname = $1 AND cl.relname = 'notifications' AND c.contype = 'p'",
        )
        .bind(SCHEMA)
        .fetch_optional(&pool)
        .await
        .unwrap()
        .is_some()
    };
    assert!(!has_pk().await, "the old schema should have no primary key");

    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("migration failed");

    assert!(
        has_pk().await,
        "migration 10 should have backfilled the notifications primary key",
    );
    assert!(
        outcome.applied.contains(&10),
        "migration 10 should report as applied, got {:?}",
        outcome.applied,
    );
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// A failing migration leaves the recorded version where it was.
///
/// The version is what every implementation reads to decide what is left to do, so advancing
/// it past work that did not happen would strand the database: the failed migration would
/// never be retried, by this process or any other.
#[tokio::test]
async fn a_failed_migration_does_not_advance_the_version() {
    let db = raw_database().await;
    let pool = db.pool().await;
    let dialect = runner::detect_dialect(&pool).await.unwrap();

    let mut migrations = dbos::sysdb::migrations::build_migrations(SCHEMA, dialect, true);
    let broken_at = 12usize; // migration 12, chosen for having no dependents before it
    migrations[broken_at - 1].sql = "THIS IS NOT VALID SQL".to_owned();

    let err = runner::apply(&pool, SCHEMA, &migrations)
        .await
        .expect_err("a broken migration should fail the run");
    match err {
        runner::MigrateError::Migration { version, .. } => assert_eq!(version, broken_at as u32),
        other => panic!("expected a migration failure, got {other:?}"),
    }

    assert_eq!(
        version(&pool).await,
        (broken_at - 1) as i64,
        "the version should stop at the last migration that succeeded",
    );

    // The real list still completes from there.
    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("recovery failed");
    assert_eq!(outcome.from_version, (broken_at - 1) as i64);
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// Re-running the online migrations is safe.
///
/// They are the ones that cannot commit with their version write — `CONCURRENTLY` is illegal
/// inside a transaction — so a crash between the statement and the bump re-runs them. That is
/// only survivable because each is `IF NOT EXISTS` or `IF EXISTS`, which this checks by
/// rewinding past all of them and running again.
#[tokio::test]
async fn online_migrations_are_idempotent() {
    let db = raw_database().await;
    let pool = db.pool().await;
    runner::run(&pool, SCHEMA, true).await.unwrap();

    // 22 is the first online migration, so rewinding to 21 makes every one of them pending
    // again against a schema where their indexes already exist.
    sqlx::raw_sql(AssertSqlSafe(format!(
        "UPDATE {}.dbos_migrations SET version = 21",
        quote_identifier(SCHEMA)
    )))
    .execute(&pool)
    .await
    .unwrap();

    let outcome = runner::run(&pool, SCHEMA, true)
        .await
        .expect("re-running the online migrations should succeed");
    assert!(
        outcome.applied.contains(&22),
        "the online migrations should have re-run, got {:?}",
        outcome.applied,
    );
    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}

/// Concurrent migrators converge instead of one of them failing.
///
/// There is no advisory lock — CockroachDB has none — so two cold starts race on the same DDL.
/// The loser re-reads the recorded version, sees the work done, and carries on.
#[tokio::test]
async fn concurrent_migrators_converge() {
    let db = raw_database().await;
    let pool = db.pool().await;

    let (a, b) = tokio::join!(
        runner::run(&pool, SCHEMA, true),
        runner::run(&pool, SCHEMA, true),
    );
    a.expect("first migrator failed");
    b.expect("second migrator failed");

    assert_eq!(version(&pool).await, i64::from(LOCAL_MIGRATIONS));
}
