//! Applies the migration corpus to a real database.
//!
//! Everything else about the corpus is checked by reading it — file counts, slot arithmetic,
//! header numbering. Those tests are cheap and they caught real problems, but they share a
//! blind spot: they assert things *about* SQL without ever asking a database whether it is
//! valid. A `%s` bound to the wrong value, a variant selected for the wrong dialect, or a
//! statement that parses everywhere but only executes on one backend all pass a static check
//! and fail here.
//!
//! These run on the raw lane, because the migrations are the thing under test.

mod support;

use dbos::sysdb::migrations::{Dialect, Migration, build_migrations};
use sqlx::{PgPool, Row};

use support::{Backend, raw_database};

fn dialect_for(backend: Backend) -> Dialect {
    match backend {
        Backend::Postgres => Dialect::Postgres,
        Backend::Cockroach => Dialect::Cockroach,
    }
}

/// Applies a migration list in order, honouring guards.
///
/// A stripped-down stand-in for `runner::run`, used where a test needs to apply an explicit
/// slice of the corpus — the runner always applies everything from the recorded version. It
/// deliberately keeps no version state.
async fn apply_all(pool: &PgPool, schema: &str, migrations: &[Migration]) {
    for m in migrations {
        if m.sql.trim().is_empty() {
            continue;
        }
        if let Some(guard) = m.guard {
            // Row presence only — never decode the value. The guard selects a literal `1`,
            // which is INT4 on PostgreSQL and INT8 on CockroachDB, so no single Rust type
            // decodes it on both.
            let already = sqlx::query(guard)
                .bind(schema)
                .fetch_optional(pool)
                .await
                .unwrap_or_else(|e| panic!("migration {} guard failed: {e}", m.version));
            if already.is_some() {
                continue;
            }
        }
        // Whole files, never split on `;` — several carry semicolons inside `$$` blocks.
        // `AssertSqlSafe` because sqlx 0.9 wants a `&'static str` otherwise; the string is
        // rendered from the embedded corpus with only the quoted schema interpolated.
        sqlx::raw_sql(sqlx::AssertSqlSafe(m.sql.clone()))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("migration {} failed: {e}\n---\n{}", m.version, m.sql));
    }
}

async fn table_names(pool: &PgPool, schema: &str) -> Vec<String> {
    let rows = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name")
        .bind(schema)
        .fetch_all(pool)
        .await
        .expect("failed to list tables");
    rows.iter().map(|r| r.get::<String, _>(0)).collect()
}

async fn columns_of(pool: &PgPool, schema: &str, table: &str) -> Vec<String> {
    let rows = sqlx::query("SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY column_name")
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await
        .expect("failed to list columns");
    rows.iter().map(|r| r.get::<String, _>(0)).collect()
}

/// The whole corpus applies cleanly and produces the expected tables.
#[tokio::test]
async fn the_corpus_applies_to_a_real_database() {
    let db = raw_database().await;
    let pool = db.pool().await;
    let schema = dbos::sysdb::DEFAULT_SCHEMA;

    let migrations = build_migrations(schema, dialect_for(db.backend()), true);
    assert_eq!(
        migrations.len(),
        dbos::sysdb::migrations::LOCAL_MIGRATIONS as usize,
        "one entry per locally-defined migration",
    );
    // Through the real runner, so this covers the path production uses.
    dbos::sysdb::runner::run(&pool, schema, true)
        .await
        .expect("migration failed");

    let tables = table_names(&pool, schema).await;
    for expected in [
        "application_versions",
        "event_dispatch_kv",
        "notifications",
        "operation_outputs",
        "queues",
        "streams",
        "workflow_events",
        "workflow_events_history",
        "workflow_schedules",
        "workflow_status",
    ] {
        assert!(
            tables.iter().any(|t| t == expected),
            "missing table {expected}; got {tables:?}",
        );
    }
}

/// Columns added by later migrations are present, so the sequence actually ran through.
///
/// Applying migration 1 alone would satisfy the table check above; this asserts that the
/// tail of the sequence took effect too.
#[tokio::test]
async fn late_migrations_take_effect() {
    let db = raw_database().await;
    let pool = db.pool().await;
    let schema = dbos::sysdb::DEFAULT_SCHEMA;
    sqlx::raw_sql(r#"CREATE SCHEMA IF NOT EXISTS "dbos""#)
        .execute(&pool)
        .await
        .unwrap();

    apply_all(
        &pool,
        schema,
        &build_migrations(schema, dialect_for(db.backend()), true),
    )
    .await;

    let cols = columns_of(&pool, schema, "workflow_status").await;
    for expected in [
        "queue_partition_key",        // 2
        "forked_from",                // 4
        "owner_xid",                  // 7
        "serialization",              // 11
        "delay_until_epoch_ms",       // 16
        "rate_limited",               // 33
        "completed_at",               // 36
        "attributes",                 // 40
        "schedule_name",              // 41
        "debounce_deadline_epoch_ms", // 42
    ] {
        assert!(
            cols.iter().any(|c| c == expected),
            "workflow_status is missing {expected}; got {cols:?}",
        );
    }
}

/// Migrations 43 and 44 leave only the notifications trigger behind.
///
/// Migration 1 installs notification and workflow-events triggers and 39 adds the streams
/// one; 43 and 44 then drop two of the three, because those writes are coalesced by the
/// notifier off the write path. Only `notifications` still fires per row.
#[tokio::test]
async fn only_the_notifications_trigger_survives() {
    let db = raw_database().await;
    if db.backend() == Backend::Cockroach {
        return; // CockroachDB installs none of them.
    }
    let pool = db.pool().await;
    let schema = dbos::sysdb::DEFAULT_SCHEMA;
    sqlx::raw_sql(r#"CREATE SCHEMA IF NOT EXISTS "dbos""#)
        .execute(&pool)
        .await
        .unwrap();
    apply_all(
        &pool,
        schema,
        &build_migrations(schema, Dialect::Postgres, true),
    )
    .await;

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT trigger_name FROM information_schema.triggers \
         WHERE trigger_schema = $1 ORDER BY 1",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await
    .expect("failed to list triggers");
    assert_eq!(names, vec!["dbos_notifications_trigger".to_owned()]);

    // The v1 partition index is created by 45 and dropped by 47; only v2 survives.
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1 \
         AND indexname LIKE 'idx_workflow_status_partition%' ORDER BY 1",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await
    .expect("failed to list indexes");
    assert_eq!(
        indexes,
        vec!["idx_workflow_status_partition_dequeue_v2".to_owned()],
    );
}

/// Migration 10 is skipped, because migration 1 already created the primary key.
///
/// Its guard is the only conditional in the corpus, and the branch it protects is one this
/// implementation should never take — migration 1 has created `message_uuid ... PRIMARY KEY`
/// inline all along. If this ever fails, either migration 1 changed or the guard is wrong.
#[tokio::test]
async fn migration_ten_is_a_no_op_on_a_schema_we_created() {
    let db = raw_database().await;
    let pool = db.pool().await;
    let schema = dbos::sysdb::DEFAULT_SCHEMA;
    sqlx::raw_sql(r#"CREATE SCHEMA IF NOT EXISTS "dbos""#)
        .execute(&pool)
        .await
        .unwrap();

    let migrations = build_migrations(schema, dialect_for(db.backend()), true);
    let ten = migrations.iter().find(|m| m.version == 10).unwrap();
    let guard = ten.guard.expect("migration 10 carries a guard");

    // Apply everything up to and including 9, then ask the guard.
    apply_all(&pool, schema, &migrations[..9]).await;
    // Presence, not value: the literal `1` is INT4 on PostgreSQL and INT8 on CockroachDB.
    let already = sqlx::query(guard)
        .bind(schema)
        .fetch_optional(&pool)
        .await
        .expect("the guard query should execute on both backends");
    assert!(
        already.is_some(),
        "migration 1 should already have created the notifications primary key",
    );
}

/// A dialect's list is the same length as any other's, empties included.
///
/// Versions are positional: a migration that does not apply on this backend has to render
/// empty rather than be dropped, or every migration after it shifts by one.
#[tokio::test]
async fn dialects_agree_on_the_number_of_versions() {
    let pg = build_migrations("dbos", Dialect::Postgres, true);
    let crdb = build_migrations("dbos", Dialect::Cockroach, true);
    let no_notify = build_migrations("dbos", Dialect::Postgres, false);

    assert_eq!(pg.len(), crdb.len());
    assert_eq!(pg.len(), no_notify.len());
    for (a, b) in pg.iter().zip(crdb.iter()) {
        assert_eq!(a.version, b.version);
    }

    // CockroachDB has no ALTER FUNCTION ... SET and no triggers here, so 20 and 39 are
    // no-ops — but they still occupy their version numbers.
    assert!(
        crdb.iter()
            .find(|m| m.version == 20)
            .unwrap()
            .sql
            .is_empty()
    );
    assert!(
        crdb.iter()
            .find(|m| m.version == 39)
            .unwrap()
            .sql
            .is_empty()
    );
    assert!(!pg.iter().find(|m| m.version == 20).unwrap().sql.is_empty());

    // No migration is marked online on CockroachDB, where DDL is online anyway.
    assert!(crdb.iter().all(|m| !m.online));
    assert_eq!(
        pg.iter().filter(|m| m.online).count(),
        dbos::sysdb::migrations::ONLINE_MIGRATIONS.len(),
    );
}

/// The corpus applies with notifications turned off, which is a supported configuration.
///
/// This is the path where migration 1 sheds its trigger half, 20 hardens only the functions
/// that exist, and 39 becomes a no-op. Getting that wrong fails here and nowhere else:
/// hardening a function that was never installed is an error the static tests cannot see.
#[tokio::test]
async fn the_corpus_applies_without_listen_notify() {
    let db = raw_database().await;
    if db.backend() == Backend::Cockroach {
        return; // CockroachDB never installs the triggers; the Postgres run covers this.
    }
    let pool = db.pool().await;
    let schema = dbos::sysdb::DEFAULT_SCHEMA;
    sqlx::raw_sql(r#"CREATE SCHEMA IF NOT EXISTS "dbos""#)
        .execute(&pool)
        .await
        .unwrap();

    apply_all(
        &pool,
        schema,
        &build_migrations(schema, Dialect::Postgres, false),
    )
    .await;

    let triggers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.triggers WHERE trigger_schema = $1",
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .expect("failed to count triggers");
    assert_eq!(
        triggers, 0,
        "no triggers should exist with notifications off"
    );

    // The *functions* go too — migration 20 must not try to harden what migration 1 never
    // installed — while the SQL-client functions, which have nothing to do with notifications,
    // must still be there.
    let functions: Vec<String> = sqlx::query_scalar(
        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = $1 ORDER BY 1",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await
    .expect("failed to list functions");
    for absent in [
        "notifications_function",
        "workflow_events_function",
        "streams_function",
    ] {
        assert!(
            !functions.iter().any(|f| f == absent),
            "{absent} should not exist with notifications off, got {functions:?}",
        );
    }
    for present in ["enqueue_workflow", "send_message"] {
        assert!(
            functions.iter().any(|f| f == present),
            "{present} is unrelated to notifications and should exist, got {functions:?}",
        );
    }

    // The tables still arrive; only the notification plumbing is absent.
    assert!(
        table_names(&pool, schema)
            .await
            .iter()
            .any(|t| t == "workflow_status")
    );
}
