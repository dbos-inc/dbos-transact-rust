//! Tests for the test harness itself.
//!
//! There is no library code to exercise yet, so what these assert is that the thing
//! everything later depends on actually works: a real database on both v1 backends,
//! containers shared rather than multiplied, and nothing left running afterwards.

use std::sync::Arc;

use dbos::sysdb::DEFAULT_SCHEMA;
use dbos_test_support::{Backend, SharedSlot, raw_database, test_database};

/// The harness reaches a real server and can run SQL on a fresh database.
///
/// Deliberately the raw lane: "no tables" is only true before migrations exist, so asserting
/// it against the pooled lane would start failing the moment that lane means what it says.
#[tokio::test]
async fn connects_to_a_fresh_database() {
    let db = raw_database().await;
    let pool = db.pool().await;

    // Explicitly `BIGINT` rather than a bare `SELECT 1`: CockroachDB types an integer
    // literal as `INT8` where Postgres types it `INT4`, so a bare literal decodes into
    // different Rust types on the two backends. Same lesson as the `SERIAL` test below —
    // never let an integer's width be inferred.
    let one: i64 = sqlx::query_scalar("SELECT 1::BIGINT")
        .fetch_one(&pool)
        .await
        .expect("SELECT 1 failed");
    assert_eq!(one, 1);

    // Fresh means empty: no leftovers from another test's database.
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
    )
    .fetch_one(&pool)
    .await
    .expect("failed to count tables");
    assert_eq!(tables, 0, "a fresh test database should have no tables");
}

/// Two databases held at the same time come from one container.
///
/// This is the property that makes the suite affordable on CockroachDB, where schema
/// changes are online and therefore slow. If this regresses, a CRDB run gets very slow
/// rather than failing, so it is worth asserting directly.
#[tokio::test]
async fn overlapping_tests_share_one_container() {
    // The raw lane, deliberately: this is about the container, not the schema, and leasing two
    // migrated databases to prove it would pay for two migrations to learn nothing extra.
    let first = raw_database().await;
    let second = raw_database().await;

    assert_eq!(
        first.server().container_id(),
        second.server().container_id(),
        "databases held at the same time should share one container",
    );
    assert_ne!(
        first.url(),
        second.url(),
        "each test should get its own database",
    );
}

/// Every part of a schema that a replay could plausibly get wrong, as sorted text.
///
/// Compared as a whole rather than table by table so that a *missing* object fails too, which
/// is the failure a dump is most likely to have: anything it forgets to emit simply is not
/// there, and a per-object assertion would never look for it.
///
/// Two normalisations, both about names the server generates rather than anything the
/// migrations asked for. `NOT NULL` reaches `table_constraints` as a `CHECK` row named after
/// internal object ids, which differ between any two databases — the corpus declares no `CHECK`
/// constraints of its own, and `is_nullable` below covers what those rows say. CockroachDB then
/// qualifies `indexdef` with the database name, which is different by definition here.
const CATALOGUE: &str = "\
    SELECT 'column|'||table_name||'|'||column_name||'|'||ordinal_position||'|'||data_type \
                ||'|'||is_nullable||'|'||coalesce(column_default, '-') \
      FROM information_schema.columns WHERE table_schema = $1 \
    UNION ALL \
    SELECT 'constraint|'||table_name||'|'||constraint_name||'|'||constraint_type \
      FROM information_schema.table_constraints \
      WHERE table_schema = $1 AND constraint_type <> 'CHECK' \
    UNION ALL \
    SELECT 'index|'||tablename||'|'||indexname||'|'||replace(indexdef, current_database()||'.', '') \
      FROM pg_indexes WHERE schemaname = $1 \
    UNION ALL \
    SELECT 'routine|'||routine_name||'|'||coalesce(data_type, '-') \
      FROM information_schema.routines WHERE routine_schema = $1 \
    ORDER BY 1";

async fn catalogue(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar(CATALOGUE)
        .bind(DEFAULT_SCHEMA)
        .fetch_all(pool)
        .await
        .expect("failed to read the catalogue")
}

async fn recorded_version(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT version FROM dbos.dbos_migrations")
        .fetch_one(pool)
        .await
        .expect("failed to read the recorded migration version")
}

/// A pooled database is indistinguishable from one the corpus built.
///
/// **This is the assertion the pool's whole design rests on.** Pooled databases are not
/// migrated; they are built from a baseline derived from one database that was. That trade is
/// only sound while the two are the same schema, and the ways it could quietly stop being true
/// — a dump that omits functions, a `TEMPLATE` clone that misses something, a new migration
/// introducing an object the dump does not emit — all look like passing tests running against
/// the wrong schema rather than like failures.
///
/// It costs one full corpus run on top of the one the pool already makes, which is the price of
/// the several the baseline spares every other test in this binary.
#[tokio::test]
async fn a_pooled_database_matches_one_the_migrations_built() {
    let migrated = raw_database().await;
    let pool = migrated.pool().await;
    dbos::sysdb::migrations::runner::run(&pool, DEFAULT_SCHEMA, true)
        .await
        .expect("failed to migrate the comparison database");
    let from_migrations = catalogue(&pool).await;
    let migrated_version = recorded_version(&pool).await;
    pool.close().await;

    let leased = test_database().await;
    let pool = leased.pool().await;
    let from_baseline = catalogue(&pool).await;
    let leased_version = recorded_version(&pool).await;
    pool.close().await;

    assert!(
        !from_migrations.is_empty(),
        "the comparison database came up empty, so this test would pass on anything",
    );
    // Reported as the difference rather than by comparing the two lists directly: they are
    // around 160 rows each, and a failure that prints both of them in full buries the one line
    // that changed.
    let missing: Vec<_> = from_migrations
        .iter()
        .filter(|row| !from_baseline.contains(row))
        .collect();
    let extra: Vec<_> = from_baseline
        .iter()
        .filter(|row| !from_migrations.contains(row))
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "a pooled database's schema differs from what the migrations build on {:?}\n\
         missing from the pooled database: {missing:#?}\n\
         present only in the pooled database: {extra:#?}",
        leased.backend(),
    );
    assert_eq!(
        leased_version, migrated_version,
        "a pooled database records a different migration version than a migrated one, so \
         anything reading it would try to migrate over the top of a finished schema",
    );
}

/// A leased database arrives migrated, which is what `test_database` promises.
#[tokio::test]
async fn a_leased_database_is_already_migrated() {
    let db = test_database().await;
    let pool = db.pool().await;

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1",
    )
    .bind(DEFAULT_SCHEMA)
    .fetch_one(&pool)
    .await
    .expect("failed to count tables");
    assert!(
        tables > 10,
        "the DBOS schema should already exist, saw {tables}"
    );
}

/// Resetting a leased database clears what a previous holder wrote.
///
/// This is what makes reuse safe. It is tested by calling `reset` directly rather than by
/// leasing twice and expecting the same database back: tests in a binary run in parallel, so
/// any given lease may come from the idle pool or be freshly migrated, and asserting which
/// would be asserting that no other test is running.
#[tokio::test]
async fn resetting_clears_a_previous_holders_rows() {
    let db = test_database().await;
    let pool = db.pool().await;

    sqlx::query(
        "INSERT INTO dbos.workflow_status (workflow_uuid, status, name) \
         VALUES ($1, 'PENDING', 'leftover')",
    )
    .bind("wf-from-a-previous-test")
    .execute(&pool)
    .await
    .expect("failed to write a row");

    let count = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dbos.workflow_status")
            .fetch_one(&pool)
            .await
            .expect("failed to count rows")
    };
    assert_eq!(count().await, 1, "the row should be there to begin with");

    db.reset().await;
    assert_eq!(count().await, 0, "reset should have cleared it");
}

/// The shared slot hands out one value and releases it once nobody holds it.
///
/// This is the property that keeps containers from accumulating one per run, and it is
/// tested here on a plain value rather than on the live server: every other test in this
/// binary holds the real one concurrently, so asserting release against it would be
/// asserting that no other test is running.
#[tokio::test]
async fn shared_slot_releases_once_nobody_holds_it() {
    let slot: SharedSlot<u32> = SharedSlot::new();

    let first = slot.get_or_init(|| async { 1 }).await;
    let second = slot.get_or_init(|| async { 2 }).await;
    assert!(
        Arc::ptr_eq(&first, &second),
        "a second caller should share the first caller's value, not create another",
    );
    assert_eq!(*second, 1, "the factory should not have run a second time");

    drop(first);
    drop(second);

    let third = slot.get_or_init(|| async { 3 }).await;
    assert_eq!(
        *third, 3,
        "once the last holder drops, the next caller should get a fresh value — if this \
         is 1, the slot is holding a strong reference and containers will leak",
    );
}

/// `SERIAL` is not the same type on both backends: `INT4` on Postgres, `INT8` on
/// CockroachDB.
///
/// Nothing in the migrations' text surfaces this; only running against CockroachDB does. It is
/// recorded here as an executable note, and it is why the DBOS schema uses explicit `BIGINT`
/// rather than `SERIAL`. It also proves the backend switch actually reaches a different engine,
/// which a `SELECT 1` cannot.
#[tokio::test]
async fn serial_width_diverges_between_backends() {
    // Raw lane: this creates a table, and a pooled database must not be handed on dirty.
    let db = raw_database().await;
    let pool = db.pool().await;

    sqlx::raw_sql("CREATE TABLE serial_probe (id SERIAL PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("failed to create the probe table");

    let data_type: String = sqlx::query_scalar(
        "SELECT data_type FROM information_schema.columns \
         WHERE table_name = 'serial_probe' AND column_name = 'id'",
    )
    .fetch_one(&pool)
    .await
    .expect("failed to read the column type");

    let expected = match db.backend() {
        Backend::Postgres => "integer",
        Backend::Cockroach => "bigint",
    };
    assert_eq!(
        data_type.to_ascii_lowercase(),
        expected,
        "SERIAL width on {:?} — if this changed, re-check every integer column in the \
         migrations before trusting it",
        db.backend(),
    );
}
