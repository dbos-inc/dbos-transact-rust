//! Test harness: a database server in a container, shared by every test that overlaps
//! with it in time.
//!
//! Several things about it are load-bearing rather than incidental, and should survive any
//! rewrite:
//!
//! **Sharing is by reference count, not by parking the container in a `static`.**
//! `testcontainers-rs` ties container removal to `Drop`, ships no Ryuk reaper, and leaves
//! its `watchdog` feature off by default — so a container parked in a `static` outlives the
//! test process and accumulates one per run. Here a `static` holds a [`Weak`] and each test
//! holds an [`Arc`]: the server starts on the first test that wants it, is reused by every
//! test overlapping that one, and is removed when the last of them finishes. The one cost is
//! that a strictly sequential run (`--test-threads=1`) has no overlap and so starts a
//! container per test — the pathological case, and never worse than not sharing at all.
//!
//! **Both v1 backends run the same suite**, switched by `DBOS_TEST_USE_COCKROACH_DB`,
//! following Java's `PgContainer`. CockroachDB is a v1 backend, not a smoke-tested
//! afterthought, and the switch is what makes the second CI leg a one-variable change.
//!
//! Note that Rust compiles each `tests/*.rs` file into its own binary, and the crate's own
//! `#[cfg(test)]` code into one more, so "the shared server" is shared within a binary rather
//! than across them. Prefer few, larger test files over many small ones — each additional one
//! is another container.
//!
//! **Pooled databases are built from a baseline, not by replaying the migrations.** One
//! database per process is migrated for real; every database the pool hands out is then built
//! from that one's finished schema. Replaying 55 versions of online DDL costs about 17s on
//! CockroachDB against 0.8s for the schema it ends at, because the corpus creates indexes it
//! later drops and the end state does none of that work. `Baseline` has the details.
//!
//! **Where that time goes is measured rather than inferred.** Starting a container and migrating
//! the baseline are paid once per test binary; building pooled databases and resetting leased
//! ones scale with the tests, and the two want opposite fixes — fewer servers against cheaper
//! ones. Set `DBOS_TEST_TIMINGS` to a path and every server appends a JSON line as it shuts
//! down, splitting boot from migration and both from the per-test cost. Unset, it reads one
//! environment variable per server and writes nothing. See [`TIMINGS_ENV`].
//!
//! **A crate rather than a module under `tests/`.** A `mod support;` is visible only to the
//! integration test binaries, so anything it tests has to be `pub` — the crate boundary,
//! not the design, would decide the public API. As a dev-dependency it is reachable from
//! `#[cfg(test)]` code in `src/` too, which is where a test of a crate-private thing belongs.

use std::future::Future;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dbos::sysdb::DEFAULT_SCHEMA;
use dbos::sysdb::migrations::quote_identifier;
use sqlx::AssertSqlSafe;
use sqlx::ConnectOptions;
use sqlx::postgres::PgConnectOptions;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::Mutex;

/// Pinned: what Go and TypeScript run CI against, and the oldest server the migrations
/// must work on.
const POSTGRES_IMAGE: (&str, &str) = ("postgres", "16");
/// Pinned: Java's choice.
const COCKROACH_IMAGE: (&str, &str) = ("cockroachdb/cockroach", "latest-v26.2");

const POSTGRES_PORT: u16 = 5432;
const COCKROACH_PORT: u16 = 26257;

/// Applied to every container this harness starts, so a leak is findable and CI can
/// assert there is none. `testcontainers-rs` adds no identifying label of its own, so
/// without this a `docker ps --filter` cleanup check would match nothing and pass
/// whether or not anything leaked.
const CONTAINER_LABEL: (&str, &str) = ("dev.dbos.test-harness", "true");

/// The most migrated databases to keep, and so the widest a suite can run.
///
/// Not a migration budget: a pooled database is cloned from a migrated baseline, so it costs
/// under a second even on CockroachDB. What bounds this number is how much parallelism a suite
/// this size can actually use, and four fits a CI runner's core count. Raising it is cheap if a
/// suite ever outgrows it.
pub const POOL_SIZE: usize = 4;

/// How long to wait for the server to accept connections after the container starts.
/// CockroachDB is the slow one; Postgres is usually ready in well under a second.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How many times to try creating a database from a template before giving up.
const CREATE_DATABASE_ATTEMPTS: u32 = 10;

/// How long to wait for a template's last session to go away before trying again.
const TEMPLATE_RETRY_WAIT: Duration = Duration::from_millis(100);

/// Names a file the harness appends one timing record to per server, or is unset.
///
/// Unset — the default, and what a developer gets — reads one environment variable per server
/// and writes nothing. Set to a path, every server appends a JSON line as it shuts down.
///
/// **A file rather than stderr, because libtest captures output per test.** A [`TestServer`]
/// drops inside whichever test happens to release the last handle, so a printed record would be
/// swallowed on success and surface only when some unrelated test failed. Appending also puts
/// every test binary of a run in one file, which is the shape the question this exists to answer
/// has: the fixed per-binary cost is only interesting summed across a leg.
pub const TIMINGS_ENV: &str = "DBOS_TEST_TIMINGS";

/// Which database engine the suite is running against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    Cockroach,
}

impl Backend {
    /// Reads `DBOS_TEST_USE_COCKROACH_DB`, matching Java's `PgContainer` switch.
    pub fn from_env() -> Self {
        match std::env::var("DBOS_TEST_USE_COCKROACH_DB") {
            Ok(v) if matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes") => {
                Backend::Cockroach
            }
            _ => Backend::Postgres,
        }
    }

    /// The superuser this backend's image starts with.
    fn admin_user(self) -> &'static str {
        match self {
            Backend::Postgres => "postgres",
            Backend::Cockroach => "root",
        }
    }

    fn port(self) -> u16 {
        match self {
            Backend::Postgres => POSTGRES_PORT,
            Backend::Cockroach => COCKROACH_PORT,
        }
    }
}

/// Whether a `sqlx` error carries a particular SQLSTATE.
fn has_code(error: &sqlx::Error, code: &str) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|c| c == code)
}

/// How a pooled database is built once one database has been migrated for real.
///
/// **Replaying the corpus per pooled database is the thing this exists to avoid.** The corpus
/// is a history, and a history does work it later undoes: it creates several indexes that later
/// migrations drop, and rewrites `enqueue_workflow` three times. CockroachDB charges for every
/// step of that as an online schema change — measured at 17s against 0.8s for the schema those
/// steps arrive at, and 2.9s against 0.1s on PostgreSQL.
///
/// **What the two arms have in common is that neither is a second definition of the schema.**
/// Both are derived, in-process, from a database the real runner migrated moments earlier, so
/// there is nothing to keep in step with `migrations/` and nothing to regenerate when a
/// migration lands. A checked-in baseline would be faster still — it would remove the one real
/// migration a process makes — and would be one more copy of the schema to go stale.
///
/// `a_pooled_database_matches_one_the_migrations_built` is what holds this honest: it compares
/// the catalogue of a leased database against one the corpus built, on both backends.
enum Baseline {
    /// PostgreSQL copies the migrated database with `CREATE DATABASE ... TEMPLATE`, which is
    /// exact by construction. Holds its name.
    Template(String),
    /// CockroachDB has no `TEMPLATE`, so it replays a dump of the finished schema. Holds the
    /// SQL.
    Replay(String),
}

/// Dumps `schema` as SQL that recreates it: tables, then functions, then the recorded version.
///
/// CockroachDB only. `SHOW CREATE ALL TABLES` emits tables with their indexes inline and their
/// foreign keys as trailing `ALTER`s — which is the whole reason the replay is fast, since an
/// index that arrives with its table costs nothing to build.
///
/// The version row is dumped as data because a database that carries the schema but not the
/// version is not migrated as far as anything else is concerned: the runner would read 0 and
/// apply the corpus over the top of it.
async fn dump_schema(pool: &sqlx::PgPool, schema: &str) -> String {
    let quoted = quote_identifier(schema);
    let mut sql = String::new();

    let tables: Vec<String> = sqlx::query_scalar("SHOW CREATE ALL TABLES")
        .fetch_all(pool)
        .await
        .expect("failed to dump the table definitions");
    for table in tables {
        sql.push_str(&table);
        sql.push('\n');
    }

    // By name, then every overload of each: `SHOW CREATE FUNCTION` takes a name and returns a
    // row per signature, and `information_schema.routines` has a row per signature too.
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT routine_name FROM information_schema.routines \
         WHERE routine_schema = $1 ORDER BY routine_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .expect("failed to list the functions to dump");
    for name in names {
        let definitions: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT create_statement FROM [SHOW CREATE FUNCTION {quoted}.{}]",
            quote_identifier(&name),
        )))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("failed to dump function {name}: {e}"));
        for definition in definitions {
            sql.push_str(&definition);
            sql.push_str(";\n");
        }
    }

    let version: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT version FROM {quoted}.dbos_migrations"
    )))
    .fetch_one(pool)
    .await
    .expect("failed to read the recorded migration version");
    sql.push_str(&format!(
        "INSERT INTO {quoted}.dbos_migrations (version) VALUES ({version});\n"
    ));

    sql
}

/// What one server spent getting ready, and what its tests spent on databases.
///
/// **The split this exists to make is fixed cost against per-test cost.** Starting the container
/// and migrating the baseline are paid once per test binary whatever the suite does; building a
/// pooled database and resetting a leased one scale with the tests. Which of those dominates
/// decides what is worth optimising, and reading it off total suite time is guesswork: a suite
/// of two tests and one of seven can finish within seconds of each other, which says the fixed
/// cost is large without saying which half of it is.
///
/// Every field is an atomic because these are written through `&TestServer`: the baseline is
/// built inside a `OnceCell` initialiser taking `&self`, and pooled databases are built during a
/// lease, from whichever task got there first.
struct Timings {
    /// When the server started coming up, so a record can say how much of its life was setup.
    created: Instant,
    /// Starting the container: image pull on a cold host, then the container itself.
    boot_nanos: AtomicU64,
    /// Waiting for the server to accept SQL, which for CockroachDB is well after the port opens.
    ready_nanos: AtomicU64,
    /// Building the baseline end to end, including the two below.
    baseline_nanos: AtomicU64,
    /// The one full corpus run a process makes — the cost a pre-migrated image would remove.
    migrate_nanos: AtomicU64,
    /// Dumping the migrated schema. CockroachDB only; PostgreSQL keeps the database as a template.
    dump_nanos: AtomicU64,
    /// Pooled databases built from the baseline, and what they cost.
    pooled_builds: AtomicU64,
    pooled_build_nanos: AtomicU64,
    /// Databases handed out, and what emptying them on acquire cost.
    leases: AtomicU64,
    reset_nanos: AtomicU64,
}

impl Timings {
    fn new() -> Self {
        Self {
            created: Instant::now(),
            boot_nanos: AtomicU64::new(0),
            ready_nanos: AtomicU64::new(0),
            baseline_nanos: AtomicU64::new(0),
            migrate_nanos: AtomicU64::new(0),
            dump_nanos: AtomicU64::new(0),
            pooled_builds: AtomicU64::new(0),
            pooled_build_nanos: AtomicU64::new(0),
            leases: AtomicU64::new(0),
            reset_nanos: AtomicU64::new(0),
        }
    }

    /// Adds an elapsed time to one of the counters above.
    fn add(counter: &AtomicU64, elapsed: Duration) {
        counter.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Appends this server's record to [`TIMINGS_ENV`]'s file, or does nothing if it is unset.
    ///
    /// Every failure here is ignored on purpose. This is a measurement, and a measurement must
    /// not be able to fail a test run: an unwritable path is a mistake in how the run was
    /// invoked, and the cost of getting it wrong should be a missing line rather than a red
    /// suite that says nothing about the code.
    fn write(&self, backend: Backend) {
        let Ok(path) = std::env::var(TIMINGS_ENV) else {
            return;
        };
        if path.is_empty() {
            return;
        }

        // The test binary's own name, which is what attributes a record to a suite. Falls back
        // to the whole path, and then to nothing, rather than refusing to write a record.
        let argv0 = std::env::args().next().unwrap_or_default();
        let binary = std::path::Path::new(&argv0)
            .file_name()
            .map_or_else(|| argv0.clone(), |name| name.to_string_lossy().into_owned());

        let ms = |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64 / 1e6;
        let line = format!(
            r#"{{"binary":"{binary}","backend":"{backend:?}","lifetime_ms":{:.1},"boot_ms":{:.1},"ready_ms":{:.1},"baseline_ms":{:.1},"migrate_ms":{:.1},"dump_ms":{:.1},"pooled_builds":{},"pooled_build_ms":{:.1},"leases":{},"reset_ms":{:.1}}}"#,
            self.created.elapsed().as_secs_f64() * 1e3,
            ms(&self.boot_nanos),
            ms(&self.ready_nanos),
            ms(&self.baseline_nanos),
            ms(&self.migrate_nanos),
            ms(&self.dump_nanos),
            self.pooled_builds.load(Ordering::Relaxed),
            ms(&self.pooled_build_nanos),
            self.leases.load(Ordering::Relaxed),
            ms(&self.reset_nanos),
        );

        // Appending, because every test binary of a run writes to the same file and they are
        // separate processes. One short `O_APPEND` write per server is atomic in practice on
        // the platforms this runs on, and nothing reads the file until the run is over.
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            let _ = writeln!(file, "{line}");
        }
    }
}

/// Set to any value to make every process migrate its own baseline.
///
/// The cache below is derived rather than checked in, so it cannot go stale in the usual way —
/// but it is keyed on the migration SQL, not on the runner that applies it, so a change in how
/// the runner behaves without any SQL changing is the one thing the key cannot see. This is the
/// escape hatch for that, and for bisecting anything that smells like a schema difference.
pub const NO_BASELINE_CACHE_ENV: &str = "DBOS_TEST_NO_BASELINE_CACHE";

/// The CockroachDB schema dump, cached on disk under the target directory.
///
/// **Scope is the whole point.** [`CACHED_DUMP`] spares a process a second corpus run; this
/// spares every process after the first one its only corpus run. Measured in CI, the cockroach
/// leg spends about 173s migrating baselines across ten test binaries — one each — and cargo
/// runs those binaries one after another, so it is 173s of straight-line wall clock to reach a
/// schema that is identical every time.
///
/// **It is still not a second definition of the schema**, which is the property worth keeping.
/// The file is written by a process that migrated a real database with the real runner, and the
/// key covers every migration's rendered SQL, so a migration that changes anything at all lands
/// on a different key and nothing stale can be read. What a checked-in baseline would cost —
/// something to regenerate, and a reviewer having to believe it matches — is exactly what
/// deriving it avoids. `a_pooled_database_matches_one_the_migrations_built` holds the whole
/// arrangement honest.
mod baseline_cache {
    use std::path::{Path, PathBuf};

    use dbos::sysdb::DEFAULT_SCHEMA;
    use dbos::sysdb::migrations::{Dialect, build_migrations};

    /// FNV-1a, inline and deterministic.
    ///
    /// Not `DefaultHasher`: its output is explicitly not stable across releases, so a toolchain
    /// upgrade would silently miss every cache entry. A miss is only ever a slow run rather than
    /// a wrong one, but a cache that quietly stops working is worse than no cache, because
    /// nothing says so.
    fn fnv1a(bytes: &[u8], mut hash: u64) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    /// Everything that decides what the dumped schema looks like.
    ///
    /// The migration corpus is the substance. The schema name is in every rendered statement.
    /// **The image tag matters too**, and less obviously: the dump is `SHOW CREATE ALL TABLES`
    /// output, so its syntax belongs to the server that produced it, and replaying one version's
    /// dump into another is not a thing to do by accident.
    pub(super) fn key(image: (&str, &str)) -> u64 {
        let mut hash = fnv1a(DEFAULT_SCHEMA.as_bytes(), 0xcbf2_9ce4_8422_2325);
        hash = fnv1a(image.0.as_bytes(), hash);
        hash = fnv1a(image.1.as_bytes(), hash);
        // `true` for `use_listen_notify`, matching the runner call in `baseline`. It changes
        // nothing on CockroachDB, which has no LISTEN/NOTIFY, and is hashed anyway so that a
        // harness which ever stops passing `true` cannot read a file written by one that did.
        for migration in build_migrations(DEFAULT_SCHEMA, Dialect::Cockroach, true) {
            hash = fnv1a(&migration.version.to_le_bytes(), hash);
            hash = fnv1a(migration.sql.as_bytes(), hash);
            hash = fnv1a(&[u8::from(migration.online)], hash);
            hash = fnv1a(migration.guard.unwrap_or("").as_bytes(), hash);
        }
        hash
    }

    /// Where the dump for this corpus lives, or `None` if caching is off or the path is unknown.
    ///
    /// Derived from the running test binary — `target/<profile>/deps/<name>` — rather than from
    /// `CARGO_TARGET_TMPDIR`, which cargo sets for integration tests but not for the unit tests
    /// compiled into `src/`, and those are a third of the binaries this needs to serve. Living
    /// under the target directory means it is already ignored, already per-workspace, and
    /// already removed by `cargo clean`.
    pub(super) fn path(image: (&str, &str)) -> Option<PathBuf> {
        if std::env::var_os(super::NO_BASELINE_CACHE_ENV).is_some() {
            return None;
        }
        let exe = std::env::current_exe().ok()?;
        let dir = exe.parent()?.parent()?;
        Some(dir.join(format!("dbos-baseline-{:016x}.sql", key(image))))
    }

    /// The cached dump, if one is there and readable.
    pub(super) fn load(path: &Path) -> Option<String> {
        let sql = std::fs::read_to_string(path).ok()?;
        (!sql.trim().is_empty()).then_some(sql)
    }

    /// Writes the dump so that no reader can observe a partial one.
    ///
    /// Write-then-rename, because `rename` within a directory is atomic: a reader sees the old
    /// file or the new one, never half of either. Two processes racing here both write the same
    /// bytes — same key, same schema — so whichever rename lands last is equally correct.
    ///
    /// Failures are ignored. A cache that cannot be written is a slower suite, and turning that
    /// into a test failure would be trading a real guarantee for a performance one.
    pub(super) fn store(path: &Path, sql: &str) {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        let temporary = path.with_file_name(format!("{name}.{}.tmp", std::process::id()));
        if std::fs::write(&temporary, sql).is_ok() && std::fs::rename(&temporary, path).is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
    }
}

/// A running database server, shared by every test holding an [`Arc`] of it.
///
/// Dropping the last `Arc` removes the container.
pub struct TestServer {
    backend: Backend,
    host: String,
    port: u16,
    /// How many migrated databases may exist at once.
    ///
    /// Uncapped, the count would follow peak parallelism — twelve on a twelve-core machine —
    /// so a lease past the cap waits for a database to come back rather than widening the pool.
    permits: Arc<tokio::sync::Semaphore>,
    /// How a pooled database is built, derived once from a database migrated for real.
    ///
    /// A [`OnceCell`](tokio::sync::OnceCell) rather than eager setup in [`TestServer::start`]:
    /// a binary whose tests all take the raw lane — the migration tests do — must not pay for
    /// a migration it never uses.
    baseline: tokio::sync::OnceCell<Baseline>,
    /// Migrated databases not currently leased, by name.
    ///
    /// A `std::sync::Mutex` rather than an async one because it is only ever held long enough
    /// to push or pop a name, never across an await — and returning one happens in `Drop`,
    /// which cannot await at all.
    idle: std::sync::Mutex<Vec<String>>,
    /// What this server spent, written out when it shuts down. See [`TIMINGS_ENV`].
    timings: Timings,
    /// Held to keep the container alive; removal is tied to this field's `Drop`.
    _container: ContainerAsync<GenericImage>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.timings.write(self.backend);
    }
}

/// A value created on demand, shared by everyone holding it, and released once nobody is.
///
/// The whole point is the `Weak`: an `Arc` parked here would keep the value alive until
/// the process exits, which for a container means one left running per test run. Kept
/// generic and separate from [`TestServer`] so the release semantics can be tested for
/// what they are, without a container and without racing the tests that use the real one.
pub struct SharedSlot<T> {
    inner: Mutex<Weak<T>>,
}

impl<T> SharedSlot<T> {
    pub const fn new() -> Self {
        Self {
            inner: Mutex::const_new(Weak::new()),
        }
    }

    /// The live value, creating it with `make` only if nothing currently holds one.
    ///
    /// The lock is deliberately held across `make`: concurrent first-callers must queue
    /// behind one creation rather than racing to start several containers.
    pub async fn get_or_init<F, Fut>(&self, make: F) -> Arc<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut slot = self.inner.lock().await;
        if let Some(existing) = slot.upgrade() {
            return existing;
        }
        let value = Arc::new(make().await);
        *slot = Arc::downgrade(&value);
        value
    }
}

impl<T> Default for SharedSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

static SHARED: SharedSlot<TestServer> = SharedSlot::new();

/// The CockroachDB schema dump, kept for the life of the process rather than the server.
///
/// **Deliberately not a `SharedSlot`.** The container must go away when the last test drops it,
/// which is what the `Weak` above is for; a dump is a `String` with nothing to leak, and keeping
/// it is what makes a mid-binary restart cost a replay instead of a corpus run. Separating the
/// two lifetimes is the whole point — the expensive artifact should not die with the cheap one.
static CACHED_DUMP: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The shared server for this test binary, starting it if no test currently holds one.
pub async fn shared_server() -> Arc<TestServer> {
    SHARED
        .get_or_init(|| TestServer::start(Backend::from_env()))
        .await
}

/// A database with the DBOS schema already applied — **the lane almost every test wants.**
///
/// The contract is: you get a working, empty DBOS schema, and you do not care how. It is a lease
/// from a pool of pre-migrated databases, reset on acquire and returned when the handle drops —
/// because re-running the migrations per test costs tens of seconds on CockroachDB, where DDL
/// is an online schema change.
///
/// Reach for this unless you specifically need one of the things the pool takes away: an
/// unmigrated database, or the freedom to run your own DDL. Those are [`raw_database`].
pub async fn test_database() -> TestDatabase {
    shared_server().await.lease_migrated().await
}

/// A fresh, unmigrated database of its own, never pooled and never reset.
///
/// For tests that need the schema *absent* or that leave a database unfit to hand to anyone
/// else: migration tests, and anything creating its own tables.
///
/// It is not expensive — `CREATE DATABASE` is ~7 ms on Postgres and ~100 ms on CockroachDB,
/// against seconds to start a container — so this shares the same container as everything
/// else. What it costs is that the caller pays for any migrations it wants.
pub async fn raw_database() -> TestDatabase {
    shared_server().await.create_database().await
}

impl TestServer {
    async fn start(backend: Backend) -> Self {
        let timings = Timings::new();
        let booting = Instant::now();
        let container = match backend {
            Backend::Postgres => {
                GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
                    .with_exposed_port(POSTGRES_PORT.tcp())
                    // The readiness poll below is the real gate. This message is only a
                    // cheap first filter: Postgres logs it once while initialising the
                    // data directory and again once it is actually listening, so waiting
                    // on it alone is a well-known way to connect too early.
                    .with_wait_for(WaitFor::message_on_stderr(
                        "database system is ready to accept connections",
                    ))
                    .with_env_var("POSTGRES_PASSWORD", "dbos")
                    .with_labels([CONTAINER_LABEL])
                    .start()
                    .await
            }
            Backend::Cockroach => {
                GenericImage::new(COCKROACH_IMAGE.0, COCKROACH_IMAGE.1)
                    .with_exposed_port(COCKROACH_PORT.tcp())
                    // An in-memory store, because nothing here outlives the container and
                    // CockroachDB's cost is dominated by DDL: the corpus is dozens of online
                    // schema changes, replayed in full to build a process's baseline and again
                    // for every migration test. Measured at roughly half the CockroachDB leg's
                    // wall clock, and two seconds off container startup besides.
                    //
                    // The size is a ceiling rather than a reservation. The suite's data is a few
                    // thousand rows; the headroom is for CockroachDB's own system ranges and the
                    // MVCC garbage a run leaves behind.
                    .with_cmd([
                        "start-single-node",
                        "--insecure",
                        "--store=type=mem,size=2GiB",
                    ])
                    .with_labels([CONTAINER_LABEL])
                    .start()
                    .await
            }
        }
        .expect("failed to start the database container — is Docker running?");
        Timings::add(&timings.boot_nanos, booting.elapsed());

        let host = container
            .get_host()
            .await
            .expect("container host unavailable")
            .to_string();
        let port = container
            .get_host_port_ipv4(backend.port())
            .await
            .expect("container port unavailable");

        let server = TestServer {
            backend,
            host,
            port,
            permits: Arc::new(tokio::sync::Semaphore::new(POOL_SIZE)),
            baseline: tokio::sync::OnceCell::new(),
            idle: std::sync::Mutex::new(Vec::new()),
            timings,
            _container: container,
        };
        let waiting = Instant::now();
        server.await_ready().await;
        Timings::add(&server.timings.ready_nanos, waiting.elapsed());
        server
    }

    /// Polls until the server accepts a connection, or panics after [`READY_TIMEOUT`].
    ///
    /// Both backends need this. Postgres logs "ready to accept connections" twice, and
    /// CockroachDB accepts TCP before it will serve SQL, so a log-message wait alone is
    /// racy on either.
    async fn await_ready(&self) {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last_error = None;
        while Instant::now() < deadline {
            match self.admin_options("postgres").connect().await {
                Ok(conn) => {
                    use sqlx::Connection;
                    let _ = conn.close().await;
                    return;
                }
                Err(e) => last_error = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "{:?} did not accept connections within {:?}: {}",
            self.backend,
            READY_TIMEOUT,
            last_error.map_or_else(|| "no error recorded".to_owned(), |e| e.to_string()),
        );
    }

    fn admin_options(&self, database: &str) -> PgConnectOptions {
        let opts = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(self.backend.admin_user())
            .database(database);
        match self.backend {
            // The image sets this password; Cockroach runs insecure and wants none.
            Backend::Postgres => opts.password("dbos"),
            Backend::Cockroach => opts,
        }
    }

    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// Stable identity of the underlying container, for asserting that sharing works.
    pub fn container_id(&self) -> &str {
        self._container.id()
    }

    /// Creates a fresh database and returns a handle to it.
    ///
    /// The handle keeps this server alive, so a test may hold only the database.
    pub async fn create_database(self: &Arc<Self>) -> TestDatabase {
        self.create_database_from(None).await
    }

    /// Creates a fresh database, optionally as a copy of `template`.
    ///
    /// Copying is PostgreSQL's `TEMPLATE`, which is a file-level copy of a database nothing is
    /// connected to — 0.1s against the 2.9s of migrating one. CockroachDB has no equivalent
    /// (`unsupported template`, cockroachdb/cockroach#10151), which is why [`Baseline`] has a
    /// second arm rather than one shared implementation.
    async fn create_database_from(self: &Arc<Self>, template: Option<&str>) -> TestDatabase {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!("dbos_test_{}", NEXT.fetch_add(1, Ordering::Relaxed));

        // sqlx 0.9 requires a query string to be `&'static str` or explicitly asserted
        // safe. Both names are generated here, never taken from input.
        let sql = match template {
            Some(template) => format!(r#"CREATE DATABASE "{name}" TEMPLATE "{template}""#),
            None => format!(r#"CREATE DATABASE "{name}""#),
        };

        // Retried, because `TEMPLATE` fails outright while any session is still attached to the
        // source. The pool that migrated it is closed before we get here, but a PostgreSQL
        // backend exits a moment after its client disconnects and is still in `pg_stat_activity`
        // until it does — a race that shows up as an occasional failed run rather than a
        // reproducible one. A plain create has no such failure mode and simply never retries.
        let mut last_error = None;
        for _ in 0..CREATE_DATABASE_ATTEMPTS {
            let mut conn = self
                .admin_options("postgres")
                .connect()
                .await
                .expect("failed to connect as admin");
            let result = sqlx::raw_sql(AssertSqlSafe(sql.clone()))
                .execute(&mut conn)
                .await;
            {
                use sqlx::Connection;
                let _ = conn.close().await;
            }
            match result {
                Ok(_) => {
                    return TestDatabase {
                        url: self.url_for(&name),
                        options: self.admin_options(&name),
                        server: Arc::clone(self),
                        name,
                        pooled: false,
                        permit: None,
                    };
                }
                // `object_in_use` — someone is still connected to the template.
                Err(e) if template.is_some() && has_code(&e, "55006") => {
                    last_error = Some(e);
                    tokio::time::sleep(TEMPLATE_RETRY_WAIT).await;
                }
                Err(e) => panic!("failed to create database {name}: {e}"),
            }
        }
        panic!(
            "failed to create database {name} from a template after \
             {CREATE_DATABASE_ATTEMPTS} attempts: {}",
            last_error.expect("the loop only exits here after an error"),
        );
    }

    /// How a pooled database is built on this server, deriving it on the first call.
    ///
    /// Deriving it means migrating one database for real, which is the only full corpus run a
    /// process makes. That database is then kept and never leased: PostgreSQL cannot copy a
    /// template anything is connected to, and letting CockroachDB alone hand it out would buy
    /// one replay — under two seconds — for a second code path.
    async fn baseline(self: &Arc<Self>) -> &Baseline {
        self.baseline
            .get_or_init(|| async {
                let building = Instant::now();
                // A dump outlives the server it came from, so a process that starts a second
                // one does not migrate again. The crate's own unit tests are the case: libtest
                // orders by name, the modules wanting a database are spread through the run,
                // and the pure tests between them release the last handle — so the container is
                // removed and restarted mid-binary, and a second corpus run costs about 22s of
                // the CockroachDB leg.
                //
                // Sound because the dump is portable SQL, derived from a database the real
                // runner migrated in this same process: a fresh server replaying it lands on
                // the schema `migrations/` defines, which is the property
                // `a_pooled_database_matches_one_the_migrations_built` asserts either way.
                // PostgreSQL has nothing to cache — its baseline is a template *database*,
                // which belongs to the container that holds it — and needs none, at 0.6s
                // against CockroachDB's 19s.
                if self.backend == Backend::Cockroach
                    && let Some(sql) = CACHED_DUMP.get()
                {
                    Timings::add(&self.timings.baseline_nanos, building.elapsed());
                    return Baseline::Replay(sql.clone());
                }
                // Then the same dump left by an earlier process, which is what spares every
                // test binary after the first its corpus run. Cheap enough to be worth trying
                // before starting a database: a file read against ~19s of online DDL.
                let cache = (self.backend == Backend::Cockroach)
                    .then(|| baseline_cache::path(COCKROACH_IMAGE))
                    .flatten();
                if let Some(path) = cache.as_deref()
                    && let Some(sql) = baseline_cache::load(path)
                {
                    let sql = CACHED_DUMP.get_or_init(|| sql).clone();
                    Timings::add(&self.timings.baseline_nanos, building.elapsed());
                    return Baseline::Replay(sql);
                }
                let db = self.create_database().await;
                let pool = db.pool().await;
                let migrating = Instant::now();
                dbos::sysdb::migrations::runner::run(&pool, DEFAULT_SCHEMA, true)
                    .await
                    .expect("failed to migrate the baseline database");
                Timings::add(&self.timings.migrate_nanos, migrating.elapsed());
                let baseline = match self.backend {
                    Backend::Postgres => Baseline::Template(db.name.clone()),
                    Backend::Cockroach => {
                        let dumping = Instant::now();
                        let sql = dump_schema(&pool, DEFAULT_SCHEMA).await;
                        Timings::add(&self.timings.dump_nanos, dumping.elapsed());
                        // Ignored if another server got there first: both dumps describe the
                        // same schema, so which one wins does not matter.
                        let _ = CACHED_DUMP.set(sql.clone());
                        if let Some(path) = cache.as_deref() {
                            baseline_cache::store(path, &sql);
                        }
                        Baseline::Replay(sql)
                    }
                };
                pool.close().await;
                Timings::add(&self.timings.baseline_nanos, building.elapsed());
                // `db` drops here. It is not pooled, so nothing hands it back and the database
                // stays on the server — which is exactly what a template needs.
                baseline
            })
            .await
    }

    /// Creates a database already in the state the migrations leave one in, without running
    /// them.
    async fn create_migrated(self: &Arc<Self>) -> TestDatabase {
        // Resolved before the clock starts: building the baseline is the fixed cost of the
        // binary, and charging the first pooled database for it would make the two
        // indistinguishable — which is the distinction this measurement exists to draw.
        let baseline = self.baseline().await;
        let building = Instant::now();
        let db = match baseline {
            Baseline::Template(template) => self.create_database_from(Some(template)).await,
            Baseline::Replay(schema_sql) => {
                let db = self.create_database().await;
                let pool = db.pool().await;
                // The dump carries no `CREATE SCHEMA` of its own — `SHOW CREATE ALL TABLES`
                // emits tables and nothing that holds them.
                let sql = format!(
                    "CREATE SCHEMA IF NOT EXISTS {};\n{schema_sql}",
                    quote_identifier(DEFAULT_SCHEMA),
                );
                sqlx::raw_sql(AssertSqlSafe(sql))
                    .execute(&pool)
                    .await
                    .expect("failed to apply the schema baseline");
                pool.close().await;
                db
            }
        };
        self.timings.pooled_builds.fetch_add(1, Ordering::Relaxed);
        Timings::add(&self.timings.pooled_build_nanos, building.elapsed());
        db
    }

    /// Leases a database with the DBOS schema applied, building one if the pool is empty.
    ///
    /// **Reusing a database rather than building one per test is the point.** CockroachDB
    /// applies schema changes online, so even the baseline path is not free — and the corpus
    /// itself takes tens of seconds there. Paying either per test would make the CockroachDB
    /// leg unusable long before the suite is finished.
    ///
    /// Cleaning happens here rather than on release: `Drop` cannot await, and a test that
    /// panics would skip its own cleanup. Doing it on acquire means a database is always
    /// clean when handed out, however the last holder ended.
    pub async fn lease_migrated(self: &Arc<Self>) -> TestDatabase {
        // Waits when the pool is full. Held by the returned handle and released on drop,
        // after the name has gone back on the idle list — so whoever is waiting finds a
        // database to recycle rather than migrating another.
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the pool semaphore is never closed");
        let recycled = self.idle.lock().ok().and_then(|mut idle| idle.pop());
        let mut db = match recycled {
            Some(name) => TestDatabase {
                url: self.url_for(&name),
                options: self.admin_options(&name),
                server: Arc::clone(self),
                name,
                pooled: true,
                permit: None,
            },
            // Only reachable POOL_SIZE times: past that a permit implies an idle database.
            None => self.create_migrated().await,
        };
        db.pooled = true;
        db.permit = Some(permit);
        let resetting = Instant::now();
        db.reset().await;
        Timings::add(&self.timings.reset_nanos, resetting.elapsed());
        self.timings.leases.fetch_add(1, Ordering::Relaxed);
        db
    }

    fn url_for(&self, database: &str) -> String {
        let (user, password) = match self.backend {
            Backend::Postgres => ("postgres", ":dbos"),
            Backend::Cockroach => ("root", ""),
        };
        format!(
            "postgresql://{user}{password}@{}:{}/{database}",
            self.host, self.port
        )
    }
}

/// A database on the shared server, for one test's exclusive use.
///
/// Holding it keeps the server alive. A leased database returns to the pool when dropped; one
/// from [`raw_database`] is simply abandoned, and goes away with the container.
pub struct TestDatabase {
    name: String,
    url: String,
    options: PgConnectOptions,
    server: Arc<TestServer>,
    /// Whether to hand this back for reuse when dropped.
    pooled: bool,
    /// Pool slot, released when this handle drops.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if !self.pooled {
            return;
        }
        // Returning is just handing the name back; the *cleaning* happens when the next test
        // takes it. Resetting here would need to await inside `Drop`, and a test that panics
        // would skip it — this way a dirty database is always cleaned before it is used.
        if let Ok(mut idle) = self.server.idle.lock() {
            idle.push(std::mem::take(&mut self.name));
        }
    }
}

impl TestDatabase {
    /// Empties every DBOS table, leaving the schema in place.
    ///
    /// Called automatically when a database is leased. Public so a test can exercise it
    /// directly: which database a lease returns is not predictable while tests run in
    /// parallel, so asserting on reuse is not a thing a test can do.
    ///
    /// `DELETE`, not `TRUNCATE`: CockroachDB implements `TRUNCATE` as a schema change, so it
    /// prices like `CREATE INDEX` however few rows a table holds — measured around 3x slower
    /// than the equivalent deletes on this schema. `TRUNCATE` would win only once a table is
    /// big enough for row count to dominate, which no test fixture is.
    ///
    /// The table list comes from the catalogue rather than a hard-coded list, so a migration
    /// that adds a table cannot silently leave it uncleaned. Deleting from all of them in any
    /// order is safe — emptying everything cannot strand a foreign key.
    pub async fn reset(&self) {
        let pool = self.pool().await;
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name <> 'dbos_migrations' \
             ORDER BY table_name",
        )
        .bind(DEFAULT_SCHEMA)
        .fetch_all(&pool)
        .await
        .expect("failed to list tables to reset");

        if !tables.is_empty() {
            let schema = quote_identifier(DEFAULT_SCHEMA);
            let stmts: String = tables
                .iter()
                .map(|t| format!("DELETE FROM {schema}.{};", quote_identifier(t)))
                .collect();
            sqlx::raw_sql(AssertSqlSafe(stmts))
                .execute(&pool)
                .await
                .expect("failed to reset the leased database");
        }
        pool.close().await;
    }

    /// Connection URL, for code that takes one as configuration.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Connection options for this database.
    pub fn options(&self) -> PgConnectOptions {
        self.options.clone()
    }

    pub fn backend(&self) -> Backend {
        self.server.backend()
    }

    pub fn server(&self) -> &Arc<TestServer> {
        &self.server
    }

    /// Opens a pool against this database.
    pub async fn pool(&self) -> sqlx::PgPool {
        self.pool_options()
            .connect_with(self.options.clone())
            .await
            .expect("failed to connect to the test database")
    }

    /// The pool settings [`pool`](Self::pool) uses, for tests that need to vary them.
    pub fn pool_options(&self) -> sqlx::pool::PoolOptions<sqlx::Postgres> {
        sqlx::pool::PoolOptions::new().max_connections(5)
    }

    /// Opens a connection outside any pool, for acting on the server rather than through it.
    pub async fn admin_connection(&self) -> sqlx::PgConnection {
        use sqlx::Connection;
        sqlx::PgConnection::connect_with(&self.options)
            .await
            .expect("failed to open an admin connection")
    }

    /// Terminates the connections tagged with `application_name`, killing them where they sit.
    ///
    /// Java's `ChaosTest.causeChaos` kills everything on the database. That cannot work here:
    /// tests run in parallel against a shared server, and CockroachDB's session list is
    /// **cluster-wide** with no database column — killing by database would take out unrelated
    /// tests. Tagging the pool under test and killing only that tag is scoped correctly on both
    /// backends, and the admin connection below is untagged, so it does not kill itself.
    pub async fn kill_connections(&self, application_name: &str) {
        use sqlx::Executor;
        let mut admin = self.admin_connection().await;
        let sql = match self.backend() {
            Backend::Cockroach => format!(
                "CANCEL SESSIONS (SELECT session_id FROM [SHOW CLUSTER SESSIONS] \
                 WHERE application_name = '{application_name}')"
            ),
            Backend::Postgres => format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE application_name = '{application_name}' AND pid <> pg_backend_pid()"
            ),
        };
        admin
            .execute(sqlx::AssertSqlSafe(sql))
            .await
            .expect("failed to terminate connections");
    }

    /// How many connections tagged with `application_name` are open on the server.
    ///
    /// The counting half of [`kill_connections`](Self::kill_connections), and tagged for the same
    /// reason: tests share a server, so "how many connections are open" is only a question a test
    /// can answer about its own. Put the tag on the pool under test by appending
    /// `?application_name=…` to the URL it is given.
    ///
    /// The admin connection below is untagged, so it does not count itself.
    pub async fn connection_count(&self, application_name: &str) -> i64 {
        let sql = match self.backend() {
            Backend::Cockroach => format!(
                "SELECT count(*) FROM [SHOW CLUSTER SESSIONS] \
                 WHERE application_name = '{application_name}'"
            ),
            Backend::Postgres => format!(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE application_name = '{application_name}'"
            ),
        };
        let mut admin = self.admin_connection().await;
        sqlx::query_scalar(AssertSqlSafe(sql))
            .fetch_one(&mut admin)
            .await
            .expect("failed to count connections")
    }
}

#[cfg(test)]
mod baseline_cache_tests {
    use super::baseline_cache::{key, load, store};

    const IMAGE: (&str, &str) = ("cockroachdb/cockroach", "latest-v26.2");

    /// The key must be a function of its inputs and nothing else, or two binaries in one run
    /// disagree about which file to read.
    #[test]
    fn the_same_inputs_give_the_same_key() {
        assert_eq!(key(IMAGE), key(IMAGE));
    }

    /// **The server version is part of the schema's identity.** The dump is `SHOW CREATE ALL
    /// TABLES` output, whose syntax belongs to the server that produced it, so replaying one
    /// version's into another is exactly the mistake this half of the key exists to prevent.
    #[test]
    fn a_different_server_version_gets_a_different_key() {
        assert_ne!(key(IMAGE), key(("cockroachdb/cockroach", "latest-v25.1")));
        assert_ne!(key(IMAGE), key(("cockroachdb/cockroach-unstable", IMAGE.1)));
    }

    /// A cached dump has to come back byte for byte: it is replayed as SQL, so anything less
    /// is a schema that silently differs from the one the migrations build.
    #[test]
    fn a_stored_dump_loads_back_unchanged() {
        let dir = std::env::temp_dir().join(format!("dbos-cache-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("failed to create the test directory");
        let path = dir.join("baseline.sql");
        let sql = "CREATE TABLE dbos.t (a INT8);\nINSERT INTO dbos.dbos_migrations VALUES (107);\n";

        assert_eq!(
            load(&path),
            None,
            "nothing is cached before anything is stored"
        );
        store(&path, sql);
        assert_eq!(load(&path).as_deref(), Some(sql));

        // Whitespace-only is treated as absent: a zero-length file is what a torn write or an
        // out-of-space failure leaves behind, and replaying it would produce an empty schema
        // that fails much later and much less clearly.
        store(&path, "   \n");
        assert_eq!(load(&path), None);

        std::fs::remove_dir_all(&dir).expect("failed to clean up the test directory");
    }

    /// Writing must not leave the temporary file behind, or the target directory accumulates
    /// one per process for the life of the checkout.
    #[test]
    fn storing_leaves_only_the_cache_file() {
        let dir = std::env::temp_dir().join(format!("dbos-cache-tidy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("failed to create the test directory");
        let path = dir.join("baseline.sql");
        store(&path, "CREATE TABLE dbos.t (a INT8);\n");

        let left: Vec<_> = std::fs::read_dir(&dir)
            .expect("failed to list the test directory")
            .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
            .collect();
        assert_eq!(left, vec!["baseline.sql".to_owned()]);

        std::fs::remove_dir_all(&dir).expect("failed to clean up the test directory");
    }
}
