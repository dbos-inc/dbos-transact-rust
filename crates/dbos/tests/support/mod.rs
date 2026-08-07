//! Test harness: a database server in a container, shared by every test that overlaps
//! with it in time.
//!
//! Two things about it are load-bearing rather than incidental, and both should survive
//! any rewrite:
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
//! Note that Rust compiles each `tests/*.rs` file into its own binary, so "the shared
//! server" is shared within a test binary, not across them. Prefer few, larger integration
//! test files over many small ones — each additional file is another container.
//!
//! A fresh database per test is only right while there is nothing to migrate. Once there
//! is, this becomes a pool of pre-migrated databases leased per test and truncated on
//! release (Java's model): migrating per test would put the whole CockroachDB online-DDL
//! cost straight back, which is the reason this harness exists at all.

#![allow(dead_code)] // Not every test binary uses every helper.

use std::future::Future;
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
/// Four is enough that tests overlap usefully and small enough that CockroachDB's setup stays
/// bounded: migrating four costs about 50s there against roughly 75s for twelve, and the
/// difference buys parallelism a suite this size cannot use.
pub const POOL_SIZE: usize = 4;

/// How long to wait for the server to accept connections after the container starts.
/// CockroachDB is the slow one; Postgres is usually ready in well under a second.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

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

/// A running database server, shared by every test holding an [`Arc`] of it.
///
/// Dropping the last `Arc` removes the container.
pub struct TestServer {
    backend: Backend,
    host: String,
    port: u16,
    /// How many migrated databases may exist at once.
    ///
    /// Each one costs a full migration run, which is seconds on PostgreSQL and around 40 of
    /// them on CockroachDB. Uncapped, the count would follow peak parallelism — twelve on a
    /// twelve-core machine — so a lease past the cap waits for a database to come back rather
    /// than paying to widen the pool.
    permits: Arc<tokio::sync::Semaphore>,
    /// Migrated databases not currently leased, by name.
    ///
    /// A `std::sync::Mutex` rather than an async one because it is only ever held long enough
    /// to push or pop a name, never across an await — and returning one happens in `Drop`,
    /// which cannot await at all.
    idle: std::sync::Mutex<Vec<String>>,
    /// Held to keep the container alive; removal is tied to this field's `Drop`.
    _container: ContainerAsync<GenericImage>,
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

/// The shared server for this test binary, starting it if no test currently holds one.
pub async fn shared_server() -> Arc<TestServer> {
    SHARED
        .get_or_init(|| TestServer::start(Backend::from_env()))
        .await
}

/// A database with the DBOS schema already applied — **the lane almost every test wants.**
///
/// The contract is: you get a working, empty DBOS schema, and you do not care how. Today
/// there are no migrations, so this hands out a fresh database and that is the whole story.
/// Once migrations exist it becomes a lease from a pool of pre-migrated databases, reset and
/// returned when the handle drops — because re-running the migrations per test costs tens of
/// seconds on CockroachDB, where DDL is an online schema change.
///
/// **The point of naming the lane now is that call sites will not change when that happens.**
/// Reach for this unless you specifically need one of the things the pool takes away: an
/// unmigrated database, or the freedom to run your own DDL. Those are [`raw_database`].
pub async fn test_database() -> TestDatabase {
    shared_server().await.lease_migrated().await
}

/// A fresh, unmigrated database of its own, never pooled and never reset.
///
/// For tests that need the schema *absent* or that leave a database unfit to hand to anyone
/// else: migration tests, and anything creating its own tables. This contract does not change
/// when the pool arrives, so a test written against it stays correct.
///
/// It is not expensive — `CREATE DATABASE` is ~7 ms on Postgres and ~100 ms on CockroachDB,
/// against seconds to start a container — so this shares the same container as everything
/// else. What it costs, when there are migrations, is that the caller pays for them.
pub async fn raw_database() -> TestDatabase {
    shared_server().await.create_database().await
}

impl TestServer {
    async fn start(backend: Backend) -> Self {
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
                    .with_cmd(["start-single-node", "--insecure"])
                    .with_labels([CONTAINER_LABEL])
                    .start()
                    .await
            }
        }
        .expect("failed to start the database container — is Docker running?");

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
            idle: std::sync::Mutex::new(Vec::new()),
            _container: container,
        };
        server.await_ready().await;
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
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!("dbos_test_{}", NEXT.fetch_add(1, Ordering::Relaxed));

        let mut conn = self
            .admin_options("postgres")
            .connect()
            .await
            .expect("failed to connect as admin");
        // sqlx 0.9 requires a query string to be `&'static str` or explicitly asserted
        // safe. The name is generated from a counter, never from input.
        sqlx::raw_sql(AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("failed to create database {name}: {e}"));
        {
            use sqlx::Connection;
            let _ = conn.close().await;
        }

        TestDatabase {
            url: self.url_for(&name),
            options: self.admin_options(&name),
            server: Arc::clone(self),
            name,
            pooled: false,
            permit: None,
        }
    }

    /// Leases a database with the DBOS schema applied, migrating one if the pool is empty.
    ///
    /// **Migrating once per database and reusing it is the point.** The corpus is 47
    /// migrations and CockroachDB applies schema changes online, so a full run there takes
    /// tens of seconds — paying that per test would make the CockroachDB leg unusable long
    /// before the suite is finished.
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
            None => {
                // Only reachable POOL_SIZE times: past that a permit implies an idle database.
                let fresh = self.create_database().await;
                let pool = fresh.pool().await;
                dbos::sysdb::migrations::runner::run(&pool, DEFAULT_SCHEMA, true)
                    .await
                    .expect("failed to migrate a pooled test database");
                pool.close().await;
                fresh
            }
        };
        db.pooled = true;
        db.permit = Some(permit);
        db.reset().await;
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
}
