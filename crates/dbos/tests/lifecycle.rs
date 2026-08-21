//! Launching and shutting down, against real databases.
//!
//! Uses the same container harness as the system database tests: a leased, migrated database per
//! test, Postgres locally and both backends in CI. Nothing here talks to a developer's own server.

use dbos::sysdb::{SystemDatabase, postgres::PostgresSystemDatabase, postgres::Settings};
use dbos::{Config, DBOS, Error};
use std::borrow::Cow;

use dbos_test_support::{TestDatabase, test_database};

/// A launched instance against a leased database.
///
/// `migrate: false` because the lease is already migrated — the harness pays that cost once per
/// container rather than once per test, which is what makes the CockroachDB leg usable at all.
async fn launched(app_name: &str) -> (DBOS, TestDatabase) {
    let db = test_database().await;
    let dbos = DBOS::new(config(app_name, &db));
    dbos.launch().await.expect("launch failed");
    (dbos, db)
}

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        ..Config::new(app_name, db.url())
    }
}

/// A handle for reading what launch wrote, independent of the instance under test.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// Launching connects, and the instance reports the identity it resolved.
#[tokio::test]
async fn launching_resolves_an_identity() {
    let (dbos, _db) = launched("lifecycle-app").await;

    assert!(dbos.is_launched());
    assert_eq!(
        dbos.executor_id().expect("launched"),
        "local",
        "the default every SDK shares"
    );

    let version = dbos.application_version().expect("launched");
    assert_eq!(version.len(), 64, "a SHA-256 in hex: {version}");

    dbos.shutdown().await;
}

/// Launch registers the running version, which is what makes an executor visible to a deploy.
#[tokio::test]
async fn launching_registers_the_application_version() {
    let (dbos, db) = launched("version-app").await;
    let version = dbos.application_version().expect("launched");

    let latest = reader(&db)
        .await
        .get_latest_application_version(Some("version-app"))
        .await
        .expect("read failed")
        .expect("launch should have registered a version");
    assert_eq!(latest.version_name, version);
    assert_eq!(latest.application_name.as_deref(), Some("version-app"));

    dbos.shutdown().await;
}

/// Registering is idempotent on the name, so a restart does not accumulate rows.
#[tokio::test]
async fn relaunching_registers_the_same_version_once() {
    let (dbos, db) = launched("relaunch-app").await;
    let version = dbos.application_version().expect("launched");
    dbos.shutdown().await;
    assert!(!dbos.is_launched(), "shutdown drops the executor");

    dbos.launch().await.expect("relaunch failed");
    assert_eq!(
        dbos.application_version().expect("launched"),
        version,
        "same binary, same version"
    );

    let versions = reader(&db)
        .await
        .list_application_versions()
        .await
        .expect("read failed");
    let ours: Vec<_> = versions
        .iter()
        .filter(|v| v.version_name == version)
        .collect();
    assert_eq!(ours.len(), 1, "two launches, one row: {versions:#?}");

    dbos.shutdown().await;
}

/// Relaunching in one process is supported, which is why the instance is not a type-state.
#[tokio::test]
async fn an_instance_outlives_the_executor_it_launched() {
    let (dbos, _db) = launched("outlive-app").await;
    let first = dbos.executor_id().expect("launched");
    dbos.shutdown().await;

    let err = dbos.application_version().unwrap_err();
    assert!(matches!(err, Error::NotLaunched { .. }), "{err}");

    dbos.launch().await.expect("relaunch failed");
    assert_eq!(dbos.executor_id().expect("launched"), first);
    dbos.shutdown().await;
}

/// Launching twice is a warning and a no-op, not an error: the usual cause is two entry points
/// both being defensive.
#[tokio::test]
async fn launching_twice_is_idempotent() {
    let (dbos, _db) = launched("idempotent-app").await;
    let version = dbos.application_version().expect("launched");

    dbos.launch()
        .await
        .expect("a second launch should be a no-op, not an error");
    assert_eq!(dbos.application_version().expect("launched"), version);

    dbos.shutdown().await;
}

/// Shutting down twice is equally uneventful.
#[tokio::test]
async fn shutting_down_twice_is_idempotent() {
    let (dbos, _db) = launched("shutdown-app").await;
    dbos.shutdown().await;
    dbos.shutdown().await;
    assert!(!dbos.is_launched());
}

/// An explicit version overrides the executable hash, which is what a deployment pins and what
/// development wants around a rebuild.
#[tokio::test]
async fn an_explicit_application_version_is_used_as_given() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        application_version: Some("v1.2.3".to_owned()),
        ..config("pinned-app", &db)
    });
    dbos.launch().await.expect("launch failed");

    assert_eq!(dbos.application_version().expect("launched"), "v1.2.3");
    let latest = reader(&db)
        .await
        .get_latest_application_version(Some("pinned-app"))
        .await
        .expect("read failed")
        .expect("a version should be registered");
    assert_eq!(latest.version_name, "v1.2.3");

    dbos.shutdown().await;
}

/// An explicit executor id identifies this process among the executors sharing a database.
#[tokio::test]
async fn an_explicit_executor_id_is_used_as_given() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        executor_id: Some("executor-7".to_owned()),
        ..config("executor-app", &db)
    });
    dbos.launch().await.expect("launch failed");
    assert_eq!(dbos.executor_id().expect("launched"), "executor-7");
    dbos.shutdown().await;
}

/// Two applications sharing a database each register their own version, and neither claims the
/// other's — the ownership half of §4.14, exercised end to end rather than argued from the column.
#[tokio::test]
async fn two_applications_sharing_a_database_own_their_own_versions() {
    let db = test_database().await;
    let one = DBOS::new(Config {
        application_version: Some("one-v1".to_owned()),
        ..config("app-one", &db)
    });
    let two = DBOS::new(Config {
        application_version: Some("two-v1".to_owned()),
        ..config("app-two", &db)
    });
    one.launch().await.expect("launch failed");
    two.launch().await.expect("launch failed");

    let reader = reader(&db).await;
    for (app, expected) in [("app-one", "one-v1"), ("app-two", "two-v1")] {
        let latest = reader
            .get_latest_application_version(Some(app))
            .await
            .expect("read failed")
            .expect("a version should be registered");
        assert_eq!(latest.version_name, expected);
        assert_eq!(latest.application_name.as_deref(), Some(app));
    }

    one.shutdown().await;
    two.shutdown().await;
}

/// Registration is a before-launch activity, because the executor holds a snapshot.
#[tokio::test]
async fn registering_after_launch_is_refused() {
    async fn noop(_: ()) -> dbos::Result<()> {
        Ok(())
    }

    let db = test_database().await;
    let dbos = DBOS::new(config("register-app", &db));
    dbos.register_workflow("before", noop)
        .expect("registration before launch is fine");

    dbos.launch().await.expect("launch failed");
    let err = dbos.register_workflow("after", noop).unwrap_err();
    assert!(
        matches!(
            err,
            Error::AlreadyLaunched {
                operation: Cow::Borrowed("register_workflow")
            }
        ),
        "{err}"
    );

    // And again after shutting down, since the instance outlives the executor.
    dbos.shutdown().await;
    dbos.register_workflow("after", noop)
        .expect("registration is open again once shut down");
}

/// Configuration is checked before anything is connected, so a bad name fails fast rather than
/// after a pool and a migration run.
#[tokio::test]
async fn an_invalid_application_name_is_refused_at_launch() {
    let db = test_database().await;
    let dbos = DBOS::new(config("Not A Valid Name", &db));

    let err = dbos.launch().await.unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
    assert!(!dbos.is_launched());
}

/// A failed launch leaves the instance launchable, rather than poisoned.
#[tokio::test]
async fn a_failed_launch_can_be_followed_by_a_good_one() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        database_url: String::new(),
        ..config("recover-app", &db)
    });
    assert!(dbos.launch().await.is_err());

    let dbos = DBOS::new(config("recover-app", &db));
    dbos.launch().await.expect("launch failed");
    assert!(dbos.is_launched());
    dbos.shutdown().await;
}
