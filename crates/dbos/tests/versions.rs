//! An application reading its version registry and promoting a version, against real databases.

use dbos::{Config, DBOS, Error};

use dbos_test_support::{TestDatabase, test_database};

fn config(app_name: &str, version: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(version.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

/// Launches and shuts down one executor per version, in order, so each registers its version.
async fn deploy(app_name: &str, versions: &[&str], db: &TestDatabase) {
    for version in versions {
        let dbos = DBOS::new(config(app_name, version, db));
        dbos.launch().await.expect("launch failed");
        dbos.shutdown().await;
    }
}

async fn latest(dbos: &DBOS) -> String {
    dbos.latest_app_version()
        .await
        .expect("read failed")
        .expect("versions were registered at launch")
        .version_name
}

/// **An application reads its versions and rolls back by promoting an older one.**
#[tokio::test]
async fn an_application_lists_its_versions_and_promotes_an_older_one() {
    let db = test_database().await;
    deploy("versions-app", &["v1"], &db).await;
    let dbos = DBOS::new(config("versions-app", "v2", &db));
    dbos.launch().await.expect("launch failed");

    let names: Vec<String> = dbos
        .list_app_versions()
        .await
        .expect("list failed")
        .into_iter()
        .map(|version| version.version_name)
        .collect();
    assert_eq!(names, ["v2", "v1"], "newest first");
    assert_eq!(latest(&dbos).await, "v2");

    dbos.set_latest_app_version("v1")
        .await
        .expect("promote failed");
    assert_eq!(latest(&dbos).await, "v1", "promoting rolls the fleet back");
    assert_eq!(
        dbos.app_version().unwrap(),
        "v2",
        "this executor still runs its own"
    );

    dbos.shutdown().await;
}

/// The version calls need a launched instance, as every call that reaches the database does.
#[tokio::test]
async fn the_version_calls_are_refused_before_launch() {
    let db = test_database().await;
    let dbos = DBOS::new(config("versions-unlaunched", "v1", &db));
    assert!(matches!(
        dbos.list_app_versions().await,
        Err(Error::NotLaunched { .. })
    ));
    assert!(matches!(
        dbos.latest_app_version().await,
        Err(Error::NotLaunched { .. })
    ));
    assert!(matches!(
        dbos.set_latest_app_version("v1").await,
        Err(Error::NotLaunched { .. })
    ));
    assert!(matches!(
        dbos.set_latest_app_version_for("v1", "versions-peer").await,
        Err(Error::NotLaunched { .. })
    ));
}
