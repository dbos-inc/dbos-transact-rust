//! An application reading its version registry and promoting a version, against real databases.

use dbos::{Config, DBOS, Error};

use dbos_test_support::{TestDatabase, skewed_database, test_database};

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

/// **A peer application's version is refused, unless the promotion names the peer.**
#[tokio::test]
async fn an_application_promotes_a_peers_version_only_by_naming_it() {
    let db = test_database().await;
    deploy("versions-peer", &["p1", "p2"], &db).await;
    let dbos = DBOS::new(config("versions-own", "v1", &db));
    dbos.launch().await.expect("launch failed");

    let refused = dbos.set_latest_app_version("p1").await;
    assert!(
        matches!(
            refused,
            Err(Error::SystemDatabase(
                dbos::sysdb::Error::RegisteredByAnother { .. }
            ))
        ),
        "{refused:?}"
    );
    dbos.set_latest_app_version_for("p1", "versions-peer")
        .await
        .expect("promote failed");
    dbos.shutdown().await;

    let peer = DBOS::new(config("versions-peer", "p2", &db));
    peer.launch().await.expect("launch failed");
    assert_eq!(latest(&peer).await, "p1");
    peer.shutdown().await;
}

/// **An application's promotion goes by the database's clock**, an hour ahead of this process's
/// here, as a client's does. Stamped by this process's clock it would sort below the version it
/// rolls back from.
#[tokio::test]
async fn an_application_promotes_on_the_database_clock() {
    let Some(db) = skewed_database(3600).await else {
        return;
    };
    let config = |version| Config {
        migrate: true,
        ..config("versions-skew", version, &db)
    };
    let first = DBOS::new(config("v1"));
    first.launch().await.expect("launch failed");
    first.shutdown().await;
    let dbos = DBOS::new(config("v2"));
    dbos.launch().await.expect("launch failed");

    dbos.set_latest_app_version("v1")
        .await
        .expect("promote failed");
    assert_eq!(latest(&dbos).await, "v1");

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
        dbos.set_latest_app_version("v1").await,
        Err(Error::NotLaunched { .. })
    ));
}
