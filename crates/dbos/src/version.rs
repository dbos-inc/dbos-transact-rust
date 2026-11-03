//! Application versions: which deployments of an application's code have announced themselves,
//! and which one a rolling deploy prefers.
//!
//! Every executor registers its version at launch, and the **latest** version, the one with the
//! highest timestamp, is what an enqueue that names no version runs on. Promoting a version moves
//! its timestamp past every other's, so promoting an older one is a rollback. Both an application
//! and a [`Client`] can read the registry and promote a version, as in the other implementations.

use crate::error::{Error, Result};
use crate::sysdb::types::VersionInfo;
use crate::{Client, DBOS};

impl Client {
    /// Every application version this client can see, newest first: the ones its own application
    /// registered, plus the unclaimed. A nameless client sees every one, which is the read half of
    /// what namelessness means — the same scoping [`list_queues`](Client::list_queues) has.
    ///
    /// What an operator reads to find out which versions of the code have ever announced
    /// themselves, and which one a workflow row's `app_version` refers to. A *named*
    /// client answers that question for its own application only — a peer's versions are not in
    /// the listing, and nothing here reads them, which is why a tool that has to see every
    /// application leaves [`app_name`](crate::ClientConfig::app_name) unset. Python and TypeScript scope
    /// their listing the same way, and take no target either.
    pub async fn list_app_versions(&self) -> Result<Vec<VersionInfo>> {
        self.connection()
            .sysdb()
            .list_application_versions()
            .await
            .map_err(Error::SystemDatabase)
    }

    /// The version a rolling deploy currently prefers, or `None` if none is registered.
    ///
    /// Scoped to this client's application, or across all of them when it is nameless. Latest by
    /// its registered timestamp rather than by creation, which is how a rollback is expressed.
    pub async fn latest_app_version(&self) -> Result<Option<VersionInfo>> {
        self.connection()
            .sysdb()
            .get_latest_application_version(self.connection().app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes an already-registered version to be the latest, by moving its timestamp to now on
    /// the database's clock, and past every other version's.
    ///
    /// The write half of [`latest_app_version`](Self::latest_app_version), and how
    /// a rolling deploy is steered from outside the fleet: the latest version is chosen by
    /// timestamp rather than by creation order, so promoting an older one is a **rollback** — the
    /// executors running it start dequeuing again, and an enqueue that names no version goes to it.
    ///
    /// The version must already exist; this does not register one. Registration is an executor's
    /// business, because a version is a claim about code that is running somewhere. **A name that
    /// matches nothing is not an error**, though: the write moves no row and reports success, so a
    /// misspelled rollback reads as a rollback. Every implementation discards the row count here;
    /// UPSTREAM item 2.
    ///
    /// Scoped to this client's application, as every write here is. Promoting a version another
    /// application registered fails rather than moving it — a timestamp is what a peer's fleet is
    /// rolling on, so it is not this client's to move without saying so. Saying so is
    /// [`set_latest_app_version_for`](Self::set_latest_app_version_for).
    ///
    /// **That scoping needs the client to have a name.** A client configured without an
    /// [`app_name`](crate::ClientConfig::app_name) has nothing to be refused under, so it
    /// promotes whatever version it names, a peer's included — which is either what namelessness
    /// is for or a shared gap, and UPSTREAM item 26 asks the team which.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// // Roll the fleet back to the version before the bad deploy.
    /// client.set_latest_app_version("1.4.2").await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_latest_app_version(&self, version_name: &str) -> Result<()> {
        self.connection()
            .sysdb()
            .promote_application_version(version_name, self.connection().app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes a version on behalf of the application that registered it.
    ///
    /// [`set_latest_app_version`](Self::set_latest_app_version) promotes within
    /// this client's own application; this one names the application to act as, which is what lets
    /// a single operator tool roll several applications on a shared system database without
    /// connecting a client per application.
    ///
    /// **The setter takes a name where the reader does not, and that asymmetry is the references'
    /// too** — Python's `set_latest_application_version` has an `application_name` keyword and
    /// TypeScript's an `applicationName` option, while neither `get_latest_application_version`
    /// takes anything. Promotion is the operation that *claims*: the write also adopts a version
    /// left unclaimed, which would otherwise read as every peer's latest. Reading claims nothing,
    /// so it has nothing to say a name about. Go is the one implementation whose client promotes
    /// without an override at all.
    ///
    /// A version some *third* application registered is still refused — naming an application is
    /// how a caller says which fleet it means, not a way around ownership.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// // One operator tool, rolling a peer application back.
    /// client.set_latest_app_version_for("1.4.2", "billing").await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_latest_app_version_for(
        &self,
        version_name: &str,
        app_name: &str,
    ) -> Result<()> {
        self.connection()
            .sysdb()
            .promote_application_version(version_name, Some(app_name))
            .await
            .map_err(Error::SystemDatabase)
    }
}

impl DBOS {
    /// Every application version this application can see, newest first: the ones it registered,
    /// plus the unclaimed. The same listing [`Client::list_app_versions`] gives a client named for
    /// this application.
    pub async fn list_app_versions(&self) -> Result<Vec<VersionInfo>> {
        self.executor("list application versions")?
            .connection()
            .sysdb()
            .list_application_versions()
            .await
            .map_err(Error::SystemDatabase)
    }

    /// The version a rolling deploy of this application currently prefers, or `None` if none is
    /// registered: latest by its timestamp rather than by creation, which is how a rollback is
    /// expressed. Not necessarily [`app_version`](Self::app_version), this executor's own.
    pub async fn latest_app_version(&self) -> Result<Option<VersionInfo>> {
        let executor = self.executor("read the latest application version")?;
        executor
            .connection()
            .sysdb()
            .get_latest_application_version(executor.connection().app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes an already-registered version of this application to be the latest, as
    /// [`Client::set_latest_app_version`] does: its timestamp moves to now on the database's
    /// clock, past every other version's.
    ///
    /// Promoting an older version is a rollback: the executors running it dequeue again, and an
    /// enqueue that names no version goes to it, this executor's included if it runs another. A
    /// version another application registered is refused, and a name that matches nothing reports
    /// success; see [`Client::set_latest_app_version`].
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// dbos.set_latest_app_version("1.4.2").await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_latest_app_version(&self, version_name: &str) -> Result<()> {
        let executor = self.executor("set the latest application version")?;
        executor
            .connection()
            .sysdb()
            .promote_application_version(version_name, executor.connection().app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes a version on behalf of the application that registered it, as
    /// [`Client::set_latest_app_version_for`] does: for an application acting for a peer on a
    /// shared system database. A version a third application registered is still refused.
    pub async fn set_latest_app_version_for(
        &self,
        version_name: &str,
        app_name: &str,
    ) -> Result<()> {
        self.executor("set the latest application version")?
            .connection()
            .sysdb()
            .promote_application_version(version_name, Some(app_name))
            .await
            .map_err(Error::SystemDatabase)
    }
}
