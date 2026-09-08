//! Queues: the workflows this application has asked for but has not yet run.
//!
//! A queue decouples asking for work from running it. A workflow started with
//! [`StartOptions::queue`](crate::StartOptions::queue) is recorded `ENQUEUED` and left there;
//! whichever executor next polls that queue claims it and runs it, under whatever limits the queue
//! carries. That is what makes a fan-out bounded, a backlog shared across a fleet, and a workflow
//! runnable by a process other than the one that asked for it.
//!
//! **Every queue here is database-backed, and that is a deliberate narrowing.** The other
//! implementations carry two kinds — an in-process registry of queues declared in code, and rows in
//! the `queues` table — and are moving users onto the second so the first can be deprecated. Rust
//! starts where they are going: a queue is a row, always, so there is one place a limit lives, one
//! answer to what a queue's concurrency currently is, and no question about which kind a name
//! refers to. It also removes the branch every reference runner carries, where a queue's
//! configuration is re-read per iteration only if it came from the database.
//!
//! The one queue with no row is [`INTERNAL_QUEUE`], which is the engine's own: `resume` and `fork`
//! put work there, and it is not a queue anybody registers.

use std::borrow::Cow;
use std::result::Result as StdResult;
use std::time::Duration;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::instance::DBOS;
use crate::sysdb::types::{
    Applications, Change, NewQueue, OnExistingQueue, QueueRecord, QueueUpdate, RateLimit,
};
use crate::sysdb::{Error as SysdbError, INTERNAL_QUEUE};

/// How often a queue is polled when nothing says otherwise.
///
/// One second in every implementation, and the floor a contended worker backs off from.
pub(crate) const DEFAULT_POLLING_INTERVAL: Duration = Duration::from_secs(1);

/// A registered queue, as the database holds it.
///
/// Returned by [`DBOS::register_queue`], and **read back from the row rather than echoed from the
/// request**: a registration that declined to overwrite an existing row returns what is actually
/// stored, which is what this executor's dequeues will honour. Go reads the row back for the same
/// reason.
///
/// Holding one is not what makes a queue work — the name is the address, and
/// [`StartOptions::queue`](crate::StartOptions::queue) takes a name. This is a receipt.
///
/// **Its own fields rather than a wrapped [`QueueRecord`]**, which is what Go's `queueFromConfig`,
/// TypeScript's `WorkflowQueue._fromRecord` and Python's `ResolvedQueueLimits` each build too. Two
/// reasons: this reports every limit at the scope it is enforced at, where the row spells them as
/// the columns a deprecated `partition_queue` flag re-scopes; and derived equality over a wrapped
/// row would compare [`application_name`](QueueRecord::application_name) and `partition_queue`,
/// neither of which this type reports, so two queues identical through every accessor here could
/// still differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queue {
    name: String,
    concurrency: Option<i32>,
    worker_concurrency: Option<i32>,
    rate_limit: Option<RateLimit>,
    priority_enabled: bool,
    partition_concurrency: Option<i32>,
    partition_worker_concurrency: Option<i32>,
    partition_rate_limit: Option<RateLimit>,
    polling_interval: Duration,
}

impl Queue {
    /// Builds the receipt from the row that was read back.
    ///
    /// **Reports the limits resolved, not as the row spells them.** For a queue this crate
    /// registered the two are the same. For one a peer wrote with the deprecated `partition_queue`
    /// flag they are not: that flag means every queue-wide limit applies per partition, so
    /// [`QueueRecord::resolved_limits`] moves them into the partition fields and the receipt says
    /// what the queue actually does rather than which columns happen to hold it.
    fn from_record(record: QueueRecord) -> Self {
        let name = record.name.clone();
        let polling_interval = record.polling_interval;
        let priority_enabled = record.priority_enabled;
        let limits = record.resolved_limits();
        Self {
            name,
            concurrency: limits.concurrency,
            worker_concurrency: limits.worker_concurrency,
            rate_limit: limits.rate_limit,
            priority_enabled,
            partition_concurrency: limits.partition_concurrency,
            partition_worker_concurrency: limits.partition_worker_concurrency,
            partition_rate_limit: limits.partition_rate_limit,
            polling_interval,
        }
    }

    /// The queue's name, which is its address.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How many of this queue's workflows may run at once **across the whole fleet**.
    pub fn concurrency(&self) -> Option<i32> {
        self.concurrency
    }

    /// How many of this queue's workflows may run at once **in one process**.
    pub fn worker_concurrency(&self) -> Option<i32> {
        self.worker_concurrency
    }

    /// How fast this queue's workflows may start, or `None` if it is unthrottled.
    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.rate_limit
    }

    /// Whether dequeue order honours a workflow's priority.
    pub fn priority_enabled(&self) -> bool {
        self.priority_enabled
    }

    /// Whether the queue is partitioned, which any per-partition limit makes it.
    pub fn is_partitioned(&self) -> bool {
        self.partition_concurrency.is_some()
            || self.partition_worker_concurrency.is_some()
            || self.partition_rate_limit.is_some()
    }

    /// How many of this queue's workflows may run at once **within one partition**, fleet-wide.
    pub fn partition_concurrency(&self) -> Option<i32> {
        self.partition_concurrency
    }

    /// How many may run at once within one partition **in one process**.
    pub fn partition_worker_concurrency(&self) -> Option<i32> {
        self.partition_worker_concurrency
    }

    /// How fast this queue's workflows may start within one partition, or `None` if unthrottled.
    pub fn partition_rate_limit(&self) -> Option<RateLimit> {
        self.partition_rate_limit
    }

    /// How often an executor polls this queue for work.
    pub fn polling_interval(&self) -> Duration {
        self.polling_interval
    }
}

/// What a queue allows, and how often it is polled.
///
/// Every field defaults to "no limit", which is what every implementation's bare registration
/// means. A queue with no limits still does something worth having: it is a place work can be left
/// for a fleet to pick up, rather than run by whoever asked.
///
/// # The four concurrency limits
///
/// They are one idea crossed two ways — over **scope**, the whole queue or one partition key, and
/// over **reach**, the whole fleet or this process alone:
///
/// |                       | whole queue                | one partition                          |
/// |-----------------------|----------------------------|----------------------------------------|
/// | **every executor**    | [`concurrency`][g]         | [`partition_concurrency`][p]           |
/// | **this process only** | [`worker_concurrency`][w]  | [`partition_worker_concurrency`][pw]   |
///
/// [g]: Self::concurrency
/// [p]: Self::partition_concurrency
/// [w]: Self::worker_concurrency
/// [pw]: Self::partition_worker_concurrency
///
/// **`worker_` is the whole of what says "this process"**, and its absence says every executor.
/// The fleet-wide pair is counted in the database against `PENDING` rows, which is what makes a
/// queue carrying either run its claim at `REPEATABLE READ`; the per-process pair is answered from
/// a local tally without a round trip, which is why it is the cheap limit.
///
/// **Every limit set is enforced, and the tightest wins.** They are not alternatives: a queue can
/// say "sixty at a time overall, four per customer, and no more than two of those in any one
/// process" by setting three of the four. A limit narrower in scope may not exceed the one that
/// contains it — a per-partition allowance above the queue-wide one could never bind — and that is
/// refused at registration rather than stored.
///
/// The rate limits pair the same way: [`rate_limit`](Self::rate_limit) governs the queue,
/// [`partition_rate_limit`](Self::partition_rate_limit) governs one key, and both are counted in
/// the database over a trailing window.
///
/// Setting any per-partition limit is what **partitions** the queue; there is no separate switch.
/// See [`partition_concurrency`](Self::partition_concurrency).
///
/// **Limits only.** What a registration does to a queue that already exists is a separate
/// [`QueueConflict`] argument to the call, on both surfaces: it is a property of the registration
/// rather than of the queue, and nothing else here differs between an application and a
/// [`Client`](crate::Client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOptions {
    /// How many of this queue's workflows may run at once across every executor.
    ///
    /// Counted in the database against `PENDING` rows, so it costs the dequeue a consistent read —
    /// a queue carrying this runs its claim at `REPEATABLE READ` rather than `READ COMMITTED`.
    ///
    /// **Fleet-wide**, which is what an unqualified concurrency limit means here: it counts the
    /// `PENDING` rows of every executor, not this process's.
    /// [`worker_concurrency`](Self::worker_concurrency) is the per-process one, and `worker_` is
    /// the whole of what marks it.
    ///
    /// Spelled as the column is. Python, TypeScript and Go renamed theirs `global_concurrency`
    /// once a per-partition limit existed, because under their deprecated `partition_queue` flag a
    /// bare `concurrency` silently *became* per-partition and the name had stopped being true.
    /// Nothing here re-scopes a limit, so this one is fleet-wide whatever else the queue carries
    /// and the qualifier would mark a distinction that does not exist.
    pub concurrency: Option<i32>,
    /// How many of this queue's workflows may run at once in **this** process.
    ///
    /// Counted locally, which is why it is the cheap limit: a process knows what it is running
    /// without asking the database. The starter app's Queues tab is this field.
    pub worker_concurrency: Option<i32>,
    /// How often this queue is polled when it is quiet.
    ///
    /// The floor rather than the cadence: a worker that meets contention backs off from here and
    /// scales back towards it, and jitters every wait so a fleet that started together does not
    /// poll in lockstep. One second in every implementation.
    pub polling_interval: Duration,
    /// How fast this queue's workflows may start, or `None` for unthrottled.
    ///
    /// Counted in the database against the starts in the trailing window, so — like
    /// [`concurrency`](Self::concurrency) — a queue carrying one runs its claim at
    /// `REPEATABLE READ` rather than `READ COMMITTED`. A queue already at its limit returns
    /// nothing without selecting anything.
    ///
    /// Queue-wide, so a partitioned queue may carry this *and* a
    /// [`partition_rate_limit`](Self::partition_rate_limit): both are enforced, and the
    /// per-partition one may not exceed it.
    pub rate_limit: Option<RateLimit>,
    /// Whether dequeue order honours a workflow's
    /// [priority](crate::Enqueue::priority), lowest first.
    ///
    /// Off by default, which is every implementation's default: an unprioritised queue is FIFO by
    /// `created_at`, and that is what most queues want.
    pub priority_enabled: bool,
    /// How many of this queue's workflows may run at once **within one partition**, across every
    /// executor.
    ///
    /// **Setting any of the three partition limits is what partitions a queue.** There is no
    /// separate switch: a partition limit is a statement that partitions exist, and the flag the
    /// row carries is derived from it. A workflow names its partition with
    /// [`Enqueue::partition_key`](crate::Enqueue::partition_key); work sharing a key contends for
    /// these limits, work under different keys does not.
    ///
    /// `partition_concurrency: Some(1)` is the one-workflow-per-key ordering that the deprecated
    /// `partition_queue` flag meant in the other implementations — see [`QueueRecord`] on how a
    /// row written by one of them is read here. It is also the only shape the batched sweep can
    /// dequeue; see [`SystemDatabase::start_queued_partitioned_workflows`].
    ///
    /// [`SystemDatabase::start_queued_partitioned_workflows`]:
    ///     crate::sysdb::SystemDatabase::start_queued_partitioned_workflows
    pub partition_concurrency: Option<i32>,
    /// How many of this queue's workflows may run at once within one partition, in **this**
    /// process.
    ///
    /// The per-partition counterpart of [`worker_concurrency`](Self::worker_concurrency), counted
    /// locally the same way, and may exceed neither that nor
    /// [`partition_concurrency`](Self::partition_concurrency).
    pub partition_worker_concurrency: Option<i32>,
    /// How fast this queue's workflows may start within one partition, or `None` for unthrottled.
    ///
    /// The per-partition counterpart of [`rate_limit`](Self::rate_limit), counted in the database
    /// against the starts in the trailing window that share the partition key, and may not allow
    /// a faster rate than that one does — the two periods need not match, so it is the rates that
    /// are compared and not the counts.
    pub partition_rate_limit: Option<RateLimit>,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            concurrency: None,
            worker_concurrency: None,
            polling_interval: DEFAULT_POLLING_INTERVAL,
            rate_limit: None,
            priority_enabled: false,
            partition_concurrency: None,
            partition_worker_concurrency: None,
            partition_rate_limit: None,
        }
    }
}

/// A change to a registered queue's limits, naming only what moves.
///
/// Every field defaults to [`Change::Leave`], so `..Default::default()` narrows an update rather
/// than widening it — an update assembled from optional inputs cannot accidentally clear a limit
/// it never mentioned. `Change::Set(None)` clears one deliberately; `Change::Set(Some(n))` sets it.
///
/// This is what the starter app's Apply button writes, and the reason a queue's configuration is a
/// row rather than a constant beside the workflows using it: the next sweep publishes it, and
/// every worker in the fleet picks it up on its next pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueChange {
    /// How many of this queue's workflows may run at once across every executor.
    pub concurrency: Change<Option<i32>>,
    /// How many may run at once in one process.
    pub worker_concurrency: Change<Option<i32>>,
    /// How often this queue is polled when it is quiet.
    pub polling_interval: Change<Duration>,
    /// How fast this queue's workflows may start.
    ///
    /// One field for both stored columns, so an update cannot leave half a limit behind.
    pub rate_limit: Change<Option<RateLimit>>,
    /// Whether dequeue order honours priority.
    pub priority_enabled: Change<bool>,
    /// How many may run at once within one partition, across every executor.
    ///
    /// **Setting or clearing this partitions or un-partitions the queue**, since partitioning is
    /// derived from the limits. Doing so to a queue with a backlog re-reads that backlog under
    /// different rules: rows already `ENQUEUED` keep whatever partition key they were given, so
    /// partitioning a queue whose backlog has no keys leaves that work in a single unnamed
    /// partition, and un-partitioning one releases every key's work at once. Neither is
    /// corruption, and neither is likely what was meant mid-flight.
    pub partition_concurrency: Change<Option<i32>>,
    /// How many may run at once within one partition, in one process.
    pub partition_worker_concurrency: Change<Option<i32>>,
    /// How fast this queue's workflows may start within one partition.
    ///
    /// One field for both stored columns, so an update cannot leave half a limit behind.
    pub partition_rate_limit: Change<Option<RateLimit>>,
}

/// What registering a queue that already exists does to the stored limits.
///
/// **Never to its owner.** A name already held by another application is
/// [`Error::SystemDatabase`] carrying `RegisteredByAnother` in every mode, because the name is the
/// queue's address across every application sharing the database — taking it would redirect a
/// peer's work.
///
/// **That holds for a caller with an application name of its own.** A nameless one — which is what
/// a [`Client`](crate::Client) is unless it was configured otherwise — is let through by the
/// ownership check every implementation shares, and then rewrites the stored limits of whatever
/// queue it names, a peer's included; only the owner column is left alone. UPSTREAM item 26 on
/// `resolve_owning_application` asks the team to settle whether that is the contract or the gap.
///
/// **The same type on both surfaces**, as in Python and TypeScript. A
/// [`Client`](crate::Client) can therefore name
/// [`UpdateIfLatestVersion`](Self::UpdateIfLatestVersion), which it has no version to answer, and
/// is refused when it does.
///
/// **Named at the call, never defaulted.** Python and TypeScript default it per surface — to
/// `update_if_latest_version` for an application (`_dbos.py:981`, `dbos.ts:2745`) and to
/// `always_update` for a client (`_client.py:382`, `client.ts:559`) — which one Rust type cannot
/// express, a default being a property of the type rather than of the caller. So a registration
/// says which it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueConflict {
    /// Overwrite only if this process is running the **latest registered application version**.
    ///
    /// An application's default, and the one that makes a rolling deploy behave. Two versions of an
    /// application run side by side during a deploy, and both register their queues at startup;
    /// without this, the old version's registration would keep reverting the new version's limits
    /// for as long as it lived. An application with no registered versions yet is the latest by
    /// default, since it is the first.
    ///
    /// **A [`Client`](crate::Client) is refused this**, having no version to be the latest of:
    /// [`Client::register_queue`](crate::Client::register_queue) returns [`Error::Config`], which
    /// is where Python and TypeScript raise on the same combination (`_client.py:455`,
    /// `client.ts:561`).
    UpdateIfLatestVersion,
    /// Always overwrite the stored limits.
    ///
    /// An operator's intent, and the usual answer for a registration made from outside the
    /// application: the limits it names take effect.
    AlwaysUpdate,
    /// Leave the stored limits alone.
    ///
    /// The registration still succeeds and still returns the queue — it just returns what is
    /// stored rather than what was asked for.
    NeverUpdate,
}

/// Rejects a queue configuration that cannot mean anything.
///
/// Its own function because every reference has one — Go's `validateQueueConfig`, Python's
/// `Queue._validate_queue`, TypeScript's `validateQueueParams` — and because the rate limit and
/// the partitioning rules put most of their checks here rather than anywhere else.
///
/// Two of these come from a minority of the references, and both are worth having:
///
/// - **The limits must be positive**, which only Java checks. It matters more here than there,
///   because the dequeue clamps its budgets with `.max(0)`: a `Some(0)` reaches the database, is
///   read back as a limit of nothing, and leaves a queue that silently never dequeues, with no
///   error and nothing in the log to explain it.
/// - **A fleet-wide limit cannot be below a per-process one**, which Python, TypeScript and Go all
///   check and Java does not. The pair is incoherent rather than merely useless: the smaller number
///   wins in the dequeue, so the configuration does not say what it appears to say.
fn validate(name: &str, options: &QueueOptions) -> Result<()> {
    validate_fields(options)
        .map_err(|(_, detail)| Error::Config(format!("queue `{name}`: {detail}")))
}

/// The rules themselves, naming the field that failed beside the message.
///
/// Split from [`validate`] because the two callers need the failure shaped differently: a
/// registration turns it into [`Error::Config`], and [`DBOS::update_queue`] hands it to the system
/// database as [`SysdbError::InvalidInput`], which names the field separately. The rules and
/// their wording live here once so the two paths cannot drift.
fn validate_fields(options: &QueueOptions) -> StdResult<(), (Cow<'static, str>, String)> {
    let refuse = |field: &'static str, message: String| Err((Cow::Borrowed(field), message));

    if let Some(global) = options.concurrency
        && global < 1
    {
        return refuse(
            "concurrency",
            format!("`concurrency` must be at least 1, got {global}"),
        );
    }
    if let Some(worker) = options.worker_concurrency
        && worker < 1
    {
        return refuse(
            "worker_concurrency",
            format!("`worker_concurrency` must be at least 1, got {worker}"),
        );
    }
    if let (Some(worker), Some(global)) = (options.worker_concurrency, options.concurrency)
        && worker > global
    {
        return refuse(
            "worker_concurrency",
            format!(
                "`concurrency` must be greater than or equal to `worker_concurrency`, \
                 got {global} and {worker}"
            ),
        );
    }
    if options.polling_interval.is_zero() {
        return refuse(
            "polling_interval",
            "`polling_interval` cannot be zero".to_owned(),
        );
    }
    if let Some(rate_limit) = options.rate_limit {
        if rate_limit.limit < 1 {
            return refuse(
                "rate_limit.limit",
                format!(
                    "`rate_limit.limit` must be at least 1, got {}",
                    rate_limit.limit
                ),
            );
        }
        if rate_limit.period.is_zero() {
            return refuse(
                "rate_limit.period",
                "`rate_limit.period` cannot be zero".to_owned(),
            );
        }
    }
    if let Some(partition) = options.partition_concurrency
        && partition < 1
    {
        return refuse(
            "partition_concurrency",
            format!("`partition_concurrency` must be at least 1, got {partition}"),
        );
    }
    if let Some(partition) = options.partition_worker_concurrency
        && partition < 1
    {
        return refuse(
            "partition_worker_concurrency",
            format!("`partition_worker_concurrency` must be at least 1, got {partition}"),
        );
    }
    if let Some(rate_limit) = options.partition_rate_limit {
        if rate_limit.limit < 1 {
            return refuse(
                "partition_rate_limit.limit",
                format!(
                    "`partition_rate_limit.limit` must be at least 1, got {}",
                    rate_limit.limit
                ),
            );
        }
        if rate_limit.period.is_zero() {
            return refuse(
                "partition_rate_limit.period",
                "`partition_rate_limit.period` cannot be zero".to_owned(),
            );
        }
    }
    // **A per-partition limit above its queue-wide counterpart never binds.** The queue-wide one
    // is reached first and is the only one that ever stops anything, so the per-partition number
    // would sit in the row saying something the dequeue can never do. Python and TypeScript
    // refuse the same four concurrency comparisons; the rate-limit pair below is Rust-only.
    if let (Some(partition), Some(worker)) = (
        options.partition_worker_concurrency,
        options.worker_concurrency,
    ) && partition > worker
    {
        return refuse(
            "partition_worker_concurrency",
            format!(
                "`worker_concurrency` must be greater than or equal to \
                 `partition_worker_concurrency`, got {worker} and {partition}"
            ),
        );
    }
    if let (Some(partition), Some(concurrency)) = (
        options.partition_worker_concurrency,
        options.partition_concurrency,
    ) && partition > concurrency
    {
        return refuse(
            "partition_worker_concurrency",
            format!(
                "`partition_concurrency` must be greater than or equal to \
                 `partition_worker_concurrency`, got {concurrency} and {partition}"
            ),
        );
    }
    if let (Some(partition), Some(global)) = (options.partition_concurrency, options.concurrency)
        && partition > global
    {
        return refuse(
            "partition_concurrency",
            format!(
                "`concurrency` must be greater than or equal to `partition_concurrency`, \
                 got {global} and {partition}"
            ),
        );
    }
    if let (Some(partition), Some(global)) =
        (options.partition_worker_concurrency, options.concurrency)
        && partition > global
    {
        return refuse(
            "partition_worker_concurrency",
            format!(
                "`concurrency` must be greater than or equal to \
                 `partition_worker_concurrency`, got {global} and {partition}"
            ),
        );
    }
    // The rate limits pair the same way, and `rate_limit`'s documentation says so — but the
    // comparison is not `>`, because a rate limit is a count over a window and the two windows
    // need not match. Compared as rates, cross-multiplied rather than divided so that neither
    // side loses precision, in `u128` so that neither product can overflow. Both `limit`s are
    // at least 1 by the checks above, so the casts are widening.
    //
    // Python and TypeScript stop after the four concurrency pairs and would store this one.
    // Refusing it here follows the rule the other four rest on rather than the reference
    // implementations, which is a deliberate divergence — `10/s` queue-wide with `100/s` per
    // partition is a row whose second number the dequeue can never reach.
    if let (Some(partition), Some(global)) = (options.partition_rate_limit, options.rate_limit)
        && u128::from(partition.limit.unsigned_abs()) * global.period.as_nanos()
            > u128::from(global.limit.unsigned_abs()) * partition.period.as_nanos()
    {
        return refuse(
            "partition_rate_limit",
            format!(
                "`rate_limit` must allow at least the rate `partition_rate_limit` does, \
                 got {}/{:?} and {}/{:?}",
                global.limit, global.period, partition.limit, partition.period
            ),
        );
    }
    Ok(())
}

impl DBOS {
    /// Registers a queue, or reports the one already registered under this name.
    ///
    /// **After [`launch`](DBOS::launch), unlike a workflow.** A workflow is registered *before*
    /// launch because the executor keeps a snapshot of the registry; a queue is a row, so
    /// registering one is a write and needs a launched instance to write it. The queue runner
    /// notices it on its next sweep, which is the same mechanism that lets a queue's limits be
    /// changed at runtime — see the Queues tab of the starter app, whose whole point is that
    /// `worker_concurrency` can be adjusted without a restart.
    ///
    /// Reserved: the engine's own [`INTERNAL_QUEUE`] cannot be registered. It has no row, takes no
    /// limits, and is where `resume` and `fork` leave work.
    ///
    /// `on_conflict` says what a re-registration does to a queue that already exists, and is
    /// named rather than defaulted — see [`QueueConflict`], whose
    /// [`UpdateIfLatestVersion`](QueueConflict::UpdateIfLatestVersion) is what an application
    /// registering at startup usually means.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let queue = dbos.register_queue("demo-queue", dbos::QueueOptions {
    ///     worker_concurrency: Some(3),
    ///     ..Default::default()
    /// }, dbos::QueueConflict::UpdateIfLatestVersion).await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_queue(
        &self,
        name: &str,
        options: QueueOptions,
        on_conflict: QueueConflict,
    ) -> Result<Queue> {
        let executor = self.executor("register a queue")?;
        executor
            .connection()
            .register_queue(name, options, on_conflict, Some(executor.app_version()))
            .await
    }

    /// The queue registered under this name, or `None` if there is none.
    ///
    /// Reads the row, so it reports what this executor's dequeues will actually honour — including
    /// changes a peer made since this process registered it.
    pub async fn queue(&self, name: &str) -> Result<Option<Queue>> {
        self.executor("read a queue")?
            .connection()
            .queue(name)
            .await
    }

    /// Every queue this application can dequeue from.
    ///
    /// Its own, plus the unclaimed ones. A peer application's queues are not listed: they are not
    /// this one's to poll, and reporting them would suggest otherwise.
    ///
    /// [`INTERNAL_QUEUE`] is **not** among them. It has no row, takes no limits, and is not a
    /// queue anybody registered.
    pub async fn list_queues(&self) -> Result<Vec<Queue>> {
        self.executor("list queues")?
            .connection()
            .list_queues()
            .await
    }

    /// Changes a registered queue's limits, leaving what the change does not name.
    ///
    /// **This is the "without restarting the app" half of the queue design.** Every worker re-reads
    /// its queue's row on every pass, so a limit written here takes effect across the whole fleet
    /// within a poll — no redeploy, no restart, and no per-process disagreement about what the
    /// limit is.
    ///
    /// The merged result is validated the way a registration is, so an update cannot leave the
    /// queue in a state [`register_queue`](Self::register_queue) would have refused.
    ///
    /// **The read, the merge, the validation and the write are one transaction.** The stored row
    /// is read under a row lock, the change is merged onto it, the result is validated, and the
    /// write lands before the lock is released — so two operators changing different limits at the
    /// same instant cannot store a pair neither asked for. The second waits for the first and
    /// validates against what it actually wrote. Go manages the same thing through
    /// `UpdateQueueConfig`; Python and TypeScript read and write separately and can store an
    /// incoherent pair.
    pub async fn update_queue(&self, name: &str, change: QueueChange) -> Result<Queue> {
        self.executor("update a queue")?
            .connection()
            .update_queue(name, change)
            .await
    }

    /// Removes a queue's registration.
    ///
    /// Removing one that is not registered is not an error: the end state is what was asked for.
    ///
    /// **Workflows already enqueued on it are not touched.** They keep the queue name they were
    /// given and stop being dequeued, because a worker only exists for a queue that has a row —
    /// so deleting a queue with a backlog strands that backlog until the queue is registered
    /// again. That is the same behaviour in every implementation, and it is why deleting is not
    /// how you pause a queue.
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.executor("delete a queue")?
            .connection()
            .delete_queue(name)
            .await
    }
}

// The five operations below are methods on `Connection` rather than on a `DBOS` instance, because
// a queue is a row and both handles that can reach the database may write it: a launched instance
// through `DBOS::register_queue` and the rest, and a `Client` through the same five names. The
// impl block lives here, next to the types it speaks in, which is where this crate already puts
// `impl DBOS`. The public methods differ only in how they come by a connection, and in the
// application version only one of them has.

impl Connection {
    /// Registers a queue, or reports the one already registered under this name.
    ///
    /// `app_version` is the caller's own, and `None` says it has none — which is what a
    /// [`Client`](crate::Client) is. It exists as a parameter rather than as something read off
    /// the handle precisely so that a client can call this: only
    /// [`QueueConflict::UpdateIfLatestVersion`] consults it, and that is the one policy a client
    /// is refused.
    ///
    /// **The refusal lives here**, for both surfaces, because the condition it tests — a caller
    /// with no version — is exactly what this parameter carries. Both surfaces take the same
    /// [`QueueConflict`], as Python's and TypeScript's do.
    pub(crate) async fn register_queue(
        &self,
        name: &str,
        options: QueueOptions,
        on_conflict: QueueConflict,
        app_version: Option<&str>,
    ) -> Result<Queue> {
        if name == INTERNAL_QUEUE {
            return Err(Error::Config(format!(
                "the queue name `{name}` is reserved for the engine's internal queue"
            )));
        }
        validate(name, &options)?;

        let on_existing = match on_conflict {
            QueueConflict::AlwaysUpdate => OnExistingQueue::Update,
            QueueConflict::NeverUpdate => OnExistingQueue::Leave,
            // **Resolved here rather than in the database**, because it is a question about *this
            // process* — whether the version it is running is the one a new registration should
            // speak for. The system database has no opinion about which of two live deployments
            // is authoritative.
            QueueConflict::UpdateIfLatestVersion => {
                // **A handle with no application version cannot answer this question**, which is
                // the case a [`Client`](crate::Client) is: it runs none of the application's code,
                // so there is no version of it to weigh against the registered ones. Python and
                // TypeScript refuse the same combination on the same grounds (`_client.py:455`,
                // `client.ts:561`). Only a client reaches it: `DBOS::register_queue` passes the
                // executor's version, which a launched instance always has.
                let Some(version) = app_version else {
                    return Err(Error::Config(format!(
                        "registering queue `{name}`: `QueueConflict::UpdateIfLatestVersion` needs \
                             an application version to compare against, and a client has none; ask \
                             for `AlwaysUpdate` or `NeverUpdate`"
                    )));
                };
                let latest = self
                    .sysdb()
                    .get_latest_application_version(self.app_name())
                    .await
                    .map_err(Error::SystemDatabase)?;
                match latest {
                    // The first registration of the first version: this process is the latest
                    // because it is the only one.
                    None => OnExistingQueue::Update,
                    Some(latest) if latest.version_name == version => OnExistingQueue::Update,
                    // An older version registering behind a newer one. Leaving the row alone is
                    // what keeps a rolling deploy from flapping.
                    Some(latest) => {
                        tracing::debug!(
                            queue = name,
                            version,
                            latest = latest.version_name,
                            "an older version registered this queue; its stored limits stand"
                        );
                        OnExistingQueue::Leave
                    }
                }
            }
        };

        let created = self
            .sysdb()
            .upsert_queue(
                &NewQueue {
                    concurrency: options.concurrency,
                    worker_concurrency: options.worker_concurrency,
                    polling_interval: options.polling_interval,
                    rate_limit: options.rate_limit,
                    priority_enabled: options.priority_enabled,
                    // **Derived, never asked for.** Partitioning here *is* the per-partition
                    // limits, so the column is written to agree with them — which is also what
                    // makes the row legible to an implementation that still reads the flag.
                    partition_queue: options.partition_concurrency.is_some()
                        || options.partition_worker_concurrency.is_some()
                        || options.partition_rate_limit.is_some(),
                    partition_concurrency: options.partition_concurrency,
                    partition_worker_concurrency: options.partition_worker_concurrency,
                    partition_rate_limit: options.partition_rate_limit,
                    application_name: self.app_name(),
                    ..NewQueue::new(name)
                },
                on_existing,
            )
            .await
            .map_err(Error::SystemDatabase)?;

        // Read back rather than echo: with `Leave`, and with a row a peer wrote, what this
        // executor will actually dequeue under is the row, not the request.
        let record = self
            .sysdb()
            .get_queue(name)
            .await
            .map_err(Error::SystemDatabase)?
            .ok_or_else(|| {
                Error::Config(format!(
                    "queue `{name}` is missing from the database after registering it"
                ))
            })?;

        if created {
            tracing::info!(queue = name, "registered a queue");
        }
        Ok(Queue::from_record(record))
    }
    /// The queue registered under this name, or `None` if there is none.
    pub(crate) async fn queue(&self, name: &str) -> Result<Option<Queue>> {
        Ok(self
            .sysdb()
            .get_queue(name)
            .await
            .map_err(Error::SystemDatabase)?
            .map(Queue::from_record))
    }
    /// Every queue this handle's application can dequeue from, its own plus the unclaimed ones.
    pub(crate) async fn list_queues(&self) -> Result<Vec<Queue>> {
        Ok(self
            .sysdb()
            .list_queues(&Applications::Unset)
            .await
            .map_err(Error::SystemDatabase)?
            .into_iter()
            .filter(|queue| queue.name != INTERNAL_QUEUE)
            .map(Queue::from_record)
            .collect())
    }
    /// Changes a registered queue's limits, leaving what the change does not name.
    pub(crate) async fn update_queue(&self, name: &str, change: QueueChange) -> Result<Queue> {
        if name == INTERNAL_QUEUE {
            return Err(Error::Config(format!(
                "the queue name `{name}` is reserved for the engine's internal queue"
            )));
        }

        // Runs inside the system database's transaction, against the row it has locked, and is
        // handed the row as the update would leave it. It reads nothing itself and may be called
        // more than once: a retried attempt judges again, against the row that attempt read.
        //
        // The merged row rather than the change, because a limit is rarely wrong on its own:
        // `worker_concurrency` is always fine by itself and only becomes wrong beside the
        // `concurrency` already stored.
        //
        // Resolved rather than raw, so an update to a peer's legacy-partitioned row is judged
        // against the scopes its limits are actually enforced at.
        let touches_partition = !(change.partition_concurrency.is_leave()
            && change.partition_worker_concurrency.is_leave()
            && change.partition_rate_limit.is_leave());
        let validate = |stored: &QueueRecord, merged: &QueueRecord| -> StdResult<(), SysdbError> {
            // **A legacy-partitioned row cannot take a per-partition limit.** Its queue-wide
            // limits already *are* its per-partition ones, so adding a second set would leave two
            // answers to the same question in one row. TypeScript refuses the same six setters
            // (`requireNotLegacyPartitioned`); re-register the queue to move it across.
            //
            // Asked of the row as stored, not as merged: a change that clears the last partition
            // limit leaves a row that *looks* legacy — flag still set, no limits — and refusing
            // that would make un-partitioning impossible.
            if touches_partition && stored.is_legacy_partitioned() {
                return Err(SysdbError::InvalidInput {
                    field: "partition_concurrency".into(),
                    detail: "this queue is registered with the deprecated `partition_queue` flag, \
                                 under which its queue-wide limits already apply per partition; \
                                 re-register it with the per-partition limits instead"
                        .to_owned(),
                });
            }
            let limits = merged.resolved_limits();
            let options = QueueOptions {
                concurrency: limits.concurrency,
                worker_concurrency: limits.worker_concurrency,
                polling_interval: merged.polling_interval,
                rate_limit: limits.rate_limit,
                priority_enabled: merged.priority_enabled,
                partition_concurrency: limits.partition_concurrency,
                partition_worker_concurrency: limits.partition_worker_concurrency,
                partition_rate_limit: limits.partition_rate_limit,
            };
            validate_fields(&options)
                .map_err(|(field, detail)| SysdbError::InvalidInput { field, detail })
        };

        // **The flag follows the limits.** Partitioning is not separately settable, so a change
        // that sets the first partition limit turns it on and one that clears the last turns it
        // off. Computing that needs the stored row, so it happens inside the system database's
        // transaction — `partition_queue_after` is applied to the row the write is locking.
        let update = QueueUpdate {
            concurrency: change.concurrency,
            worker_concurrency: change.worker_concurrency,
            polling_interval: change.polling_interval,
            rate_limit: change.rate_limit,
            priority_enabled: change.priority_enabled,
            partition_queue: Change::Leave,
            partition_concurrency: change.partition_concurrency,
            partition_worker_concurrency: change.partition_worker_concurrency,
            partition_rate_limit: change.partition_rate_limit,
        };

        // The row as written, so there is no read back to do: it left the transaction that wrote
        // it, which is a stronger guarantee than re-reading afterwards ever was.
        let record = self
            .sysdb()
            .update_queue(name, &update, &validate)
            .await
            .map_err(|error| match error {
                // The refusal `validate` handed down, restored to the shape a registration's would
                // have taken: the caller made an API mistake, not the database.
                SysdbError::InvalidInput { detail, .. } => {
                    Error::Config(format!("queue `{name}`: {detail}"))
                }
                SysdbError::NotRegistered { .. } => {
                    Error::Config(format!("no queue named `{name}` is registered"))
                }
                other => Error::SystemDatabase(other),
            })?;
        tracing::info!(queue = name, "updated the queue's limits");
        Ok(Queue::from_record(record))
    }
    /// Removes a queue's registration.
    pub(crate) async fn delete_queue(&self, name: &str) -> Result<()> {
        if name == INTERNAL_QUEUE {
            return Err(Error::Config(format!(
                "the queue name `{name}` is reserved for the engine's internal queue"
            )));
        }
        self.sysdb()
            .delete_queue(name)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(queue = name, "deleted the queue");
        Ok(())
    }
}
