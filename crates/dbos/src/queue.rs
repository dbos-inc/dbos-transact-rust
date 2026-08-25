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
//! The one queue with no row is [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE), which is the
//! engine's own: `resume` and `fork` put work there, and it is not a queue anybody registers.

use std::time::Duration;

use crate::dbos::DBOS;
use crate::error::{Error, Result};
use crate::sysdb::INTERNAL_QUEUE;
use crate::sysdb::types::{NewQueue, OnExistingQueue, QueueRecord};

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
/// reasons, and the second is why it is worth the mapping: the engine names a limit for the scope
/// it applies at while the row names it for its column — [`concurrency`](Self::concurrency)
/// against `concurrency`, and 4b's limiter and partition limits diverge further — and derived
/// equality over a wrapped row would compare columns this type does not report, so two queues
/// identical through every accessor here could still differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queue {
    name: String,
    concurrency: Option<i32>,
    worker_concurrency: Option<i32>,
    polling_interval: Duration,
}

impl Queue {
    /// Builds the receipt from the row that was read back.
    ///
    /// **Destructured exhaustively on purpose.** A column added to [`QueueRecord`] is then a
    /// compile error here, which is a question — does the public surface report this? — rather
    /// than a limit this type silently never mentions.
    fn from_record(record: QueueRecord) -> Self {
        let QueueRecord {
            name,
            concurrency,
            worker_concurrency,
            polling_interval,
            // Registered but not reported yet: the limiter and the partition limits are 4b's
            // public surface, and ownership moves only by rename.
            rate_limit: _,
            priority_enabled: _,
            partition_queue: _,
            application_name: _,
        } = record;
        Self {
            name,
            concurrency: concurrency,
            worker_concurrency,
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
    /// What to do when the queue is already registered.
    pub on_conflict: QueueConflict,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            concurrency: None,
            worker_concurrency: None,
            polling_interval: DEFAULT_POLLING_INTERVAL,
            on_conflict: QueueConflict::default(),
        }
    }
}

/// What registering a queue that already exists does to the stored limits.
///
/// **Never to its owner.** A name already held by another application is
/// [`Error::SystemDatabase`] carrying `RegisteredByAnother` in every mode, because the name is the
/// queue's address across every application sharing the database — taking it would redirect a
/// peer's work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueueConflict {
    /// Overwrite only if this process is running the **latest registered application version**.
    ///
    /// The default, and the one that makes a rolling deploy behave. Two versions of an application
    /// run side by side during a deploy, and both register their queues at startup; without this,
    /// the old version's registration would keep reverting the new version's limits for as long as
    /// it lived. An application with no registered versions yet is the latest by default, since it
    /// is the first.
    #[default]
    UpdateIfLatestVersion,
    /// Always overwrite the stored limits.
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
/// `Queue._validate_queue`, TypeScript's `validateQueueParams` — and because 4b's rate limits and
/// partition limits add most of their checks here rather than anywhere else.
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
    let refuse = |message: String| Err(Error::Config(format!("queue `{name}`: {message}")));

    if let Some(global) = options.concurrency
        && global < 1
    {
        return refuse(format!("`concurrency` must be at least 1, got {global}"));
    }
    if let Some(worker) = options.worker_concurrency
        && worker < 1
    {
        return refuse(format!(
            "`worker_concurrency` must be at least 1, got {worker}"
        ));
    }
    if let (Some(worker), Some(global)) = (options.worker_concurrency, options.concurrency)
        && worker > global
    {
        return refuse(format!(
            "`concurrency` must be greater than or equal to `worker_concurrency`, \
             got {global} and {worker}"
        ));
    }
    if options.polling_interval.is_zero() {
        return refuse("`polling_interval` cannot be zero".to_owned());
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
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let queue = dbos.register_queue("demo-queue", dbos::QueueOptions {
    ///     worker_concurrency: Some(3),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_queue(&self, name: &str, options: QueueOptions) -> Result<Queue> {
        let executor = self.executor("register a queue")?;
        if name == INTERNAL_QUEUE {
            return Err(Error::Config(format!(
                "the queue name `{name}` is reserved for the engine's internal queue"
            )));
        }
        validate(name, &options)?;

        let on_existing = match options.on_conflict {
            QueueConflict::AlwaysUpdate => OnExistingQueue::Update,
            QueueConflict::NeverUpdate => OnExistingQueue::Leave,
            // **Resolved here rather than in the database**, because it is a question about *this
            // process* — whether the version it is running is the one a new registration should
            // speak for. The system database has no opinion about which of two live deployments
            // is authoritative.
            QueueConflict::UpdateIfLatestVersion => {
                let latest = executor
                    .sysdb()
                    .get_latest_application_version(Some(executor.app_name()))
                    .await
                    .map_err(Error::SystemDatabase)?;
                match latest {
                    // The first registration of the first version: this process is the latest
                    // because it is the only one.
                    None => OnExistingQueue::Update,
                    Some(latest) if latest.version_name == executor.application_version() => {
                        OnExistingQueue::Update
                    }
                    // An older version registering behind a newer one. Leaving the row alone is
                    // what keeps a rolling deploy from flapping.
                    Some(latest) => {
                        tracing::debug!(
                            queue = name,
                            version = executor.application_version(),
                            latest = latest.version_name,
                            "an older version registered this queue; its stored limits stand"
                        );
                        OnExistingQueue::Leave
                    }
                }
            }
        };

        let created = executor
            .sysdb()
            .upsert_queue(
                &NewQueue {
                    concurrency: options.concurrency,
                    worker_concurrency: options.worker_concurrency,
                    polling_interval: options.polling_interval,
                    application_name: Some(executor.app_name()),
                    ..NewQueue::new(name)
                },
                on_existing,
            )
            .await
            .map_err(Error::SystemDatabase)?;

        // Read back rather than echo: with `Leave`, and with a row a peer wrote, what this
        // executor will actually dequeue under is the row, not the request.
        let record = executor
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
}
