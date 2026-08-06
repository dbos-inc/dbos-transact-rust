//! The shapes the system database stores and returns.
//!
//! Two conventions run through all of these, and both are deliberate.
//!
//! **Payloads are opaque.** Workflow inputs, outputs, and errors cross this boundary as
//! already-encoded strings. This layer never serializes or deserializes them, never inspects
//! them, and does not know which format they are in — the `serialization` column records that
//! for whoever reads them next. It is what lets one workflow's `rust_serde` payload and
//! another's `portable_json` sit in the same table, and what keeps this layer usable from a
//! language that does not share Rust's idea of a value.
//!
//! **Times are typed, because the columns come in three shapes that a bare integer conflates.**
//! Some are instants ([`Timestamp`]); some are durations ([`std::time::Duration`]); one, in the
//! same table as both, is neither — `recovery_attempts` is a count. Worse, the durations do not
//! share a unit: `workflow_timeout_ms` is integer milliseconds while `polling_interval_sec` is a
//! float in seconds. `workflow_deadline_epoch_ms` and `workflow_timeout_ms` sit next to each
//! other, which is precisely the pair that gets swapped.
//!
//! Nothing here depends on a date library. `Duration` is in `std`, and [`Timestamp`] is a thin
//! wrapper over the epoch milliseconds actually stored, so it converts exactly rather than
//! through someone's calendar. Callers wanting a calendar type convert at their own edge.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// An instant, as epoch milliseconds.
///
/// Exactly what the columns hold, so reading and writing are lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Wraps a stored value.
    pub const fn from_epoch_ms(ms: i64) -> Self {
        Self(ms)
    }

    /// The stored value.
    pub const fn as_epoch_ms(self) -> i64 {
        self.0
    }

    /// The current time, truncated to milliseconds.
    pub fn now() -> Self {
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
    }

    /// Converts to a `SystemTime`, or `None` for an instant before the epoch.
    ///
    /// No DBOS timestamp should be negative; this reports rather than saturates so a corrupt
    /// value surfaces instead of becoming 1970.
    pub fn to_system_time(self) -> Option<SystemTime> {
        u64::try_from(self.0)
            .ok()
            .map(|ms| UNIX_EPOCH + Duration::from_millis(ms))
    }

    /// Converts from a `SystemTime`, or `None` if it precedes the epoch.
    pub fn from_system_time(time: SystemTime) -> Option<Self> {
        time.duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| Self(d.as_millis() as i64))
    }

    /// This instant plus a duration — a deadline from a start and a timeout.
    ///
    /// The reason the two are different types: adding a timeout to a deadline, or a deadline to
    /// a deadline, will not compile.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        i64::try_from(duration.as_millis())
            .ok()
            .and_then(|ms| self.0.checked_add(ms))
            .map(Self)
    }

    /// How long after `earlier` this instant is, or `None` if it is not after it.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0
            .checked_sub(earlier.0)
            .and_then(|ms| u64::try_from(ms).ok())
            .map(Duration::from_millis)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

/// Reads a duration stored as integer milliseconds, such as `workflow_timeout_ms`.
///
/// `None` for a negative value, which no valid duration column holds.
pub fn duration_from_ms(ms: i64) -> Option<Duration> {
    u64::try_from(ms).ok().map(Duration::from_millis)
}

/// Reads a duration stored as fractional seconds, such as `polling_interval_sec`.
///
/// The other unit durations are stored in — see the module documentation.
pub fn duration_from_secs(secs: f64) -> Option<Duration> {
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs))
}

/// Where a workflow is in its lifecycle.
///
/// Stored as text and shared with every other DBOS implementation, so the spellings are a wire
/// format rather than an internal choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkflowStatus {
    /// Claimed by an executor and running.
    Pending,
    /// On a queue, waiting to be dequeued.
    Enqueued,
    /// On a queue but not yet eligible, because a delay or a debounce holds it back.
    Delayed,
    /// Finished, with an output.
    Success,
    /// Finished, with an error.
    Error,
    /// Cancelled before finishing.
    Cancelled,
    /// Recovered more times than allowed, and parked rather than retried again.
    MaxRecoveryAttemptsExceeded,
}

impl WorkflowStatus {
    /// The stored spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "PENDING",
            WorkflowStatus::Enqueued => "ENQUEUED",
            WorkflowStatus::Delayed => "DELAYED",
            WorkflowStatus::Success => "SUCCESS",
            WorkflowStatus::Error => "ERROR",
            WorkflowStatus::Cancelled => "CANCELLED",
            WorkflowStatus::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        }
    }

    /// Parses a stored value.
    ///
    /// Returns `None` for anything unrecognised rather than guessing. A status written by a
    /// newer implementation is a real possibility in a shared database, and silently mapping it
    /// onto the nearest known value would be worse than reporting that it is unknown.
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "PENDING" => WorkflowStatus::Pending,
            "ENQUEUED" => WorkflowStatus::Enqueued,
            "DELAYED" => WorkflowStatus::Delayed,
            "SUCCESS" => WorkflowStatus::Success,
            "ERROR" => WorkflowStatus::Error,
            "CANCELLED" => WorkflowStatus::Cancelled,
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => WorkflowStatus::MaxRecoveryAttemptsExceeded,
            _ => return None,
        })
    }

    /// Whether the workflow has finished and will not run again.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            WorkflowStatus::Success | WorkflowStatus::Error | WorkflowStatus::Cancelled
        )
    }
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A row of `workflow_status`.
///
/// **Shaped like the row, not like the domain.** Fields mirror columns one for one, including
/// their nullability: `name` is `Option` because the column is. Identity concepts that group
/// several columns — the `(name, class_name, config_name)` triple the registry keys on — are
/// built from a record where they are needed, rather than being baked into it. Grouping them
/// here meant claiming `name` was non-null and quietly substituting an empty string when it
/// was not, which is the kind of loss this layer should report rather than absorb.
///
/// Every payload field holds encoded text — see the module documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRecord {
    /// Primary key, and the identity a caller uses everywhere else.
    pub workflow_id: String,
    /// Lifecycle state.
    pub status: WorkflowStatus,
    /// Registered function name. Nullable in the schema, so nullable here.
    pub name: Option<String>,
    /// The type the function belongs to, for a method or a configured instance.
    pub class_name: Option<String>,
    /// The configured instance, if any.
    pub config_name: Option<String>,
    /// Encoded input, in whatever format `serialization` names.
    pub input: Option<String>,
    /// Encoded output, present once the workflow succeeds.
    pub output: Option<String>,
    /// Encoded error, present once the workflow fails.
    pub error: Option<String>,
    /// Which format the payloads above are in. `None` on rows written before the column
    /// existed, which means the writer's own default.
    pub serialization: Option<String>,
    /// The executor that most recently claimed this workflow.
    pub executor_id: Option<String>,
    /// Application version that created it, used to keep recovery on compatible code.
    pub application_version: Option<String>,
    /// Recovery attempts so far, against the dead-letter limit.
    pub recovery_attempts: i64,
    /// Queue this workflow was enqueued on, if it was.
    pub queue_name: Option<String>,
    /// When the row was created.
    pub created_at: Timestamp,
    /// When the row last changed.
    pub updated_at: Timestamp,
    /// When execution began, if it has.
    pub started_at: Option<Timestamp>,
    /// When the workflow reached a terminal state.
    pub completed_at: Option<Timestamp>,
    /// The workflow that forked this one, if any.
    pub forked_from: Option<String>,
    /// The workflow that started this one as a child, if any.
    pub parent_workflow_id: Option<String>,
    /// Whether this workflow has been forked from at least once.
    pub was_forked_from: bool,

    // ── Ownership and attribution ──────────────────────────────────────────────
    /// Identity of the attempt currently holding this workflow.
    ///
    /// The single-execution guard: distinct per attempt, unlike `executor_id`, which defaults
    /// to `"local"` and collides between processes on one machine.
    pub owner_xid: Option<String>,
    /// Deployment identifier, for installations running several applications.
    pub application_id: Option<String>,
    /// User on whose behalf the workflow runs.
    pub authenticated_user: Option<String>,
    /// Roles that user holds, encoded as the references encode them.
    pub authenticated_roles: Option<String>,
    /// Role actually assumed for this execution.
    pub assumed_role: Option<String>,
    /// Request context captured at creation.
    pub request: Option<String>,

    // ── Queueing ───────────────────────────────────────────────────────────────
    /// Deduplication key within the queue. At most one live workflow may hold a given key.
    pub deduplication_id: Option<String>,
    /// Dequeue priority; lower runs sooner.
    ///
    /// `i32` and not optional, mirroring `INT4 NOT NULL DEFAULT 0`. Widening to `i64` would let
    /// values round-trip through a type the column cannot hold, and making it optional would
    /// invite writing a NULL the column rejects.
    pub priority: i32,
    /// Partition this workflow belongs to, on a partitioned queue.
    pub queue_partition_key: Option<String>,
    /// Whether a rate limiter is currently holding this workflow back.
    pub rate_limited: bool,
    /// Schedule that enqueued this workflow, if a schedule did.
    pub schedule_name: Option<String>,

    // ── Timing ─────────────────────────────────────────────────────────────────
    /// How long the workflow may run for.
    ///
    /// A **duration**, unlike `deadline` below, which is an instant. The two are adjacent
    /// columns and mean different things: this is a budget, that is a wall-clock cutoff.
    pub timeout: Option<Duration>,
    /// Wall-clock instant the workflow must finish by.
    pub deadline: Option<Timestamp>,
    /// Instant before which the workflow must not be dequeued.
    pub delay_until: Option<Timestamp>,
    /// Cap past which a debounce may not push the delay any further.
    pub debounce_deadline: Option<Timestamp>,
    /// Whether the deduplication id is a debounce key, cleared on DELAYED to ENQUEUED.
    pub is_debounced: bool,

    // ── Caller-supplied metadata ───────────────────────────────────────────────
    /// Arbitrary attributes attached at creation, stored as JSON.
    ///
    /// Opaque here like every other payload: this layer stores and returns the text and does
    /// not parse it, even though the column is `jsonb` and is queried by containment.
    pub attributes: Option<String>,
}

/// A workflow being created, as opposed to one being read back.
///
/// This is deliberately *not* [`WorkflowRecord`]. A row has 37 columns; a caller creating a
/// workflow can meaningfully set 24 of them. The rest are the database's to write:
///
/// - `status` and `recovery_attempts` are **derived** from the queue and delay below, so a
///   caller cannot enqueue a workflow and then label it `SUCCESS`.
/// - `created_at`, `updated_at`, and `owner_xid` are stamped at insert.
/// - `output`, `error`, `started_at`, `completed_at`, `forked_from`, `was_forked_from`, and
///   `rate_limited` belong to execution, forking, and the rate limiter. A workflow that has
///   not started has no output to offer.
///
/// Java draws the same line with `WorkflowStatusInternal`. Python and Go pass their full row
/// type instead, but Go's is a package-internal call taking a transaction, and Python's carries
/// the same fields it then ignores.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NewWorkflow {
    /// The id, which is what makes a retried submission the same workflow.
    pub workflow_id: String,
    /// The registered function name.
    pub name: Option<String>,
    /// The class the function belongs to, for class-bound workflows.
    pub class_name: Option<String>,
    /// The configured instance name, for instance-bound workflows.
    pub config_name: Option<String>,
    /// Encoded arguments.
    pub input: Option<String>,
    /// How `input` and, later, the outcome are encoded.
    pub serialization: Option<String>,

    /// The queue to enqueue on. `None` runs the workflow directly.
    ///
    /// This decides the initial status: no queue means `PENDING`, a queue means `ENQUEUED`, and
    /// a queue with a `delay` means `DELAYED`.
    pub queue_name: Option<String>,
    /// Deduplication key within the queue.
    pub deduplication_id: Option<String>,
    /// Dequeue priority; lower runs sooner.
    pub priority: i32,
    /// Partition within the queue.
    pub queue_partition_key: Option<String>,
    /// How long to hold the workflow before it becomes eligible to dequeue.
    ///
    /// A duration, not an instant: the wall-clock time is stamped by the database layer against
    /// the same clock it writes `created_at` with. Letting the caller compute `now + delay`
    /// would put its clock skew into the row.
    pub delay: Option<Duration>,
    /// Marks `deduplication_id` as a debounce key to clear on the `DELAYED` → `ENQUEUED` move.
    pub is_debounced: bool,
    /// Cap beyond which bounces may not extend `delay`.
    pub debounce_deadline: Option<Timestamp>,

    /// Wall-clock budget for the whole workflow.
    pub timeout: Option<Duration>,
    /// Absolute expiry, when the parent already fixed one.
    ///
    /// Distinct from `timeout` because a child inherits its parent's deadline rather than
    /// restarting the clock.
    pub deadline: Option<Timestamp>,

    /// The executor claiming this workflow.
    pub executor_id: Option<String>,
    /// Application version, which recovery uses to avoid resuming under changed code.
    pub application_version: Option<String>,
    /// Application id, as assigned by the platform.
    pub application_id: Option<String>,

    /// Authenticated principal at submission.
    pub authenticated_user: Option<String>,
    /// Encoded JSON list of that principal's roles.
    pub authenticated_roles: Option<String>,
    /// The role actually assumed.
    pub assumed_role: Option<String>,

    /// The workflow that started this one.
    pub parent_workflow_id: Option<String>,
    /// The schedule that triggered this workflow. Set only by the scheduler.
    pub schedule_name: Option<String>,
    /// Caller-supplied JSON attributes, stored in a `jsonb` column.
    pub attributes: Option<String>,
}

impl NewWorkflow {
    /// A workflow with an id and nothing else set.
    pub fn new(workflow_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            ..Self::default()
        }
    }

    /// The status this workflow starts in, which follows from the queue and the delay.
    ///
    /// Not a caller's choice in any implementation: a workflow is `PENDING` when it runs here,
    /// `ENQUEUED` when it waits on a queue, and `DELAYED` when it waits on a queue and a clock.
    pub fn initial_status(&self) -> WorkflowStatus {
        match (&self.queue_name, &self.delay) {
            (None, _) => WorkflowStatus::Pending,
            (Some(_), None) => WorkflowStatus::Enqueued,
            (Some(_), Some(_)) => WorkflowStatus::Delayed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Instants and durations do not mix, which is the point of separating them.
    #[test]
    fn a_deadline_is_a_start_plus_a_timeout() {
        let start = Timestamp::from_epoch_ms(1_700_000_000_000);
        let timeout = Duration::from_secs(30);
        let deadline = start.checked_add(timeout).unwrap();

        assert_eq!(deadline.as_epoch_ms(), 1_700_000_030_000);
        assert_eq!(deadline.duration_since(start), Some(timeout));
        // Going backwards is not a duration.
        assert_eq!(start.duration_since(deadline), None);
    }

    #[test]
    fn timestamps_round_trip_through_system_time() {
        let t = Timestamp::from_epoch_ms(1_700_000_000_123);
        let round_tripped = Timestamp::from_system_time(t.to_system_time().unwrap()).unwrap();
        assert_eq!(round_tripped, t);
    }

    /// A negative instant is reported rather than silently becoming 1970.
    #[test]
    fn a_pre_epoch_instant_does_not_convert() {
        assert_eq!(Timestamp::from_epoch_ms(-1).to_system_time(), None);
    }

    /// The two units durations are stored in, read through their own helpers.
    #[test]
    fn durations_read_from_either_stored_unit() {
        assert_eq!(duration_from_ms(1_500), Some(Duration::from_millis(1_500)));
        assert_eq!(duration_from_secs(1.5), Some(Duration::from_millis(1_500)));
        // Neither unit has a meaningful negative, and a float column can be NaN.
        assert_eq!(duration_from_ms(-1), None);
        assert_eq!(duration_from_secs(-1.0), None);
        assert_eq!(duration_from_secs(f64::NAN), None);
        assert_eq!(duration_from_secs(f64::INFINITY), None);
    }

    /// Both readers accept zero, and for the same reason.
    ///
    /// Zero is a legitimate stored value — these read what a column holds, and rejecting it
    /// here would turn a readable row into an error. The rule that a duration *option* must be
    /// strictly positive belongs at the input boundary, where a caller supplies one, not at the
    /// point a stored value is read back.
    ///
    /// `-0.0` counts as zero: it compares equal to `0.0`, which is what the guard tests.
    #[test]
    fn both_readers_accept_zero() {
        assert_eq!(duration_from_ms(0), Some(Duration::ZERO));
        assert_eq!(duration_from_secs(0.0), Some(Duration::ZERO));
        assert_eq!(duration_from_secs(-0.0), Some(Duration::ZERO));
    }

    /// The spellings are a wire format shared with the other implementations.
    #[test]
    fn status_round_trips_through_its_stored_spelling() {
        for status in [
            WorkflowStatus::Pending,
            WorkflowStatus::Enqueued,
            WorkflowStatus::Delayed,
            WorkflowStatus::Success,
            WorkflowStatus::Error,
            WorkflowStatus::Cancelled,
            WorkflowStatus::MaxRecoveryAttemptsExceeded,
        ] {
            assert_eq!(WorkflowStatus::parse(status.as_str()), Some(status));
        }
    }

    /// Pinned against the other implementations, which store these exact strings.
    #[test]
    fn stored_spellings_match_the_other_implementations() {
        assert_eq!(WorkflowStatus::Pending.as_str(), "PENDING");
        assert_eq!(WorkflowStatus::Enqueued.as_str(), "ENQUEUED");
        assert_eq!(WorkflowStatus::Delayed.as_str(), "DELAYED");
        assert_eq!(WorkflowStatus::Success.as_str(), "SUCCESS");
        assert_eq!(WorkflowStatus::Error.as_str(), "ERROR");
        assert_eq!(WorkflowStatus::Cancelled.as_str(), "CANCELLED");
        assert_eq!(
            WorkflowStatus::MaxRecoveryAttemptsExceeded.as_str(),
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        );
    }

    /// An unknown status is reported as unknown, not mapped onto a neighbour.
    #[test]
    fn an_unrecognised_status_does_not_parse() {
        assert_eq!(WorkflowStatus::parse("SOMETHING_NEWER"), None);
        assert_eq!(WorkflowStatus::parse("pending"), None, "matching is exact");
    }

    #[test]
    fn terminal_states_are_the_three_that_stop_execution() {
        assert!(WorkflowStatus::Success.is_terminal());
        assert!(WorkflowStatus::Error.is_terminal());
        assert!(WorkflowStatus::Cancelled.is_terminal());
        assert!(!WorkflowStatus::Pending.is_terminal());
        assert!(!WorkflowStatus::Enqueued.is_terminal());
        assert!(!WorkflowStatus::Delayed.is_terminal());
        // Parked rather than finished: it can still be resumed.
        assert!(!WorkflowStatus::MaxRecoveryAttemptsExceeded.is_terminal());
    }
}

/// Which workflows to list, and how much of each to load.
///
/// Every field is a narrowing, and the default narrows nothing — so
/// `WorkflowFilter::default()` lists everything. List fields match any of their entries and are
/// ignored when empty; `Option<bool>` fields are three-valued, where `None` does not filter.
///
/// The set is the union of all four implementations, which do not agree on it. Go has 28
/// filters, Python 26, Java adds two Go lacks. Where they diverge it is noted on the field, so a
/// missing filter reads as a decision rather than an oversight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowFilter {
    /// Exact workflow ids.
    pub workflow_ids: Vec<String>,
    /// Workflow ids starting with any of these.
    pub workflow_id_prefixes: Vec<String>,

    /// Registered function names.
    pub names: Vec<String>,
    /// Class names, for class-bound workflows. **Java only.**
    pub class_names: Vec<String>,
    /// Configured instance names. **Java only**, where it is `instanceName`.
    pub config_names: Vec<String>,
    /// Statuses to include.
    pub status: Vec<WorkflowStatus>,

    /// Application versions.
    pub application_versions: Vec<String>,
    /// Executors that claimed the workflow.
    pub executor_ids: Vec<String>,
    /// Authenticated principals at submission.
    pub authenticated_users: Vec<String>,

    /// Queues the workflow was submitted to.
    pub queue_names: Vec<String>,
    /// Only workflows that went through a queue at all.
    pub queues_only: bool,
    /// Schedules that triggered the workflow.
    pub schedule_names: Vec<String>,
    /// Deduplication keys. **Go only.**
    pub deduplication_ids: Vec<String>,
    /// Whether the deduplication key is a debounce key. **Go only.**
    pub is_debounced: Option<bool>,

    /// Workflows started by any of these.
    pub parent_workflow_ids: Vec<String>,
    /// Whether the workflow has a parent at all.
    pub has_parent: Option<bool>,
    /// Workflows forked from any of these.
    pub forked_from: Vec<String>,
    /// Whether this workflow was itself forked from another.
    pub was_forked_from: Option<bool>,

    /// Created at or after this instant.
    ///
    /// Go and Python call this `start_time`, which reads as "when the workflow started". It does
    /// not — it filters `created_at`, and a workflow may be created long before it starts.
    /// [`started_after`](Self::started_after) is the one that filters on starting.
    pub created_after: Option<Timestamp>,
    /// Created at or before this instant. Go and Python call this `end_time`.
    pub created_before: Option<Timestamp>,
    /// Finished at or after this instant.
    pub completed_after: Option<Timestamp>,
    /// Finished at or before this instant.
    pub completed_before: Option<Timestamp>,
    /// Started at or after this instant.
    ///
    /// Every other implementation calls this `dequeued_after`, because the column is written
    /// when a queued workflow is dequeued. It is not queue-specific — it is
    /// [`WorkflowRecord::started_at`], the `started_at_epoch_ms` column — and naming it after
    /// the queue implies a filter that would exclude workflows that never sat on one.
    pub started_after: Option<Timestamp>,
    /// Started at or before this instant. Elsewhere `dequeued_before`.
    pub started_before: Option<Timestamp>,

    /// Encoded JSON the workflow's attributes must contain.
    ///
    /// Containment, not equality: `{"tenant": "acme"}` matches a workflow with that key among
    /// others. Served by the GIN index on the column.
    pub attributes: Option<String>,

    /// Most rows to return.
    pub limit: Option<i64>,
    /// Rows to skip.
    pub offset: Option<i64>,
    /// Newest first, rather than oldest first.
    pub sort_desc: bool,
    /// Whether to load the `input` column.
    ///
    /// Listing thousands of workflows pulls their arguments with them, which is usually not what
    /// the caller wanted. Turning this off leaves [`WorkflowRecord::input`] `None` — which is
    /// indistinguishable from a workflow that has no input.
    pub load_input: bool,
    /// Whether to load the `output` and `error` columns, with the same caveat.
    pub load_output: bool,
}

impl Default for WorkflowFilter {
    /// Narrows nothing, and loads everything.
    ///
    /// `load_input` and `load_output` default *on*, following Python, so a caller that does not
    /// think about them gets whole records rather than silently empty payloads. That is the one
    /// place this type cannot be `#[derive(Default)]`.
    fn default() -> Self {
        Self {
            workflow_ids: Vec::new(),
            workflow_id_prefixes: Vec::new(),
            names: Vec::new(),
            class_names: Vec::new(),
            config_names: Vec::new(),
            status: Vec::new(),
            application_versions: Vec::new(),
            executor_ids: Vec::new(),
            authenticated_users: Vec::new(),
            queue_names: Vec::new(),
            queues_only: false,
            schedule_names: Vec::new(),
            deduplication_ids: Vec::new(),
            is_debounced: None,
            parent_workflow_ids: Vec::new(),
            has_parent: None,
            forked_from: Vec::new(),
            was_forked_from: None,
            created_after: None,
            created_before: None,
            completed_after: None,
            completed_before: None,
            started_after: None,
            started_before: None,
            attributes: None,
            limit: None,
            offset: None,
            sort_desc: false,
            load_input: true,
            load_output: true,
        }
    }
}
