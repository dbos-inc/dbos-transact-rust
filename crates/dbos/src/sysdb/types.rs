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

use super::Error;
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
pub struct NewWorkflow<'a> {
    /// The id, which is what makes a retried submission the same workflow.
    pub workflow_id: &'a str,
    /// The registered function name.
    pub name: Option<&'a str>,
    /// The class the function belongs to, for class-bound workflows.
    pub class_name: Option<&'a str>,
    /// The configured instance name, for instance-bound workflows.
    pub config_name: Option<&'a str>,
    /// Encoded arguments.
    pub input: Option<&'a str>,
    /// How `input` and, later, the outcome are encoded.
    pub serialization: Option<&'a str>,

    /// The queue to enqueue on. `None` runs the workflow directly.
    ///
    /// This decides the initial status: no queue means `PENDING`, a queue means `ENQUEUED`, and
    /// a queue with a `delay` means `DELAYED`.
    pub queue_name: Option<&'a str>,
    /// Deduplication key within the queue.
    pub deduplication_id: Option<&'a str>,
    /// Dequeue priority; lower runs sooner.
    pub priority: i32,
    /// Partition within the queue.
    pub queue_partition_key: Option<&'a str>,
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
    pub executor_id: Option<&'a str>,
    /// Application version, which recovery uses to avoid resuming under changed code.
    pub application_version: Option<&'a str>,
    /// Application id, as assigned by the platform.
    pub application_id: Option<&'a str>,

    /// Authenticated principal at submission.
    pub authenticated_user: Option<&'a str>,
    /// Encoded JSON list of that principal's roles.
    pub authenticated_roles: Option<&'a str>,
    /// The role actually assumed.
    pub assumed_role: Option<&'a str>,

    /// The workflow that started this one.
    pub parent_workflow_id: Option<&'a str>,
    /// The schedule that triggered this workflow. Set only by the scheduler.
    pub schedule_name: Option<&'a str>,
    /// Caller-supplied JSON attributes, stored in a `jsonb` column.
    pub attributes: Option<&'a str>,
}

impl<'a> NewWorkflow<'a> {
    /// A workflow with an id and nothing else set.
    pub fn new(workflow_id: &'a str) -> Self {
        Self {
            workflow_id,
            ..Self::default()
        }
    }

    /// Rejects values the system database will not store.
    ///
    /// Java validates the same list in `WorkflowStatusInternal`'s constructor, so a bad value
    /// cannot be built at all. `NewWorkflow` has public fields and so has no construction hook;
    /// this runs at the point of use instead, which catches the same mistakes one step later.
    ///
    /// **An empty string is not a missing value — `None` is.** A `Some("")` queue name would
    /// enqueue a workflow onto a queue called `""`, and an empty `workflow_id` would create a
    /// row no caller can ever name again. Both are caller bugs worth reporting rather than
    /// storing.
    ///
    /// The two auth fields are exempt: TypeScript and Go send `""` rather than null when there
    /// is no auth context, so those are normalised at write time instead of rejected.
    pub fn validate(&self) -> Result<(), Error> {
        if self.workflow_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "workflow_id",
                detail: "must not be empty".to_owned(),
            });
        }
        for (field, value) in [
            ("name", &self.name),
            ("class_name", &self.class_name),
            ("config_name", &self.config_name),
            ("queue_name", &self.queue_name),
            ("deduplication_id", &self.deduplication_id),
            ("queue_partition_key", &self.queue_partition_key),
            ("schedule_name", &self.schedule_name),
            ("input", &self.input),
            ("serialization", &self.serialization),
            ("application_version", &self.application_version),
        ] {
            if *value == Some("") {
                return Err(Error::InvalidInput {
                    field,
                    detail: "must be absent rather than empty".to_owned(),
                });
            }
        }
        // Java rejects a zero or negative duration outright; `Duration` already rules out
        // negatives, so only zero is left to catch. A zero delay is a caller that meant `None`,
        // and a zero timeout would expire the workflow before it ran.
        for (field, value) in [("delay", self.delay), ("timeout", self.timeout)] {
            if value == Some(Duration::ZERO) {
                return Err(Error::InvalidInput {
                    field,
                    detail: "must be a positive, non-zero duration".to_owned(),
                });
            }
        }
        Ok(())
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
pub struct WorkflowFilter<'a> {
    /// Exact workflow ids.
    pub workflow_ids: Vec<&'a str>,
    /// Workflow ids starting with any of these.
    pub workflow_id_prefixes: Vec<&'a str>,

    /// Registered function names.
    pub names: Vec<&'a str>,
    /// Class names, for class-bound workflows. **Java only.**
    pub class_names: Vec<&'a str>,
    /// Configured instance names. **Java only**, where it is `instanceName`.
    pub config_names: Vec<&'a str>,
    /// Statuses to include.
    pub status: Vec<WorkflowStatus>,

    /// Application versions.
    pub application_versions: Vec<&'a str>,
    /// Executors that claimed the workflow.
    pub executor_ids: Vec<&'a str>,
    /// Authenticated principals at submission.
    pub authenticated_users: Vec<&'a str>,

    /// Queues the workflow was submitted to.
    pub queue_names: Vec<&'a str>,
    /// Only workflows that went through a queue at all.
    pub queues_only: bool,
    /// Schedules that triggered the workflow.
    pub schedule_names: Vec<&'a str>,
    /// Deduplication keys. **Go only.**
    pub deduplication_ids: Vec<&'a str>,
    /// Whether the deduplication key is a debounce key. **Go only.**
    pub is_debounced: Option<bool>,

    /// Workflows started by any of these.
    pub parent_workflow_ids: Vec<&'a str>,
    /// Whether the workflow has a parent at all.
    pub has_parent: Option<bool>,
    /// Workflows forked from any of these.
    pub forked_from: Vec<&'a str>,
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
    pub attributes: Option<&'a str>,

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

impl Default for WorkflowFilter<'_> {
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

/// A step's recorded result, as `operation_outputs` holds it.
///
/// One row per `(workflow_id, step_id)` — that pair is the primary key, and is what makes a
/// replayed workflow skip work it has already done.
///
/// **The field names do not match the column names.** `step_id` and `step_name` are stored in
/// `function_id` and `function_name`, which is what the schema has called them since migration 1
/// and what every implementation's SQL still says. Java's `StepResult` draws the same
/// distinction, and the reason is that "function" is what these were called before steps had a
/// name of their own — the columns cannot be renamed without a migration every SDK must agree
/// on, but the Rust API need not inherit the old word.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRecord {
    /// The workflow the step belongs to.
    pub workflow_id: String,
    /// The step's position in the workflow, counted from zero. Column `function_id`.
    ///
    /// `i32`, matching `INT4`. The count is per workflow execution, not global.
    pub step_id: i32,
    /// The registered step name, checked on replay. Column `function_name`.
    pub step_name: String,
    /// Encoded return value, if the step returned one.
    pub output: Option<String>,
    /// Encoded error, if the step raised one. Never set alongside `output`.
    pub error: Option<String>,
    /// The workflow this step started, for steps that are child-workflow calls.
    pub child_workflow_id: Option<String>,
    /// How `output` and `error` are encoded.
    pub serialization: Option<String>,
    /// When the step began.
    pub started_at: Option<Timestamp>,
    /// When the step finished.
    ///
    /// Also the tie-breaker on a duplicate record: a second write carrying a *different*
    /// completion time is another executor, while one carrying the same is this caller's own
    /// retry. See [`SystemDatabase::record_step`](crate::sysdb::SystemDatabase::record_step).
    pub completed_at: Option<Timestamp>,
}

/// How a step or a workflow ended: with a value, or with an error.
///
/// One type for both, because they end the same way and are stored the same way — an `output`
/// column and an `error` column, on `operation_outputs` and `workflow_status` respectively. The
/// variants are named after those columns.
///
/// Those two columns are independently nullable, so a row could carry both — which would make
/// the work simultaneously successful and failed, and leave a replay believing whichever the
/// reader happened to check first. Every implementation forbids it; Python asserts
/// `error is None or output is None`. A sum type means there is nothing to assert.
///
/// For a workflow this also settles the status, which is why
/// [`record_workflow_outcome`](crate::sysdb::SystemDatabase::record_workflow_outcome) does not
/// take one. No implementation treats it as a free choice: Go computes
/// `status := Success; if err != nil { status = Error }`, and Java splits the call into
/// `recordWorkflowOutput` and `recordWorkflowError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome<'a> {
    /// The work returned.
    ///
    /// `None` is a void return, which is a *success* and not an absent result. Whether a step ran
    /// at all is answered by the presence of its row, never by this being empty.
    Output(Option<&'a str>),
    /// The work raised, carrying the encoded error.
    Error(&'a str),
}

impl<'a> Outcome<'a> {
    /// The terminal status this outcome puts a workflow in.
    pub fn status(self) -> WorkflowStatus {
        match self {
            Outcome::Output(_) => WorkflowStatus::Success,
            Outcome::Error(_) => WorkflowStatus::Error,
        }
    }

    /// The pair of column values, in the order the tables hold them.
    pub(crate) fn columns(self) -> (Option<&'a str>, Option<&'a str>) {
        match self {
            Outcome::Output(value) => (value, None),
            Outcome::Error(message) => (None, Some(message)),
        }
    }
}

/// When a step ran, start and finish together.
///
/// A pair rather than two fields because the database records completed steps: a start with no
/// finish is not a state this table has, and half a pair yields a duration nobody can compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepTiming {
    /// When the step began.
    pub started_at: Timestamp,
    /// When the step finished, and the token that makes recording it idempotent.
    ///
    /// Fixed before the write and **it must not move between retries of the same logical
    /// record**. On a duplicate insert the stored value is compared against this one: equal means
    /// this caller's own write whose acknowledgement was lost, different means another execution
    /// recorded the step first. Re-stamping the clock per attempt would turn every lost
    /// acknowledgement into a spurious conflict — so hold the `StepTiming` in a variable rather
    /// than building it at the call site inside a retry loop.
    ///
    /// Omitting the timing altogether gives up that detection: with no recorded completion there
    /// is nothing to compare, so a duplicate write is accepted rather than reported. Java
    /// behaves the same way, guarding its comparison with `if (endTimeEpochMs != null)`.
    pub completed_at: Timestamp,
}

/// When a delayed workflow should become eligible to run.
///
/// A sum type because the two forms are alternatives, not options: Python takes
/// `delay_seconds` and `delay_until_epoch_ms` as separate keyword arguments and raises when both
/// are given, and Java models it as a sealed interface. Both resolve to the same column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowDelay {
    /// Wait this long from now.
    ///
    /// Resolved against the database layer's clock, for the same reason
    /// [`NewWorkflow::delay`] is: the caller's skew should not reach the row.
    For(Duration),
    /// Wait until this instant.
    Until(Timestamp),
}

impl WorkflowDelay {
    /// The instant this delay expires, resolved against `now` if it is relative.
    pub(crate) fn resolve(self, now: Timestamp) -> Timestamp {
        match self {
            WorkflowDelay::For(d) => {
                Timestamp::from_epoch_ms(now.as_epoch_ms() + d.as_millis() as i64)
            }
            WorkflowDelay::Until(t) => t,
        }
    }
}

/// Why a workflow is being submitted, which decides whether it may claim a row someone holds.
///
/// The references model this as two booleans, `is_recovery_request` and `is_dequeued_request`,
/// but **no call site in any of them sets both** — Python's dispatchers pass exactly
/// `(True, False)` from recovery and `(False, True)` from the queue, and nothing else. A
/// three-way choice is what it has always been, and naming it removes an unreadable pair of
/// adjacent `bool` arguments that would compile just as happily swapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Submission {
    /// A first attempt, which does not claim a workflow another owner already holds.
    #[default]
    Fresh,
    /// Recovering a workflow a dead executor left behind.
    Recovery,
    /// Dequeuing, which claims a workflow that was enqueued.
    Dequeue,
}

impl Submission {
    /// Whether this submission is being told it owns the workflow.
    ///
    /// Recovery and dequeue are kept apart because the references keep them apart, even though
    /// both answer this the same way: both count against the recovery budget, and both may claim
    /// a row another owner holds, which from a fresh start would be theft.
    pub(crate) fn claims_ownership(self) -> bool {
        matches!(self, Submission::Recovery | Submission::Dequeue)
    }
}

/// The result of trying to record a workflow's final outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeWrite {
    /// This process recorded the outcome.
    Recorded,
    /// Another process got there first, and the row is already terminal.
    ///
    /// Not an error: the caller has lost a race it was allowed to lose, and should adopt the
    /// recorded outcome rather than overwrite it.
    AlreadyFinished,
}

/// What the database said about a workflow after initialising it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowInitResult {
    /// The stored status, which is the existing one when the row was already there.
    pub status: WorkflowStatus,
    /// Recovery attempts recorded against the workflow, after this one.
    pub recovery_attempts: i64,
    /// The workflow's absolute expiry, as the database now holds it.
    ///
    /// Reported back because it may not be the one offered: a timeout is turned into a deadline
    /// against the database layer's clock, and an existing row keeps the deadline it already had.
    pub deadline: Option<Timestamp>,
    /// The serialization format actually stored.
    ///
    /// May differ from what the caller offered: the first writer decides the format, and every
    /// later attempt has to read the payloads that are actually there.
    pub serialization: Option<String>,
    /// Whether this caller should go on to run the workflow.
    ///
    /// `false` means another owner holds it and this attempt is not a recovery — so the row is
    /// recorded, but running it would be a second execution.
    pub should_execute: bool,
}
