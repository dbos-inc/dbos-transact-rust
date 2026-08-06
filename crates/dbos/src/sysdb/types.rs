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
    /// `i32`, not `i64`: the column is `INT4` on both backends, and widening here would make
    /// values round-trip through a type the column cannot hold.
    pub priority: Option<i32>,
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
