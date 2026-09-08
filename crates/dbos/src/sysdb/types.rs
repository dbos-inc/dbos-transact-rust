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
//! [`Timestamp`] is a thin wrapper over the epoch milliseconds actually stored, so it converts
//! exactly rather than through someone's calendar, and `Duration` is in `std`. Callers wanting a
//! calendar type convert at their own edge.
//!
//! One exception, and it is the schema's rather than a preference: `workflow_schedules.last_fired_at`
//! holds ISO-8601 text, so a calendar is unavoidable for that column. `std` has none, so
//! [`Timestamp::to_iso8601`] and [`Timestamp::parse_iso8601`] go through `time` — parsing and
//! formatting only, with no timezone database, since the column is always UTC.

use super::Error;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// An instant, as epoch milliseconds.
///
/// Exactly what the columns hold, so reading and writing are lossless.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
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
    ///
    /// The `as` cast is safe by construction rather than by luck: it narrows a `u128` of
    /// milliseconds since 1970, which does not reach `i64::MAX` until the year 292{,}277{,}024.
    /// Elsewhere a caller supplies the duration and the conversion has to report — see
    /// [`checked_add`](Self::checked_add) — but here the clock supplies it.
    ///
    /// A clock set before 1970 gives the epoch rather than panicking. That is the lesser wrong:
    /// the timestamps this layer writes are for ordering and display, and a machine with a broken
    /// clock should record misleading times rather than fail every workflow it touches.
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

    /// Converts from a `SystemTime`, or `None` if it cannot be stored.
    ///
    /// Two ways it cannot: an instant before the epoch, and one so far after it that the
    /// milliseconds overflow `i64`. Both report rather than saturate, for the reason given on
    /// [`to_system_time`](Self::to_system_time) — a value this layer cannot hold is the caller's
    /// to hear about, not one to replace with a different instant.
    ///
    /// The second case is reachable where [`now`](Self::now)'s is not: the instant comes from the
    /// caller, and a `SystemTime` can hold seconds that do not survive being multiplied by a
    /// thousand.
    pub fn from_system_time(time: SystemTime) -> Option<Self> {
        time.duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .map(Self)
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

    /// Reads an ISO-8601 instant, or `None` if the text is not one.
    ///
    /// The inverse of [`to_iso8601`](Self::to_iso8601), and it has to read what the other four
    /// implementations wrote: Python's `+00:00` offset, TypeScript's `.000Z`, Go's `RFC3339Nano`
    /// and Java's bare `Z` all parse to the same instant, as does Conductor's explicit numeric
    /// offset — which it writes rather than `Z` because `datetime.fromisoformat` rejects `Z`
    /// before Python 3.11.
    ///
    /// Strict RFC 3339 otherwise: an offset is required and must carry its colon. That is
    /// stricter than a hand-written reader would be tempted to make it, and it turns away nothing
    /// any writer produces — Conductor's own reader is Go's `time.RFC3339`, which draws the line
    /// in the same place.
    ///
    /// **Sub-millisecond precision is truncated**, since that is all [`Timestamp`] holds. Go
    /// writes nanoseconds, so a schedule it fired can carry them; cron granularity is seconds, so
    /// the digits being dropped cannot change a firing decision.
    pub fn parse_iso8601(text: &str) -> Option<Self> {
        let parsed = OffsetDateTime::parse(text, &Rfc3339).ok()?;
        // Nanoseconds to milliseconds, flooring, so an instant before the epoch truncates towards
        // the earlier millisecond rather than towards zero.
        i64::try_from(parsed.unix_timestamp_nanos().div_euclid(1_000_000))
            .ok()
            .map(Self)
    }

    /// Formats as ISO-8601 in UTC: `2026-08-12T14:30:00.123Z`, or `2026-08-12T14:30:00Z` on a
    /// whole second.
    ///
    /// For [`workflow_schedules.last_fired_at`](crate::sysdb::SystemDatabase::update_schedule_last_fired_at),
    /// the one column in the schema holding a formatted instant rather than epoch milliseconds.
    /// This is Go's and Java's spelling of the four; every implementation's reader accepts it.
    ///
    /// Infallible in practice and `String` rather than `Result` because of it: the only way
    /// `time` refuses to format is a year outside its range, which is four orders of magnitude
    /// further out than any millisecond an `i64` can hold.
    pub fn to_iso8601(self) -> String {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(self.0) * 1_000_000)
            .ok()
            .and_then(|t| t.format(&Rfc3339).ok())
            .unwrap_or_default()
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
///
/// `None` for anything that is not one: negative, NaN, infinite, or larger than [`Duration`] can
/// hold. The last is not hypothetical — the columns are `DOUBLE PRECISION`, so a row can carry
/// `1e300`, and `Duration::from_secs_f64` **panics** on it. Checking `is_finite()` does not cover
/// it; `try_from_secs_f64` covers all four.
pub fn duration_from_secs(secs: f64) -> Option<Duration> {
    Duration::try_from_secs_f64(secs).ok()
}

/// Where a workflow is in its lifecycle.
///
/// Stored as text and shared with every other DBOS implementation, so the spellings are a wire
/// format rather than an internal choice.
///
/// The serde representation is those same spellings, which is what the rename on the derive is
/// for: a status that round-trips through a checkpoint — see [`DBOS::list_workflows`] — must
/// come back as what [`as_str`](Self::as_str) would have written.
///
/// [`DBOS::list_workflows`]: crate::DBOS::list_workflows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
///
/// `Serialize`/`Deserialize` because a listing made from inside a workflow is checkpointed and
/// replays from what it recorded, so the rows have to round-trip. No other implementation reads
/// that encoding — it is the engine talking to its own replay.
///
/// **It is still a stored format, and the build that reads it is rarely the one that wrote it.**
/// A workflow checkpoints a listing on one deployment and replays it on the next, which is the
/// ordinary case rather than the exotic one — recovery after a deploy, and forking onto fixed
/// code, both land there. So **every field here carries `#[serde(default)]`** bar the two the
/// schema declares `NOT NULL` and this layer always writes, and a field added later must carry it
/// too. Without it, adding a column — this struct mirrors `workflow_status`, which gains them —
/// makes an older checkpoint unreadable, and `run_transactional_step` reports that as
/// [`Error::Malformed`]: not a retryable class, raised on the
/// replay path, so the workflow can never get past that step. Unknown fields are already
/// tolerated, `serde` ignoring them by default, so the reverse direction needs nothing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowRecord {
    /// Primary key, and the identity a caller uses everywhere else.
    pub workflow_id: String,
    /// Lifecycle state.
    pub status: WorkflowStatus,
    /// Registered function name. Nullable in the schema, so nullable here.
    #[serde(default)]
    pub name: Option<String>,
    /// The type the function belongs to, for a method or a configured instance.
    #[serde(default)]
    pub class_name: Option<String>,
    /// The configured instance, if any.
    #[serde(default)]
    pub config_name: Option<String>,
    /// Encoded input, in whatever format `serialization` names.
    #[serde(default)]
    pub input: Option<String>,
    /// Encoded output, present once the workflow succeeds.
    #[serde(default)]
    pub output: Option<String>,
    /// Encoded error, present once the workflow fails.
    #[serde(default)]
    pub error: Option<String>,
    /// Which format the payloads above are in. `None` on rows written before the column
    /// existed, which means the writer's own default.
    #[serde(default)]
    pub serialization: Option<String>,
    /// The executor that most recently claimed this workflow.
    #[serde(default)]
    pub executor_id: Option<String>,
    /// Application version that created it, used to keep recovery on compatible code.
    #[serde(default)]
    pub application_version: Option<String>,
    /// Recovery attempts so far, against the dead-letter limit.
    #[serde(default)]
    pub recovery_attempts: i64,
    /// Queue this workflow was enqueued on, if it was.
    #[serde(default)]
    pub queue_name: Option<String>,
    /// When the row was created.
    #[serde(default)]
    pub created_at: Timestamp,
    /// When the row last changed.
    #[serde(default)]
    pub updated_at: Timestamp,
    /// When execution began, if it has.
    #[serde(default)]
    pub started_at: Option<Timestamp>,
    /// When the workflow reached a terminal state.
    #[serde(default)]
    pub completed_at: Option<Timestamp>,
    /// The workflow that forked this one, if any.
    #[serde(default)]
    pub forked_from: Option<String>,
    /// The workflow that started this one as a child, if any.
    #[serde(default)]
    pub parent_workflow_id: Option<String>,
    /// Whether this workflow has been forked from at least once.
    #[serde(default)]
    pub was_forked_from: bool,

    // ── Ownership and attribution ──────────────────────────────────────────────
    /// Identity of the attempt currently holding this workflow.
    ///
    /// The single-execution guard: distinct per attempt, unlike `executor_id`, which defaults
    /// to `"local"` and collides between processes on one machine.
    #[serde(default)]
    pub owner_xid: Option<String>,
    /// Deployment identifier, for installations running several applications.
    #[serde(default)]
    pub application_id: Option<String>,
    /// User on whose behalf the workflow runs.
    #[serde(default)]
    pub authenticated_user: Option<String>,
    /// Roles that user holds.
    ///
    /// Decoded from the column's JSON array, which this layer owns — see
    /// [`NewWorkflow::authenticated_roles`]. A NULL column reads as empty.
    #[serde(default)]
    pub authenticated_roles: Vec<String>,
    /// Role actually assumed for this execution.
    #[serde(default)]
    pub assumed_role: Option<String>,
    /// Request context captured at creation.
    #[serde(default)]
    pub request: Option<String>,
    /// The application that owns this workflow, or `None` if it is unclaimed.
    ///
    /// Unclaimed means no application has taken it — a row written before any implementation
    /// supported ownership, or by a handle with no application of its own. Every application may
    /// run it, and the first to dequeue it claims it.
    #[serde(default)]
    pub application_name: Option<String>,

    // ── Queueing ───────────────────────────────────────────────────────────────
    /// Deduplication key within the queue. At most one live workflow may hold a given key.
    #[serde(default)]
    pub deduplication_id: Option<String>,
    /// Dequeue priority; lower runs sooner.
    ///
    /// `i32` and not optional, mirroring `INT4 NOT NULL DEFAULT 0`. Widening to `i64` would let
    /// values round-trip through a type the column cannot hold, and making it optional would
    /// invite writing a NULL the column rejects.
    #[serde(default)]
    pub priority: i32,
    /// Partition this workflow belongs to, on a partitioned queue.
    #[serde(default)]
    pub queue_partition_key: Option<String>,
    /// Whether a rate limiter is currently holding this workflow back.
    #[serde(default)]
    pub rate_limited: bool,
    /// Schedule that enqueued this workflow, if a schedule did.
    #[serde(default)]
    pub schedule_name: Option<String>,

    // ── Timing ─────────────────────────────────────────────────────────────────
    /// How long the workflow may run for.
    ///
    /// A **duration**, unlike `deadline` below, which is an instant. The two are adjacent
    /// columns and mean different things: this is a budget, that is a wall-clock cutoff.
    #[serde(default)]
    pub timeout: Option<Duration>,
    /// Wall-clock instant the workflow must finish by.
    #[serde(default)]
    pub deadline: Option<Timestamp>,
    /// Instant before which the workflow must not be dequeued.
    #[serde(default)]
    pub delay_until: Option<Timestamp>,
    /// Cap past which a debounce may not push the delay any further.
    #[serde(default)]
    pub debounce_deadline: Option<Timestamp>,
    /// Whether the deduplication id is a debounce key, cleared on DELAYED to ENQUEUED.
    #[serde(default)]
    pub is_debounced: bool,

    // ── Caller-supplied metadata ───────────────────────────────────────────────
    /// Arbitrary attributes attached at creation, stored as JSON.
    ///
    /// Opaque here like every other payload: this layer stores and returns the text and does
    /// not parse it, even though the column is `jsonb` and is queried by containment.
    #[serde(default)]
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
/// - `parent_workflow_id` belongs to the child start, and travels on [`InitWorkflowCaller`] with
///   the rest of what the parent contributes. A workflow has a parent exactly when its start was
///   recorded against one — the two are one event, and one of them writing without the other is
///   the state the pair exists to make unreachable, so there is no caller who can meaningfully
///   set the column alone.
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

    /// The application this workflow belongs to; `None` means the writing handle's own.
    ///
    /// On the input rather than taken from the handle for the same reason
    /// [`executor_id`](Self::executor_id) is: a client enqueueing *for* another application names
    /// it here, and only the insert decides a workflow's owner — nothing re-owns it afterwards.
    /// Leaving both this and the handle's name unset writes an unclaimed workflow, which every
    /// application may run and the first to dequeue claims.
    pub application_name: Option<&'a str>,
    /// Application version, which recovery uses to avoid resuming under changed code.
    pub application_version: Option<&'a str>,
    /// Application id, as assigned by the platform.
    pub application_id: Option<&'a str>,

    /// Authenticated principal at submission.
    pub authenticated_user: Option<&'a str>,
    /// The roles that principal held.
    ///
    /// A list rather than an encoded string, unlike `input` and the outcome payloads: those are
    /// opaque to this layer and cross as whatever the caller encoded, but the column is *always*
    /// a JSON array of strings, so the encoding belongs here. Java calls
    /// `JsonUtility.toJson(List<String>)` and Python's field comment reads "JSON list of roles".
    ///
    /// Empty is stored as NULL, on the same reasoning as the empty-string normalisation for
    /// `authenticated_user`: "no roles" should have one representation in the column.
    pub authenticated_roles: Vec<&'a str>,
    /// The role actually assumed.
    pub assumed_role: Option<&'a str>,

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
                field: "workflow_id".into(),
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
                    field: field.into(),
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
                    field: field.into(),
                    detail: "must be a positive, non-zero duration".to_owned(),
                });
            }
        }
        validate_attributes(self.attributes)?;
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

    /// The millisecond conversion either side of `time`, which is the part this crate owns.
    #[test]
    fn an_instant_formats_as_iso8601() {
        let iso = |ms| Timestamp::from_epoch_ms(ms).to_iso8601();

        assert_eq!(iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso(1_786_492_800_000), "2026-08-12T00:00:00Z");
        // A fraction appears only when there is one, which is Go's and Java's spelling.
        assert_eq!(iso(1_786_492_800_123), "2026-08-12T00:00:00.123Z");
        // Before the epoch the millisecond floors rather than truncating towards zero, which is
        // the direction `i128::div_euclid` is chosen for.
        assert_eq!(iso(-1), "1969-12-31T23:59:59.999Z");
        assert_eq!(iso(-86_400_000), "1969-12-31T00:00:00Z");
    }

    /// The four spellings the other implementations write all read back as the same instant.
    #[test]
    fn an_iso8601_instant_parses_from_every_implementations_format() {
        let parse = Timestamp::parse_iso8601;
        let expected = Some(Timestamp::from_epoch_ms(1_786_492_800_000));

        assert_eq!(parse("2026-08-12T00:00:00.000Z"), expected, "TypeScript");
        assert_eq!(parse("2026-08-12T00:00:00Z"), expected, "Go and Java");
        assert_eq!(parse("2026-08-12T00:00:00+00:00"), expected, "Python");
        assert_eq!(
            parse("2026-08-12T00:00:00.000000000Z"),
            expected,
            "RFC3339Nano"
        );
        // An offset is applied rather than ignored, in either direction.
        assert_eq!(parse("2026-08-12T01:30:00+01:30"), expected);
        assert_eq!(parse("2026-08-11T19:00:00-05:00"), expected);

        // Conductor formats with an explicit numeric offset rather than `Z`, deliberately:
        // `datetime.fromisoformat` rejects `Z` before Python 3.11 and dbos-transact-py supports
        // 3.10 (`services/conductor/openmetrics.go`, layout `2006-01-02T15:04:05.999-07:00`).
        // Its fraction is elided on a whole second, so both of these reach a reader.
        assert_eq!(parse("2026-08-12T00:00:00+00:00"), expected);
        assert_eq!(
            parse("2026-08-12T00:00:00.123+00:00"),
            Some(Timestamp::from_epoch_ms(1_786_492_800_123))
        );

        // Sub-millisecond digits truncate rather than round, and a short fraction pads.
        let ms = |t: Option<Timestamp>| t.unwrap().as_epoch_ms() % 1_000;
        assert_eq!(ms(parse("2026-08-12T00:00:00.5Z")), 500);
        assert_eq!(ms(parse("2026-08-12T00:00:00.123999Z")), 123);

        // Strict RFC 3339: the offset is required and carries its colon. Neither rejection is
        // reachable from a writer — Python's `isoformat` always emits the colon, and Conductor's
        // layout spells the offset `-07:00` — and Conductor's own reader is Go's `time.RFC3339`,
        // which rejects exactly the same two. Accepting them would be this crate inventing
        // leniency nobody asked for.
        for bad in [
            "",
            "2026-08-12",
            "2026-08-12T00:00",
            "not-a-date-at-all",
            "2026-13-01T00:00:00Z",
            "2026-08-12T24:00:00Z",
            "2026-08-12T00:00:00",
            "2026-08-12T00:00:00+0130",
        ] {
            assert_eq!(parse(bad), None, "{bad:?} is not an instant this reads");
        }
    }

    /// Formatting and parsing are inverses, which is what makes the column round-trip.
    #[test]
    fn an_iso8601_instant_round_trips() {
        for ms in [
            0,
            -1,
            1_786_492_800_000,
            1_709_209_845_123,
            951_827_445_999,
            -2_203_977_600_000,
        ] {
            let instant = Timestamp::from_epoch_ms(ms);
            assert_eq!(
                Timestamp::parse_iso8601(&instant.to_iso8601()),
                Some(instant),
                "{ms} did not survive the round trip"
            );
        }
    }

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

    /// An instant this layer cannot store is reported, whichever side of the epoch it falls.
    #[test]
    fn an_unstorable_system_time_is_not_a_timestamp() {
        assert_eq!(
            Timestamp::from_system_time(UNIX_EPOCH - Duration::from_secs(1)),
            None,
            "before the epoch"
        );
        // Representable as a `SystemTime`, but not as milliseconds in an `i64`.
        let far = UNIX_EPOCH
            .checked_add(Duration::from_secs(i64::MAX as u64 / 100))
            .expect("a SystemTime that far out is constructible");
        assert_eq!(Timestamp::from_system_time(far), None, "too far after it");
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
        // Finite and non-negative, and still not a duration: the guard this replaced let it
        // through to a panic, and the columns it reads are wide enough to hold it.
        assert_eq!(duration_from_secs(1e300), None);
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

/// Which applications' rows a query covers.
///
/// Three cases rather than a list, because "no applications named" is genuinely ambiguous and the
/// two readings are opposites. Python and TypeScript encode the same three states as
/// `Optional[List[str]]`, where an *empty* list means every application and an *absent* one means
/// this handle's own — a distinction they reach through `if not value` and `??` respectively,
/// state in no comment, and cover with no test. Spelled out here so it cannot be collapsed by a
/// later tidy-up that sees an empty list and a missing one as the same thing.
///
/// Unclaimed rows are matched in every case: they belong to no application, so they belong to all
/// of them. See [`Applications::Named`] for the one exception that is not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Applications<'a> {
    /// Let the query decide, which is what a caller who has not thought about it wants.
    ///
    /// A **search** scopes to the handle's own application: listing workflows without saying whose
    /// should not return a peer's. An **id-keyed read** matches every application instead, because
    /// a workflow id is a global address — asking for one by id is an identity read, not a search,
    /// and answering "no such workflow" for one that plainly exists would be a lie.
    #[default]
    Unset,
    /// Every application, said deliberately. What an operator's cross-application view asks for.
    Any,
    /// These applications, plus the unclaimed rows.
    ///
    /// An empty list is [`Applications::Any`] rather than "no applications": there is no useful
    /// query for rows belonging to none of the applications you named, and reading it as a
    /// narrowing would make an unfiltered UI silently show nothing.
    Named(Vec<&'a str>),
}

/// Which workflows to list, and how much of each to load.
///
/// Every field is a narrowing, and the default narrows nothing — so
/// `WorkflowFilter::default()` lists everything. List fields match any of their entries and are
/// ignored when empty; `Option<bool>` fields are three-valued, where `None` does not filter.
///
/// [`applications`](Self::applications) is the exception, and deliberately so: its default scopes
/// to the caller's own application rather than to everything.
///
/// The set is the union of all four implementations, which do not agree on it. Go has 28
/// filters, Python 26, Java adds two Go lacks. Where they diverge it is noted on the field, so a
/// missing filter reads as a decision rather than an oversight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowFilter<'a> {
    /// Exact workflow ids.
    ///
    /// Supplying any makes this an id-keyed read, which changes what
    /// [`applications`](Self::applications) defaults to — see [`Applications::Unset`].
    pub workflow_ids: Vec<&'a str>,
    /// Workflow ids starting with any of these.
    ///
    /// A prefix is a search, not an address, so unlike [`workflow_ids`](Self::workflow_ids) it
    /// does not make this an id-keyed read.
    pub workflow_id_prefixes: Vec<&'a str>,

    /// Whose workflows to list. Defaults to the caller's own application plus unclaimed ones.
    pub applications: Applications<'a>,

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
            applications: Applications::Unset,
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
///
/// Serde-able for the same reason as [`WorkflowRecord`], and `#[serde(default)]` per field for
/// the same reason too: a step listing taken from inside a workflow is itself a checkpointed
/// step, replays from what it recorded, and the build that reads the record is rarely the one
/// that wrote it. The exemptions here are the primary key and the name a replay matches on —
/// defaulting those would let a record this build cannot fully read pass as step 0 of a step
/// with no name, which is worse than reporting it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    #[serde(default)]
    pub output: Option<String>,
    /// Encoded error, if the step raised one. Never set alongside `output`.
    #[serde(default)]
    pub error: Option<String>,
    /// The workflow this step started, for steps that are child-workflow calls.
    #[serde(default)]
    pub child_workflow_id: Option<String>,
    /// How `output` and `error` are encoded.
    #[serde(default)]
    pub serialization: Option<String>,
    /// When the step began.
    #[serde(default)]
    pub started_at: Option<Timestamp>,
    /// When the step finished.
    ///
    /// Also the tie-breaker on a duplicate record: a second write carrying a *different*
    /// completion time is another executor, while one carrying the same is this caller's own
    /// retry. See [`SystemDatabase::record_step`](crate::sysdb::SystemDatabase::record_step).
    #[serde(default)]
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

/// How somebody else's workflow ended.
///
/// The read counterpart of [`Outcome`], and it has four variants where that has two. `Outcome` is
/// what a *run* reports, and a run can only return or raise. Cancelling and parking are done to a
/// workflow rather than by it, so they are states no run ever reports — and it is precisely the
/// caller waiting on a workflow it is not running that has to be told about them.
///
/// **"Awaited" is the references' own word for that side of the relationship**, and it is load-bearing
/// rather than decorative: Python raises `DBOSAwaitedWorkflowCancelledError` specifically so a
/// cancelled *awaited* workflow is not mistaken for the *awaiting* one having been cancelled. The
/// same distinction is why the variants below are values — see the second bullet.
///
/// Not named for terminality, because one variant is not terminal:
/// [`WorkflowStatus::is_terminal`] is deliberately false for
/// [`MaxRecoveryAttemptsExceeded`](WorkflowStatus::MaxRecoveryAttemptsExceeded), which
/// [`Parked`](Self::Parked) is. It ends a *wait* without ending the workflow, since the workflow can
/// still be resumed.
///
/// **All four come back as values, including the two that are failures.** Two reasons, and the
/// second is the one that would be a bug:
///
/// - A failed workflow's error is encoded in whatever the workflow chose, and this layer does not
///   deserialize payloads. So a failure is already data here rather than something to raise, and
///   Go's `AwaitWorkflowResult` returns the same string for the same reason.
/// - **A cancelled workflow must not be reported as the caller being cancelled.**
///   [`Error::WorkflowCancelled`] means the opposite thing
///   — the step-replay check refusing to run *this* workflow's steps — and returning it here would
///   read as the waiter having been cancelled. Python is explicit about the distinction, raising a
///   separate `DBOSAwaitedWorkflowCancelledError` "because the awaiting workflow is not being
///   cancelled". Reporting all four uniformly leaves the user-facing error taxonomy to the engine,
///   which owns it.
///
/// Go returns a result *and* an error together for the same call, which in Rust is only
/// expressible as a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AwaitedOutcome {
    /// It returned. `None` is a void return, which is a success and not an absent result.
    Succeeded {
        /// Encoded return value.
        output: Option<String>,
        /// How `output` is encoded.
        serialization: Option<String>,
    },
    /// It raised, carrying the encoded error.
    Failed {
        /// Encoded error.
        error: String,
        /// How `error` is encoded.
        serialization: Option<String>,
    },
    /// It was cancelled, so it has neither a value nor an error of its own.
    Cancelled,
    /// It was recovered too many times and parked, so it has neither.
    ///
    /// The limit it exceeded is not stored — `max_recovery_attempts` is an input to
    /// [`init_workflow`](crate::sysdb::SystemDatabase::init_workflow) rather than a column — so
    /// the count is what can be reported. Go reports `attempts - 2` as the limit, which is an
    /// approximation of the same missing number.
    Parked {
        /// Recovery attempts recorded against the workflow.
        recovery_attempts: i64,
    },
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
    /// **A step recorded on the same transaction that checked for it is the exception**, and
    /// `run_transactional_step` is the one that does: an attempt whose commit was acknowledged to
    /// nobody is caught by the check on the next attempt and replayed, so it never reaches the
    /// insert. Only a genuine rival survives to be compared there, and a completion that differs
    /// from this attempt's is the right answer rather than a spurious one — which is why that
    /// path stamps the clock after its work rather than before it.
    ///
    /// Omitting the timing altogether gives up that detection: with no recorded completion there
    /// is nothing to compare, so a duplicate write is accepted rather than reported. Java
    /// behaves the same way, guarding its comparison with `if (endTimeEpochMs != null)`.
    pub completed_at: Timestamp,
}

/// Rejects attributes that are not a JSON *object*.
///
/// The contract is an object, not arbitrary JSON: every other implementation takes a map — Java
/// `Map<String, Object>`, Python `Dict[str, Any]`, Go `map[string]any` — and TypeScript rejects
/// arrays outright with *"must be a key-value object"*. Since this layer takes the encoded form,
/// so a host across an FFI boundary can hand over bytes it already has, the same check has to
/// happen here rather than falling out of the type.
///
/// It matters beyond tidiness: `attributes @> …` in [`WorkflowFilter`] is containment against an
/// object, so a stored array or scalar would silently never match.
pub(crate) fn validate_attributes(attributes: Option<&str>) -> Result<(), Error> {
    let Some(json) = attributes else {
        return Ok(());
    };
    serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json).map_err(|e| {
        Error::InvalidInput {
            field: "attributes".into(),
            detail: format!("must be a JSON object: {e}"),
        }
    })?;
    Ok(())
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
    /// Resolved by the system database layer rather than by the caller, for the same reason
    /// [`NewWorkflow::delay`] is a duration: `now + delay` computed at a call site is one more
    /// place for the two to disagree.
    ///
    /// The clock is **this process's**, not the database's, which leaves the stamp out by whatever
    /// this host and the releasing supervisor's disagree by — UPSTREAM item 22, shared with all
    /// four implementations.
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

/// Whether a name is usable as an application name.
///
/// Three to 256 characters of lowercase letters, digits, dashes and underscores — the rule
/// every implementation enforces, so a name registered by one is accepted by the others. The
/// length is in bytes, which is the same as characters here because nothing outside ASCII passes.
pub fn is_valid_application_name(name: &str) -> bool {
    (3..=256).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

/// Which rows a rename moves.
///
/// A sum type rather than a name plus an `adopt_unclaimed` flag. That pair admits four states and
/// only three are valid requests: naming no application and not adopting unclaimed selects no rows
/// at all. Python and TypeScript both reject that combination at runtime with
/// *"Nothing to re-own"*.
///
/// **Unclaimed rows are never implied**, unlike everywhere else in this feature, where
/// `IS NULL` rides along with every ownership predicate. An unclaimed row belongs to every
/// application, so taking it away from all of them is a decision rather than a consequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameFrom<'a> {
    /// Only what this application already holds.
    Application(&'a str),
    /// What this application holds, and the unclaimed rows with it.
    ApplicationAndUnclaimed(&'a str),
    /// Only the unclaimed rows — adopting work that no application owns.
    Unclaimed,
}

impl<'a> RenameFrom<'a> {
    /// The application being renamed, if the source names one.
    pub fn application(self) -> Option<&'a str> {
        match self {
            RenameFrom::Application(name) | RenameFrom::ApplicationAndUnclaimed(name) => Some(name),
            RenameFrom::Unclaimed => None,
        }
    }
}

/// The batch size every implementation defaults to.
pub const DEFAULT_RENAME_BATCH_SIZE: u32 = 10_000;

/// How a rename moves the rows that do not have to move atomically.
///
/// Only the terminal workflows and their steps are batched. Those can be an application's whole
/// history, and they scope observability and garbage collection alone, so they may lag the rest of
/// the rename without anything reading a half-renamed state.
///
/// The references say this with a nullable integer, where `None` reads as *unbatched* rather
/// than as "unset": Python takes its default from the parameter's default value, and TypeScript
/// separates `null` (unbatched) from `undefined` (absent, so the default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameBatching {
    /// One statement per table, however many rows match. Simplest, and fine for a short history.
    Unbatched,
    /// At most this many distinct workflow ids per statement.
    Batched(u32),
}

impl Default for RenameBatching {
    fn default() -> Self {
        RenameBatching::Batched(DEFAULT_RENAME_BATCH_SIZE)
    }
}

/// What a rename moved, by table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplicationRowCounts {
    /// Queue registrations.
    pub queues: u64,
    /// Schedule registrations.
    pub schedules: u64,
    /// Registered application versions.
    pub versions: u64,
    /// Workflows, in-flight and terminal together.
    pub workflows: u64,
    /// Recorded steps.
    pub steps: u64,
}

/// How many workflows a queue may start per window.
///
/// One value rather than two `Option`s, because the two columns behind it are only meaningful
/// together: a limit with no window and a window with no limit are both unenforceable. Java draws
/// the line in the same place, as `record RateLimit(int limit, Duration period)`.
///
/// The schema stores the pair as two nullable columns and cannot enforce that, so a row with one
/// set and not the other is [`Error::Malformed`] on read — a state this type says cannot exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// Workflows that may start in each window.
    pub limit: i32,
    /// The window the starts are counted over.
    pub period: Duration,
}

/// Whether a partial update touches a field.
///
/// The third state a plain `Option` cannot carry: for a nullable column, "leave it alone" and
/// "set it to NULL" are different requests, and `Option<T>` has one spelling for both.
///
/// Python and TypeScript pass a dictionary of columns to values, and the three states fall out of
/// it: a key that is absent leaves the column alone, and a key that is present sets it — including
/// to null. `Change` is that, typed. [`Change::Leave`] is the absent key, [`Change::Set`] the
/// present one, and the inner value is what the key held.
///
/// Typing it also closes a gap their dictionaries leave open: nothing stops a caller passing a key
/// whose value is missing rather than null, and the two are indistinguishable by the time the
/// `SET` clause is built. Here they are separate constructors, so there is no third thing to
/// write.
///
/// Clearability lives in the *inner* type rather than in a third variant, so it mirrors the
/// column: `Change<Option<i32>>` can leave, clear or set, while `Change<bool>` can only leave or
/// set, because the column behind it is `NOT NULL`. Java splits the same line with two
/// mechanisms — `Field<Integer>` for nullable columns, `Optional<Boolean>` for the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change<T> {
    /// Leave whatever is stored.
    Leave,
    /// Store this instead.
    Set(T),
}

impl<T> Default for Change<T> {
    /// Leaves the field alone, so `..Default::default()` narrows an update rather than widening
    /// it. Hand-written because deriving would demand `T: Default`, which says nothing here.
    fn default() -> Self {
        Change::Leave
    }
}

impl<T> Change<T> {
    /// The value to store, if this changes anything.
    pub fn set(self) -> Option<T> {
        match self {
            Change::Set(value) => Some(value),
            Change::Leave => None,
        }
    }

    /// Whether this leaves the field alone.
    pub fn is_leave(&self) -> bool {
        matches!(self, Change::Leave)
    }
}

/// The fields of a registered queue that a partial update may change.
///
/// Separate from [`NewQueue`] because they answer different questions: registering describes a
/// whole queue, updating names only what moves. Java uses one type for both and its documentation
/// has to say that an absent field means "no limit **on creation** or leave unchanged **on
/// update**" — the same value meaning two things depending on which call it is passed to.
///
/// **Ownership is not here.** A queue changes hands only through
/// [`SystemDatabase::rename_application`](crate::sysdb::SystemDatabase::rename_application);
/// letting an update reassign it would be a silent takeover of a peer's queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueUpdate {
    /// See [`QueueRecord::concurrency`].
    pub concurrency: Change<Option<i32>>,
    /// See [`QueueRecord::worker_concurrency`].
    pub worker_concurrency: Change<Option<i32>>,
    /// See [`QueueRecord::rate_limit`]. One field for both columns, so an update cannot leave
    /// half a limit behind.
    pub rate_limit: Change<Option<RateLimit>>,
    /// See [`QueueRecord::priority_enabled`].
    pub priority_enabled: Change<bool>,
    /// See [`QueueRecord::partition_queue`].
    pub partition_queue: Change<bool>,
    /// See [`QueueRecord::partition_concurrency`].
    pub partition_concurrency: Change<Option<i32>>,
    /// See [`QueueRecord::partition_worker_concurrency`].
    pub partition_worker_concurrency: Change<Option<i32>>,
    /// See [`QueueRecord::partition_rate_limit`]. One field for both columns, so an update cannot
    /// leave half a limit behind.
    pub partition_rate_limit: Change<Option<RateLimit>>,
    /// See [`QueueRecord::polling_interval`].
    pub polling_interval: Change<Duration>,
}

impl QueueUpdate {
    /// This update applied to a record, giving the row as it would be after the write.
    ///
    /// What a caller's validation is handed: a limit is rarely wrong on its own and usually wrong
    /// only beside another, so the merged result is the only thing worth checking.
    pub fn apply_to(&self, record: &QueueRecord) -> QueueRecord {
        let partition_concurrency = self
            .partition_concurrency
            .set()
            .unwrap_or(record.partition_concurrency);
        let partition_worker_concurrency = self
            .partition_worker_concurrency
            .set()
            .unwrap_or(record.partition_worker_concurrency);
        let partition_rate_limit = self
            .partition_rate_limit
            .set()
            .unwrap_or(record.partition_rate_limit);
        // **The flag follows the limits**, so the merged row carries the flag the write will
        // store rather than the one the stored row happened to have. Three cases, and they are
        // the three the `UPDATE` assigns by: an update naming the flag is taken at its word; one
        // moving any per-partition limit has the flag rewritten to match what the row will hold;
        // one touching neither keeps what the row says, which is how a peer's
        // flag-without-limits row survives an unrelated update.
        //
        // Derived here rather than only in the `UPDATE` so that a validator is handed the row it
        // will actually get. An update clearing the last partition limit would otherwise be
        // judged against a row that still looked legacy-partitioned, and so through the
        // re-scoping in [`QueueRecord::resolved_limits`] — which points a refusal at the wrong
        // field even where the verdict comes out the same.
        let partition_queue = match self.partition_queue.set() {
            Some(flag) => flag,
            None if !(self.partition_concurrency.is_leave()
                && self.partition_worker_concurrency.is_leave()
                && self.partition_rate_limit.is_leave()) =>
            {
                partition_concurrency.is_some()
                    || partition_worker_concurrency.is_some()
                    || partition_rate_limit.is_some()
            }
            None => record.partition_queue,
        };
        QueueRecord {
            name: record.name.clone(),
            concurrency: self.concurrency.set().unwrap_or(record.concurrency),
            worker_concurrency: self
                .worker_concurrency
                .set()
                .unwrap_or(record.worker_concurrency),
            rate_limit: self.rate_limit.set().unwrap_or(record.rate_limit),
            priority_enabled: self
                .priority_enabled
                .set()
                .unwrap_or(record.priority_enabled),
            partition_queue,
            partition_concurrency,
            partition_worker_concurrency,
            partition_rate_limit,
            polling_interval: self
                .polling_interval
                .set()
                .unwrap_or(record.polling_interval),
            // Not updatable: the name is the queue's address, and ownership moves only by rename.
            application_name: record.application_name.clone(),
        }
    }

    /// Whether this would change nothing.
    pub fn is_empty(&self) -> bool {
        self.concurrency.is_leave()
            && self.worker_concurrency.is_leave()
            && self.rate_limit.is_leave()
            && self.priority_enabled.is_leave()
            && self.partition_queue.is_leave()
            && self.partition_concurrency.is_leave()
            && self.partition_worker_concurrency.is_leave()
            && self.partition_rate_limit.is_leave()
            && self.polling_interval.is_leave()
    }
}

/// A request to debounce a workflow onto a deduplication key.
///
/// A struct rather than nine parameters because five of them are `Option<&str>`, and transposing
/// two would compile. Python guards the same signature by making every argument
/// keyword-only (`def debounce_delayed_workflow(self, *, …)`); named fields are the same
/// protection.
///
/// **No `new` and no `Default`**, unlike [`NewWorkflow`] and [`NewQueue`], which take a single
/// identifying string. A constructor here would take three `&str` in a row and hand back the
/// transposition this type exists to prevent, and a default would fabricate a request with empty
/// names — the values the enqueue side rejects. Every field is named at the call site on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DebounceRequest<'a> {
    /// The registered function name, matched so a key collision between unrelated workflows never
    /// overwrites the wrong one's inputs.
    pub workflow_name: &'a str,
    /// The class the function belongs to, for class-bound workflows.
    pub class_name: Option<&'a str>,
    /// The configured instance name, for instance-bound workflows.
    ///
    /// Matched alongside the name and class because all three identify a workflow: two configured
    /// instances of one class share a name *and* a class, so without this a bounce for one could
    /// extend the other and replace its inputs. TypeScript matches the name and class; Python
    /// matches the name alone.
    pub config_name: Option<&'a str>,
    /// The queue the debounced workflow sits on.
    pub queue_name: &'a str,
    /// The debounce key, held in `deduplication_id` while the workflow is delayed.
    pub deduplication_id: &'a str,
    /// When the workflow should be released, capped at its debounce deadline.
    pub delay_until: Timestamp,
    /// The inputs to run with, replacing whatever the previous request left.
    pub inputs: Option<&'a str>,
    /// How `inputs` is encoded.
    pub serialization: Option<&'a str>,
    /// The application the bounce acts for; `None` means the writing handle's own.
    pub application_name: Option<&'a str>,
}

impl DebounceRequest<'_> {
    /// Rejects the values no writer could have stored, so a bounce cannot silently match nothing.
    ///
    /// The same rule [`NewWorkflow::validate`] applies on the way in: an empty class or instance
    /// is absent rather than a value, and matching one would find no row and report the key unheld
    /// — indistinguishable from a key nobody holds.
    pub fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("workflow_name", self.workflow_name),
            ("queue_name", self.queue_name),
            ("deduplication_id", self.deduplication_id),
        ] {
            if value.is_empty() {
                return Err(Error::InvalidInput {
                    field: field.into(),
                    detail: "must not be empty".to_owned(),
                });
            }
        }
        for (field, value) in [
            ("class_name", self.class_name),
            ("config_name", self.config_name),
            ("inputs", self.inputs),
            ("serialization", self.serialization),
            ("application_name", self.application_name),
        ] {
            if value == Some("") {
                return Err(Error::InvalidInput {
                    field: field.into(),
                    detail: "must be absent rather than empty".to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// What a debounce did, or why it could not.
///
/// Recorded as a debounce step's output when one runs inside a workflow, so a replay reports what
/// the first run did rather than bouncing again — see
/// [`SystemDatabase::debounce_delayed_workflow`](crate::sysdb::SystemDatabase::debounce_delayed_workflow).
///
/// Three outcomes rather than the flat record the references return: their `DebounceResult`
/// carries `bounced_workflow_id` alongside a run of `holder_*` fields, of which exactly one group
/// is ever populated. Reading it means checking which — and a stored one means *guessing* which,
/// since nothing in the shape rules out both groups being set at once. Named states cannot encode
/// that, so the ambiguity is gone rather than resolved by convention.
///
/// **The serialized form is this implementation's own, and it is this type's derive.** A step is
/// only ever replayed by the SDK that wrote it — workflows cross languages by enqueue alone — so
/// the encoding is recorded in the `serialization` column rather than agreed between
/// implementations. Python's default is `py_pickle`, base64-encoded pickle, and it is the *user's*
/// configurable serializer at that: there is no common wire form to conform to. The consequence is
/// that renaming a variant or field here changes what a replay expects to read, so a rename wants
/// the same care a schema change does.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Debounce {
    /// The existing delayed workflow was extended, and its inputs replaced.
    Bounced {
        /// The workflow whose delay moved.
        workflow_id: String,
    },
    /// The key is held by a workflow this bounce could not extend, described so the caller can
    /// tell a legitimate collision from a coincidence.
    Held(DebounceHolder),
    /// Nothing holds the key, so the caller should start a fresh workflow.
    Unheld,
}

/// The workflow holding a deduplication key, when a debounce could not extend it.
///
/// A bounce fails for more than one reason and they want different responses, so the holder is
/// described rather than merely reported: a **different name, class or instance** under the same
/// key is a collision between unrelated workflows (`"a" + "b-c"` against `"a-b" + "c"`, or two
/// configured instances of one class), a holder that is **not debounced** is an ordinary
/// deduplicated enqueue, and a **different application** means the collision is across
/// applications sharing the database.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DebounceHolder {
    /// The workflow holding the key.
    pub workflow_id: String,
    /// Whether the holder is itself a debounced workflow.
    pub is_debounced: bool,
    /// The holder's registered function name.
    pub workflow_name: Option<String>,
    /// The holder's class, for a class-bound workflow.
    pub class_name: Option<String>,
    /// The holder's configured instance name, for an instance-bound workflow.
    pub config_name: Option<String>,
    /// The application that owns the holder, or `None` if it is unclaimed.
    pub application_name: Option<String>,
}

/// A queue as the registry holds it.
///
/// The registry exists so a queue's limits can be read and changed without the executor that
/// declared it — a running deployment is not the only thing that knows what a queue is.
///
/// Both periods are `DOUBLE PRECISION` seconds in the schema rather than the integer
/// milliseconds used elsewhere, which is the divergence the module documentation warns about;
/// they are read through [`duration_from_secs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRecord {
    /// The queue's name, which is its address across every application sharing the database.
    pub name: String,
    /// Workflows this queue may have running at once, across all executors. `None` is unlimited.
    pub concurrency: Option<i32>,
    /// Workflows one executor may have running at once. `None` is unlimited.
    pub worker_concurrency: Option<i32>,
    /// How fast workflows may start, or `None` for unthrottled.
    pub rate_limit: Option<RateLimit>,
    /// Whether dequeue order honours a workflow's priority.
    pub priority_enabled: bool,
    /// Whether the queue is partitioned, so a dequeue names the partition it wants.
    ///
    /// Stored rather than derived, because it is how the implementations that predate the
    /// per-partition limits say the same thing: under the deprecated flag every queue-wide limit
    /// applies per partition instead. A row with any partition limit set is partitioned whatever
    /// this column says.
    pub partition_queue: bool,
    /// Workflows one partition may have running at once, across all executors.
    ///
    /// Setting any of the three partition limits is what partitions a queue. Each applies within
    /// one partition rather than to the queue as a whole, so they sit beside the queue-wide
    /// limits rather than replacing them: both are enforced, and neither is allowed to exceed
    /// its queue-wide counterpart.
    pub partition_concurrency: Option<i32>,
    /// Workflows one executor may have running at once within one partition.
    pub partition_worker_concurrency: Option<i32>,
    /// How fast workflows may start within one partition, or `None` for unthrottled.
    pub partition_rate_limit: Option<RateLimit>,
    /// How often an idle executor asks this queue for work.
    pub polling_interval: Duration,
    /// The application that owns the queue, or `None` if it is unclaimed.
    pub application_name: Option<String>,
}

/// Every limit on a queue, resolved to the scope it is actually enforced at.
///
/// The queue-wide fields are `None` for a legacy-partitioned row, which is the whole reason this
/// type exists rather than the dequeue reading [`QueueRecord`]'s columns directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResolvedLimits {
    /// See [`QueueRecord::concurrency`].
    pub concurrency: Option<i32>,
    /// See [`QueueRecord::worker_concurrency`].
    pub worker_concurrency: Option<i32>,
    /// See [`QueueRecord::rate_limit`].
    pub rate_limit: Option<RateLimit>,
    /// See [`QueueRecord::partition_concurrency`].
    pub partition_concurrency: Option<i32>,
    /// See [`QueueRecord::partition_worker_concurrency`].
    pub partition_worker_concurrency: Option<i32>,
    /// See [`QueueRecord::partition_rate_limit`].
    pub partition_rate_limit: Option<RateLimit>,
}

impl ResolvedLimits {
    /// Whether the queue is partitioned, which any per-partition limit makes it.
    pub fn is_partitioned(&self) -> bool {
        self.partition_concurrency.is_some()
            || self.partition_worker_concurrency.is_some()
            || self.partition_rate_limit.is_some()
    }
}

impl QueueRecord {
    /// Whether any per-partition limit is set, which is what partitions a queue.
    ///
    /// The [`partition_queue`](Self::partition_queue) column can say so too, and for a row written
    /// by an implementation that predates these limits it is the only thing that does.
    pub fn has_partition_limits(&self) -> bool {
        self.partition_concurrency.is_some()
            || self.partition_worker_concurrency.is_some()
            || self.partition_rate_limit.is_some()
    }

    /// A row written with the deprecated flag and no per-partition limits.
    ///
    /// Nothing this crate registers is one — partitioning here *is* the limits — but Go and Java
    /// still write them, and a database is shared.
    pub fn is_legacy_partitioned(&self) -> bool {
        self.partition_queue && !self.has_partition_limits()
    }

    /// This row's limits, each at the scope it is actually enforced at.
    ///
    /// **The deprecated flag re-scopes rather than adds.** Under `partition_queue`, `concurrency`,
    /// `worker_concurrency` and the rate limit all apply *per partition* — so they move into the
    /// partition fields and the queue-wide ones are dropped, leaving nothing enforced queue-wide.
    /// That is what the flag has always meant; Python spells it `_resolve_limits` and TypeScript
    /// `resolveQueueLimits`, both returning exactly this, and reading such a row any other way
    /// would either over-admit or strand a peer's backlog.
    ///
    /// **Everything that enforces a limit reads it through here**, never off the columns: the
    /// dequeue included, since a legacy row's `concurrency` is not a queue-wide number.
    pub fn resolved_limits(&self) -> ResolvedLimits {
        if self.is_legacy_partitioned() {
            return ResolvedLimits {
                partition_concurrency: self.concurrency,
                partition_worker_concurrency: self.worker_concurrency,
                partition_rate_limit: self.rate_limit,
                ..ResolvedLimits::default()
            };
        }
        ResolvedLimits {
            concurrency: self.concurrency,
            worker_concurrency: self.worker_concurrency,
            rate_limit: self.rate_limit,
            partition_concurrency: self.partition_concurrency,
            partition_worker_concurrency: self.partition_worker_concurrency,
            partition_rate_limit: self.partition_rate_limit,
        }
    }
}

/// A queue to register, as the caller supplies it.
///
/// Borrowed and separate from [`QueueRecord`] for the reason [`NewWorkflow`] is separate from
/// [`WorkflowRecord`]: registering does not require an owner, and the record always reports one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewQueue<'a> {
    /// The queue's name.
    pub name: &'a str,
    /// See [`QueueRecord::concurrency`].
    pub concurrency: Option<i32>,
    /// See [`QueueRecord::worker_concurrency`].
    pub worker_concurrency: Option<i32>,
    /// See [`QueueRecord::rate_limit`].
    pub rate_limit: Option<RateLimit>,
    /// See [`QueueRecord::priority_enabled`].
    pub priority_enabled: bool,
    /// See [`QueueRecord::partition_queue`].
    pub partition_queue: bool,
    /// See [`QueueRecord::partition_concurrency`].
    pub partition_concurrency: Option<i32>,
    /// See [`QueueRecord::partition_worker_concurrency`].
    pub partition_worker_concurrency: Option<i32>,
    /// See [`QueueRecord::partition_rate_limit`].
    pub partition_rate_limit: Option<RateLimit>,
    /// See [`QueueRecord::polling_interval`].
    pub polling_interval: Duration,
    /// The application to register the queue for; `None` means the writing handle's own.
    pub application_name: Option<&'a str>,
}

impl<'a> NewQueue<'a> {
    /// A queue with no limits, polling once a second — the defaults every implementation shares.
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            concurrency: None,
            worker_concurrency: None,
            rate_limit: None,
            priority_enabled: false,
            partition_queue: false,
            partition_concurrency: None,
            partition_worker_concurrency: None,
            partition_rate_limit: None,
            polling_interval: Duration::from_secs(1),
            application_name: None,
        }
    }
}

/// What registering a queue that already exists should do to it.
///
/// Never to its owner, which moves only by rename: a name already held by another application is
/// [`Error::RegisteredByAnother`] in both cases, because the name is the queue's address — as long
/// as the caller has a name to be refused under. A nameless one is let through and [`Update`]
/// replaces the peer's limits; UPSTREAM item 26.
///
/// [`Update`]: OnExistingQueue::Update
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExistingQueue {
    /// Overwrite the stored limits with the ones supplied.
    Update,
    /// Leave the stored row exactly as it is.
    Leave,
}

/// A registered version of the application.
///
/// The registry is what lets a firing schedule stamp the *latest* version, so only executors
/// running that code dequeue it, and what `application_version` on a workflow row refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    /// The application that registered this version, or `None` if it is unclaimed.
    pub application_name: Option<String>,
    /// Generated identity, distinct from the name.
    pub version_id: String,
    /// The version as the application names it — the value stored on workflow rows.
    pub version_name: String,
    /// Orders the registry: the latest version is the one with the highest value here, not the
    /// one created most recently.
    pub version_timestamp: Timestamp,
    /// When the row was first written.
    pub created_at: Timestamp,
}

/// A message sent to a workflow, as `notifications` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationRecord {
    /// The message's identity, and the table's primary key.
    ///
    /// Defaulted by the database with `gen_random_uuid()`, so a sender never supplies one. It is
    /// what a `recv` marks consumed, and the only thing distinguishing two identical messages on
    /// the same topic.
    pub message_uuid: String,
    /// The topic it was sent on, or `None` for the default topic.
    pub topic: Option<String>,
    /// Encoded payload.
    pub message: String,
    /// How `message` is encoded.
    pub serialization: Option<String>,
    /// When it was sent.
    pub created_at: Timestamp,
    /// Whether a `recv` has taken it.
    ///
    /// Receiving marks rather than deletes, so a consumed message stays visible to export and to
    /// anyone auditing what a workflow was sent.
    pub consumed: bool,
}

/// The workflow a [`get_event`](crate::sysdb::SystemDatabase::get_event) runs on behalf of, and the
/// steps it records against.
///
/// A struct where every other method here takes `caller: Option<(&str, i32)>`, because this one
/// carries **two** step ids and they are not interchangeable: one records the read itself, the
/// other the deadline it waits until. `Some(("wf", 4, 5))` says nothing about which is which, and
/// transposing them is a mistake nothing catches until a recovery replays the wrong step under the
/// wrong name. Every reference passes these three together too — Python's
/// `GetEventWorkflowContext`, TypeScript's `{workflowID, functionID, timeoutFunctionID}`.
///
/// `None` at the call site is a caller outside a workflow: it has no steps to record and nothing
/// to replay, so it waits on the wall clock and returns whatever it found. That optionality is why
/// this is a type at all — `recv` blocks and checkpoints the same two steps, but requires its
/// caller, so it takes them as plain parameters with nothing to wrap. Java draws the line in the
/// same place, with a `GetEventCaller` record and none for `recv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetEventCaller<'a> {
    /// The workflow doing the reading — the caller, never the workflow being read from.
    pub workflow_id: &'a str,
    /// The step the read itself is recorded under, which is what makes it replay rather than
    /// wait a second time.
    pub step_id: i32,
    /// The step the deadline is checkpointed under, recorded as a `DBOS.sleep` the caller may
    /// abandon early rather than one it waits out.
    ///
    /// Separate from `step_id` because a recovery has to resume the *same* deadline rather than
    /// start the timeout again — a read that waited fifty of its sixty seconds and crashed has ten
    /// left.
    pub timeout_step_id: i32,
}

/// The workflow an [`init_workflow`](crate::sysdb::SystemDatabase::init_workflow) creates a child
/// on behalf of, and the step the start is recorded under.
///
/// Named for its one method, as [`GetEventCaller`] is, and a struct for the same reason and then a
/// second: this carries a step *name* as well as the two ids, and the name is the child workflow's
/// own rather than a cross-SDK constant, so `Some(("wf", 4, "checkout"))` reads as three unrelated
/// values. `None` is a root start — a workflow begun from outside any workflow, which has no
/// parent to record against.
///
/// **Everything the parent contributes is here, including the id the child's own row carries.**
/// It was on [`NewWorkflow`] as well to begin with, which is one fact spelled twice and two
/// chances to disagree: nothing would have caught a caller naming one parent on the row and
/// another on the step. Grouping it here makes the agreement structural rather than checked — a
/// row gets a parent exactly when a start is recorded against that parent, because the same value
/// writes both — and it is where [`NewWorkflow`]'s own rule puts it, that type holding the columns
/// a caller may meaningfully set and leaving the ones a mechanism owns to the mechanism.
///
/// **Passing this is what makes the two writes one.** The child's row and the parent's record of
/// having started it commit together, so no observer and no replay ever sees a child that exists
/// with nothing pointing at it. The alternative — the caller writing the record itself, on the
/// next round trip — leaves a window in which a crash, or merely a dropped future, produces
/// exactly that: a workflow nothing started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitWorkflowCaller<'a> {
    /// The workflow doing the starting: the record is written against it, and the child's row
    /// points back at it.
    pub parent_workflow_id: &'a str,
    /// The step the start occupies in the parent, claimed before this call was made.
    pub step_id: i32,
    /// What the step is called, which is the child workflow's bare name — see
    /// [`record_child_workflow`](crate::sysdb::SystemDatabase::record_child_workflow), whose
    /// column this shares.
    pub step_name: &'a str,
    /// When the start began, for the row's `started_at`. The completion is stamped by the write
    /// itself, since the step spans the launch alone.
    pub started_at: Timestamp,
}

/// An encoded payload and the format it is encoded in.
///
/// What the blocking reads return: this layer moves payloads as opaque strings and never decodes
/// one, so the format has to travel with the value for the caller to make sense of it.
/// [`get_event`](crate::sysdb::SystemDatabase::get_event) returns this; `recv` and
/// `read_stream_value` will return it too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedValue {
    /// The payload, exactly as it was stored.
    pub value: String,
    /// How `value` is encoded, or `None` if whoever wrote it recorded no format.
    pub serialization: Option<String>,
}

/// A key/value a workflow published, as `workflow_events` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    /// The key it was published under.
    pub key: String,
    /// Encoded value.
    pub value: String,
    /// How `value` is encoded.
    pub serialization: Option<String>,
}

/// One offset of a stream, read together with its producer's liveness.
///
/// The pair is the point. A reader deciding whether to wait needs to know both whether a value is
/// there and whether the workflow that would write one is still running, and it needs them to agree
/// — read separately, a reader can find nothing at the offset, then find the workflow finished, and
/// stop one value short of a stream that was complete all along.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRead {
    /// The producing workflow's status, from the same snapshot as `value`.
    ///
    /// **A terminal status does not mean the stream is finished.** Cancelling a workflow and
    /// timing one out both set the status from outside while it is still writing, so a reader that
    /// stops here must first drain to the first empty offset.
    pub status: WorkflowStatus,
    /// The value at the offset, or `None` if nothing is written there yet.
    ///
    /// The closing sentinel [`STREAM_CLOSED`](crate::sysdb::STREAM_CLOSED) arrives here like any
    /// other value; recognising it belongs to the loop, which is the only thing that knows the
    /// stream is being read rather than inspected.
    pub value: Option<EncodedValue>,
}

/// One entry of a workflow's stream, as `streams` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    /// The stream this entry belongs to.
    pub key: String,
    /// Position within that stream, counted from zero.
    pub offset: i32,
    /// Encoded value.
    pub value: String,
    /// How `value` is encoded.
    pub serialization: Option<String>,
    /// The step that wrote the entry. Column `function_id`.
    ///
    /// Added by migration 6 alongside `workflow_events_history`, "to enable tracking event
    /// history by step ID and copying events during workflow forking" — so a fork can carry the
    /// entries written before its start step and leave the rest behind.
    pub step_id: i32,
}

/// One workflow to fork, and where its fork picks up.
///
/// A struct per fork rather than three parallel lists. Python takes `original_workflow_ids`,
/// `forked_workflow_ids` and `start_steps` and raises when their lengths disagree; pairing them
/// here means they cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fork<'a> {
    /// The workflow to fork from. It is not modified beyond being marked as forked from.
    pub source_id: &'a str,
    /// The id the fork gets, or `None` to have one generated.
    ///
    /// Optional because Go and TypeScript both generate one when the caller does not care, and
    /// the generated ids come back from [`SystemDatabase::fork_workflows`](crate::sysdb::SystemDatabase::fork_workflows).
    pub forked_id: Option<&'a str>,
    /// The first step the fork will run.
    ///
    /// Every step *below* this is copied to the fork, so it replays them instead of running them.
    /// Steps are numbered from zero, so `0` copies nothing and restarts the workflow from the
    /// beginning, and `1` carries step 0 across.
    ///
    /// That last case is where the references disagree: TypeScript copies for any
    /// `start_step > 0` and Python only for `step > 1`, so a Python fork from step 1 re-runs the
    /// step it was given the result of. This follows TypeScript.
    pub start_step: i32,
}

impl<'a> Fork<'a> {
    /// A fork of `source_id` restarting from the beginning, with a generated id.
    pub fn new(source_id: &'a str) -> Self {
        Self {
            source_id,
            forked_id: None,
            start_step: 0,
        }
    }

    /// Rejects ids the schema cannot key on, matching [`NewWorkflow::validate`].
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.source_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "source_id".into(),
                detail: "must not be empty".to_owned(),
            });
        }
        // `None` asks for one to be generated; `Some("")` is an id that cannot be looked up.
        if self.forked_id == Some("") {
            return Err(Error::InvalidInput {
                field: "forked_id".into(),
                detail: "must be absent rather than empty".to_owned(),
            });
        }
        if self.start_step < 0 {
            return Err(Error::InvalidInput {
                field: "start_step".into(),
                detail: "must not be negative".to_owned(),
            });
        }
        Ok(())
    }
}

/// Which step a fork restarts from, when the caller wants it worked out rather than stated.
///
/// A sum type because the four are alternatives, not options: Python and TypeScript both take
/// them as four independent flags and raise unless exactly one is set. The resolved value is a
/// [`Fork::start_step`], so the named step *re-runs* and everything below it replays — forking
/// "from the failure" means running the failed step again, not skipping it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkPoint<'a> {
    /// The step that failed, or the last recorded step if none did.
    ///
    /// The fallback matters: a workflow can be left unfinished without any step recording an
    /// error — a process killed mid-step records nothing at all — and forking it should still
    /// resume where it stopped.
    LastFailure,
    /// The last recorded step, failed or not.
    LastStep,
    /// A step chosen by the caller, with no lookup.
    Step(i32),
    /// The last step recorded under this name.
    ///
    /// For a workflow whose shape is known: fork every one of them from `charge_card`, whatever
    /// position it happens to occupy in each.
    StepNamed(&'a str),
}

/// How forked workflows are created.
///
/// The defaults are what every implementation does when the caller says nothing: the fork is
/// enqueued on the internal queue, inherits its source's version, and carries no timeout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions<'a> {
    /// The version the fork runs under. `None` inherits the source's.
    ///
    /// Forking onto a *new* version is the point of the parameter: a workflow that failed on a
    /// broken deployment is forked onto the fixed one.
    pub application_version: Option<&'a str>,
    /// The queue the fork is enqueued on. `None` means [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE).
    ///
    /// A fork is always enqueued rather than started: it is created by whoever asked for the
    /// fork, and run by whichever executor picks it up.
    pub queue_name: Option<&'a str>,
    /// The partition of that queue.
    pub queue_partition_key: Option<&'a str>,
    /// How long the fork may run before it is cancelled.
    ///
    /// Java and TypeScript both carry this on their fork options; Python and Go do not.
    pub timeout: Option<Duration>,
    /// Child workflow ids to rewrite as the fork's steps are copied.
    ///
    /// When a tree of workflows is forked together, the parent's recorded children are the
    /// *original* children — replaying them would make the fork adopt the originals rather than
    /// its own. Each `(from, to)` pair rewrites one `child_workflow_id` during the copy. Python
    /// and TypeScript both take this map; Go and Java do not.
    pub replacement_children: &'a [(&'a str, &'a str)],
}

impl ForkOptions<'_> {
    /// Applies the same rules [`NewWorkflow::validate`] applies to the same columns.
    ///
    /// Worth stating why an empty string is not simply treated as absent: the columns here are
    /// `COALESCE`d against the source's, so `Some("")` would win over the value it was meant to
    /// leave alone — a fork stamped with an empty version rather than its source's, and so
    /// invisible to the recovery that scopes by version.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("application_version", &self.application_version),
            ("queue_name", &self.queue_name),
            ("queue_partition_key", &self.queue_partition_key),
        ] {
            if *value == Some("") {
                return Err(Error::InvalidInput {
                    field: field.into(),
                    detail: "must be absent rather than empty".to_owned(),
                });
            }
        }
        // As on `NewWorkflow`: a zero timeout would expire the fork before it ran.
        if self.timeout == Some(Duration::ZERO) {
            return Err(Error::InvalidInput {
                field: "timeout".into(),
                detail: "must be absent rather than zero".to_owned(),
            });
        }
        // The replacements are applied by joining against them, so one child named twice would
        // match a copied step twice and duplicate it. The primary key would catch that as a
        // constraint violation, which says nothing about the map that caused it. Python and
        // TypeScript build a `CASE` instead, which silently takes the first match — but two
        // replacements for one child is a caller that does not know which it wants, and picking
        // for them is worse than saying so.
        for (index, (original, _)) in self.replacement_children.iter().enumerate() {
            if original.is_empty() {
                return Err(Error::InvalidInput {
                    field: "replacement_children".into(),
                    detail: "a replaced child id must not be empty".to_owned(),
                });
            }
            if self.replacement_children[..index]
                .iter()
                .any(|(earlier, _)| earlier == original)
            {
                return Err(Error::InvalidInput {
                    field: "replacement_children".into(),
                    detail: format!("{original} is replaced more than once"),
                });
            }
        }
        Ok(())
    }
}

/// The `DBOS.*` step names the engine records for its own operations.
///
/// **Here rather than in a backend, because they are stored contract.** Each of these lands in
/// `operation_outputs.function_name`, where a replay compares it, Conductor renders it, and another
/// SDK's step listing has to agree with it — so they belong beside the row shapes rather than beside
/// the SQL of whichever backend happens to write them. A second backend that redeclared them could
/// drift from this one silently, and nothing would notice until a workflow crossed between the two.
///
/// **Public because a caller may legitimately need to name one** — filtering a step listing to the
/// engine's own operations, or grouping by them — and retyping a string the engine already owns is
/// how the two come apart.
///
/// **Not for asserting the contract, though.** A test that checks a recorded name against the
/// constant the writer used cannot notice the constant changing, which is exactly the change worth
/// noticing: these are cross-SDK strings, and renaming one silently breaks a workflow that crosses
/// implementations. Those assertions keep their literals on purpose.
///
/// Each name's own doc says how far it is actually agreed, and three of them are **not** cross-SDK
/// contract: the references disagree on `sendBulk` and `debounceDelayedWorkflow`, and
/// `upsertSchedule` is this crate's own coinage.
pub mod step_names {
    /// The step name `record_sleep` records. A cross-SDK constant, like [`SET_EVENT`].
    pub const SLEEP: &str = "DBOS.sleep";

    /// The step names the two send methods record, one each: `send_message` writes [`SEND`] and
    /// `send_messages` writes [`SEND_BULK`]. The name is therefore the API surface the caller
    /// reached for, never the batch's length — a batch of one is still a batch.
    ///
    /// `"DBOS.send"` is unanimous — all four implementations record exactly that for a single send,
    /// and a workflow replayed by another must find the name it expects or raise `UnexpectedStep`.
    ///
    /// The batch name is not: Python writes `DBOS.send_bulk` and Java `DBOS.sendBulk`, while
    /// TypeScript and Go have no batch send to name. Java's spelling is taken because the rest of
    /// this constant family is camelCase already — `DBOS.setEvent`, `DBOS.getEvent` — so
    /// `DBOS.send_bulk` would be the odd one out in our own schema as well as in Java's.
    ///
    /// A method apiece is why: the name comes from which one was called, so nothing has to infer
    /// it. Inferring it from the batch size — as this once did — recorded a one-message bulk send
    /// as `DBOS.send`, which is not the call the caller made. Python and Java pass the name down
    /// from their two surfaces in exactly the same way.
    pub const SEND: &str = "DBOS.send";
    pub const SEND_BULK: &str = "DBOS.sendBulk";

    /// The step names a stream write records, chosen by whether the value is the closing sentinel.
    ///
    /// Python and Java both derive them the same way, from the same two strings. Deriving rather
    /// than passing means a close is always recorded as a close, whichever entry point reached it.
    pub const WRITE_STREAM: &str = "DBOS.writeStream";
    pub const CLOSE_STREAM: &str = "DBOS.closeStream";

    /// The step name `set_event` records, which a replay compares against.
    ///
    /// A cross-SDK constant: Java and Python both record exactly `"DBOS.setEvent"`, and a workflow
    /// replayed by another implementation must find the name it expects or raise `UnexpectedStep`.
    pub const SET_EVENT: &str = "DBOS.setEvent";

    /// The step name `get_event` records. A cross-SDK constant, like [`SET_EVENT`]: all four
    /// implementations record exactly `"DBOS.getEvent"`.
    pub const GET_EVENT: &str = "DBOS.getEvent";

    /// What every implementation names the step a parent writes when it awaits a child.
    ///
    /// Python's `function_name="DBOS.getResult"`, Go's `StepName`, and TypeScript's and Java's the
    /// same. Written by
    /// [`record_child_result`](crate::sysdb::SystemDatabase::record_child_result) and read back by
    /// [`check_child_result`](crate::sysdb::SystemDatabase::check_child_result), which is the only
    /// reason both of those exist rather than the caller passing a name.
    pub const GET_RESULT: &str = "DBOS.getResult";

    /// The step name a wait for the *first* of several workflows records.
    ///
    /// **The one exception in this table: named for the call that writes it rather than for a
    /// reference.** Every other constant here is a string some other implementation already writes,
    /// because a step row a Python or TypeScript reader may see should say what that reader calls
    /// the operation. This one does not follow that rule. Python records `"DBOS.waitFirst"`
    /// (`_dbos.py:1634`) and TypeScript records the same string from `DBOS.waitFirst`; this crate
    /// names the call [`select_workflow`](crate::select_workflow()), after the concurrency shape
    /// rather than after the wait, and the step a caller reads in a listing is named for the call
    /// they wrote — so this follows the call.
    ///
    /// **What that costs, stated plainly.** A Rust workflow's wait steps do not line up with the
    /// same wait's steps in Python or TypeScript: a cross-SDK reader — Conductor's step listing, or
    /// anything grouping steps by name across implementations — sees two names for one operation.
    /// Nothing breaks, because no implementation reads another's step *names* to decide anything;
    /// the name is what a replay of this workflow checks against its own row, and that stays
    /// internally consistent. The spelling still follows the table's convention, `DBOS.` and
    /// camelCase, so the divergence is the word and not the shape.
    ///
    /// **There is no constant for the all-wait, because it records nothing.** TypeScript writes a
    /// `"DBOS.waitAll"` row from the `runInternalStep` label in `DBOS.waitAll`, and Go, Java and
    /// Python have the call nowhere. [`join_workflows`](crate::join_workflows()) is a plain wait
    /// on every surface: it makes no choice a replay could make differently, and the
    /// [`GET_RESULT`] steps it is written to precede already record the outcomes a replay reads.
    /// The free [`join_workflows`](crate::join_workflows()) sets out the argument.
    pub const SELECT_WORKFLOW: &str = "DBOS.selectWorkflow";

    /// The step name `recv` records. A cross-SDK constant, like [`GET_EVENT`].
    pub const RECV: &str = "DBOS.recv";

    /// The step a debounce records when a workflow does the bouncing.
    ///
    /// camelCase, where Python writes `DBOS.debounce_delayed_workflow`. The implementations disagree
    /// on the spelling — as they do for `sendBulk` — and this crate follows TypeScript's, which is the
    /// form DBOS's own type names take. Nothing reads a step name across languages, since a workflow
    /// only crosses one by enqueue, so this is a convention rather than a wire format.
    pub const DEBOUNCE: &str = "DBOS.debounceDelayedWorkflow";

    /// The step names the management surface records, which a replay compares against.
    ///
    /// Every one of these is written by a management call made *from inside a workflow* —
    /// [`DBOS::cancel`](crate::DBOS::cancel) and its neighbours — and is what another execution
    /// of that workflow looks the recorded answer up by. Four are unanimous across the
    /// implementations: `resumeWorkflow`, `setWorkflowDelay`, `listWorkflows` and
    /// `forkWorkflow`. The rest are worth their reasons.
    ///
    /// **The singular name covers the bulk form too.** Python, TypeScript and Java record the
    /// singular whatever the batch size; Go pluralizes, and inconsistently — `DBOS.cancelWorkflow`
    /// for one and `DBOS.cancelWorkflows` for many, but `DBOS.deleteWorkflows` even for one.
    /// Three of four decides it, and one name per operation is worth having on its own: the
    /// singular forms here *are* the bulk ones with a single id, so a workflow that switches
    /// between [`cancel`](crate::DBOS::cancel) and [`cancel_all`](crate::DBOS::cancel_all)
    /// between runs still replays instead of raising [`Error::UnexpectedStep`](crate::sysdb::Error::UnexpectedStep).
    ///
    /// **[`LIST_WORKFLOW_STEPS`] follows the three, not Go**, which records
    /// `DBOS.getWorkflowSteps` where Python, TypeScript and Java all say `listWorkflowSteps`.
    ///
    /// **[`UPDATE_WORKFLOW_ATTRIBUTES`] is what Go's method records, not what it is called**: Go
    /// spells the method `SetWorkflowAttributes` and the step `DBOS.updateWorkflowAttributes`, so
    /// the step name is the half Python and Java agree with. TypeScript has no attributes method
    /// at all.
    ///
    /// **There is no `DBOS.listQueuedWorkflows`.** Python and TypeScript record one because they
    /// have a second entry point for it; here
    /// [`WorkflowFilter::queues_only`](super::WorkflowFilter::queues_only) is that method, so a
    /// queues-only listing records [`LIST_WORKFLOWS`] like any other.
    ///
    /// **Nothing names a retrieve.** Python and Go check the row and so record `DBOS.getStatus`
    /// and `DBOS.retrieveWorkflow`; [`retrieve_workflow`](crate::DBOS::retrieve_workflow) does no
    /// I/O, and a call that reads nothing has nothing to replay.
    ///
    /// All of these are recorded from down here rather than by the engine, because each
    /// checkpoint commits in the same transaction as the operation it records — see
    /// [`fork_workflows`](crate::sysdb::SystemDatabase::fork_workflows).
    ///
    /// **[`FORK_WORKFLOW`] covers every fork point.** Java splits its from-failure batch out as
    /// `DBOS.forkFromFailure`; Go keeps one name whatever the fork point, and so does this,
    /// because [`fork_from`](crate::sysdb::SystemDatabase::fork_from) resolves all four
    /// [`ForkPoint`](super::ForkPoint)s through one method.
    pub const CANCEL_WORKFLOW: &str = "DBOS.cancelWorkflow";
    pub const RESUME_WORKFLOW: &str = "DBOS.resumeWorkflow";
    pub const DELETE_WORKFLOW: &str = "DBOS.deleteWorkflow";
    pub const FORK_WORKFLOW: &str = "DBOS.forkWorkflow";
    pub const SET_WORKFLOW_DELAY: &str = "DBOS.setWorkflowDelay";
    pub const UPDATE_WORKFLOW_ATTRIBUTES: &str = "DBOS.updateWorkflowAttributes";
    pub const LIST_WORKFLOWS: &str = "DBOS.listWorkflows";
    pub const LIST_WORKFLOW_STEPS: &str = "DBOS.listWorkflowSteps";

    /// The step names the schedule methods record, which a replay compares against.
    ///
    /// TypeScript's spellings, from the `runTransactionalInternalStep` call sites in `dbos.ts`. Pause
    /// and resume are two names there because they are two API calls; they reach one method here, so
    /// the name follows the status being set rather than the method being called.
    ///
    /// **`DBOS.upsertSchedule` is the exception**: TypeScript has no such method — its upsert is
    /// inlined in `applySchedules` — and Python's `upsert_schedule` is never a step. The name is this
    /// crate's, camelCased from Python's by analogy with the seven that are verbatim.
    pub const CREATE_SCHEDULE: &str = "DBOS.createSchedule";
    pub const UPSERT_SCHEDULE: &str = "DBOS.upsertSchedule";
    pub const GET_SCHEDULE: &str = "DBOS.getSchedule";
    pub const LIST_SCHEDULES: &str = "DBOS.listSchedules";
    pub const UPDATE_SCHEDULE: &str = "DBOS.updateSchedule";
    pub const PAUSE_SCHEDULE: &str = "DBOS.pauseSchedule";
    pub const RESUME_SCHEDULE: &str = "DBOS.resumeSchedule";
    pub const DELETE_SCHEDULE: &str = "DBOS.deleteSchedule";
}

/// One message to deliver to a workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Message<'a> {
    /// The workflow it is for.
    pub destination_id: &'a str,
    /// The topic it is filed under, or `None` for the untopicked default.
    ///
    /// `None` is stored as a sentinel string rather than `NULL`, because a receiver selects on
    /// equality and `topic = NULL` matches nothing. Every implementation uses the same sentinel,
    /// so it is part of the cross-SDK contract rather than an encoding choice.
    pub topic: Option<&'a str>,
    /// The payload, already encoded by the caller.
    pub message: &'a str,
    /// A key that makes re-sending this message a no-op.
    ///
    /// Absent, each send is a distinct message. Present, it becomes the row's primary key, so a
    /// second send with the same key is discarded by the database rather than delivered twice.
    pub idempotency_key: Option<&'a str>,
}

/// Who wrote to a stream, which decides whether the write is itself a durable step.
///
/// Python, Java and TypeScript encode this in the method name — `write_stream_from_workflow`
/// versus `write_stream_from_step` — and differ in nothing else. Go takes one method and no
/// distinction. Naming the difference instead of duplicating the method is the same choice made
/// for [`send_messages`](crate::sysdb::SystemDatabase::send_messages): the split is in the
/// behaviour, not the operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrittenBy {
    /// The workflow body itself. The write *is* a step: it is recorded, and a replay finds it
    /// and writes nothing rather than appending a second entry.
    Workflow,
    /// Code running inside a step. The enclosing step is already the durable unit, so this
    /// records nothing of its own — a step that reruns rewrites its stream entries, which is
    /// what makes the step the thing being replayed.
    Step,
}

/// Whether a schedule fires.
///
/// Two states in every implementation. Pausing does not delete the row or forget
/// [`ScheduleRecord::last_fired_at`], so resuming picks up where it left off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ScheduleStatus {
    /// Firing on its cron expression.
    Active,
    /// Registered but not firing.
    Paused,
}

impl ScheduleStatus {
    /// The stored spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            ScheduleStatus::Active => "ACTIVE",
            ScheduleStatus::Paused => "PAUSED",
        }
    }

    /// Parses a stored value, for the reason [`WorkflowStatus::parse`] returns an option.
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "ACTIVE" => ScheduleStatus::Active,
            "PAUSED" => ScheduleStatus::Paused,
            _ => return None,
        })
    }
}

/// A registered schedule, as stored.
///
/// The row carries both a *definition* — the cron expression, the workflow it fires, its context
/// and queue — and *runtime state*: the status and when it last fired. Which half a write may
/// touch is the distinction the update methods are built around, so a redeployment that re-applies
/// an unchanged definition does not restart a schedule or forget where it had got to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduleRecord {
    /// Generated identity, distinct from the name. Preserved across re-registration.
    pub schedule_id: String,
    /// The schedule's name, which is its address — unique across the whole table.
    pub schedule_name: String,
    /// The registered function the schedule fires.
    pub workflow_name: String,
    /// The class that function belongs to, for class-bound workflows.
    pub workflow_class_name: Option<String>,
    /// The cron expression, uninterpreted. This layer stores it and never parses it.
    pub schedule: String,
    /// Whether the schedule fires.
    pub status: ScheduleStatus,
    /// The encoded context handed to each firing, opaque here as every payload is.
    pub context: String,
    /// When the schedule last fired.
    ///
    /// Stored as ISO-8601 text rather than the epoch milliseconds every other time column holds,
    /// and read back through [`Timestamp::parse_iso8601`] so a caller gets an instant either way.
    /// The four implementations write four spellings of it — see
    /// [`SystemDatabase::update_schedule_last_fired_at`](crate::sysdb::SystemDatabase::update_schedule_last_fired_at).
    pub last_fired_at: Option<Timestamp>,
    /// Whether missed firings are made up when a paused or stopped schedule resumes.
    pub automatic_backfill: bool,
    /// The timezone the cron expression is read in, or `None` for UTC.
    pub cron_timezone: Option<String>,
    /// The queue firings are enqueued onto, or `None` for [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE).
    pub queue_name: Option<String>,
    /// The application that owns the schedule, or `None` if it is unclaimed.
    pub application_name: Option<String>,
}

/// A schedule to register, as the caller supplies it.
///
/// Borrowed and separate from [`ScheduleRecord`] for the reason [`NewQueue`] is separate from
/// [`QueueRecord`]: registering does not require an owner, and the record always reports one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSchedule<'a> {
    /// See [`ScheduleRecord::schedule_id`]. `None` generates one.
    ///
    /// All four implementations end up with a generated UUID; they differ only in which layer
    /// generates it. Java's DAO does, as this does. TypeScript and Python generate one layer
    /// higher, at every call site that registers a schedule, and hand this layer a value it must
    /// take. That is the same split `application_name` has, and it resolves the same way: the
    /// fallback lives here because nothing sits above this layer yet, and becomes a second line
    /// of defence rather than the only one once Phase 2's registration layer does.
    pub schedule_id: Option<&'a str>,
    /// See [`ScheduleRecord::schedule_name`].
    pub schedule_name: &'a str,
    /// See [`ScheduleRecord::workflow_name`].
    pub workflow_name: &'a str,
    /// See [`ScheduleRecord::workflow_class_name`].
    pub workflow_class_name: Option<&'a str>,
    /// See [`ScheduleRecord::schedule`].
    pub schedule: &'a str,
    /// See [`ScheduleRecord::status`].
    pub status: ScheduleStatus,
    /// See [`ScheduleRecord::context`].
    pub context: &'a str,
    /// See [`ScheduleRecord::last_fired_at`]. Only used on a fresh insert: the upsert's conflict
    /// clause keeps the stored value, so re-registering a schedule cannot rewind it.
    pub last_fired_at: Option<Timestamp>,
    /// See [`ScheduleRecord::automatic_backfill`].
    pub automatic_backfill: bool,
    /// See [`ScheduleRecord::cron_timezone`].
    pub cron_timezone: Option<&'a str>,
    /// See [`ScheduleRecord::queue_name`].
    pub queue_name: Option<&'a str>,
    /// The application to register the schedule for; `None` means the writing handle's own.
    pub application_name: Option<&'a str>,
}

impl<'a> NewSchedule<'a> {
    /// An active schedule with no context, firing the named workflow on the given expression.
    pub fn new(schedule_name: &'a str, workflow_name: &'a str, schedule: &'a str) -> Self {
        Self {
            schedule_id: None,
            schedule_name,
            workflow_name,
            workflow_class_name: None,
            schedule,
            status: ScheduleStatus::Active,
            context: "null",
            last_fired_at: None,
            automatic_backfill: false,
            cron_timezone: None,
            queue_name: None,
            application_name: None,
        }
    }
}

/// The fields of a registered schedule that a partial update may change.
///
/// **Definition only.** The identity, the status and the last firing are runtime state, moved by
/// their own methods, so re-applying a definition cannot silently restart a schedule or forget
/// where it had got to. TypeScript draws the same line and says so in a comment; this makes it
/// unrepresentable instead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduleUpdate<'a> {
    /// See [`ScheduleRecord::schedule`].
    pub schedule: Change<&'a str>,
    /// See [`ScheduleRecord::context`].
    pub context: Change<&'a str>,
    /// See [`ScheduleRecord::automatic_backfill`].
    pub automatic_backfill: Change<bool>,
    /// See [`ScheduleRecord::cron_timezone`].
    pub cron_timezone: Change<Option<&'a str>>,
    /// See [`ScheduleRecord::queue_name`].
    pub queue_name: Change<Option<&'a str>>,
}

impl ScheduleUpdate<'_> {
    /// Whether this would change nothing.
    pub fn is_empty(&self) -> bool {
        self.schedule.is_leave()
            && self.context.is_leave()
            && self.automatic_backfill.is_leave()
            && self.cron_timezone.is_leave()
            && self.queue_name.is_leave()
    }
}

/// Which schedules a listing returns.
///
/// Every field narrows; an empty filter returns the table. `applications` takes the
/// *observability* form — unset means the reading handle's own plus the unclaimed — because this
/// is a search rather than an addressed read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduleFilter<'a> {
    /// Only these statuses, or every status when empty.
    pub statuses: Vec<ScheduleStatus>,
    /// Only schedules firing these workflows, or every workflow when empty.
    pub workflow_names: Vec<&'a str>,
    /// Only schedules whose name starts with one of these, or every name when empty.
    pub schedule_name_prefixes: Vec<&'a str>,
    /// Which applications' schedules to return.
    pub applications: Applications<'a>,
}
