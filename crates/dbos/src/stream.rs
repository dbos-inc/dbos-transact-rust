//! Durable streams: an append-only sequence of values a workflow writes under a key, and anyone
//! may read in order as it grows.
//!
//! [`write_stream`] and [`close_stream`] are free functions, callable from a workflow or from a
//! step inside one: they take the executor and the step-id sequence from the ambient context, as
//! [`set_event`](crate::set_event) does. A write from the workflow body is a step of its own, and a
//! replay skips it; a write from inside a step is plain, because the step's own checkpoint stands
//! for everything its body did, and a step that reruns writes again.
//!
//! The reader is a [`StreamReader`], from three places as for [`get_event`](crate::get_event): the
//! free [`read_stream`] in a workflow body, [`DBOS::read_stream`] for code outside a workflow, and
//! [`Client::read_stream`](crate::Client::read_stream) from outside the application. Each value
//! is one call to [`StreamReader::next`], and **when the reader is a workflow, each value is a
//! step**: the value is recorded before it is handed over, so a replay yields the values the first
//! run read even if the stream has grown since, and stops where the first run stopped. Each has a
//! `read_stream_from` twin that starts at an offset rather than the beginning, for resuming a read.
//! [`read_stream_value`] reads the one value at one offset, on the same terms.
//!
//! `sysdb` owns the stored stream, the replay skip on a write, and the wait for a value. What the
//! engine adds is the step ids, the encoding, the read-ahead buffer, and the checkpoint of what a
//! reader delivered.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::DBOS;
use crate::checkpoint::{Built, PendingStep, StepPlacement, revive};
use crate::connection::Connection;
use crate::context::Ctx;
use crate::error::{DurableError, EngineOnly, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{
    AwaitedStream, EncodedValue, Outcome, StepTiming, Timestamp, WrittenBy, step_names,
};
use crate::sysdb::{STREAM_CLOSED, STREAM_CLOSED_SERIALIZATION, is_stream_closed};

/// How long a reader waits before looking again when nothing wakes it, unless
/// [`ReadStreamOptions::polling_interval`] says otherwise.
///
/// A second whether or not a listener is delivering writes: a reader also waits for the producer
/// to *finish*, and nothing pushes that.
pub const DEFAULT_STREAM_POLLING_INTERVAL: Duration = Duration::from_secs(1);

/// The shortest [`ReadStreamOptions::polling_interval`] a reader accepts. A shorter one would have
/// a waiting reader spin on the database.
pub const MIN_STREAM_POLLING_INTERVAL: Duration = Duration::from_millis(1);

/// How many values a reader fetches in one round trip. Written values never change, so reading
/// ahead is safe; the ones not yet delivered wait in the reader.
const STREAM_PAGE: i32 = 100;

/// How a stream read waits: how often it looks, and how long it waits for each value.
///
/// Constructed with `..Default::default()`, as [`SendOptions`](crate::SendOptions) is. Where the
/// read starts is an argument rather than an option: [`read_stream`] starts at the beginning,
/// [`read_stream_from`] at an offset, and [`read_stream_value`] reads exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadStreamOptions {
    /// How often to look again while waiting, if nothing wakes the reader sooner. `None` is
    /// [`DEFAULT_STREAM_POLLING_INTERVAL`]; anything under [`MIN_STREAM_POLLING_INTERVAL`] is
    /// refused.
    pub polling_interval: Option<Duration>,
    /// How long to wait for **each** value, or `None` to wait as long as the producer runs.
    ///
    /// The clock restarts every time a value is delivered. Expiry is [`Error::StreamTimeout`].
    pub timeout: Option<Duration>,
}

/// Appends a value to stream `key` of the current workflow.
///
/// From the workflow body this is a step: a replay finds it and does not write again. From inside
/// a step it is a plain write, stamped with the enclosing step's id, which a rerun of that step
/// writes again. Outside a workflow there is no stream to write to, which is
/// [`Error::NotInWorkflow`].
///
/// **The value is encoded before the step id is taken**, as for [`set_event`](crate::set_event): a
/// value that cannot be encoded is a write that never happens, and must not move the counter.
pub fn write_stream<'a, T, E>(key: &'a str, value: &T) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    pending_write(key, StreamWrite::Value(value))
}

/// Closes stream `key` of the current workflow, so its readers end once they reach the close.
///
/// A close is an ordinary append of the closing sentinel, durable on the same terms as
/// [`write_stream`]: a step of its own from the workflow body, and a plain write from inside a
/// step. Closing is not required — a reader also ends once the workflow finishes — but it ends a
/// reader as soon as it catches up rather than when the workflow does.
pub fn close_stream<'a, E>(key: &'a str) -> PendingStep<'a, (), E>
where
    E: DurableError + 'a,
{
    pending_write::<(), E>(key, StreamWrite::Close)
}

/// What a stream write appends: a value, or the close.
///
/// The one thing that tells a write from a close, so the step name, the call a refusal names and
/// the `sysdb` call all follow from it rather than being passed alongside each other.
enum StreamWrite<'v, T> {
    Value(&'v T),
    Close,
}

impl<T> StreamWrite<'_, T> {
    /// The step name the append records.
    fn name(&self) -> &'static str {
        match self {
            StreamWrite::Value(_) => step_names::WRITE_STREAM,
            StreamWrite::Close => step_names::CLOSE_STREAM,
        }
    }

    /// The call a refusal names.
    fn operation(&self) -> &'static str {
        match self {
            StreamWrite::Value(_) => "write_stream",
            StreamWrite::Close => "close_stream",
        }
    }
}

/// A write or a close as a [`PendingStep`].
///
/// Refuses outside a workflow, then encodes a value, then takes the step id — in that order, so a
/// value that cannot be encoded is a write that never happens and moves no counter.
fn pending_write<'a, T, E>(key: &'a str, write: StreamWrite<'_, T>) -> PendingStep<'a, (), E>
where
    T: Serialize,
    E: DurableError + 'a,
{
    let name = write.name();
    let built = Ctx::current()
        .ok_or_else(|| Error::NotInWorkflow {
            operation: write.operation().into(),
        })
        .and_then(|ctx| {
            // `None` is the close, and is only ever made here, from the write it stands for.
            let encoded = match write {
                StreamWrite::Value(value) => Some(encode(value, "stream value")?),
                StreamWrite::Close => None,
            };
            Ok((
                (Arc::clone(ctx.executor().connection()), encoded),
                StepPlacement::at(ctx),
            ))
        });
    PendingStep::placed(name, built, move |(conn, encoded), placement| async move {
        // A step id of the workflow's own when written from its body; the enclosing step's id when
        // written from inside one, where the write records nothing and the column is all it gets.
        let (workflow_id, step_id, written_by) = match &placement {
            StepPlacement::Recorded { ctx, step_id } => {
                (ctx.workflow_id(), *step_id, WrittenBy::Workflow)
            }
            StepPlacement::InsideStep { ctx } => (
                ctx.workflow_id(),
                ctx.step_id()
                    .expect("a context inside a step knows its step id"),
                WrittenBy::Step,
            ),
            StepPlacement::Outside | StepPlacement::ClientConnection => {
                unreachable!("a stream write is placed from the ambient workflow's own context")
            }
        };
        let sysdb = conn.sysdb();
        match &encoded {
            Some(value) => {
                sysdb
                    .write_stream(
                        workflow_id,
                        step_id,
                        key,
                        value,
                        Some(conn.serializer().name()),
                        written_by,
                    )
                    .await
            }
            None => {
                sysdb
                    .close_stream(workflow_id, step_id, key, written_by)
                    .await
            }
        }
        .map_err(Error::SystemDatabase)
        .map_err(Error::lift)
    })
}

/// Reads stream `key` of workflow `workflow_id` from the beginning, from a workflow body.
///
/// The reader for a workflow, and the counterpart of [`write_stream`]: it takes its connection from
/// the ambient context, so a workflow that reads a stream needs no [`DBOS`] handle. Each value is
/// a step of the reading workflow — see [`StreamReader`]. From inside a step the reads are plain.
///
/// Outside a workflow this is [`Error::NotInWorkflow`], reported by the first
/// [`next`](StreamReader::next); [`DBOS::read_stream`] is the call there.
pub fn read_stream<T, E>(
    workflow_id: &str,
    key: &str,
    options: ReadStreamOptions,
) -> StreamReader<T, E>
where
    T: DeserializeOwned,
    E: DurableError,
{
    StreamReader::new(
        StepPlacement::ambient_connection("read_stream"),
        workflow_id,
        key,
        0,
        options,
        "read_stream",
    )
}

/// [`read_stream`], starting at `offset` rather than the beginning.
///
/// For picking up where an earlier read stopped: [`StreamReader::offset`] is the offset to start
/// the next one at. A reader that ended on the close stopped *at* the close, so a read started
/// there finds it again and ends at once.
///
/// **A close before `offset` is not looked for.** The reader sees the stream from `offset` on, so
/// one started past the close does not end there: it ends when the workflow stops running, and
/// delivers anything written after the close on the way.
pub fn read_stream_from<T, E>(
    workflow_id: &str,
    key: &str,
    offset: i32,
    options: ReadStreamOptions,
) -> StreamReader<T, E>
where
    T: DeserializeOwned,
    E: DurableError,
{
    StreamReader::new(
        StepPlacement::ambient_connection("read_stream_from"),
        workflow_id,
        key,
        offset,
        options,
        "read_stream_from",
    )
}

/// Reads the one value at `offset` of stream `key` of workflow `workflow_id`, waiting for it to be
/// written. From a workflow body, where the read is a step.
///
/// If the stream ends with nothing at the offset — the close is there, or the workflow stops
/// running before writing it — no value will ever arrive, and the answer is
/// [`Error::StreamTimeout`] with no timeout in it. Otherwise a timeout, if one is set, bounds the
/// wait.
///
/// **A close before `offset` is not looked for**, as in [`read_stream_from`]: an offset past the
/// close waits for the workflow to stop, and answers with a value if one is written there first.
pub fn read_stream_value<'a, T, E>(
    workflow_id: &'a str,
    key: &'a str,
    offset: i32,
    options: ReadStreamOptions,
) -> PendingStep<'a, T, E>
where
    T: DeserializeOwned + 'a,
    E: DurableError + 'a,
{
    read_value(
        StepPlacement::ambient_connection("read_stream_value"),
        workflow_id,
        key,
        offset,
        options,
    )
}

impl DBOS {
    /// Reads stream `key` of workflow `workflow_id` from the beginning.
    ///
    /// The reader for code outside a workflow — an HTTP handler forwarding a workflow's output as
    /// it is produced. **Inside a workflow, reach for the free [`read_stream`] instead**: it needs
    /// no handle, and a captured [`DBOS`] is a cycle with the registry that holds the closure.
    ///
    /// Called from inside a workflow anyway, it behaves as the free function does — each value a
    /// step, and plain reads from inside a step — except that, read from the workflow body, a
    /// handle to some *other* instance is [`Error::WrongInstance`]. Inside a step the read is plain
    /// whichever instance the handle belongs to.
    pub fn read_stream<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> StreamReader<T> {
        StreamReader::new(
            self.executor("read_stream")
                .map(|executor| Arc::clone(executor.connection())),
            workflow_id,
            key,
            0,
            options,
            "read_stream",
        )
    }

    /// [`read_stream`](Self::read_stream), starting at `offset` rather than the beginning.
    ///
    /// For picking up where an earlier read stopped — a client reconnecting after it had some of a
    /// workflow's output: [`StreamReader::offset`] is the offset to start the next read at.
    pub fn read_stream_from<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i32,
        options: ReadStreamOptions,
    ) -> StreamReader<T> {
        StreamReader::new(
            self.executor("read_stream_from")
                .map(|executor| Arc::clone(executor.connection())),
            workflow_id,
            key,
            offset,
            options,
            "read_stream_from",
        )
    }

    /// Reads the one value at `offset` of a stream, waiting for it to be written.
    ///
    /// See [`read_stream_value`] for the answer when the stream ends first, and
    /// [`read_stream`](Self::read_stream) for where this may stand.
    pub fn read_stream_value<'a, T: DeserializeOwned + 'a>(
        &self,
        workflow_id: &'a str,
        key: &'a str,
        offset: i32,
        options: ReadStreamOptions,
    ) -> PendingStep<'a, T> {
        read_value(
            self.executor("read_stream_value")
                .map(|executor| Arc::clone(executor.connection())),
            workflow_id,
            key,
            offset,
            options,
        )
    }
}

impl crate::Client {
    /// Reads stream `key` of workflow `workflow_id` from the beginning, from outside the
    /// application.
    ///
    /// Nothing is checkpointed: a client has no workflow to record a read against.
    pub fn read_stream<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> StreamReader<T> {
        StreamReader::new(
            Ok(Arc::clone(self.connection())),
            workflow_id,
            key,
            0,
            options,
            "read_stream",
        )
    }

    /// [`read_stream`](Self::read_stream), starting at `offset` rather than the beginning.
    pub fn read_stream_from<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i32,
        options: ReadStreamOptions,
    ) -> StreamReader<T> {
        StreamReader::new(
            Ok(Arc::clone(self.connection())),
            workflow_id,
            key,
            offset,
            options,
            "read_stream_from",
        )
    }

    /// Reads the one value at `offset` of a stream, waiting for it to be written.
    ///
    /// See [`read_stream_value`] for the answer when the stream ends first.
    pub async fn read_stream_value<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i32,
        options: ReadStreamOptions,
    ) -> Result<T> {
        read_value(
            Ok(Arc::clone(self.connection())),
            workflow_id,
            key,
            offset,
            options,
        )
        .await
    }
}

/// A read of one stream, value by value, in order.
///
/// ```ignore
/// let mut chunks = dbos::read_stream::<String, _>(&workflow_id, "output", Default::default());
/// while let Some(chunk) = chunks.next().await? {
///     reply.send(chunk).await;
/// }
/// ```
///
/// [`next`](Self::next) answers `Ok(None)` once the stream is closed, or once its workflow is no
/// longer running and a last look past the values delivered finds nothing more. The end is final for
/// this reader: a workflow that is cancelled or parked and later resumed may write again, and a new
/// reader from [`offset`](Self::offset) sees what it writes.
///
/// **When the reader is a workflow, each value is a step**, named `DBOS.readStream`. The value is
/// recorded before `next` returns it, and the end of the stream is recorded too, so a replay
/// returns the values the first run read — not the ones the stream holds now — and ends where the
/// first run ended. A timeout, and a stream whose workflow does not exist, are recorded as the
/// step's error and replay as one. While it waits, the reader stops with
/// [`Error::WorkflowCancelled`] if its own workflow is cancelled. From inside a step, from a
/// [`Client`](crate::Client), or from outside a workflow, nothing is recorded.
///
/// **Reading from more than one place in a workflow body** follows the rule for any durable calls
/// driven together: each `next` takes its step id where it is called, so calls built in a fixed
/// order — `tokio::join!(a.next(), b.next())` — replay exactly. Two read *loops* running side by
/// side do not: which loop asks next follows scheduling, and so do the ids. Interleave them in one
/// loop, or read in a step.
///
/// Not a `futures::Stream`, deliberately: a [`PendingStep`] takes its step id where it is called,
/// and `Stream::poll_next` has no such moment — its id would follow the first poll instead.
///
/// After a read fails, the reader is spent: every later `next` is `Ok(None)`.
#[must_use = "a stream reader reads nothing until `next` is awaited"]
pub struct StreamReader<T, E = EngineOnly> {
    /// The connection the reads go through, or what stopped the reader being built — reported by
    /// the first `next`, taken out of here as it is.
    source: std::result::Result<Arc<Connection>, Option<Error>>,
    /// The call that made this reader, which a refusal names.
    operation: &'static str,
    /// Which stream, and how each value is waited for.
    read: ReadOf,
    /// The offset of the next value to deliver.
    offset: i32,
    /// Values already read, from `offset` on, not yet delivered.
    ///
    /// Holds a close only as its first entry, and nothing after one: a rewind of the producer
    /// deletes a close past its cut and the stream carries on from that offset, so a close is acted
    /// on only when it is read at the offset being delivered.
    buffer: VecDeque<EncodedValue>,
    /// Whether the stream has ended for this reader. Every later `next` is `Ok(None)`.
    ended: bool,
    /// Whether a workflow reader may still find its values recorded. Ids are taken in order, so
    /// the first one with no record means none after it has one either.
    replaying: bool,
    marker: PhantomData<fn() -> (T, E)>,
}

impl<T, E> std::fmt::Debug for StreamReader<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamReader")
            .field("workflow_id", &self.read.workflow_id)
            .field("key", &self.read.key)
            .field("offset", &self.offset)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl<T, E> StreamReader<T, E>
where
    T: DeserializeOwned,
    E: DurableError,
{
    /// A reader of the stream from `offset` on. `operation` is the call a refusal names.
    fn new(
        source: Result<Arc<Connection>>,
        workflow_id: &str,
        key: &str,
        offset: i32,
        options: ReadStreamOptions,
        operation: &'static str,
    ) -> Self {
        let read = ReadOf::new(workflow_id, key, options);
        let source = source.and_then(|conn| {
            read.validate(offset, operation)?;
            Ok(conn)
        });
        Self {
            source: source.map_err(Some),
            operation,
            read,
            offset,
            buffer: VecDeque::new(),
            ended: false,
            replaying: true,
            marker: PhantomData,
        }
    }

    /// The next value, or `Ok(None)` once the stream has ended.
    ///
    /// **The step id is taken here, at the call**, as every [`PendingStep`] takes its own: see
    /// [`StreamReader`] for what that means for a workflow reading from more than one place.
    //
    // Not `Iterator::next`, which it cannot be: the answer is a future, and each one claims a step.
    // `next` is what an async reader's method is called — `while let Some(v) = r.next().await?`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> PendingStep<'_, Option<T>, E> {
        let Some(built) = self.place() else {
            return PendingStep::settled(step_names::READ_STREAM, || Ok(None));
        };
        PendingStep::placed(
            step_names::READ_STREAM,
            built,
            move |conn, placement| async move {
                let read = self.read_next(&conn, &placement).await;
                if read.is_err() {
                    self.ended = true;
                }
                read.map_err(Error::lift)
            },
        )
    }

    /// The offset of the next value this reader will deliver.
    #[must_use]
    pub fn offset(&self) -> i32 {
        self.offset
    }

    /// The next read's placement, or `None` once the stream has ended for this reader.
    fn place(&mut self) -> Option<Built<Arc<Connection>>> {
        if self.ended {
            return None;
        }
        let built = match &mut self.source {
            Ok(conn) => place(Arc::clone(conn), self.operation),
            Err(refused) => Err(refused
                .take()
                .expect("a refusal is reported once, and the reader then ends")),
        };
        // A refused read ends the reader like any other failure, though it took no id.
        self.ended = built.is_err();
        Some(built)
    }

    /// One value: replayed if this workflow recorded it, otherwise read and then recorded.
    async fn read_next(
        &mut self,
        conn: &Connection,
        placement: &StepPlacement,
    ) -> Result<Option<T>> {
        let name = step_names::READ_STREAM;
        // When the step began, so the recorded step spans the wait for its value.
        let started_at = Timestamp::now();

        if self.replaying {
            match check(conn, placement, name).await? {
                Some(Replayed::Value(output)) => {
                    self.offset += 1;
                    return decode(Some(&output), "stream value").map(Some);
                }
                Some(Replayed::Ended) => {
                    self.ended = true;
                    return Ok(None);
                }
                // Ids are taken in order, so once one has no record, none after it does.
                None => self.replaying = placement.step().is_none(),
            }
        }

        if self.buffer.is_empty() {
            match self
                .read
                .await_values(conn, placement, name, self.offset, STREAM_PAGE, started_at)
                .await?
            {
                AwaitedStream::Values(values) => {
                    let mut values = values.into_iter();
                    let first = values.next().expect("a page of values is never empty");
                    let closed = is_close(&first);
                    self.buffer.push_back(first);
                    if !closed {
                        self.buffer
                            .extend(values.take_while(|later| !is_close(later)));
                    }
                }
                AwaitedStream::Ended => {
                    record_end(conn, placement, name, started_at).await?;
                    self.ended = true;
                    return Ok(None);
                }
                AwaitedStream::TimedOut => {
                    return Err(self
                        .read
                        .time_out(conn, placement, name, started_at)
                        .await?);
                }
            }
        }

        // Looked at, not taken: a workflow's read can be dropped while its record is in flight —
        // the losing branch of a `select_step!` — and the value must then still be here, at the
        // offset that has not moved, for the next read.
        let front = self.buffer.front().expect("a value is buffered by now");
        if is_close(front) {
            record_end(conn, placement, name, started_at).await?;
            self.buffer.clear();
            self.ended = true;
            return Ok(None);
        }
        record_value(conn, placement, name, front, started_at).await?;
        let entry = self.buffer.pop_front().expect("the value just recorded");
        self.offset += 1;
        // Decoded only as it is delivered, so a value that cannot be decoded fails its own read
        // and none before it. After the record rather than before: what was read is settled either
        // way, and a replay decodes the recorded entry exactly as this does.
        decode(Some(&entry.value), "stream value").map(Some)
    }
}

/// The single-value read behind every `read_stream_value`: the one value at `offset`, as one step
/// named `DBOS.readStreamValue`, and [`Error::StreamTimeout`] if the stream ends first.
///
/// The same steps as one [`StreamReader::next`] — replay, wait, record — with a page of one and
/// nothing kept afterwards.
fn read_value<'a, T, E>(
    source: Result<Arc<Connection>>,
    workflow_id: &str,
    key: &str,
    offset: i32,
    options: ReadStreamOptions,
) -> PendingStep<'a, T, E>
where
    T: DeserializeOwned + 'a,
    E: DurableError + 'a,
{
    let name = step_names::READ_STREAM_VALUE;
    let read = ReadOf::new(workflow_id, key, options);
    let built = source
        .and_then(|conn| {
            read.validate(offset, "read_stream_value")?;
            Ok(conn)
        })
        .and_then(|conn| place(conn, "read_stream_value"));
    PendingStep::placed(name, built, move |conn, placement| async move {
        async {
            let started_at = Timestamp::now();
            // No value will ever arrive: the stream ended before the offset.
            let ended = || Error::StreamTimeout {
                workflow_id: read.workflow_id.clone(),
                key: read.key.clone(),
                timeout: None,
            };
            match check(&conn, &placement, name).await? {
                Some(Replayed::Value(output)) => return decode(Some(&output), "stream value"),
                Some(Replayed::Ended) => return Err(ended()),
                None => {}
            }
            let entry = match read
                .await_values(&conn, &placement, name, offset, 1, started_at)
                .await?
            {
                AwaitedStream::Values(values) => values
                    .into_iter()
                    .next()
                    .expect("a page of values is never empty"),
                AwaitedStream::Ended => {
                    record_end(&conn, &placement, name, started_at).await?;
                    return Err(ended());
                }
                AwaitedStream::TimedOut => {
                    return Err(read.time_out(&conn, &placement, name, started_at).await?);
                }
            };
            if is_close(&entry) {
                record_end(&conn, &placement, name, started_at).await?;
                return Err(ended());
            }
            record_value(&conn, &placement, name, &entry, started_at).await?;
            decode(Some(&entry.value), "stream value")
        }
        .await
        .map_err(Error::lift)
    })
}

/// Which stream a read reads, and how it waits for each value — what both reads fix at the call.
#[derive(Debug, Clone)]
struct ReadOf {
    workflow_id: String,
    key: String,
    polling_interval: Duration,
    timeout: Option<Duration>,
}

impl ReadOf {
    fn new(workflow_id: &str, key: &str, options: ReadStreamOptions) -> Self {
        Self {
            workflow_id: workflow_id.to_owned(),
            key: key.to_owned(),
            polling_interval: options
                .polling_interval
                .unwrap_or(DEFAULT_STREAM_POLLING_INTERVAL),
            timeout: options.timeout,
        }
    }

    /// Refuses an offset or a polling interval no read can use, naming `operation`.
    fn validate(&self, offset: i32, operation: &'static str) -> Result<()> {
        if offset < 0 {
            return Err(Error::InvalidArgument {
                operation: operation.into(),
                detail: format!("the offset must not be negative, got {offset}"),
            });
        }
        if self.polling_interval < MIN_STREAM_POLLING_INTERVAL {
            return Err(Error::InvalidArgument {
                operation: operation.into(),
                detail: format!(
                    "the polling interval must be at least {}ms, got {:?}",
                    MIN_STREAM_POLLING_INTERVAL.as_millis(),
                    self.polling_interval
                ),
            });
        }
        Ok(())
    }

    /// Waits for the value at `offset`, or for the stream to end, or for the timeout, and reads up
    /// to `page` values from there.
    ///
    /// A workflow's read also watches its own workflow, and stops if that is cancelled: the wait
    /// takes no step of its own, so nothing else would notice until the stream moved.
    ///
    /// **A refusal that is an answer is recorded** as the step's error — a stream whose workflow
    /// does not exist — so a workflow that catches it replays the same branch, rather than reading
    /// live once the workflow has been created. A failure of the database is not: the read ran into
    /// it rather than learning anything, and a replay reads again.
    async fn await_values(
        &self,
        conn: &Connection,
        placement: &StepPlacement,
        name: &str,
        offset: i32,
        page: i32,
        started_at: Timestamp,
    ) -> Result<AwaitedStream> {
        // A timeout too large to represent is one that never elapses.
        let deadline = self.timeout.map(|timeout| {
            started_at
                .checked_add(timeout)
                .unwrap_or(Timestamp::from_epoch_ms(i64::MAX))
        });
        let wait = conn.sysdb().await_stream_values(
            &self.workflow_id,
            &self.key,
            offset,
            page,
            deadline,
            self.polling_interval,
        );
        let awaited = match placement {
            StepPlacement::Recorded { ctx, .. } => tokio::select! {
                awaited = wait => awaited,
                () = crate::step::observe_cancellation(ctx) => {
                    return Err(Error::WorkflowCancelled {
                        workflow_id: ctx.workflow_id().to_owned(),
                    });
                }
            },
            _ => wait.await,
        };
        match awaited {
            Err(refused) if refused.should_record() => {
                let refused = Error::SystemDatabase(refused);
                let encoded = encode(&refused, "stream read refusal")?;
                record(
                    conn,
                    placement,
                    name,
                    Outcome::Error(&encoded),
                    Some(conn.serializer().name()),
                    started_at,
                )
                .await?;
                Err(refused)
            }
            awaited => awaited.map_err(Error::SystemDatabase),
        }
    }

    /// Records that the wait for a value timed out, and returns the timeout to raise.
    ///
    /// An outcome rather than a failure of the read, so it is recorded like one and a replay raises
    /// it without waiting.
    async fn time_out(
        &self,
        conn: &Connection,
        placement: &StepPlacement,
        name: &str,
        started_at: Timestamp,
    ) -> Result<Error> {
        let timed_out = Error::StreamTimeout {
            workflow_id: self.workflow_id.clone(),
            key: self.key.clone(),
            timeout: self.timeout,
        };
        let encoded = encode(&timed_out, "stream timeout")?;
        record(
            conn,
            placement,
            name,
            Outcome::Error(&encoded),
            Some(conn.serializer().name()),
            started_at,
        )
        .await?;
        Ok(timed_out)
    }
}

/// Everything a read does before it runs: the step id, where it takes one.
fn place(conn: Arc<Connection>, operation: &'static str) -> Built<Arc<Connection>> {
    let placement = StepPlacement::of(&conn, operation)?;
    Ok((conn, placement))
}

/// What a read's step recorded, found on replay.
enum Replayed {
    /// The value it delivered, as recorded.
    Value(String),
    /// The end of the stream.
    Ended,
}

/// The read's recorded outcome, if this workflow has run this far before.
///
/// The read-back half of the pair whose other half is [`record`], named for the same pair in
/// `sysdb` (`check_step` / `record_step`).
///
/// `None` where the read is not recorded at all — from a step, a client, or outside a workflow —
/// as well as where it has not run yet. A recorded error — a timeout, or a stream whose workflow
/// does not exist — is raised again.
async fn check(
    conn: &Connection,
    placement: &StepPlacement,
    name: &str,
) -> Result<Option<Replayed>> {
    let Some(step) = placement.check(conn, name).await? else {
        return Ok(None);
    };
    tracing::debug!(step_id = ?placement.step_id(), name, "replaying a stream read");
    if let Some(error) = step.error {
        return Err(revive(&error, name));
    }
    let output = step.output.unwrap_or_default();
    Ok(Some(
        if is_stream_closed(&output, step.serialization.as_deref()) {
            Replayed::Ended
        } else {
            Replayed::Value(output)
        },
    ))
}

/// Records a value a read delivers, before it is handed over: the entry's own encoding, under its
/// own label.
async fn record_value(
    conn: &Connection,
    placement: &StepPlacement,
    name: &str,
    entry: &EncodedValue,
    started_at: Timestamp,
) -> Result<()> {
    record(
        conn,
        placement,
        name,
        Outcome::Output(Some(&entry.value)),
        entry.serialization.as_deref(),
        started_at,
    )
    .await
}

/// Records the end of the stream, so a replay ends at the same place.
async fn record_end(
    conn: &Connection,
    placement: &StepPlacement,
    name: &str,
    started_at: Timestamp,
) -> Result<()> {
    record(
        conn,
        placement,
        name,
        Outcome::Output(Some(STREAM_CLOSED)),
        Some(STREAM_CLOSED_SERIALIZATION),
        started_at,
    )
    .await
}

/// Whether a stream entry is the close.
fn is_close(entry: &EncodedValue) -> bool {
    is_stream_closed(&entry.value, entry.serialization.as_deref())
}

/// Records a read's outcome under its step, where it has one.
///
/// Through `record_step` rather than [`StepPlacement::record`], because a value is recorded under
/// the label it was written with rather than this connection's serializer.
async fn record(
    conn: &Connection,
    placement: &StepPlacement,
    name: &str,
    outcome: Outcome<'_>,
    serialization: Option<&str>,
    started_at: Timestamp,
) -> Result<()> {
    let Some((workflow_id, step_id)) = placement.step() else {
        return Ok(());
    };
    conn.sysdb()
        .record_step(
            workflow_id,
            step_id,
            name,
            outcome,
            serialization,
            Some(StepTiming {
                started_at,
                completed_at: Timestamp::now(),
            }),
        )
        .await
        .map_err(Error::SystemDatabase)
}
