//! Encoding values on their way into the system database, and back out.
//!
//! Everything the engine stores is an opaque string as far as `sysdb` is concerned, and the
//! `serialization` column on each row records which encoding produced it. That is what lets a
//! reader interpret what it finds without the two sides having agreed in advance.
//!
//! **Three encodings.** [`PayloadEncoding::Native`] (`rust_serde`) is `serde_json` with the
//! application's own derives, and is read only by this SDK. [`PayloadEncoding::Portable`]
//! (`portable_json`) is the encoding every DBOS SDK reads and writes: the same JSON for a plain
//! value, but a workflow input travels in a `{"positionalArgs", "namedArgs"}` envelope and an error
//! in a `{"name", "message", "code", "data"}` one, because those are the shapes a workflow or an
//! exception has in a language with neither a single typed argument nor a typed error.
//! [`PayloadEncoding::Custom`] is the application's own [`CustomSerializer`], under the name it
//! gives.
//!
//! **Every read honours the row's tag**, whatever the reader would itself have written. A tag
//! this process cannot read — neither built-in, nor the name of the serializer it is configured
//! with — is [`Error::UnknownSerialization`], never a guess.
//!
//! **Non-finite floats round-trip natively and are refused portably.** JSON has no way to write
//! `NaN` or an infinity, and `serde_json` would write `null` in its place, which reads back as
//! something else. The native encoding writes them as the strings `"NaN"`, `"Infinity"` and
//! `"-Infinity"` and reads those back as floats wherever a float is asked for. The portable one
//! refuses them with [`Error::Serialization`] naming the value, since the SDKs that read it agree
//! on no form for them. A target that buffers its input before deciding what it is — a
//! `#[serde(untagged)]` or tagged enum, a `#[serde(flatten)]` field, a [`serde_json::Value`] —
//! reads the native strings as strings.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::error::{DurableError, EngineOnly, Error, Result};
use crate::sysdb::types::EncodedValue;

mod non_finite;
mod shape;
pub(crate) use non_finite::{Floats, NonFinite};
use shape::{Shape, takes_the_envelope};

/// The tag a [`PayloadEncoding::Native`] row carries.
pub(crate) const RUST_SERDE: &str = "rust_serde";

/// The tag a [`PayloadEncoding::Portable`] row carries, the one every DBOS SDK reads.
pub(crate) const PORTABLE_JSON: &str = "portable_json";

/// The struct name [`PortableWorkflowArgs`] gives `serde`: how an envelope is recognized, writing
/// it and reading it, through whatever wraps it.
const ENVELOPE: &str = "dbos::PortableWorkflowArgs";

/// An application's own serializer, configured with [`Serializer::Custom`].
///
/// It sees every value with its real type, through `serde`: [`serialize`](Self::serialize) gets a
/// [`Payload`], which is [`Serialize`], and [`deserialize`](Self::deserialize) hands a
/// `serde::Deserializer` to a [`PayloadSlot`]. So any self-describing `serde` format will do —
/// MessagePack, CBOR, JSON — and so will a wrapper that encrypts or compresses what one of them
/// writes.
///
/// ```
/// use dbos::{CustomSerializer, Payload, PayloadSlot};
///
/// /// JSON, reversed: a stand-in for an encrypting serializer.
/// #[derive(Debug)]
/// struct Backwards;
///
/// impl CustomSerializer for Backwards {
///     fn name(&self) -> &str {
///         "backwards_json"
///     }
///
///     fn serialize(
///         &self,
///         value: Payload<'_>,
///     ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
///         Ok(serde_json::to_string(&value)?.chars().rev().collect())
///     }
///
///     fn deserialize(
///         &self,
///         text: &str,
///         into: PayloadSlot<'_>,
///     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///         let json: String = text.chars().rev().collect();
///         let mut deserializer = serde_json::Deserializer::from_str(&json);
///         into.fill(&mut deserializer)?;
///         // Refuses anything after the value, which the slot does not look at.
///         Ok(deserializer.end()?)
///     }
/// }
/// ```
///
/// **The output is text**, because the columns are. A binary format has to encode its bytes —
/// base64, say — on the way out and decode them on the way in.
///
/// **The format must be self-describing**: able to read a value without knowing its type in
/// advance, which `serde` calls `deserialize_any`. Some of what the engine writes holds a
/// `serde_json::Value` — a schedule's context, the `code` and `data` of an
/// [`Error::Portable`] — and a format like bincode cannot read one back.
///
/// **What it encodes.** Everything this process writes in its default encoding: workflow inputs,
/// results and errors, step results and errors, events, messages and stream values, and what the
/// engine records about the calls it checkpoints, and schedules' contexts, which it carries as the
/// JSON data they were given as, so a context reads back the same in a binary format as in JSON.
/// Not what is asked to be
/// [`Portable`](crate::SerializationFormat::Portable) or
/// [`Native`](crate::SerializationFormat::Native). That includes the result a management call made
/// from a workflow records as its step — a listing, a forked id, a refused change — which the
/// system database hands over as JSON data and this serializer encodes.
///
/// **Keep it configured while its rows exist.** A row records the name it was written under, and
/// is readable only by a process configured with a serializer of that name: one without it reports
/// [`Error::UnknownSerialization`] rather than guess. The built-in encodings stay readable either
/// way, so adding a serializer to an application that already has rows is safe — except for
/// schedules: a schedule's context records no name, and is read with whatever serializer is
/// configured, so a schedule created before the serializer was configured has to be written again,
/// with [`update_schedule`](crate::DBOS::update_schedule) or
/// [`apply_schedules`](crate::DBOS::apply_schedules).
pub trait CustomSerializer: Send + Sync + std::fmt::Debug {
    /// The name written to each row's `serialization` column.
    ///
    /// Must be non-empty and not one of the built-in names, `rust_serde` and `portable_json`,
    /// which launch checks. Changing it makes every row written under the old one unreadable.
    fn name(&self) -> &str;

    /// Encodes one value.
    fn serialize(
        &self,
        value: Payload<'_>,
    ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>>;

    /// Decodes `text`, which this serializer wrote, by handing a deserializer for it to `into`.
    fn deserialize(
        &self,
        text: &str,
        into: PayloadSlot<'_>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// A value for a [`CustomSerializer`] to encode: whatever the application handed the engine, seen
/// through [`Serialize`].
pub struct Payload<'a>(&'a dyn erased_serde::Serialize);

impl Serialize for Payload<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        erased_serde::serialize(self.0, serializer)
    }
}

impl std::fmt::Debug for Payload<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Payload")
    }
}

/// Where a [`CustomSerializer`] puts the value it decodes.
///
/// The engine knows the type it wants and the serializer knows the format, so the serializer
/// builds a deserializer over its text and [`fill`](Self::fill)s this from it, which decodes the
/// type the engine asked for.
pub struct PayloadSlot<'a> {
    visit: &'a mut dyn for<'de> FnMut(
        &mut dyn erased_serde::Deserializer<'de>,
    ) -> std::result::Result<(), erased_serde::Error>,
}

impl PayloadSlot<'_> {
    /// Decodes the value from `deserializer`.
    pub fn fill<'de, D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut erased = <dyn erased_serde::Deserializer>::erase(deserializer);
        (self.visit)(&mut erased).map_err(|error| error.to_string().into())
    }
}

impl std::fmt::Debug for PayloadSlot<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PayloadSlot")
    }
}

/// How payloads are encoded on their way into the system database, by default.
///
/// This is what a workflow writes unless it, or the call, asks for
/// [`SerializationFormat::Portable`] or [`SerializationFormat::Native`], and what a step's result
/// and the engine's record of a checkpointed call are written in. A workflow is only ever replayed
/// by the SDK that wrote it — workflows cross languages by enqueue, not by execution — so the
/// default is this application's own to choose. What matters is that the choice is *recorded* in
/// the `serialization` column of every row, so a later reader knows how to read what it finds.
///
/// The built-in encodings, `rust_serde` and `portable_json`, are always readable, whichever this
/// is. A [`Custom`](Self::Custom) serializer's rows are readable only by a process configured with
/// a serializer of the same name; see [`CustomSerializer`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum Serializer {
    /// `serde_json` with this crate's own derives, written as `rust_serde`.
    #[default]
    RustSerde,
    /// The application's own serializer, written under its [`name`](CustomSerializer::name).
    Custom(Arc<dyn CustomSerializer>),
}

/// Two serializers are the same when they are the same instance: one configured `Arc` and its
/// clones.
impl PartialEq for Serializer {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Serializer::RustSerde, Serializer::RustSerde) => true,
            (Serializer::Custom(a), Serializer::Custom(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for Serializer {}

impl Serializer {
    /// A [`Custom`](Self::Custom) serializer.
    pub fn custom(serializer: impl CustomSerializer + 'static) -> Self {
        Serializer::Custom(Arc::new(serializer))
    }

    /// The name written to the `serialization` column.
    pub fn name(&self) -> &str {
        match self {
            Serializer::RustSerde => RUST_SERDE,
            Serializer::Custom(custom) => custom.name(),
        }
    }

    /// The encoding this writes in.
    pub(crate) fn encoding(&self) -> PayloadEncoding {
        match self {
            Serializer::RustSerde => PayloadEncoding::Native,
            Serializer::Custom(custom) => PayloadEncoding::Custom(Arc::clone(custom)),
        }
    }

    /// Refuses a custom name a reader could not tell from a built-in one, or from no name at all.
    pub(crate) fn validate(&self) -> crate::Result<()> {
        let Serializer::Custom(custom) = self else {
            return Ok(());
        };
        match custom.name() {
            "" => Err(crate::Error::Config(
                "a custom serializer's name cannot be empty".to_owned(),
            )),
            name @ (RUST_SERDE | PORTABLE_JSON) => Err(crate::Error::Config(format!(
                "a custom serializer cannot be named {name}, which is a built-in encoding"
            ))),
            _ => Ok(()),
        }
    }
}

/// The configured serializer is how a management call made from a workflow records its result.
///
/// That result is a checkpoint like any step's output, and only this SDK replays it, so it is
/// written in what the application configured — `rust_serde` by default, its own serializer
/// otherwise — and tagged with that name. Reads honour the row's tag, as every other read does.
impl crate::sysdb::types::StepCodec for Serializer {
    fn encode(&self, value: Value) -> std::result::Result<EncodedValue, crate::sysdb::Error> {
        let encoding = self.encoding();
        let text = encode::<_, EngineOnly>(&value, &encoding, "checkpoint").map_err(to_sysdb)?;
        Ok(EncodedValue {
            value: text,
            serialization: Some(encoding.name().to_owned()),
        })
    }

    fn decode(
        &self,
        value: &str,
        serialization: Option<&str>,
    ) -> std::result::Result<Value, crate::sysdb::Error> {
        decode_tagged::<Value, EngineOnly>(Some(value), serialization, self, "checkpoint")
            .map_err(to_sysdb)
    }
}

/// An engine encoding failure, in the system database's terms.
///
/// An unknown tag keeps its meaning; anything else is a value that could not be written or read,
/// which the system database calls [`Malformed`](crate::sysdb::Error::Malformed).
fn to_sysdb(error: Error) -> crate::sysdb::Error {
    match error {
        Error::UnknownSerialization { name } => crate::sysdb::Error::UnknownSerialization { name },
        other => crate::sysdb::Error::Malformed(other.to_string()),
    }
}

/// Which encoding a workflow, an enqueue, or a single write uses.
///
/// **[`Portable`](Self::Portable) is for crossing languages.** It writes `portable_json`, the
/// encoding every DBOS SDK reads, so a workflow another language enqueues, or a message, event or
/// stream value another language reads, should be portable. [`Native`](Self::Native) writes this
/// SDK's own `rust_serde`, even where a [`Serializer::Custom`] is configured.
///
/// **[`Default`](Self::Default) defers**, which is what makes the choice made once reach the
/// writes under it:
///
/// - A start of a workflow takes its registration's choice —
///   [`WorkflowOptions::serialization`](crate::WorkflowOptions::serialization) — unless an
///   [`Enqueue`](crate::Enqueue) makes its own, and a registration that made none takes the
///   configured [`Serializer`]. A [`Client`](crate::Client), with no registration, takes its own
///   configured serializer unless its enqueue says otherwise.
/// - A send, event or stream write made inside a workflow takes that workflow's encoding; one made
///   outside a workflow takes the configured serializer.
///
/// Reading never needs this: every read decodes by what the row says it holds, whatever the
/// reader would itself have written.
///
/// What portable writes, and what it does not: a portable workflow's input, output and error are
/// portable, and its steps' outputs and errors are in the configured serializer, since only this
/// SDK replays them. Its record of a child it awaited keeps the child's own encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SerializationFormat {
    /// Whatever the context says: the registration's choice for a workflow, the enclosing
    /// workflow's encoding for a write.
    #[default]
    Default,
    /// `portable_json`, readable by every DBOS SDK.
    Portable,
    /// `rust_serde`, this SDK's own built-in encoding.
    Native,
}

/// How one payload is encoded, as its row's `serialization` column records it.
///
/// What a caller *asks for* is a [`SerializationFormat`], which may defer to its context; this is
/// what that request resolves to, and what a row says it holds. Two encodings are the same when
/// their names are, which is all a row can tell apart.
#[derive(Debug, Clone, Default)]
pub(crate) enum PayloadEncoding {
    /// `rust_serde`: this SDK's own encoding.
    #[default]
    Native,
    /// `portable_json`: readable by every DBOS SDK.
    Portable,
    /// The application's own serializer, under its own name.
    Custom(Arc<dyn CustomSerializer>),
}

impl PartialEq for PayloadEncoding {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl PayloadEncoding {
    /// The tag written to the `serialization` column.
    pub(crate) fn name(&self) -> &str {
        match self {
            PayloadEncoding::Native => RUST_SERDE,
            PayloadEncoding::Portable => PORTABLE_JSON,
            PayloadEncoding::Custom(custom) => custom.name(),
        }
    }

    /// The encoding a row's tag names, to a process configured with `configured`.
    ///
    /// The built-in names are always readable. Any other is readable only as the configured
    /// serializer's own name. An absent or empty tag is this SDK's own: a row with no tag was
    /// written before tags were written, by this SDK, since a workflow is replayed only by the
    /// SDK that wrote it.
    pub(crate) fn of<E>(tag: Option<&str>, configured: &Serializer) -> Result<PayloadEncoding, E> {
        match tag {
            None | Some("") | Some(RUST_SERDE) => Ok(PayloadEncoding::Native),
            Some(PORTABLE_JSON) => Ok(PayloadEncoding::Portable),
            Some(other) => match configured {
                Serializer::Custom(custom) if custom.name() == other => {
                    Ok(PayloadEncoding::Custom(Arc::clone(custom)))
                }
                _ => Err(Error::UnknownSerialization {
                    name: other.to_owned(),
                }),
            },
        }
    }

    /// What a request resolves to, given the encoding it defers to.
    pub(crate) fn resolve(
        requested: SerializationFormat,
        inherited: PayloadEncoding,
    ) -> PayloadEncoding {
        match requested {
            SerializationFormat::Default => inherited,
            SerializationFormat::Portable => PayloadEncoding::Portable,
            SerializationFormat::Native => PayloadEncoding::Native,
        }
    }

    /// What a request resolves to where the encoding it defers to is the ambient workflow's.
    ///
    /// Inside a workflow — its body or one of its steps — a [`SerializationFormat::Default`] takes
    /// that workflow's encoding, so a portable workflow's events, messages and stream values are
    /// portable too. Outside one there is nothing to inherit, and it is the `configured` default.
    pub(crate) fn resolve_ambient(
        requested: SerializationFormat,
        configured: &Serializer,
    ) -> PayloadEncoding {
        let inherited = crate::context::Ctx::with_current(|ctx| {
            ctx.map_or_else(|| configured.encoding(), crate::context::Ctx::encoding)
        });
        PayloadEncoding::resolve(requested, inherited)
    }
}

/// Decodes a value stored by [`encode`] in `encoding`.
///
/// An absent value reads as JSON `null`, whatever the encoding, which is what `()` decodes from.
/// That is what lets a zero-argument workflow share one erased signature with a one-argument one:
/// its row has no input, and it must not need a literal `"null"` written into the column to run.
///
/// `what` names the value for the error message — `argument`, `result`, `error` — because "could
/// not deserialize" without saying which half is the least useful thing this could report.
///
/// Generic over the caller's error channel rather than returning the engine's own, so a workflow
/// with its own error type needs no conversion at the call site: the failure is an engine variant
/// either way, and `E` only says which channel it travels in.
pub(crate) fn decode<T: DeserializeOwned, E>(
    value: Option<&str>,
    encoding: &PayloadEncoding,
    what: &'static str,
) -> Result<T, E> {
    match (value, encoding) {
        (Some(text), PayloadEncoding::Custom(custom)) => {
            decode_custom(&**custom, text).map_err(|message| Error::Deserialization {
                what: what.into(),
                message,
                source: None,
            })
        }
        (_, PayloadEncoding::Native) => non_finite::from_str(value.unwrap_or("null"))
            .map_err(|source| Error::decode_failed(what, source)),
        _ => serde_json::from_str(value.unwrap_or("null"))
            .map_err(|source| Error::decode_failed(what, source)),
    }
}

/// Decodes `text` with `custom`, into the type the caller asked for.
fn decode_custom<T: DeserializeOwned>(
    custom: &dyn CustomSerializer,
    text: &str,
) -> std::result::Result<T, String> {
    let mut decoded = None;
    let mut visit = |deserializer: &mut dyn erased_serde::Deserializer<'_>| {
        decoded = Some(erased_serde::deserialize::<T>(deserializer)?);
        Ok(())
    };
    custom
        .deserialize(text, PayloadSlot { visit: &mut visit })
        .map_err(|error| error.to_string())?;
    decoded.ok_or_else(|| {
        format!(
            "serializer {} returned without filling its PayloadSlot",
            custom.name()
        )
    })
}

/// [`decode`], for a value whose row says how it is encoded, read by a process configured with
/// `configured`.
pub(crate) fn decode_tagged<T: DeserializeOwned, E>(
    value: Option<&str>,
    tag: Option<&str>,
    configured: &Serializer,
    what: &'static str,
) -> Result<T, E> {
    decode(value, &PayloadEncoding::of(tag, configured)?, what)
}

/// Encodes a value for the system database, in `encoding`.
///
/// Always a string, never an absence — a value that encodes to nothing does not exist, and `()`
/// encodes to `"null"` rather than to no value at all. Nullability belongs to the column, so a
/// caller writing into a nullable one wraps this in `Some` at that point.
///
/// Native and portable are the same JSON for a plain value — a workflow's output, an event, a
/// message, a stream value. What they change is the envelope around a workflow's input or error,
/// which [`encode_input`] and [`encode_error`] add. They also differ on a non-finite float, which
/// `serde_json` would write as `null`: native writes it as its name, and portable refuses it.
pub(crate) fn encode<T: Serialize, E>(
    value: &T,
    encoding: &PayloadEncoding,
    what: &'static str,
) -> Result<String, E> {
    match encoding {
        PayloadEncoding::Custom(custom) => {
            custom
                .serialize(Payload(value))
                .map_err(|error| Error::Serialization {
                    what: what.into(),
                    message: error.to_string(),
                    source: None,
                })
        }
        PayloadEncoding::Native | PayloadEncoding::Portable => {
            let mode = if matches!(encoding, PayloadEncoding::Native) {
                NonFinite::Name
            } else {
                NonFinite::Refuse
            };
            serde_json::to_string(&Floats::new(value, mode))
                .map_err(|source| Error::encode_failed(what, source))
        }
    }
}

/// A portable workflow input, as the envelope every DBOS SDK writes it in.
///
/// A Rust workflow takes one typed input. A tuple is written as the envelope's positional
/// arguments, one per element, and any other input as its single positional argument. This type
/// is for the cases that need the envelope itself:
///
/// - **Calling a workflow in another language** that takes named arguments, or arguments a tuple
///   cannot express — [`Client::enqueue_portable`](crate::Client::enqueue_portable) writes it as
///   given.
/// - **A workflow that takes whatever it is called with** — one registered with this as its input
///   type receives the envelope as stored, rather than its first argument.
///
/// The fields are JSON values rather than typed ones because their types are the callee's.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
// The name `serde` hands a serializer, never written to the wire: it is how `encode_input`
// recognises an envelope it must not wrap in another.
// Spelt out because an attribute takes no constant: it must stay equal to `ENVELOPE`.
#[serde(rename = "dbos::PortableWorkflowArgs")]
pub struct PortableWorkflowArgs {
    /// The arguments by position, `positionalArgs` on the wire.
    #[serde(
        rename = "positionalArgs",
        default,
        deserialize_with = "null_as_default"
    )]
    pub positional_args: Vec<Value>,
    /// The arguments by name, `namedArgs` on the wire.
    ///
    /// Not every SDK binds these to a callee's parameters; check that the callee reads them.
    #[serde(rename = "namedArgs", default, deserialize_with = "null_as_default")]
    pub named_args: Map<String, Value>,
}

/// Reads a missing or `null` field as empty, which is how every SDK reads the envelope's fields:
/// some write `"namedArgs": null`, some leave a field out altogether.
fn null_as_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// The envelope around typed arguments, written without going through a [`Value`].
///
/// A `Value` object is a sorted map, so building the envelope from one would reorder the fields of
/// every struct input; serializing straight through keeps them in declaration order, as the other
/// SDKs write theirs.
#[derive(Serialize)]
struct Envelope<'a, A: ?Sized> {
    /// Anything that serializes as a JSON array: a tuple of the arguments, or an array of one.
    #[serde(rename = "positionalArgs")]
    positional_args: &'a A,
    #[serde(rename = "namedArgs")]
    named_args: Map<String, Value>,
}

/// Encodes a workflow's input in `encoding`.
///
/// Native or custom, it is the input alone. Portable, it is the envelope, and the input's shape
/// decides what goes in it:
///
/// | Input | `positionalArgs` |
/// |---|---|
/// | a tuple of two or more, `(a, b)`, or an array of two or more, `[a, b]` | its elements, `[a, b]`: one argument each |
/// | the unit, `()` | `[]`: no arguments |
/// | a [`PortableWorkflowArgs`] | the envelope as given, not wrapped in another |
/// | anything else — a struct, a tuple struct, a one-element tuple, a `Vec`, a scalar | `[input]`: one argument |
///
/// So a tuple calls a workflow in another language with that many arguments, and every shape reads
/// back as itself through [`decode_input`]. **A fixed-size array is spread like a tuple**, because
/// `serde` writes the two identically; a list meant as one argument is a `Vec` or a slice.
pub(crate) fn encode_input<P: Serialize, E>(
    input: &P,
    encoding: &PayloadEncoding,
) -> Result<String, E> {
    const WHAT: &str = "argument";
    if !matches!(encoding, PayloadEncoding::Portable) {
        return encode(input, encoding, WHAT);
    }
    match Shape::of(input) {
        Shape::Arguments => encode_envelope(input, encoding),
        Shape::NoArguments => encode_envelope(&[(); 0], encoding),
        Shape::Envelope => encode(input, encoding, WHAT),
        Shape::OneArgument => encode_envelope(&[input], encoding),
    }
}

/// An envelope whose positional arguments are what `positional_args` serializes as, a sequence.
fn encode_envelope<A: Serialize + ?Sized, E>(
    positional_args: &A,
    encoding: &PayloadEncoding,
) -> Result<String, E> {
    encode(
        &Envelope {
            positional_args,
            named_args: Map::new(),
        },
        encoding,
        "argument",
    )
}

/// Decodes a workflow's input, stored in `encoding`, into the workflow's input type.
///
/// Portable, the envelope's shape decides what `P` is decoded from:
///
/// | Envelope | Decoded into `P` from |
/// |---|---|
/// | one positional argument, no named ones | that argument |
/// | named arguments only | the `namedArgs` object, so a struct's fields are the names |
/// | two or more positional arguments, no named ones | the array of them, so a tuple takes them |
/// | no arguments at all | `null`, which `()` and `Option` read |
/// | both positional and named arguments | nothing: an error |
///
/// Each argument is decoded from the exact text written for it, never through a
/// [`serde_json::Value`], so an integer past 64 bits reads back as written.
///
/// A workflow whose input type is [`PortableWorkflowArgs`] — or a `Box` of it, or a
/// `#[serde(transparent)]` wrapper around it, which [`encode_input`] also writes as the envelope —
/// receives the envelope itself. A one-element tuple is the one input type the table reads ambiguously: one positional argument
/// is decoded into it, not wrapped in it. [`encode_input`] writes `(x,)` as the one argument
/// `[x]`, which reads back as itself; another SDK calling with the one argument `x` needs `x` to
/// be an array.
pub(crate) fn decode_input<P: DeserializeOwned + 'static, E>(
    value: Option<&str>,
    encoding: &PayloadEncoding,
) -> Result<P, E> {
    const WHAT: &str = "argument";
    if !matches!(encoding, PayloadEncoding::Portable) {
        return decode(value, encoding, WHAT);
    }
    // An absent or `null` input is an envelope with no arguments, whoever reads it.
    let stored = value.filter(|value| *value != "null").unwrap_or("{}");
    if takes_the_envelope::<P>() {
        return decode(Some(stored), encoding, WHAT);
    }
    let RawEnvelope { positional, named } =
        serde_json::from_str(stored).map_err(|source| Error::decode_failed(WHAT, source))?;
    // Reassembled as text from the arguments' own text, which is what keeps every number exact.
    let from = match (positional.len(), named.is_empty()) {
        (0, true) => "null".to_owned(),
        (1, true) => positional[0].get().to_owned(),
        (_, true) => {
            let elements: Vec<&str> = positional.iter().map(|arg| arg.get()).collect();
            format!("[{}]", elements.join(","))
        }
        (0, false) => {
            let fields: Vec<String> = named
                .iter()
                .map(|(name, arg)| {
                    let name = serde_json::to_string(name).expect("a string is JSON");
                    format!("{name}:{}", arg.get())
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        (_, false) => {
            return Err(Error::Deserialization {
                what: WHAT.into(),
                message: "a portable input with both positional and named arguments cannot be \
                          decoded into one value"
                    .to_owned(),
                source: None,
            });
        }
    };
    serde_json::from_str(&from).map_err(|source| Error::decode_failed(WHAT, source))
}

/// The input envelope with each argument kept as the text it was written as.
#[derive(Deserialize)]
struct RawEnvelope<'a> {
    #[serde(
        rename = "positionalArgs",
        default,
        borrow,
        deserialize_with = "null_as_default"
    )]
    positional: Vec<&'a RawValue>,
    #[serde(
        rename = "namedArgs",
        default,
        borrow,
        deserialize_with = "null_as_default"
    )]
    named: BTreeMap<String, &'a RawValue>,
}

/// A workflow's error, as every DBOS SDK writes it portably.
///
/// All four fields are always written, `null` where there is nothing to say. On the way in,
/// `name` is required — every SDK writes it, and it is what makes an object an envelope rather
/// than some other JSON — while a missing or `null` `message` reads as empty, since a Java
/// exception may have none.
#[derive(Debug, Serialize, Deserialize)]
struct PortableError {
    name: String,
    #[serde(default, deserialize_with = "null_as_default")]
    message: String,
    #[serde(default)]
    code: Option<Value>,
    /// Kept as the text written, so an application error's fields read back exactly — a `Value`
    /// would round an integer past 64 bits.
    #[serde(default)]
    data: Option<Box<RawValue>>,
}

/// Encodes a workflow's error in `encoding`.
///
/// Native or custom, it is the whole [`Error`], application variant and all, which reads back as
/// the same value. Portable, it is the `{name, message, code, data}` envelope:
///
/// - **`name`** is the error's type, with its path so it cannot be mistaken for another: the
///   application error's type name without its generic arguments (`shop::errors::CheckoutError`),
///   or for one of DBOS's own errors its variant under [`DBOS_ERRORS`] (`dbos::StepTimeout`).
/// - **`message`** is what the error displays.
/// - **`data`** is the error's own fields as JSON: the application error as serde writes it, or
///   the variant's fields. It is what lets [`decode_error`] give a reader with the same error type
///   back the error itself, and what gives a reader in another language the structured details.
/// - **`code`** is `null`.
///
/// An error that was already portable keeps all four fields it came with.
pub(crate) fn encode_error<E: DurableError, E2>(
    error: &Error<E>,
    encoding: &PayloadEncoding,
) -> Result<String, E2> {
    match encoding {
        PayloadEncoding::Portable => {
            let portable =
                to_portable(error).map_err(|source| Error::encode_failed("error", source))?;
            encode(&portable, encoding, "error")
        }
        PayloadEncoding::Native | PayloadEncoding::Custom(_) => encode(error, encoding, "error"),
    }
}

/// The envelope for `error`, or why the application error's fields could not be written.
///
/// A failure is reported rather than written as `"data": null`: the error would then be recorded
/// in a shape no reader could rebuild it from, which is the same failure as the native encoding's,
/// only silent.
fn to_portable<E: DurableError>(
    error: &Error<E>,
) -> std::result::Result<PortableError, serde_json::Error> {
    Ok(match error {
        Error::Application(application) => PortableError {
            name: type_name::<E>().to_owned(),
            message: application.to_string(),
            code: None,
            data: present(serde_json::value::to_raw_value(&Floats::new(
                application,
                NonFinite::Refuse,
            ))?),
        },
        Error::Portable {
            name,
            message,
            code,
            data,
        } => PortableError {
            name: name.clone(),
            message: message.clone(),
            code: code.clone(),
            data: data
                .as_ref()
                .map(serde_json::value::to_raw_value)
                .transpose()?,
        },
        engine => {
            let (name, data) = variant(engine)?;
            PortableError {
                name,
                message: engine.to_string(),
                code: None,
                data: data
                    .map(|data| serde_json::value::to_raw_value(&data))
                    .transpose()?
                    .and_then(present),
            }
        }
    })
}

/// `None` for a JSON `null`, so an error with nothing in it writes `"data":null` either way.
fn present(data: Box<RawValue>) -> Option<Box<RawValue>> {
    (data.get() != "null").then_some(data)
}

/// The namespace one of DBOS's own errors is named under in a portable envelope:
/// `dbos::StepTimeout`.
///
/// Fixed here rather than taken from [`std::any::type_name`], which would name the module the
/// type is defined in, and which keeps another writer's bare name for a similar error —
/// `StepTimeout`, say — from ever reading as this one.
const DBOS_ERRORS: &str = "dbos::";

/// `E`'s name with its path but without its generic arguments: `shop::errors::CheckoutError` for
/// `shop::errors::CheckoutError<String>`.
///
/// The path is what keeps two types of the same name apart; the arguments are left to `data`,
/// which only reads back as the type they fit. [`std::any::type_name`] promises a readable name
/// rather than a stable one, so a name that one day reads differently, or a type that moves, makes
/// an error written before stay [`Error::Portable`] rather than read as anything else.
fn type_name<E>() -> &'static str {
    let full = std::any::type_name::<E>();
    full.split('<').next().unwrap_or(full)
}

/// The variant one of DBOS's own errors is, under [`DBOS_ERRORS`], and its fields.
fn variant<E: DurableError>(
    error: &Error<E>,
) -> std::result::Result<(String, Option<Value>), serde_json::Error> {
    let (name, fields) = match serde_json::to_value(Floats::new(error, NonFinite::Refuse))? {
        Value::String(unit) => (unit, None),
        Value::Object(tagged) if tagged.len() == 1 => {
            let (name, fields) = tagged.into_iter().next().expect("one entry");
            (name, Some(fields))
        }
        _ => ("Error".to_owned(), None),
    };
    Ok((format!("{DBOS_ERRORS}{name}"), fields))
}

/// Rebuilds the error a portable envelope was written from, if the reader's types can hold it.
///
/// - **The application's own error**, when `name` is `E`'s type name and `data` reads as an `E`.
/// - **One of DBOS's own errors**, when `name` is one of [`Error`]'s variants under
///   [`DBOS_ERRORS`] and `data` reads as its fields.
///
/// A row says nothing about which language wrote it, so the name and the shape of `data` together
/// are what recognize an error. Anything else — another language's error, an application error of
/// a type the reader does not have — is `None`, and stays portable.
fn rebuild<E: DurableError>(name: &str, data: Option<&RawValue>) -> Option<Error<E>> {
    // From the text written, so the fields read back exactly as they were.
    let data = data.map_or("null", RawValue::get);
    if name == type_name::<E>()
        && let Ok(application) = serde_json::from_str::<E>(data)
    {
        return Some(Error::Application(application));
    }
    let variant = serde_json::to_string(name.strip_prefix(DBOS_ERRORS)?).expect("a string is JSON");
    // The externally tagged form serde reads a variant from: the bare name for a unit variant,
    // `{"Variant": fields}` otherwise.
    let tagged = match data {
        "null" => variant,
        fields => format!("{{{variant}:{fields}}}"),
    };
    serde_json::from_str(&tagged).ok()
}

/// Decodes a workflow's error, stored in `encoding`, or `None` if it is not one.
///
/// A native or custom one is the [`Error`] it was written as. A portable one is the error it was
/// written from where the envelope says what that was and the reader's types can hold it — see
/// [`encode_error`] — and [`Error::Portable`] otherwise, which is how another language's error
/// arrives. `None` is for the caller to report — what it reports depends on whose error it was.
///
/// **A custom one is never `None`**: one the serializer cannot decode is
/// [`Error::Deserialization`], since its text is no message to pass on.
pub(crate) fn decode_error<E: DurableError>(
    encoded: &str,
    encoding: &PayloadEncoding,
) -> Option<Error<E>> {
    match encoding {
        PayloadEncoding::Native => {
            decode::<Error<E>, EngineOnly>(Some(encoded), encoding, "error").ok()
        }
        // The text is the serializer's, not a message anyone could read, so a failure to decode
        // it is reported as that.
        PayloadEncoding::Custom(_) => {
            Some(decode::<Error<E>, E>(Some(encoded), encoding, "error").unwrap_or_else(|e| e))
        }
        PayloadEncoding::Portable => {
            let PortableError {
                name,
                message,
                code,
                data,
            } = serde_json::from_str(encoded).ok()?;
            if let Some(rebuilt) = rebuild(&name, data.as_deref()) {
                return Some(rebuilt);
            }
            Some(Error::Portable {
                name,
                message,
                code,
                data: data.and_then(|data| serde_json::from_str(data.get()).ok()),
            })
        }
    }
}

/// A workflow's result as its row holds it, with the encoding that says how to read it.
///
/// The encoding travels with the bytes for the reason it does with an
/// [`EncodedError`](crate::error::EncodedError): a custom serializer's text is not JSON, so a
/// reader that guessed would misread it.
#[derive(Debug, Clone)]
pub(crate) struct EncodedOutput {
    /// The encoded result, or `None` for a workflow that finished without one.
    pub(crate) encoded: Option<String>,
    /// How it is encoded.
    pub(crate) encoding: PayloadEncoding,
}

impl EncodedOutput {
    /// The result this records, in the caller's type.
    ///
    /// A result that does not decode is [`Error::Deserialization`]: unlike an error, a value has
    /// nothing to stand in for it.
    pub(crate) fn decode<R: DeserializeOwned, E>(&self) -> Result<R, E> {
        decode(self.encoded.as_deref(), &self.encoding, "result")
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::error::EngineOnly;

    #[test]
    fn an_absent_value_decodes_as_the_unit() {
        assert_eq!(
            decode::<(), EngineOnly>(None, &PayloadEncoding::Native, "argument").unwrap(),
            ()
        );
        assert_eq!(
            decode::<Option<u32>, EngineOnly>(None, &PayloadEncoding::Native, "argument").unwrap(),
            None
        );
    }

    #[test]
    fn a_value_round_trips() {
        let encoded =
            encode::<_, EngineOnly>(&(1u32, "two"), &PayloadEncoding::Native, "argument").unwrap();
        assert_eq!(encoded, r#"[1,"two"]"#);
        assert_eq!(
            decode::<(u32, String), EngineOnly>(
                Some(&encoded),
                &PayloadEncoding::Native,
                "argument"
            )
            .unwrap(),
            (1, "two".to_owned())
        );
    }

    #[test]
    fn the_unit_encodes_as_a_value_rather_than_an_absence() {
        assert_eq!(
            encode::<_, EngineOnly>(&(), &PayloadEncoding::Native, "argument").unwrap(),
            "null"
        );
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Order {
        sku: String,
        quantity: u32,
    }

    fn portable_input<P: DeserializeOwned + 'static>(stored: &str) -> Result<P> {
        decode_input(Some(stored), &PayloadEncoding::Portable)
    }

    #[test]
    fn the_serializer_names_itself_as_the_column_records_it() {
        assert_eq!(Serializer::default().name(), "rust_serde");
    }

    #[test]
    fn a_tag_names_its_format_and_an_unknown_one_is_refused() {
        for tag in [None, Some(""), Some("rust_serde")] {
            assert_eq!(
                PayloadEncoding::of::<EngineOnly>(tag, &Serializer::RustSerde).unwrap(),
                PayloadEncoding::Native,
                "{tag:?}"
            );
        }
        assert_eq!(
            PayloadEncoding::of::<EngineOnly>(Some("portable_json"), &Serializer::RustSerde)
                .unwrap(),
            PayloadEncoding::Portable
        );
        let err = PayloadEncoding::of::<EngineOnly>(Some("py_pickle"), &Serializer::RustSerde)
            .unwrap_err();
        assert!(
            matches!(&err, Error::UnknownSerialization { name } if name == "py_pickle"),
            "{err}"
        );
        assert_eq!(err.to_string(), "serialization py_pickle is not available");
    }

    #[test]
    fn a_portable_input_is_the_envelopes_one_positional_argument() {
        let order = Order {
            sku: "tea".to_owned(),
            quantity: 2,
        };
        let encoded = encode_input::<_, EngineOnly>(&order, &PayloadEncoding::Portable).unwrap();
        // Field order is the struct's, not a sorted map's.
        assert_eq!(
            encoded,
            r#"{"positionalArgs":[{"sku":"tea","quantity":2}],"namedArgs":{}}"#
        );
        assert_eq!(portable_input::<Order>(&encoded).unwrap(), order);
        // Native, the input is the value alone.
        assert_eq!(
            encode_input::<_, EngineOnly>(&order, &PayloadEncoding::Native).unwrap(),
            r#"{"sku":"tea","quantity":2}"#
        );
    }

    /// Writes `input` portably, checks the envelope, and checks that it reads back as itself.
    fn portable_round_trip<P>(input: P, expected: &str)
    where
        P: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + 'static,
    {
        let encoded = encode_input::<_, EngineOnly>(&input, &PayloadEncoding::Portable).unwrap();
        assert_eq!(encoded, expected);
        assert_eq!(portable_input::<P>(&encoded).unwrap(), input);
    }

    #[test]
    fn a_tuple_is_written_as_one_argument_per_element() {
        portable_round_trip(
            ("a".to_owned(), 1i64),
            r#"{"positionalArgs":["a",1],"namedArgs":{}}"#,
        );
        portable_round_trip(
            (1u8, (2u8, 3u8), vec![4u8]),
            r#"{"positionalArgs":[1,[2,3],[4]],"namedArgs":{}}"#,
        );
    }

    #[test]
    fn the_unit_is_written_as_no_arguments() {
        portable_round_trip((), r#"{"positionalArgs":[],"namedArgs":{}}"#);
    }

    /// A unit is no arguments through a reference or a box, as it is bare: what decides is what
    /// `serde` is asked to write, not how the value is held.
    #[test]
    fn a_borrowed_or_boxed_unit_is_no_arguments() {
        const NONE: &str = r#"{"positionalArgs":[],"namedArgs":{}}"#;
        let portable = PayloadEncoding::Portable;
        assert_eq!(
            encode_input::<_, EngineOnly>(&&(), &portable).unwrap(),
            NONE
        );
        assert_eq!(
            encode_input::<_, EngineOnly>(&Box::new(()), &portable).unwrap(),
            NONE
        );
    }

    /// One `null` argument is `None`: it writes `serialize_none`, not a unit, so a callee in
    /// another language is called with `null` rather than with nothing.
    #[test]
    fn none_is_one_null_argument() {
        portable_round_trip(None::<u32>, r#"{"positionalArgs":[null],"namedArgs":{}}"#);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Pair(String, i64);

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nothing;

    /// Only a bare tuple of two or more is spread: every other shape is one argument, and reads
    /// back as itself.
    #[test]
    fn anything_else_is_one_argument() {
        portable_round_trip((7u32,), r#"{"positionalArgs":[[7]],"namedArgs":{}}"#);
        portable_round_trip(
            Pair("a".to_owned(), 1),
            r#"{"positionalArgs":[["a",1]],"namedArgs":{}}"#,
        );
        portable_round_trip(vec![1u8, 2], r#"{"positionalArgs":[[1,2]],"namedArgs":{}}"#);
        portable_round_trip(
            Some(("a".to_owned(), 1i64)),
            r#"{"positionalArgs":[["a",1]],"namedArgs":{}}"#,
        );
        portable_round_trip(None::<u8>, r#"{"positionalArgs":[null],"namedArgs":{}}"#);
        portable_round_trip(Nothing, r#"{"positionalArgs":[null],"namedArgs":{}}"#);
        portable_round_trip(7u32, r#"{"positionalArgs":[7],"namedArgs":{}}"#);
    }

    /// `serde` writes a fixed-size array as it writes a tuple, so it is spread like one; a `Vec`
    /// is the list that stays one argument.
    #[test]
    fn a_fixed_size_array_is_spread_and_a_vec_is_not() {
        portable_round_trip([1u8, 2, 3], r#"{"positionalArgs":[1,2,3],"namedArgs":{}}"#);
        portable_round_trip(
            vec![1u8, 2, 3],
            r#"{"positionalArgs":[[1,2,3]],"namedArgs":{}}"#,
        );
    }

    /// No input at all is an envelope with no arguments, for a workflow taking the envelope as
    /// for any other.
    #[test]
    fn an_absent_input_is_an_empty_envelope() {
        assert_eq!(
            decode_input::<PortableWorkflowArgs, EngineOnly>(None, &PayloadEncoding::Portable)
                .unwrap(),
            PortableWorkflowArgs::default()
        );
        assert_eq!(
            portable_input::<PortableWorkflowArgs>("null").unwrap(),
            PortableWorkflowArgs::default()
        );
    }

    /// An envelope written by this SDK is the envelope, not one argument holding it.
    #[test]
    fn an_envelope_input_is_written_as_given() {
        portable_round_trip(
            PortableWorkflowArgs {
                positional_args: vec![serde_json::json!(1)],
                named_args: Map::from_iter([("two".to_owned(), serde_json::json!(2))]),
            },
            r#"{"positionalArgs":[1],"namedArgs":{"two":2}}"#,
        );
    }

    #[test]
    fn named_arguments_alone_decode_as_a_struct() {
        assert_eq!(
            portable_input::<Order>(
                r#"{"positionalArgs":[],"namedArgs":{"quantity":3,"sku":"x"}}"#
            )
            .unwrap(),
            Order {
                sku: "x".to_owned(),
                quantity: 3
            }
        );
    }

    #[test]
    fn several_positional_arguments_decode_as_a_tuple() {
        assert_eq!(
            portable_input::<(String, u32)>(r#"{"positionalArgs":["x",3],"namedArgs":{}}"#)
                .unwrap(),
            ("x".to_owned(), 3)
        );
    }

    #[test]
    fn no_arguments_decode_as_the_unit() {
        portable_input::<()>(r#"{"positionalArgs":[],"namedArgs":{}}"#).unwrap();
        portable_input::<()>("{}").unwrap();
        portable_input::<()>("null").unwrap();
        assert_eq!(
            decode_input::<(), EngineOnly>(None, &PayloadEncoding::Portable).unwrap(),
            ()
        );
        assert_eq!(portable_input::<Option<u32>>("{}").unwrap(), None);
    }

    #[test]
    fn positional_and_named_arguments_together_are_refused() {
        let err = portable_input::<Order>(r#"{"positionalArgs":["x"],"namedArgs":{"quantity":3}}"#)
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Deserialization {
                    what: Cow::Borrowed("argument"),
                    ..
                }
            ),
            "{err}"
        );
    }

    /// The SQL `enqueue_workflow` function builds the envelope with `json_build_object(...)::TEXT`,
    /// which spaces it out and may leave `namedArgs` null.
    #[test]
    fn the_envelope_is_read_however_it_is_spelt() {
        assert_eq!(
            portable_input::<u32>(r#"{"positionalArgs" : [7], "namedArgs" : null}"#).unwrap(),
            7
        );
        assert_eq!(
            portable_input::<u32>(r#"{"namedArgs":{},"positionalArgs":[7]}"#).unwrap(),
            7
        );
        assert_eq!(
            portable_input::<u32>(r#"{"positionalArgs":[7]}"#).unwrap(),
            7
        );
    }

    #[test]
    fn a_workflow_taking_the_envelope_gets_it_whole() {
        let args = portable_input::<PortableWorkflowArgs>(
            r#"{"positionalArgs":[1,"two"],"namedArgs":{"three":3}}"#,
        )
        .unwrap();
        assert_eq!(
            args.positional_args,
            [serde_json::json!(1), serde_json::json!("two")]
        );
        assert_eq!(args.named_args["three"], 3);
        assert_eq!(
            portable_input::<PortableWorkflowArgs>(r#"{"positionalArgs":null}"#).unwrap(),
            PortableWorkflowArgs::default()
        );
    }

    /// The envelope is recognized reading it as it is writing it: through a `Box`, or a
    /// `#[serde(transparent)]` wrapper, it is read whole rather than as a struct's fields.
    #[test]
    fn a_wrapped_envelope_is_read_as_it_is_written() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Args(PortableWorkflowArgs);

        let args = PortableWorkflowArgs {
            positional_args: vec![serde_json::json!(1)],
            named_args: Map::from_iter([("x".to_owned(), serde_json::json!(2))]),
        };
        let written = r#"{"positionalArgs":[1],"namedArgs":{"x":2}}"#;
        portable_round_trip(Box::new(args.clone()), written);
        portable_round_trip(Args(args), written);
    }

    /// An argument is read from the text written for it, so an integer past 64 bits survives the
    /// envelope, in whichever of its shapes it travels.
    #[test]
    fn an_integer_past_64_bits_survives_the_envelope() {
        let big = (1u128 << 64) + 7;
        let small = i128::MIN + 7;
        portable_round_trip(
            big,
            &format!(r#"{{"positionalArgs":[{big}],"namedArgs":{{}}}}"#),
        );
        portable_round_trip(
            (big, small),
            &format!(r#"{{"positionalArgs":[{big},{small}],"namedArgs":{{}}}}"#),
        );

        #[derive(Debug, PartialEq, Deserialize)]
        struct Named {
            big: u128,
        }
        assert_eq!(
            portable_input::<Named>(&format!(r#"{{"namedArgs":{{"big":{big}}}}}"#)).unwrap(),
            Named { big }
        );
    }

    /// An application error's fields travel as the text written, so one holding an integer past
    /// 64 bits encodes and reads back as itself, as it does natively.
    #[test]
    fn an_application_error_with_a_large_integer_round_trips() {
        #[derive(Debug, PartialEq, thiserror::Error, Serialize, Deserialize)]
        #[error("over by {0}")]
        struct Over(u128);

        let error: Error<Over> = Over((1u128 << 64) + 7).into();
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        assert!(
            matches!(
                decode_error::<Over>(&encoded, &PayloadEncoding::Portable),
                Some(Error::Application(Over(n))) if n == (1u128 << 64) + 7
            ),
            "{encoded}"
        );
    }

    #[derive(Debug, thiserror::Error, Serialize, Deserialize)]
    #[error("the card was declined")]
    struct CardDeclined;

    fn envelope(encoded: &str) -> serde_json::Value {
        serde_json::from_str(encoded).unwrap()
    }

    #[test]
    fn an_application_error_is_written_under_its_type_name() {
        let error: Error<CardDeclined> = Error::Application(CardDeclined);
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        assert_eq!(
            encoded,
            r#"{"name":"dbos::serialization::tests::CardDeclined","message":"the card was declined","code":null,"data":null}"#
        );
    }

    #[test]
    fn a_dbos_error_is_written_under_its_variant() {
        let error: Error<CardDeclined> = Error::StepTimeout {
            step: "charge".to_owned(),
            timeout: std::time::Duration::from_millis(1500),
        };
        let encoded =
            envelope(&encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap());
        assert_eq!(encoded["name"], "dbos::StepTimeout");
        assert_eq!(encoded["message"], error.to_string());
        assert!(encoded["code"].is_null());
        assert_eq!(
            encoded["data"],
            serde_json::to_value(&error).unwrap()["StepTimeout"],
            "the variant's fields"
        );
    }

    #[derive(Debug, thiserror::Error, Serialize, Deserialize, PartialEq)]
    enum Checkout {
        #[error("declined after {attempts} attempts")]
        Declined { attempts: u32 },
    }

    #[test]
    fn an_application_errors_fields_are_its_data() {
        let error: Error<Checkout> = Error::Application(Checkout::Declined { attempts: 3 });
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        assert_eq!(
            encoded,
            r#"{"name":"dbos::serialization::tests::Checkout","message":"declined after 3 attempts","code":null,"data":{"Declined":{"attempts":3}}}"#
        );
    }

    #[test]
    fn a_portable_error_keeps_what_it_came_with() {
        let error: Error<CardDeclined> = Error::Portable {
            name: "ValueError".to_owned(),
            message: "bad".to_owned(),
            code: Some(serde_json::json!(42)),
            data: Some(serde_json::json!({"field": "x"})),
        };
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        assert_eq!(
            encoded,
            r#"{"name":"ValueError","message":"bad","code":42,"data":{"field":"x"}}"#
        );
    }

    #[test]
    fn an_application_error_reads_back_as_itself() {
        let error: Error<Checkout> = Error::Application(Checkout::Declined { attempts: 3 });
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        let read = decode_error::<Checkout>(&encoded, &PayloadEncoding::Portable).unwrap();
        assert!(
            matches!(read, Error::Application(Checkout::Declined { attempts: 3 })),
            "{read:?}"
        );

        // A unit struct has no fields to write, and comes back all the same.
        let encoded = encode_error::<_, EngineOnly>(
            &Error::Application(CardDeclined),
            &PayloadEncoding::Portable,
        )
        .unwrap();
        assert!(matches!(
            decode_error::<CardDeclined>(&encoded, &PayloadEncoding::Portable),
            Some(Error::Application(CardDeclined))
        ));
    }

    #[test]
    fn a_dbos_error_reads_back_as_itself() {
        let error: Error<Checkout> = Error::StepTimeout {
            step: "charge".to_owned(),
            timeout: std::time::Duration::from_millis(1500),
        };
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        let read = decode_error::<Checkout>(&encoded, &PayloadEncoding::Portable).unwrap();
        assert!(
            matches!(&read, Error::StepTimeout { step, timeout }
                if step == "charge" && *timeout == std::time::Duration::from_millis(1500)),
            "{read:?}"
        );
    }

    #[test]
    fn an_error_the_reader_cannot_hold_stays_portable() {
        let error: Error<Checkout> = Error::Application(Checkout::Declined { attempts: 3 });
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap();
        // Another error type: the name does not match.
        let read = decode_error::<CardDeclined>(&encoded, &PayloadEncoding::Portable).unwrap();
        assert!(
            matches!(&read, Error::Portable { name, data: Some(_), .. }
                if name == "dbos::serialization::tests::Checkout"),
            "{read:?}"
        );
        let stays_portable = |envelope: &str| {
            let read = decode_error::<Checkout>(envelope, &PayloadEncoding::Portable).unwrap();
            assert!(
                matches!(read, Error::Portable { .. }),
                "{envelope}: {read:?}"
            );
        };
        // The name matches, but the fields are not this type's.
        stays_portable(
            r#"{"name":"dbos::serialization::tests::Checkout","message":"x","data":{"field":"y"}}"#,
        );
        // The type's bare name, or another path to a type of that name: another type.
        stays_portable(r#"{"name":"Checkout","message":"x","data":{"Declined":{"attempts":3}}}"#);
        stays_portable(
            r#"{"name":"other::Checkout","message":"x","data":{"Declined":{"attempts":3}}}"#,
        );
        // A variant whose fields do not fit.
        stays_portable(r#"{"name":"dbos::StepTimeout","message":"x","data":null}"#);
        // A variant's bare name, outside the namespace: another writer's error.
        stays_portable(
            r#"{"name":"StepTimeout","message":"x","data":{"step":"charge","timeout":{"secs":1,"nanos":0}}}"#,
        );
    }

    /// An application error whose fields cannot be written as JSON is a failure to encode, as it
    /// is natively, rather than an envelope with its `data` quietly left out.
    #[test]
    fn an_application_error_that_cannot_be_written_is_refused() {
        #[derive(Debug, thiserror::Error, Serialize, Deserialize)]
        #[error("no keys fit")]
        struct BadKeys {
            by_pair: std::collections::HashMap<(u8, u8), u8>,
        }
        let error: Error<BadKeys> = BadKeys {
            by_pair: [((1, 2), 3)].into_iter().collect(),
        }
        .into();
        let refused =
            encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).unwrap_err();
        assert!(
            matches!(&refused, Error::Serialization { what, .. } if what == "error"),
            "{refused:?}"
        );
    }

    /// An object with no `name` is not an envelope, so it is not passed off as an empty one: the
    /// caller gets `None`, and keeps the stored text.
    #[test]
    fn an_object_without_a_name_is_not_an_error_envelope() {
        for stored in [r#"{"error":"boom"}"#, r#"{"name":null,"message":"boom"}"#] {
            assert!(
                decode_error::<EngineOnly>(stored, &PayloadEncoding::Portable).is_none(),
                "{stored}"
            );
        }
    }

    #[test]
    fn another_sdks_error_is_read_whatever_it_leaves_out() {
        // A writer may leave the message null, or leave code and data out.
        let java = decode_error::<EngineOnly>(
            r#"{"name":"IllegalStateException","message":null,"code":null,"data":null}"#,
            &PayloadEncoding::Portable,
        )
        .unwrap();
        assert!(
            matches!(&java, Error::Portable { name, message, .. } if name == "IllegalStateException" && message.is_empty()),
            "{java:?}"
        );
        let ts = decode_error::<EngineOnly>(
            r#"{"name":"Error","message":"boom"}"#,
            &PayloadEncoding::Portable,
        )
        .unwrap();
        assert!(
            matches!(
                &ts,
                Error::Portable {
                    code: None,
                    data: None,
                    ..
                }
            ),
            "{ts:?}"
        );
        // A string code survives as one.
        let go = decode_error::<EngineOnly>(
            r#"{"name":"WorkflowExecution","message":"x","code":"E1"}"#,
            &PayloadEncoding::Portable,
        )
        .unwrap();
        assert!(
            matches!(&go, Error::Portable { code: Some(code), .. } if code == "E1"),
            "{go:?}"
        );
        assert!(decode_error::<EngineOnly>("not json", &PayloadEncoding::Portable).is_none());
    }

    #[test]
    fn a_native_error_still_round_trips_as_itself() {
        let error: Error<CardDeclined> = Error::Application(CardDeclined);
        let encoded = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Native).unwrap();
        assert!(matches!(
            decode_error::<CardDeclined>(&encoded, &PayloadEncoding::Native),
            Some(Error::Application(CardDeclined))
        ));
    }

    #[test]
    fn a_type_name_keeps_its_path_and_loses_its_arguments() {
        assert_eq!(
            type_name::<CardDeclined>(),
            "dbos::serialization::tests::CardDeclined"
        );
        assert_eq!(type_name::<Vec<String>>(), "alloc::vec::Vec");
    }

    /// What `serde_json` does with the portable value rules: an integer key becomes a string, and
    /// any other non-string key is refused.
    #[test]
    fn values_follow_serde_json() {
        let map = std::collections::BTreeMap::from([(1u32, "one")]);
        assert_eq!(
            encode::<_, EngineOnly>(&map, &PayloadEncoding::Native, "value").unwrap(),
            r#"{"1":"one"}"#
        );
        let tuple_keys = std::collections::BTreeMap::from([((1u32, 2u32), "pair")]);
        assert!(encode::<_, EngineOnly>(&tuple_keys, &PayloadEncoding::Native, "value").is_err());
    }

    /// A non-finite float round-trips through the native encoding, wherever it is in the value, and
    /// is refused by the portable one with an error naming the value. Finite floats, and everything
    /// around them, are written the same way by both.
    #[test]
    fn non_finite_floats_round_trip_natively_and_are_refused_portably() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Reading {
            name: String,
            value: Option<f64>,
            many: Vec<f32>,
        }
        #[derive(Debug, Serialize, Deserialize)]
        enum Sample {
            Point(f32),
            Pair(f64, f64),
            Range { low: f64, high: f64 },
        }

        fn round_trip<T: Serialize + DeserializeOwned>(value: &T) -> (String, T) {
            let text = encode::<_, EngineOnly>(value, &PayloadEncoding::Native, "result")
                .expect("a native write");
            let back = decode::<T, EngineOnly>(Some(&text), &PayloadEncoding::Native, "result")
                .unwrap_or_else(|e| panic!("{text} did not read back: {e}"));
            (text, back)
        }
        fn same(a: f64, b: f64) -> bool {
            a == b || (a.is_nan() && b.is_nan())
        }

        let (text, back) = round_trip(&f64::NAN);
        assert_eq!(text, r#""NaN""#);
        assert!(back.is_nan());
        assert_eq!(
            round_trip(&f64::INFINITY),
            (r#""Infinity""#.to_owned(), f64::INFINITY)
        );
        assert_eq!(
            round_trip(&f64::NEG_INFINITY),
            (r#""-Infinity""#.to_owned(), f64::NEG_INFINITY)
        );
        assert!(round_trip(&f32::NAN).1.is_nan());
        assert_eq!(round_trip(&f32::NEG_INFINITY).1, f32::NEG_INFINITY);

        let reading = Reading {
            name: "NaN".to_owned(),
            value: Some(f64::INFINITY),
            many: vec![1.5, f32::NAN, f32::NEG_INFINITY],
        };
        let (text, back) = round_trip(&reading);
        assert_eq!(
            text,
            r#"{"name":"NaN","value":"Infinity","many":[1.5,"NaN","-Infinity"]}"#
        );
        assert_eq!(back.name, "NaN", "a string field keeps the string");
        assert_eq!(back.value, Some(f64::INFINITY));
        assert_eq!(back.many[0], 1.5);
        assert!(back.many[1].is_nan());
        assert_eq!(back.many[2], f32::NEG_INFINITY);

        let map = BTreeMap::from([("k".to_owned(), f64::NAN)]);
        assert!(round_trip(&map).1["k"].is_nan());
        let (_, tuple) = round_trip(&(1u32, f64::INFINITY));
        assert_eq!(tuple, (1, f64::INFINITY));
        for sample in [
            Sample::Point(f32::INFINITY),
            Sample::Pair(f64::NAN, 2.0),
            Sample::Range {
                low: f64::NEG_INFINITY,
                high: 0.0,
            },
        ] {
            let (_, back) = round_trip(&sample);
            let ok = match (&sample, &back) {
                (Sample::Point(a), Sample::Point(b)) => same(f64::from(*a), f64::from(*b)),
                (Sample::Pair(a, b), Sample::Pair(c, d)) => same(*a, *c) && same(*b, *d),
                (Sample::Range { low: a, high: b }, Sample::Range { low: c, high: d }) => {
                    same(*a, *c) && same(*b, *d)
                }
                _ => false,
            };
            assert!(ok, "{sample:?} read back as {back:?}");
        }

        // A name is read as a float only where a float is asked for; elsewhere it is refused as
        // any other string would be.
        assert!(
            decode::<u32, EngineOnly>(Some(r#""NaN""#), &PayloadEncoding::Native, "result")
                .is_err()
        );
        assert!(
            decode::<f64, EngineOnly>(Some(r#""nan""#), &PayloadEncoding::Native, "result")
                .is_err(),
            "only the exact names are floats"
        );

        fn refused<T: Serialize>(value: &T) {
            let error = encode::<_, EngineOnly>(value, &PayloadEncoding::Portable, "result")
                .expect_err("a non-finite float was written portably");
            let Error::Serialization { what, message, .. } = &error else {
                panic!("not a serialization error: {error}");
            };
            assert_eq!(what, "result");
            assert!(message.contains("not a finite number"), "{message}");
        }
        refused(&f64::NAN);
        refused(&f64::INFINITY);
        refused(&f32::NEG_INFINITY);
        refused(&reading);
        refused(&map);
        refused(&Sample::Pair(1.0, f64::NAN));

        for encoding in [PayloadEncoding::Native, PayloadEncoding::Portable] {
            assert_eq!(
                encode::<_, EngineOnly>(
                    &Sample::Range {
                        low: -1.0,
                        high: 2.0
                    },
                    &encoding,
                    "r"
                )
                .unwrap(),
                r#"{"Range":{"low":-1.0,"high":2.0}}"#
            );
            assert_eq!(
                encode::<_, EngineOnly>(&f64::MAX, &encoding, "result").unwrap(),
                serde_json::to_string(&f64::MAX).unwrap()
            );
        }

        // A float map key is written as a quoted number, as `serde_json` always writes keys, and
        // read back as the number it spells; so is a string holding a number where a float is
        // asked for.
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        struct Key(f64);
        impl PartialEq for Key {
            fn eq(&self, other: &Self) -> bool {
                self.0.total_cmp(&other.0).is_eq()
            }
        }
        impl Eq for Key {}
        impl PartialOrd for Key {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Key {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.total_cmp(&other.0)
            }
        }
        let keyed = BTreeMap::from([(Key(1.5), 7u32), (Key(-2.0), 8)]);
        let (text, back) = round_trip(&keyed);
        assert_eq!(text, r#"{"-2.0":8,"1.5":7}"#);
        assert_eq!(back, keyed);
        assert_eq!(
            decode::<f32, EngineOnly>(Some(r#""2.5""#), &PayloadEncoding::Native, "result")
                .unwrap(),
            2.5
        );

        // A portable error's fields go through a `Value`, which can't hold a non-finite float.
        #[derive(Debug, thiserror::Error, Serialize, Deserialize)]
        #[error("insufficient funds")]
        struct Insufficient {
            balance: f64,
        }
        let error: Error<Insufficient> = Error::Application(Insufficient { balance: f64::NAN });
        assert!(
            encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Portable).is_err(),
            "a portable error wrote a non-finite float"
        );
        let native = encode_error::<_, EngineOnly>(&error, &PayloadEncoding::Native)
            .expect("a native error");
        let Some(Error::Application(Insufficient { balance })) =
            decode_error::<Insufficient>(&native, &PayloadEncoding::Native)
        else {
            panic!("{native} did not read back as the application error");
        };
        assert!(balance.is_nan());

        // A workflow input is checked inside the portable envelope too, and round-trips natively.
        assert!(
            encode_input::<_, EngineOnly>(&(1u32, f64::NAN), &PayloadEncoding::Portable).is_err()
        );
        let input = encode_input::<_, EngineOnly>(&(1u32, f64::NAN), &PayloadEncoding::Native)
            .expect("a native input");
        let (n, x) = decode_input::<(u32, f64), EngineOnly>(Some(&input), &PayloadEncoding::Native)
            .expect("the input read back");
        assert!(n == 1 && x.is_nan(), "{input}");
    }

    /// JSON, reversed: text no built-in reader would accept.
    #[derive(Debug)]
    struct Backwards;

    impl CustomSerializer for Backwards {
        fn name(&self) -> &str {
            "backwards_json"
        }

        fn serialize(
            &self,
            value: Payload<'_>,
        ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(serde_json::to_string(&value)?.chars().rev().collect())
        }

        fn deserialize(
            &self,
            text: &str,
            into: PayloadSlot<'_>,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let json: String = text.chars().rev().collect();
            let mut deserializer = serde_json::Deserializer::from_str(&json);
            into.fill(&mut deserializer)?;
            Ok(deserializer.end()?)
        }
    }

    fn backwards() -> (Serializer, PayloadEncoding) {
        let serializer = Serializer::custom(Backwards);
        let format = serializer.encoding();
        (serializer, format)
    }

    /// The serializer sees the value's own type: `u128::MAX` is past what a JSON value holds.
    #[test]
    fn a_custom_serializer_sees_the_real_type() {
        let (_, format) = backwards();
        let value = (u128::MAX, "two".to_owned());
        let encoded = encode::<_, EngineOnly>(&value, &format, "value").unwrap();
        assert_eq!(
            encoded,
            r#"]"owt",554112867134706473364364839029663282043["#
        );
        assert_eq!(
            decode::<(u128, String), EngineOnly>(Some(&encoded), &format, "value").unwrap(),
            value
        );
        // An absent value is still `null`, whoever would have written it.
        decode::<(), EngineOnly>(None, &format, "value").unwrap();
    }

    #[test]
    fn a_custom_tag_is_readable_only_where_that_serializer_is_configured() {
        let (serializer, format) = backwards();
        assert_eq!(
            PayloadEncoding::of::<EngineOnly>(Some("backwards_json"), &serializer).unwrap(),
            format
        );
        // The built-ins stay readable alongside it.
        assert_eq!(
            PayloadEncoding::of::<EngineOnly>(Some("rust_serde"), &serializer).unwrap(),
            PayloadEncoding::Native
        );
        let err = PayloadEncoding::of::<EngineOnly>(Some("backwards_json"), &Serializer::RustSerde)
            .unwrap_err();
        assert!(
            matches!(&err, Error::UnknownSerialization { name } if name == "backwards_json"),
            "{err}"
        );
    }

    #[test]
    fn a_custom_serializer_carries_an_input_and_an_error_whole() {
        let (_, format) = backwards();
        let order = Order {
            sku: "tea".to_owned(),
            quantity: 2,
        };
        let encoded = encode_input::<_, EngineOnly>(&order, &format).unwrap();
        assert_eq!(encoded, r#"}2:"ytitnauq","aet":"uks"{"#);
        assert_eq!(
            decode_input::<Order, EngineOnly>(Some(&encoded), &format).unwrap(),
            order
        );

        let error: Error<CardDeclined> = Error::Application(CardDeclined);
        let encoded = encode_error::<_, EngineOnly>(&error, &format).unwrap();
        assert!(matches!(
            decode_error::<CardDeclined>(&encoded, &format),
            Some(Error::Application(CardDeclined))
        ));
    }

    /// A custom serializer's text is no message, so an error it cannot read is reported as a
    /// failure to decode rather than passed off as the error itself.
    #[test]
    fn an_error_a_custom_serializer_cannot_read_is_a_decode_failure() {
        let (_, format) = backwards();
        let decoded = decode_error::<CardDeclined>("sealed", &format);
        assert!(
            matches!(&decoded, Some(Error::Deserialization { what, .. }) if what == "error"),
            "{decoded:?}"
        );
    }

    /// Text after the value is refused, as the documented pattern ends the deserializer.
    #[test]
    fn a_custom_serializer_refuses_trailing_text() {
        let (_, format) = backwards();
        let err = decode::<u32, EngineOnly>(Some("x 7"), &format, "result").unwrap_err();
        assert!(matches!(&err, Error::Deserialization { .. }), "{err}");
    }

    #[test]
    fn a_serializer_that_fills_nothing_is_an_error_not_a_value() {
        #[derive(Debug)]
        struct Forgetful;

        impl CustomSerializer for Forgetful {
            fn name(&self) -> &str {
                "forgetful"
            }

            fn serialize(
                &self,
                _: Payload<'_>,
            ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
                Ok(String::new())
            }

            fn deserialize(
                &self,
                _: &str,
                _: PayloadSlot<'_>,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
            }
        }

        let format = Serializer::custom(Forgetful).encoding();
        let err = decode::<u32, EngineOnly>(Some("x"), &format, "result").unwrap_err();
        assert!(
            matches!(&err, Error::Deserialization { message, .. } if message.contains("forgetful")),
            "{err}"
        );
    }

    #[test]
    fn a_mismatch_names_the_half_that_failed() {
        let err = decode::<u32, EngineOnly>(
            Some(r#""not a number""#),
            &PayloadEncoding::Native,
            "result",
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Deserialization {
                    what: Cow::Borrowed("result"),
                    ..
                }
            ),
            "{err}"
        );
    }
}
