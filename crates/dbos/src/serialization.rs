//! Encoding values on their way into the system database, and back out.
//!
//! Everything the engine stores is an opaque string as far as `sysdb` is concerned, and the
//! `serialization` column on each row records which encoding produced it. That is what lets a
//! reader interpret what it finds without the two sides having agreed in advance.
//!
//! **Two encodings.** [`PayloadEncoding::Native`] (`rust_serde`) is `serde_json` with the
//! application's own derives, and is read only by this SDK. [`PayloadEncoding::Portable`]
//! (`portable_json`) is the encoding every DBOS SDK reads and writes: the same JSON for a plain
//! value, but a workflow input travels in a `{"positionalArgs", "namedArgs"}` envelope and an error
//! in a `{"name", "message", "code", "data"}` one, because those are the shapes a workflow or an
//! exception has in a language with neither a single typed argument nor a typed error.
//!
//! **Every read honours the row's tag**, whatever the reader would itself have written. A tag
//! this process cannot read — another SDK's native encoding — is [`Error::UnknownSerialization`],
//! never a guess.

use std::any::TypeId;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::error::{DurableError, EngineOnly, Error, Result};

/// The tag a [`PayloadEncoding::Native`] row carries.
pub(crate) const RUST_SERDE: &str = "rust_serde";

/// The tag a [`PayloadEncoding::Portable`] row carries, the one every DBOS SDK reads.
pub(crate) const PORTABLE_JSON: &str = "portable_json";

/// How payloads are encoded on their way into the system database, by default.
///
/// This is what a workflow writes unless it, or the call, asks for [`Serialization::Portable`] or
/// [`Serialization::Native`], and what a step's result and the engine's record of a checkpointed
/// call are written in. A workflow is only ever replayed by the SDK that wrote it — workflows
/// cross languages by enqueue, not by execution — so the default is this application's own to
/// choose. What matters is that the choice is *recorded* in the `serialization` column of every
/// row, so a later reader knows how to read what it finds.
///
/// The built-in encodings, `rust_serde` and `portable_json`, are always readable, whichever this
/// is.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Serializer {
    /// `serde_json` with this crate's own derives, written as `rust_serde`.
    #[default]
    RustSerde,
}

impl Serializer {
    /// The name written to the `serialization` column.
    pub fn name(&self) -> &str {
        match self {
            Serializer::RustSerde => RUST_SERDE,
        }
    }

    /// The encoding this writes in.
    pub(crate) fn encoding(&self) -> PayloadEncoding {
        match self {
            Serializer::RustSerde => PayloadEncoding::Native,
        }
    }
}

/// Which encoding a workflow, an enqueue, or a single write uses.
///
/// **[`Portable`](Self::Portable) is for crossing languages.** It writes `portable_json`, the
/// encoding every DBOS SDK reads, so a workflow another language enqueues, or a message, event or
/// stream value another language reads, should be portable. [`Native`](Self::Native) writes this
/// SDK's own `rust_serde`, whatever [`Serializer`] is configured.
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
pub enum Serialization {
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
/// What a caller *asks for* is a [`Serialization`], which may defer to its context; this is what
/// that request resolves to, and what a row says it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum PayloadEncoding {
    /// `rust_serde`: this SDK's own encoding.
    #[default]
    Native,
    /// `portable_json`: readable by every DBOS SDK.
    Portable,
}

impl PayloadEncoding {
    /// The tag written to the `serialization` column.
    pub(crate) fn name(&self) -> &str {
        match self {
            PayloadEncoding::Native => RUST_SERDE,
            PayloadEncoding::Portable => PORTABLE_JSON,
        }
    }

    /// The encoding a row's tag names.
    ///
    /// The built-in names are always readable, and no other is. An absent or empty tag is this
    /// SDK's own: a row with no tag was written before tags were written, by this SDK, since a
    /// workflow is replayed only by the SDK that wrote it.
    pub(crate) fn of<E>(tag: Option<&str>) -> Result<PayloadEncoding, E> {
        match tag {
            None | Some("") | Some(RUST_SERDE) => Ok(PayloadEncoding::Native),
            Some(PORTABLE_JSON) => Ok(PayloadEncoding::Portable),
            Some(other) => Err(Error::UnknownSerialization {
                name: other.to_owned(),
            }),
        }
    }

    /// What a request resolves to, given the encoding it defers to.
    pub(crate) fn resolve(requested: Serialization, inherited: PayloadEncoding) -> PayloadEncoding {
        match requested {
            Serialization::Default => inherited,
            Serialization::Portable => PayloadEncoding::Portable,
            Serialization::Native => PayloadEncoding::Native,
        }
    }

    /// What a request resolves to where the encoding it defers to is the ambient workflow's.
    ///
    /// Inside a workflow — its body or one of its steps — a [`Serialization::Default`] takes that
    /// workflow's encoding, so a portable workflow's events, messages and stream values are
    /// portable too. Outside one there is nothing to inherit, and it is the `configured` default.
    pub(crate) fn resolve_ambient(
        requested: Serialization,
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
    match encoding {
        PayloadEncoding::Native | PayloadEncoding::Portable => {
            serde_json::from_str(value.unwrap_or("null"))
                .map_err(|source| deserialization(what, source))
        }
    }
}

/// [`decode`], for a value whose row says how it is encoded.
pub(crate) fn decode_tagged<T: DeserializeOwned, E>(
    value: Option<&str>,
    tag: Option<&str>,
    what: &'static str,
) -> Result<T, E> {
    decode(value, &PayloadEncoding::of(tag)?, what)
}

/// Encodes a value for the system database, in `encoding`.
///
/// Always a string, never an absence — a value that encodes to nothing does not exist, and `()`
/// encodes to `"null"` rather than to no value at all. Nullability belongs to the column, so a
/// caller writing into a nullable one wraps this in `Some` at that point.
///
/// Native and portable are the same JSON for a plain value: what they change is the envelope
/// around a workflow's input or error, which [`encode_input`] and [`encode_error`] add.
pub(crate) fn encode<T: Serialize, E>(
    value: &T,
    encoding: &PayloadEncoding,
    what: &'static str,
) -> Result<String, E> {
    match encoding {
        PayloadEncoding::Native | PayloadEncoding::Portable => serde_json::to_string(value)
            .map_err(|source| Error::Serialization {
                what: what.into(),
                message: source.to_string(),
                source: Some(source),
            }),
    }
}

fn deserialization<E>(what: &'static str, source: serde_json::Error) -> Error<E> {
    Error::Deserialization {
        what: what.into(),
        message: source.to_string(),
        source: Some(source),
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
/// Native, it is the input alone. Portable, it is the envelope, and the input's shape
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

/// How an input goes into the portable envelope, by the first thing it asks `serde` to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A tuple, or a fixed-size array, of two or more: each element is an argument.
    Arguments,
    /// The unit: no arguments.
    NoArguments,
    /// A [`PortableWorkflowArgs`]: already the envelope.
    Envelope,
    /// Anything else: the one argument.
    OneArgument,
}

impl Shape {
    fn of<P: Serialize + ?Sized>(input: &P) -> Shape {
        match input.serialize(Probe) {
            Err(Probed(shape)) => shape,
            Ok(()) => Shape::OneArgument,
        }
    }
}

/// A serializer that writes nothing: it answers the input's first call with the [`Shape`] that
/// call implies, as an error, so serializing stops there.
///
/// `serde` tells a tuple from a sequence — `serialize_tuple` against `serialize_seq` — where the
/// JSON both become does not, which is why this asks `serde` rather than looking at a [`Value`].
struct Probe;

#[derive(Debug)]
struct Probed(Shape);

impl std::fmt::Display for Probed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::error::Error for Probed {}

impl serde::ser::Error for Probed {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        Probed(Shape::OneArgument)
    }
}

/// Every call but the three that matter is one argument.
macro_rules! one_argument {
    ($($method:ident($($arg:ty),*);)*) => {
        $(fn $method(self, $(_: $arg),*) -> std::result::Result<(), Probed> {
            Err(Probed(Shape::OneArgument))
        })*
    };
}

impl serde::Serializer for Probe {
    type Ok = ();
    type Error = Probed;
    type SerializeSeq = serde::ser::Impossible<(), Probed>;
    type SerializeTuple = serde::ser::Impossible<(), Probed>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Probed>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Probed>;
    type SerializeMap = serde::ser::Impossible<(), Probed>;
    type SerializeStruct = serde::ser::Impossible<(), Probed>;
    type SerializeStructVariant = serde::ser::Impossible<(), Probed>;

    one_argument! {
        serialize_bool(bool);
        serialize_i8(i8);
        serialize_i16(i16);
        serialize_i32(i32);
        serialize_i64(i64);
        serialize_i128(i128);
        serialize_u8(u8);
        serialize_u16(u16);
        serialize_u32(u32);
        serialize_u64(u64);
        serialize_u128(u128);
        serialize_f32(f32);
        serialize_f64(f64);
        serialize_char(char);
        serialize_str(&str);
        serialize_bytes(&[u8]);
        serialize_none();
        serialize_unit_struct(&'static str);
        serialize_unit_variant(&'static str, u32, &'static str);
    }

    fn serialize_unit(self) -> std::result::Result<(), Probed> {
        Err(Probed(Shape::NoArguments))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> std::result::Result<(), Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> std::result::Result<(), Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> std::result::Result<(), Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_seq(self, _: Option<usize>) -> std::result::Result<Self::SerializeSeq, Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Probed> {
        // One element stays one argument, so `(x,)` reads back as itself rather than as `x`.
        Err(Probed(if len >= 2 {
            Shape::Arguments
        } else {
            Shape::OneArgument
        }))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_map(self, _: Option<usize>) -> std::result::Result<Self::SerializeMap, Probed> {
        Err(Probed(Shape::OneArgument))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _: usize,
    ) -> std::result::Result<Self::SerializeStruct, Probed> {
        Err(Probed(if name == "dbos::PortableWorkflowArgs" {
            Shape::Envelope
        } else {
            Shape::OneArgument
        }))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Probed> {
        Err(Probed(Shape::OneArgument))
    }
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
/// A workflow whose input type is [`PortableWorkflowArgs`] receives the envelope itself. A
/// one-element tuple is the one input type the table reads ambiguously: one positional argument
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
    let args: Option<PortableWorkflowArgs> = decode(value, encoding, WHAT)?;
    let args = args.unwrap_or_default();
    if TypeId::of::<P>() == TypeId::of::<PortableWorkflowArgs>() {
        let whole: Box<dyn std::any::Any> = Box::new(args);
        return Ok(*whole
            .downcast::<P>()
            .expect("P is PortableWorkflowArgs, by the check above"));
    }
    let PortableWorkflowArgs {
        mut positional_args,
        named_args,
    } = args;
    let from = match (positional_args.len(), named_args.is_empty()) {
        (0, true) => Value::Null,
        (1, true) => positional_args.remove(0),
        (_, true) => Value::Array(positional_args),
        (0, false) => Value::Object(named_args),
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
    serde_json::from_value(from).map_err(|source| deserialization(WHAT, source))
}

/// A workflow's error, as every DBOS SDK writes it portably.
///
/// All four fields are always written, `null` where there is nothing to say. On the way in, a
/// missing `name` or `message` reads as empty, since not every SDK writes both.
#[derive(Debug, Serialize, Deserialize)]
struct PortableError {
    #[serde(default, deserialize_with = "null_as_default")]
    name: String,
    #[serde(default, deserialize_with = "null_as_default")]
    message: String,
    #[serde(default)]
    code: Option<Value>,
    #[serde(default)]
    data: Option<Value>,
}

/// Encodes a workflow's error in `encoding`.
///
/// Native, it is the whole [`Error`], application variant and all, which reads back as
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
        PayloadEncoding::Portable => encode(&to_portable(error), encoding, "error"),
        PayloadEncoding::Native => encode(error, encoding, "error"),
    }
}

fn to_portable<E: DurableError>(error: &Error<E>) -> PortableError {
    match error {
        Error::Application(application) => PortableError {
            name: type_name::<E>().to_owned(),
            message: application.to_string(),
            code: None,
            data: present(serde_json::to_value(application).ok()),
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
            data: data.clone(),
        },
        engine => {
            let (name, data) = variant(engine);
            PortableError {
                name,
                message: engine.to_string(),
                code: None,
                data: present(data),
            }
        }
    }
}

/// `None` for a JSON `null`, so an error with nothing in it writes `"data":null` either way.
fn present(data: Option<Value>) -> Option<Value> {
    data.filter(|data| !data.is_null())
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
fn variant<E: DurableError>(error: &Error<E>) -> (String, Option<Value>) {
    let (name, fields) = match serde_json::to_value(error) {
        Ok(Value::String(unit)) => (unit, None),
        Ok(Value::Object(tagged)) if tagged.len() == 1 => {
            let (name, fields) = tagged.into_iter().next().expect("one entry");
            (name, Some(fields))
        }
        _ => ("Error".to_owned(), None),
    };
    (format!("{DBOS_ERRORS}{name}"), fields)
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
fn rebuild<E: DurableError>(name: &str, data: Option<&Value>) -> Option<Error<E>> {
    let data = data.cloned().unwrap_or(Value::Null);
    if name == type_name::<E>()
        && let Ok(application) = serde_json::from_value::<E>(data.clone())
    {
        return Some(Error::Application(application));
    }
    let variant = name.strip_prefix(DBOS_ERRORS)?;
    let tagged = match data {
        Value::Null => Value::String(variant.to_owned()),
        fields => Value::Object([(variant.to_owned(), fields)].into_iter().collect()),
    };
    serde_json::from_value(tagged).ok()
}

/// Decodes a workflow's error, stored in `encoding`, or `None` if it is not one.
///
/// A native one is the [`Error`] it was written as. A portable one is the error it was
/// written from where the envelope says what that was and the reader's types can hold it — see
/// [`encode_error`] — and [`Error::Portable`] otherwise, which is how another language's error
/// arrives. `None` is for the caller to report — what it reports depends on whose error it was.
pub(crate) fn decode_error<E: DurableError>(
    encoded: &str,
    encoding: &PayloadEncoding,
) -> Option<Error<E>> {
    match encoding {
        PayloadEncoding::Native => {
            decode::<Error<E>, EngineOnly>(Some(encoded), encoding, "error").ok()
        }
        PayloadEncoding::Portable => {
            let PortableError {
                name,
                message,
                code,
                data,
            } = serde_json::from_str(encoded).ok()?;
            if let Some(rebuilt) = rebuild(&name, data.as_ref()) {
                return Some(rebuilt);
            }
            Some(Error::Portable {
                name,
                message,
                code,
                data,
            })
        }
    }
}

/// A workflow's result as its row holds it, with the encoding that says how to read it.
///
/// The encoding travels with the bytes, as it does with an
/// [`EncodedError`](crate::error::EncodedError), so a reader decodes the result as it was written
/// rather than as it would itself have written it.
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
                PayloadEncoding::of::<EngineOnly>(tag).unwrap(),
                PayloadEncoding::Native,
                "{tag:?}"
            );
        }
        assert_eq!(
            PayloadEncoding::of::<EngineOnly>(Some("portable_json")).unwrap(),
            PayloadEncoding::Portable
        );
        let err = PayloadEncoding::of::<EngineOnly>(Some("py_pickle")).unwrap_err();
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

    /// What `serde_json` does with the portable value rules, which this crate documents rather than
    /// enforces: an integer key becomes a string, any other non-string key is refused, and a
    /// non-finite float is `null`.
    #[test]
    fn values_follow_serde_json() {
        let map = std::collections::BTreeMap::from([(1u32, "one")]);
        assert_eq!(
            encode::<_, EngineOnly>(&map, &PayloadEncoding::Native, "value").unwrap(),
            r#"{"1":"one"}"#
        );
        let tuple_keys = std::collections::BTreeMap::from([((1u32, 2u32), "pair")]);
        assert!(encode::<_, EngineOnly>(&tuple_keys, &PayloadEncoding::Native, "value").is_err());
        assert_eq!(
            encode::<_, EngineOnly>(&f64::NAN, &PayloadEncoding::Native, "value").unwrap(),
            "null"
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
