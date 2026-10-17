//! Telling a workflow input's shape from what it asks `serde` to do.
//!
//! A portable workflow input travels in the `{positionalArgs, namedArgs}` envelope, and how an
//! input goes into it depends on its Rust shape: a tuple's elements are separate arguments, a unit
//! is none, the envelope type is written as given. The JSON can't tell a tuple from a list, so
//! both directions ask `serde` instead: [`Shape::of`] serializes the input into a [`Probe`] that
//! stops at the first call, and [`takes_the_envelope`] deserializes into a [`NameProbe`] that
//! stops at the first request. Each answers by the struct name `serde` reports, so a type is
//! recognized as the envelope the same way on the way in and out.

use serde::de::DeserializeOwned;
use serde::{Deserializer, Serialize};

use super::ENVELOPE;

/// How an input goes into the portable envelope, by the first thing it asks `serde` to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    /// A tuple, or a fixed-size array, of two or more: each element is an argument.
    Arguments,
    /// The unit: no arguments.
    NoArguments,
    /// A [`PortableWorkflowArgs`](super::PortableWorkflowArgs): already the envelope.
    Envelope,
    /// Anything else: the one argument.
    OneArgument,
}

impl Shape {
    pub(super) fn of<P: Serialize + ?Sized>(input: &P) -> Shape {
        // A unit is no arguments however it is reached: `()`, `&()`, `Box<()>`, and a JSON `null`
        // too, which `serde` writes the same way. One `null` argument is `None`, which writes
        // `serialize_none` and so is one argument.
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
/// JSON both become does not, which is why this asks `serde` rather than looking at a [`Value`](serde_json::Value).
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
        Err(Probed(if name == ENVELOPE {
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

/// Whether `P` reads the input envelope whole: whether deserializing it starts by asking for
/// [`PortableWorkflowArgs`](super::PortableWorkflowArgs)'s struct, as
/// [`encode_input`](super::encode_input) recognizes one by the struct `serde`
/// is asked to write.
///
/// The same test in both directions, so what is written as the envelope is read as it: a `Box` or
/// a `#[serde(transparent)]` wrapper forwards the request to the struct inside, and is the
/// envelope both ways.
pub(super) fn takes_the_envelope<P: DeserializeOwned>() -> bool {
    matches!(P::deserialize(NameProbe), Err(Named(true)))
}

/// A deserializer that reads nothing: it answers the first request with whether it was for the
/// envelope's struct, as an error, so deserializing stops there.
struct NameProbe;

#[derive(Debug)]
struct Named(bool);

impl std::fmt::Display for Named {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Named {}

impl serde::de::Error for Named {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        Named(false)
    }
}

impl<'de> Deserializer<'de> for NameProbe {
    type Error = Named;

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        _: V,
    ) -> std::result::Result<V::Value, Named> {
        Err(Named(false))
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> std::result::Result<V::Value, Named> {
        Err(Named(name == ENVELOPE))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option
        unit unit_struct newtype_struct seq tuple tuple_struct map enum identifier ignored_any
    }
}
