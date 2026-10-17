//! Non-finite floats in JSON payloads: written as names in the native encoding, refused in the
//! portable one.
//!
//! JSON has no way to write `NaN` or an infinity, and `serde_json` writes all three as `null`
//! without an error, so the value reads back as something else: an `f64` field fails to decode and
//! an `Option<f64>` becomes `None`. Checking the encoded text afterwards can't catch it, since a
//! `null` there may be a real `None`. So the check happens on the way in: [`Floats`] serializes a
//! value through a serializer that passes every call on to the real one, except for a non-finite
//! `f32` or `f64`, wherever in the value it is. What it does with one depends on the encoding:
//!
//! - **Native** (`rust_serde`) writes it as the string [`NAN`], [`INFINITY`] or [`NEG_INFINITY`].
//!   Only this SDK reads native payloads, so it can choose a form, and [`from_str`] reads the three
//!   strings back as floats wherever a float is asked for.
//! - **Portable** (`portable_json`) refuses it. Every SDK reads portable payloads, and there is no
//!   form for these values that they agree on, so writing one would be read differently, or not at
//!   all, elsewhere. The same goes for anything converted to a [`serde_json::Value`] before it is
//!   encoded, such as a portable error's fields or a schedule's context: a `Value` can't hold a
//!   non-finite float either.
//!
//! **How a float request is answered.** `serde_json` refuses a string where a float is asked for,
//! so [`Lenient`] asks for whatever is there instead (`deserialize_any`) and decides itself. That
//! changes three things beyond reading the names:
//!
//! - A map key is always a string in JSON, so a float key is read as the number its text spells,
//!   as `serde_json` reads one. So is any other string that spells a number where a float is
//!   asked for, which `serde_json` would refuse.
//! - With `serde_json`'s `arbitrary_precision` feature, which any crate in an application can turn
//!   on, a number arrives as a map holding its text; it is read through [`serde_json::Number`].
//! - An `f32` is parsed as an `f64` and narrowed, as `serde_json` does unless its `float_roundtrip`
//!   feature is on.
//!
//! **What doesn't read a name back as a float:** a target that buffers its input before deciding
//! what it is — an untagged, internally tagged or adjacently tagged enum, a `#[serde(flatten)]`
//! field — sees the string, not a float request, and so does a [`serde_json::Value`]. Such a type
//! reads a stored name as a decode error.

use std::fmt::{self, Display};

use serde::Deserialize;
use serde::de::{self, DeserializeOwned, DeserializeSeed, Deserializer, Visitor};
use serde::ser::{self, Serialize, Serializer};

/// How the native encoding writes `f64::NAN` (and `f32::NAN`).
pub(super) const NAN: &str = "NaN";
/// How the native encoding writes positive infinity.
pub(super) const INFINITY: &str = "Infinity";
/// How the native encoding writes negative infinity.
pub(super) const NEG_INFINITY: &str = "-Infinity";

/// What to do with a non-finite float on the way into JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonFinite {
    /// Write it as its name, which [`from_str`] reads back.
    Name,
    /// Fail the write.
    Refuse,
}

/// `value`, with every non-finite float in it handled as `mode` says when serialized.
pub(crate) struct Floats<'a, T: ?Sized> {
    value: &'a T,
    mode: NonFinite,
}

impl<'a, T: ?Sized> Floats<'a, T> {
    pub(crate) fn new(value: &'a T, mode: NonFinite) -> Self {
        Self { value, mode }
    }
}

impl<T: Serialize + ?Sized> Serialize for Floats<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(Checked {
            inner: serializer,
            mode: self.mode,
        })
    }
}

/// Decodes native JSON, reading [`NAN`], [`INFINITY`] and [`NEG_INFINITY`] as floats wherever the
/// target asks for one.
pub(super) fn from_str<T: DeserializeOwned>(text: &str) -> serde_json::Result<T> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = T::deserialize(Lenient(&mut deserializer))?;
    deserializer.end()?;
    Ok(value)
}

/// The float a name stands for, if it is one of the three.
fn named(name: &str) -> Option<f64> {
    match name {
        NAN => Some(f64::NAN),
        INFINITY => Some(f64::INFINITY),
        NEG_INFINITY => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// The name a non-finite float is written as.
fn name_of(v: f64) -> &'static str {
    if v.is_nan() {
        NAN
    } else if v > 0.0 {
        INFINITY
    } else {
        NEG_INFINITY
    }
}

// ---------------------------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------------------------

/// A serializer, or one of its compound serializers, that handles non-finite floats and wraps
/// nested values in [`Floats`] in turn.
struct Checked<S> {
    inner: S,
    mode: NonFinite,
}

impl<S> Checked<S> {
    fn nested<'a, T: ?Sized>(&self, value: &'a T) -> Floats<'a, T> {
        Floats::new(value, self.mode)
    }
}

fn refuse<E: ser::Error>(value: impl Display) -> E {
    E::custom(format_args!(
        "{value} is not a finite number, and portable JSON has no way to store one"
    ))
}

macro_rules! pass {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, v: $ty) -> Result<S::Ok, S::Error> {
            self.inner.$method(v)
        })*
    };
}

impl<S: Serializer> Checked<S> {
    fn float(
        self,
        v: f64,
        finite: impl FnOnce(S) -> Result<S::Ok, S::Error>,
    ) -> Result<S::Ok, S::Error> {
        if v.is_finite() {
            return finite(self.inner);
        }
        match self.mode {
            NonFinite::Name => self.inner.serialize_str(name_of(v)),
            NonFinite::Refuse => Err(refuse(v)),
        }
    }
}

impl<S: Serializer> Serializer for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Checked<S::SerializeSeq>;
    type SerializeTuple = Checked<S::SerializeTuple>;
    type SerializeTupleStruct = Checked<S::SerializeTupleStruct>;
    type SerializeTupleVariant = Checked<S::SerializeTupleVariant>;
    type SerializeMap = Checked<S::SerializeMap>;
    type SerializeStruct = Checked<S::SerializeStruct>;
    type SerializeStructVariant = Checked<S::SerializeStructVariant>;

    pass!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
        serialize_unit_struct(&'static str),
    );

    fn serialize_f32(self, v: f32) -> Result<S::Ok, S::Error> {
        self.float(f64::from(v), |inner| inner.serialize_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<S::Ok, S::Error> {
        self.float(v, |inner| inner.serialize_f64(v))
    }

    fn serialize_none(self) -> Result<S::Ok, S::Error> {
        self.inner.serialize_none()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<S::Ok, S::Error> {
        let value = self.nested(value);
        self.inner.serialize_some(&value)
    }

    fn serialize_unit(self) -> Result<S::Ok, S::Error> {
        self.inner.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<S::Ok, S::Error> {
        self.inner.serialize_unit_variant(name, index, variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        let value = self.nested(value);
        self.inner.serialize_newtype_struct(name, &value)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        let value = self.nested(value);
        self.inner
            .serialize_newtype_variant(name, index, variant, &value)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_seq(len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_tuple(len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_tuple_struct(name, len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_tuple_variant(name, index, variant, len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_map(len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_struct(name, len)
            .map(|inner| Checked { inner, mode })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, S::Error> {
        let mode = self.mode;
        self.inner
            .serialize_struct_variant(name, index, variant, len)
            .map(|inner| Checked { inner, mode })
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

impl<S: ser::SerializeSeq> ser::SerializeSeq for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_element(&value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeTuple> ser::SerializeTuple for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_element(&value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeTupleStruct> ser::SerializeTupleStruct for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_field(&value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeTupleVariant> ser::SerializeTupleVariant for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_field(&value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeMap> ser::SerializeMap for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), S::Error> {
        let key = self.nested(key);
        self.inner.serialize_key(&key)
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_value(&value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeStruct> ser::SerializeStruct for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_field(key, &value)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.inner.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: ser::SerializeStructVariant> ser::SerializeStructVariant for Checked<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        let value = self.nested(value);
        self.inner.serialize_field(key, &value)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.inner.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

// ---------------------------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------------------------

/// A deserializer that passes every request on to `D`, except that a request for a float also
/// accepts the three names. Everything it hands a visitor is wrapped the same way, so the rule
/// holds at any depth.
struct Lenient<D>(D);

/// A visitor whose nested deserializers and accessors are wrapped in [`Lenient`] and its kin.
struct Wrap<V>(V);

/// The visitor a float request is answered through: a name becomes its float, and anything else
/// goes to the float's own visitor, which accepts or refuses it as it would have.
struct FloatVisitor<V> {
    inner: V,
    asked_for: FloatType,
}

/// Which float type a request asked for, so a name is handed on as that type.
#[derive(Debug, Clone, Copy)]
enum FloatType {
    F32,
    F64,
}

/// A seed whose deserializer is wrapped in [`Lenient`].
struct Seed<S>(S);

/// A sequence, map, enum or variant accessor whose values are wrapped in [`Lenient`].
struct Access<A>(A);

macro_rules! forward_requests {
    ($($method:ident),* $(,)?) => {
        $(fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
            self.0.$method(Wrap(visitor))
        })*
    };
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for Lenient<D> {
    type Error = D::Error;

    forward_requests!(
        deserialize_any,
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any,
    );

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_any(FloatVisitor {
            inner: visitor,
            asked_for: FloatType::F32,
        })
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_any(FloatVisitor {
            inner: visitor,
            asked_for: FloatType::F64,
        })
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_unit_struct(name, Wrap(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_newtype_struct(name, Wrap(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_tuple(len, Wrap(visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_tuple_struct(name, len, Wrap(visitor))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_struct(name, fields, Wrap(visitor))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_enum(name, variants, Wrap(visitor))
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

macro_rules! forward_visits {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method<E: de::Error>(self, v: $ty) -> Result<Self::Value, E> {
            self.0.$method(v)
        })*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Wrap<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    forward_visits!(
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_borrowed_str(&'de str),
        visit_string(String),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>),
    );

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.0.visit_none()
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.0.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.0.visit_some(Lenient(deserializer))
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.0.visit_newtype_struct(Lenient(deserializer))
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.0.visit_seq(Access(seq))
    }

    fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        self.0.visit_map(Access(map))
    }

    fn visit_enum<A: de::EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        self.0.visit_enum(Access(data))
    }
}

impl<V> FloatVisitor<V> {
    /// Hands `float` on as the type that was asked for.
    fn float<'de, E: de::Error>(self, float: f64) -> Result<V::Value, E>
    where
        V: Visitor<'de>,
    {
        match self.asked_for {
            // Narrowed only as `serde_json` narrows an `f64` it parsed for an `f32` request.
            FloatType::F32 => self.inner.visit_f32(float as f32),
            FloatType::F64 => self.inner.visit_f64(float),
        }
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for FloatVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
    }

    /// A float that arrives as a string: one of the three names, or a map key, which JSON always
    /// quotes, so a float key is the number its text spells. Anything else goes to the float's
    /// own visitor, which refuses it.
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        if let Some(float) = named(v) {
            return self.float(float);
        }
        let spelled = match self.asked_for {
            FloatType::F32 => serde_json::from_str::<f32>(v).map(f64::from),
            FloatType::F64 => serde_json::from_str::<f64>(v),
        };
        match spelled {
            Ok(float) => self.float(float),
            Err(_) => self.inner.visit_str(v),
        }
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        self.visit_str(v)
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        self.visit_str(&v)
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        self.inner.visit_f64(v)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        self.inner.visit_u64(v)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        self.inner.visit_i64(v)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        self.inner.visit_bool(v)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(Access(seq))
    }

    /// A number that isn't an integer, when `serde_json`'s `arbitrary_precision` feature is on:
    /// `deserialize_any` then hands it over as a map holding its text, which [`serde_json::Number`]
    /// knows how to read. Feature unification can turn that feature on from any crate in the
    /// application. Without it, a map here is a real JSON object, which `Number` refuses.
    fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let number = serde_json::Number::deserialize(de::value::MapAccessDeserializer::new(map))?;
        match number.as_f64() {
            Some(float) => self.float(float),
            None => Err(de::Error::invalid_type(
                de::Unexpected::Other("an unrepresentable number"),
                &self,
            )),
        }
    }
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<S::Value, D::Error> {
        self.0.deserialize(Lenient(deserializer))
    }
}

impl<'de, A: de::SeqAccess<'de>> de::SeqAccess<'de> for Access<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        self.0.next_element_seed(Seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: de::MapAccess<'de>> de::MapAccess<'de> for Access<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        self.0.next_key_seed(Seed(seed))
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        self.0.next_value_seed(Seed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: de::EnumAccess<'de>> de::EnumAccess<'de> for Access<A> {
    type Error = A::Error;
    type Variant = Access<A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self::Variant), A::Error> {
        self.0
            .variant_seed(Seed(seed))
            .map(|(value, variant)| (value, Access(variant)))
    }
}

impl<'de, A: de::VariantAccess<'de>> de::VariantAccess<'de> for Access<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.0.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        self.0.newtype_variant_seed(Seed(seed))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        self.0.tuple_variant(len, Wrap(visitor))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        self.0.struct_variant(fields, Wrap(visitor))
    }
}
