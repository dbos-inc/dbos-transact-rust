//! Encoding values on their way into the system database, and back out.
//!
//! Everything the engine stores is an opaque string as far as `sysdb` is concerned, and the
//! `serialization` column on each row records which encoding produced it. That is what lets a
//! reader interpret what it finds without the two sides having agreed in advance.
//!
//! **Every read honours the row's tag**, whatever the reader would itself have written. A tag
//! this process cannot read — another SDK's native encoding — is [`Error::UnknownSerialization`],
//! never a guess.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// The tag a [`PayloadEncoding::Native`] row carries.
pub(crate) const RUST_SERDE: &str = "rust_serde";

/// How payloads are encoded on their way into the system database.
///
/// A workflow is only ever replayed by the SDK that wrote it — workflows cross languages by
/// enqueue, not by execution — so the encoding is this application's own to choose. What matters
/// is that the choice is *recorded* in the `serialization` column of every row, so a later reader
/// knows how to read what it finds.
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

/// How one payload is encoded, as its row's `serialization` column records it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum PayloadEncoding {
    /// `rust_serde`: this SDK's own encoding.
    #[default]
    Native,
}

impl PayloadEncoding {
    /// The tag written to the `serialization` column.
    pub(crate) fn name(&self) -> &str {
        match self {
            PayloadEncoding::Native => RUST_SERDE,
        }
    }

    /// The encoding a row's tag names.
    ///
    /// An absent or empty tag is this SDK's own: a row with no tag was written before tags were
    /// written, by this SDK, since a workflow is replayed only by the SDK that wrote it.
    pub(crate) fn of<E>(tag: Option<&str>) -> Result<PayloadEncoding, E> {
        match tag {
            None | Some("") | Some(RUST_SERDE) => Ok(PayloadEncoding::Native),
            Some(other) => Err(Error::UnknownSerialization {
                name: other.to_owned(),
            }),
        }
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
        PayloadEncoding::Native => serde_json::from_str(value.unwrap_or("null"))
            .map_err(|source| Error::decode_failed(what, source)),
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
pub(crate) fn encode<T: Serialize, E>(
    value: &T,
    encoding: &PayloadEncoding,
    what: &'static str,
) -> Result<String, E> {
    match encoding {
        PayloadEncoding::Native => {
            serde_json::to_string(value).map_err(|source| Error::encode_failed(what, source))
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
        let err = PayloadEncoding::of::<EngineOnly>(Some("py_pickle")).unwrap_err();
        assert!(
            matches!(&err, Error::UnknownSerialization { name } if name == "py_pickle"),
            "{err}"
        );
        assert_eq!(err.to_string(), "serialization py_pickle is not available");
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
