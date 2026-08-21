//! Encoding values on their way into the system database, and back out.
//!
//! Everything the engine stores is an opaque string as far as `sysdb` is concerned, and the
//! `serialization` column on each row records which encoding produced it. That is what lets a
//! reader interpret what it finds without the two sides having agreed in advance — and it is why
//! there is no wire format to conform to here: a workflow is only ever replayed by the SDK that
//! wrote it, so this encoding is ours to choose.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// Decodes a value stored by [`encode`].
///
/// An absent value reads as JSON `null`, which is what `()` decodes from. That is what lets a
/// zero-argument workflow share one erased signature with a one-argument one: its row has no
/// input, and it must not need a literal `"null"` written into the column to run.
///
/// `what` names the value for the error message — `argument`, `result`, `error` — because "could
/// not deserialize" without saying which half is the least useful thing this could report.
///
/// Generic over the caller's error channel rather than returning the engine's own, so a workflow
/// with its own error type needs no conversion at the call site: the failure is an engine variant
/// either way, and `E` only says which channel it travels in.
pub(crate) fn decode<T: DeserializeOwned, E>(
    value: Option<&str>,
    what: &'static str,
) -> Result<T, E> {
    serde_json::from_str(value.unwrap_or("null")).map_err(|source| Error::Deserialization {
        what: what.into(),
        message: source.to_string(),
        source: Some(source),
    })
}

/// Encodes a value for the system database.
///
/// Always a string, never an absence — a value that encodes to nothing does not exist, and `()`
/// encodes to `"null"` rather than to no value at all. Nullability belongs to the column, so a
/// caller writing into a nullable one wraps this in `Some` at that point.
pub(crate) fn encode<T: Serialize, E>(value: &T, what: &'static str) -> Result<String, E> {
    serde_json::to_string(value).map_err(|source| Error::Serialization {
        what: what.into(),
        message: source.to_string(),
        source: Some(source),
    })
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::error::EngineOnly;

    #[test]
    fn an_absent_value_decodes_as_the_unit() {
        assert_eq!(decode::<(), EngineOnly>(None, "argument").unwrap(), ());
        assert_eq!(
            decode::<Option<u32>, EngineOnly>(None, "argument").unwrap(),
            None
        );
    }

    #[test]
    fn a_value_round_trips() {
        let encoded = encode::<_, EngineOnly>(&(1u32, "two"), "argument").unwrap();
        assert_eq!(encoded, r#"[1,"two"]"#);
        assert_eq!(
            decode::<(u32, String), EngineOnly>(Some(&encoded), "argument").unwrap(),
            (1, "two".to_owned())
        );
    }

    #[test]
    fn the_unit_encodes_as_a_value_rather_than_an_absence() {
        assert_eq!(encode::<_, EngineOnly>(&(), "argument").unwrap(), "null");
    }

    #[test]
    fn a_mismatch_names_the_half_that_failed() {
        let err = decode::<u32, EngineOnly>(Some(r#""not a number""#), "result").unwrap_err();
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
