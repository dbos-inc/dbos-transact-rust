//! Encoding values on their way into the system database, and back out.
//!
//! Everything the engine stores is an opaque string as far as `sysdb` is concerned, and the
//! `serialization` column on each row records which encoding produced it. That is what lets a
//! reader interpret what it finds without the two sides having agreed in advance — and it is why
//! there is no wire format to conform to here: a workflow is only ever replayed by the SDK that
//! wrote it, so this encoding is ours to choose.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Error, Result};

/// Decodes a value stored by [`encode`].
///
/// An absent value reads as JSON `null`, which is what `()` decodes from. That is what lets a
/// zero-argument workflow share one erased signature with a one-argument one: its row has no
/// input, and it must not need a literal `"null"` written into the column to run.
///
/// `what` names the value for the error message — `argument`, `result` — because "could not
/// deserialize" without saying which half is the least useful thing this could report.
pub(crate) fn decode<T: DeserializeOwned>(value: Option<&str>, what: &'static str) -> Result<T> {
    serde_json::from_str(value.unwrap_or("null"))
        .map_err(|source| Error::Deserialization { what, source })
}

/// Encodes a value for the system database.
pub(crate) fn encode<T: Serialize>(value: &T, what: &'static str) -> Result<Option<String>> {
    serde_json::to_string(value)
        .map(Some)
        .map_err(|source| Error::Serialization { what, source })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_value_decodes_as_the_unit() {
        assert_eq!(decode::<()>(None, "argument").unwrap(), ());
        assert_eq!(decode::<Option<u32>>(None, "argument").unwrap(), None);
    }

    #[test]
    fn a_value_round_trips() {
        let encoded = encode(&(1u32, "two"), "argument").unwrap();
        assert_eq!(encoded.as_deref(), Some(r#"[1,"two"]"#));
        assert_eq!(
            decode::<(u32, String)>(encoded.as_deref(), "argument").unwrap(),
            (1, "two".to_owned())
        );
    }

    #[test]
    fn a_mismatch_names_the_half_that_failed() {
        let err = decode::<u32>(Some(r#""not a number""#), "result").unwrap_err();
        assert!(
            matches!(err, Error::Deserialization { what: "result", .. }),
            "{err}"
        );
    }
}
