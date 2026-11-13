//! A database URL as it may be printed: everything but its passwords.

/// What a password is replaced with.
const MASK: &str = "***";

/// Query parameters a PostgreSQL connection URL may carry a secret in.
const SECRET_PARAMS: &[&str] = &["password", "sslpassword"];

/// `raw` with every password in it replaced by `***`: the one in the user-info part, and the value
/// of any `password` or `sslpassword` query parameter.
///
/// The host, port, database, user and the other parameters are kept, since they are what someone
/// reading a log needs to tell where a connection went. A URL that does not parse is redacted
/// whole rather than printed: if it cannot be read, its secrets cannot be found in it either.
pub(crate) fn masked_url(raw: &str) -> String {
    if raw.is_empty() {
        return "<unset>".to_owned();
    }
    let redacted = || "<redacted>".to_owned();
    let Ok(mut url) = url::Url::parse(raw) else {
        return redacted();
    };
    if url.password().is_some() && url.set_password(Some(MASK)).is_err() {
        return redacted();
    }
    // Rewritten only when there is something to mask, so a URL with no secrets in its query keeps
    // its exact spelling.
    let has_secret = url
        .query_pairs()
        .any(|(key, _)| SECRET_PARAMS.contains(&key.as_ref()));
    if has_secret {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| {
                let value = if SECRET_PARAMS.contains(&key.as_ref()) {
                    MASK.to_owned()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_password_in_the_user_info_is_masked_and_the_rest_is_kept() {
        assert_eq!(
            masked_url("postgres://dbos:s3cret@db.example:5433/app?sslmode=require"),
            "postgres://dbos:***@db.example:5433/app?sslmode=require"
        );
    }

    #[test]
    fn password_query_parameters_are_masked() {
        let masked = masked_url(
            "postgresql://db.example/app?user=dbos&password=s3cret&sslmode=verify-full&sslpassword=k3y",
        );
        assert!(
            !masked.contains("s3cret") && !masked.contains("k3y"),
            "{masked}"
        );
        assert_eq!(
            masked,
            "postgresql://db.example/app?user=dbos&password=***&sslmode=verify-full&sslpassword=***"
        );
    }

    #[test]
    fn a_url_without_secrets_is_printed_as_given() {
        for raw in [
            "postgres://dbos@db.example/app",
            "postgres:///app?host=/var/run/postgresql",
            "postgres://localhost:5432/dbos_test?sslmode=disable&application_name=x",
        ] {
            assert_eq!(masked_url(raw), raw);
        }
    }

    #[test]
    fn an_unparseable_url_is_redacted_whole() {
        assert_eq!(
            masked_url("host=db.example password=s3cret dbname=app"),
            "<redacted>"
        );
        assert_eq!(masked_url("not a url :// s3cret"), "<redacted>");
    }

    #[test]
    fn an_empty_url_is_reported_as_unset() {
        assert_eq!(masked_url(""), "<unset>");
    }

    #[test]
    fn the_system_database_config_masks_its_url_when_printed() {
        let printed = format!(
            "{:?}",
            crate::sysdb::postgres::Config::new("postgres://dbos:s3cret@db.example/app")
        );
        assert!(!printed.contains("s3cret"), "{printed}");
        assert!(printed.contains("db.example"), "{printed}");
    }
}
