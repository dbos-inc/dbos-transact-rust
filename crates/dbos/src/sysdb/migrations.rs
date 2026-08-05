//! The system database schema: the migration SQL and the rendering that makes it executable.
//!
//! Every DBOS implementation must produce the same schema — a Rust application can share a
//! system database with one written in Python, TypeScript, Go, or Java — so the statements in
//! `migrations/` are not free to change. What has to match is the schema the database ends up
//! with, including column types; the filenames and comments are this crate's own.
//!
//! Porting a migration:
//!
//! - Add the file to `migrations/` and list it in [`SOURCES`]. The number comes from the
//!   filename.
//! - Use `INT4`, never `INTEGER`. CockroachDB reads `INTEGER` as 64-bit where PostgreSQL reads
//!   32-bit, so `INTEGER` produces a different schema per backend.
//! - Write `{{schema}}` and `{{concurrently}}` for the values filled in at runtime, and
//!   render with [`render`]. Single braces are literal, so `'{}'::JSON` and plpgsql's own
//!   `%s` format strings need no escaping.
//! - Give whole files to the driver. Splitting statements on `;` breaks the `$$`-quoted blocks
//!   in migrations 1, 14, 38, and 39.
//!
//! Applying the corpus to both backends is what verifies it; reading the SQL is not enough.

/// Migrations whose index DDL uses `CONCURRENTLY` and so cannot run inside a transaction on
/// PostgreSQL. Each takes the keyword as its first `%s` — render with [`RenderArgs::online`].
///
/// Empty on CockroachDB, which applies schema changes online regardless.
pub const ONLINE_MIGRATIONS: &[u32] = &[22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35, 37];

/// Returns whether `version` is an online migration — see [`ONLINE_MIGRATIONS`].
pub fn is_online(version: u32) -> bool {
    ONLINE_MIGRATIONS.contains(&version)
}

/// Quotes a schema name for use as a SQL identifier.
///
/// The migration files interpolate the schema *already quoted* — they contain
/// `%s."workflow_status"`, not `"%s"."workflow_status"` — so the rendered value carries its
/// own quotes. Embedded double quotes are doubled, per SQL's quoted-identifier rules.
///
/// ```
/// use dbos::sysdb::migrations::quote_identifier;
///
/// assert_eq!(quote_identifier("dbos"), r#""dbos""#);
/// assert_eq!(quote_identifier(r#"we"ird"#), r#""we""ird""#);
/// ```
pub fn quote_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// The values a migration's `{{name}}` placeholders are replaced with.
#[derive(Debug, Clone, Copy)]
pub struct Placeholders<'a> {
    /// The system schema, already quoted — the files write `{{schema}}."workflow_status"`,
    /// so the value carries its own quotes. Use [`quote_identifier`].
    pub schema: &'a str,
    /// The `CONCURRENTLY` keyword, or empty on a backend that does not want it.
    pub concurrently: &'a str,
}

/// Why a migration could not be rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderError {
    /// A `{{name}}` this renderer has no value for.
    Unknown {
        /// The placeholder name, as written.
        name: String,
        /// Byte offset of the opening brace.
        offset: usize,
    },
    /// A `{{` with no matching `}}`.
    Unterminated {
        /// Byte offset of the opening brace.
        offset: usize,
    },
}

impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenderError::Unknown { name, offset } => {
                write!(f, "unknown placeholder `{{{{{name}}}}}` at byte {offset}")
            }
            RenderError::Unterminated { offset } => {
                write!(f, "unterminated placeholder at byte {offset}")
            }
        }
    }
}

impl std::error::Error for RenderError {}

/// Replaces the `{{schema}}` and `{{concurrently}}` placeholders in a migration.
///
/// Placeholders are named rather than positional, so the meaning of a slot is visible in the
/// SQL and cannot depend on the order arguments are passed. Single braces are literal, which
/// leaves `'{}'::JSON` and plpgsql's own `%s` format strings untouched.
///
/// ```
/// use dbos::sysdb::migrations::{Placeholders, render};
///
/// let values = Placeholders { schema: r#""dbos""#, concurrently: "CONCURRENTLY" };
/// assert_eq!(
///     render(r#"CREATE TABLE {{schema}}."queues" (name TEXT)"#, values).unwrap(),
///     r#"CREATE TABLE "dbos"."queues" (name TEXT)"#,
/// );
/// assert_eq!(
///     render(r#"CREATE INDEX {{concurrently}} ON {{schema}}."t" ("x")"#, values).unwrap(),
///     r#"CREATE INDEX CONCURRENTLY ON "dbos"."t" ("x")"#,
/// );
/// ```
pub fn render(sql: &str, values: Placeholders<'_>) -> Result<String, RenderError> {
    let mut out = String::with_capacity(sql.len() + 64);
    let mut rest = sql;
    let mut consumed = 0usize;

    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            return Err(RenderError::Unterminated {
                offset: consumed + open,
            });
        };
        let name = &after[..close];
        let value = match name {
            "schema" => values.schema,
            "concurrently" => values.concurrently,
            _ => {
                return Err(RenderError::Unknown {
                    name: name.to_owned(),
                    offset: consumed + open,
                });
            }
        };
        out.push_str(value);
        consumed += open + 2 + close + 2;
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// A migration file as copied from upstream: its number, its filename, and its contents.
///
/// Numbers are not unique across the corpus — several migrations ship variant files for
/// CockroachDB or for LISTEN/NOTIFY, and those share the number of the migration they vary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationSource {
    /// The migration number, taken from the filename. **Never from the header comment**,
    /// which is wrong for 22 through 35.
    pub version: u32,
    /// The filename, which is what distinguishes variants sharing a version.
    pub name: &'static str,
    /// The file's verbatim contents, still carrying its `%s` slots.
    pub sql: &'static str,
}

macro_rules! sources {
    ($(($version:expr, $file:literal)),* $(,)?) => {
        /// Every migration file in the corpus, in filename order.
        ///
        /// Assembling these into the ordered list a runner applies — including the variant
        /// selection and the padding to the shared numbering base — is a separate concern.
        pub const SOURCES: &[MigrationSource] = &[
            $(MigrationSource {
                version: $version,
                name: $file,
                sql: include_str!(concat!("../../migrations/", $file)),
            }),*
        ];
    };
}

sources![
    (1, "1_initial_dbos_schema.sql"),
    (1, "1_initial_dbos_schema_listen_notify.sql"),
    (2, "2_add_queue_partition_key.sql"),
    (3, "3_add_workflow_status_index.sql"),
    (4, "4_add_forked_from.sql"),
    (5, "5_add_step_timestamps.sql"),
    (6, "6_add_workflow_events_history.sql"),
    (7, "7_add_owner_xid.sql"),
    (8, "8_add_parent_workflow_id.sql"),
    (9, "9_add_workflow_schedules.sql"),
    (10, "10_add_notifications_pkey.sql"),
    (11, "11_add_serialization_columns.sql"),
    (12, "12_add_notifications_consumed.sql"),
    (13, "13_add_application_versions.sql"),
    (14, "14_add_pgsql_client_functions.sql"),
    (15, "15_add_workflow_schedule_columns.sql"),
    (16, "16_add_delay_until.sql"),
    (17, "17_add_workflow_schedule_queue_name.sql"),
    (18, "18_add_was_forked_from.sql"),
    (19, "19_add_operation_outputs_completed_at_index.sql"),
    (20, "20_set_function_search_path.sql"),
    (20, "20_set_notify_function_search_path.sql"),
    (21, "21_create_queues_table.sql"),
    (22, "22_drop_forked_from_index.sql"),
    (23, "23_create_partial_forked_from_index.sql"),
    (24, "24_drop_parent_workflow_id_index.sql"),
    (25, "25_create_partial_parent_workflow_id_index.sql"),
    (26, "26_drop_executor_id_index.sql"),
    (27, "27_create_partial_dedup_id_index.sql"),
    (28, "28_drop_dedup_id_constraint.sql"),
    (28, "28_drop_dedup_id_constraint_cockroach.sql"),
    (29, "29_create_pending_index.sql"),
    (30, "30_create_failed_index.sql"),
    (31, "31_drop_status_index.sql"),
    (32, "32_create_in_flight_index.sql"),
    (33, "33_add_rate_limited.sql"),
    (34, "34_create_rate_limited_index.sql"),
    (35, "35_drop_queue_status_started_index.sql"),
    (36, "36_add_completed_at.sql"),
    (37, "37_create_started_at_index.sql"),
    (38, "38_update_enqueue_workflow.sql"),
    (38, "38_set_enqueue_workflow_search_path.sql"),
    (39, "39_create_streams_trigger.sql"),
    (40, "40_add_attributes.sql"),
    (41, "41_add_schedule_name.sql"),
    (42, "42_add_debounce_columns.sql"),
];

/// Asks whether the `notifications` primary key already exists, so migration 10 can skip its
/// `ALTER`. Kept out of [`SOURCES`] because it is a query, not a migration.
///
/// Takes the schema as a bind parameter (`$1`), so it is executed rather than rendered.
///
/// The `ALTER` it guards backfills a key that only databases created by very old versions
/// lack; migration 1 has created it inline for a long time, so in practice the guard always
/// skips.
pub const MIGRATION_10_PK_PROBE: MigrationSource = MigrationSource {
    version: 10,
    name: "10_check_notifications_pkey.sql",
    sql: include_str!("../../migrations/10_check_notifications_pkey.sql"),
};

/// Looks up a migration file by name.
pub fn source(name: &str) -> Option<&'static MigrationSource> {
    SOURCES.iter().find(|s| s.name == name)
}

/// Which SQL dialect the system database speaks.
///
/// Both are v1 backends. This lives here for now and moves to its own module when the
/// dialect trait proper arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Cockroach,
}

impl Dialect {
    /// The `CONCURRENTLY` keyword, or empty where it does not apply.
    ///
    /// CockroachDB applies schema changes online regardless, so it renders empty and the
    /// migration runs on the ordinary transactional path.
    fn concurrently(self) -> &'static str {
        match self {
            Dialect::Postgres => "CONCURRENTLY",
            Dialect::Cockroach => "",
        }
    }

    fn is_cockroach(self) -> bool {
        matches!(self, Dialect::Cockroach)
    }
}

/// One migration, rendered and ready to apply.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Position in the sequence, and what gets recorded once applied.
    pub version: u32,
    /// The SQL to run. **Empty is meaningful**: several migrations are no-ops on one
    /// dialect or configuration, and still consume their version number.
    pub sql: String,
    /// Whether this migration contains `CONCURRENTLY` index DDL and therefore cannot run
    /// inside a transaction. Never true on CockroachDB.
    pub online: bool,
    /// A query to run first; when it returns a row, `sql` is skipped.
    ///
    /// Only migration 10 has one. It is executed with the **unquoted** schema as a bind
    /// parameter, not rendered.
    pub guard: Option<&'static str>,
}

fn file(name: &str) -> &'static str {
    source(name)
        .unwrap_or_else(|| panic!("migration file {name} is missing from the corpus"))
        .sql
}

/// Assembles the ordered migration list for a dialect and configuration.
///
/// Six migrations vary, and the variance lives here rather than in the files:
///
/// - **1** gains its LISTEN/NOTIFY half only when notifications are on and supported.
/// - **10** carries a guard ([`MIGRATION_10_PK_PROBE`]) instead of being conditional.
/// - **20** hardens `search_path` on the functions that exist: the client functions always,
///   the trigger functions only if migration 1 installed them. Empty on CockroachDB, which
///   has no `ALTER FUNCTION ... SET`.
/// - **28** drops a constraint PostgreSQL exposes as a constraint and CockroachDB as an index.
/// - **38** appends a PostgreSQL-only `search_path` tail.
/// - **39** installs a trigger, so it follows the same gate as 1's notify half.
///
/// A migration that does not apply renders empty rather than being dropped: versions are
/// positional, so removing one would renumber everything after it.
pub fn build_migrations(schema: &str, dialect: Dialect, use_listen_notify: bool) -> Vec<Migration> {
    let quoted = quote_identifier(schema);
    let notify = use_listen_notify && !dialect.is_cockroach();
    let values = Placeholders {
        schema: &quoted,
        concurrently: dialect.concurrently(),
    };
    let sql_of = |name: &str| render(file(name), values).expect("corpus renders");

    (1..=42)
        .map(|version| {
            let mut guard = None;
            let sql = match version {
                1 => {
                    let mut sql = sql_of("1_initial_dbos_schema.sql");
                    if notify {
                        sql.push('\n');
                        sql.push_str(&sql_of("1_initial_dbos_schema_listen_notify.sql"));
                    }
                    sql
                }
                10 => {
                    guard = Some(MIGRATION_10_PK_PROBE.sql);
                    sql_of("10_add_notifications_pkey.sql")
                }
                20 if dialect.is_cockroach() => String::new(),
                20 => {
                    let mut sql = sql_of("20_set_function_search_path.sql");
                    if notify {
                        sql.push('\n');
                        sql.push_str(&sql_of("20_set_notify_function_search_path.sql"));
                    }
                    sql
                }
                28 if dialect.is_cockroach() => sql_of("28_drop_dedup_id_constraint_cockroach.sql"),
                38 => {
                    let mut sql = sql_of("38_update_enqueue_workflow.sql");
                    if !dialect.is_cockroach() {
                        sql.push('\n');
                        sql.push_str(&sql_of("38_set_enqueue_workflow_search_path.sql"));
                    }
                    sql
                }
                39 if !notify => String::new(),
                v => {
                    let name = SOURCES
                        .iter()
                        .find(|s| s.version == v && !s.name.contains("cockroach"))
                        .unwrap_or_else(|| panic!("no file for migration {v}"))
                        .name;
                    sql_of(name)
                }
            };
            Migration {
                version,
                sql,
                online: is_online(version) && !dialect.is_cockroach(),
                guard,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#""dbos""#;
    const VALUES: Placeholders<'_> = Placeholders {
        schema: SCHEMA,
        concurrently: "CONCURRENTLY",
    };

    #[test]
    fn corpus_matches_upstream_shape() {
        // 47 files: 46 the runner applies, plus the migration-10 probe, which is bound
        // separately so it cannot be applied by mistake.
        assert_eq!(SOURCES.len(), 46);

        let mut versions: Vec<u32> = SOURCES.iter().map(|s| s.version).collect();
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(
            versions,
            (1..=42).collect::<Vec<_>>(),
            "the corpus should cover migrations 1 through 42 with no gaps",
        );

        // Four files share a version with another: the LISTEN/NOTIFY halves of migrations 1
        // and 20, the CockroachDB form of 28, and the search-path half of 38.
        assert_eq!(
            SOURCES.len() - versions.len(),
            4,
            "expected 4 variant files"
        );
    }

    #[test]
    fn every_file_is_non_empty_and_named_for_its_version() {
        for s in SOURCES {
            assert!(!s.sql.trim().is_empty(), "{} is empty", s.name);
            let prefix = s
                .name
                .split('_')
                .next()
                .and_then(|p| p.parse::<u32>().ok())
                .unwrap_or_else(|| panic!("{} has no numeric prefix", s.name));
            assert_eq!(
                prefix, s.version,
                "{} is filed under the wrong version",
                s.name
            );
        }
    }

    /// Every header names the migration its filename says it is.
    ///
    /// Upstream's copies do not: fourteen files — the base files of 22 through 35 — name
    /// themselves exactly four lower, and three of those also cross-reference other
    /// migrations in the same stale numbering. That is a real defect rather than a
    /// convention worth preserving, so the copies here are corrected, and this test is what
    /// keeps them corrected when new files arrive.
    ///
    /// The correction is confined to comments; the SQL is untouched.
    #[test]
    fn headers_name_the_migration_their_filename_claims() {
        let mut checked = 0;
        for s in SOURCES
            .iter()
            .chain(std::iter::once(&MIGRATION_10_PK_PROBE))
        {
            let header = s.sql.lines().next().unwrap_or_default();
            let Some(claimed) = header
                .split_whitespace()
                .skip_while(|w| !w.eq_ignore_ascii_case("migration"))
                .nth(1)
                .and_then(|w| w.trim_end_matches(':').parse::<u32>().ok())
            else {
                continue;
            };
            assert_eq!(
                claimed, s.version,
                "{}: header says migration {claimed}",
                s.name,
            );
            checked += 1;
        }
        // 47 files, less migration 1's two, which open with a description rather than a
        // numbered header.
        assert_eq!(checked, 45, "expected 45 files to carry a numbered header");
    }

    #[test]
    fn online_migrations_match_the_reference_set() {
        assert_eq!(ONLINE_MIGRATIONS.len(), 13);
        assert!(is_online(22) && is_online(37));
        // The gaps are real: 28, 33, and 36 are not online.
        assert!(!is_online(28) && !is_online(33) && !is_online(36));
    }

    /// Only the online migrations carry a `{{concurrently}}` placeholder.
    ///
    /// If a synced file gained or lost one, its index DDL would render without the keyword on
    /// PostgreSQL and quietly take a table lock.
    #[test]
    fn only_online_migrations_carry_a_concurrently_slot() {
        for s in SOURCES {
            assert_eq!(
                s.sql.contains("{{concurrently}}"),
                is_online(s.version),
                "{}: concurrently placeholder disagrees with the online set",
                s.name,
            );
        }
    }

    /// Every file renders with no placeholder left behind.
    #[test]
    fn every_file_renders_completely() {
        for s in SOURCES
            .iter()
            .chain(std::iter::once(&MIGRATION_10_PK_PROBE))
        {
            let rendered = render(s.sql, VALUES)
                .unwrap_or_else(|e| panic!("{} failed to render: {e}", s.name));
            assert!(
                !rendered.contains("{{"),
                "{}: unrendered placeholder",
                s.name
            );
        }
    }

    /// plpgsql's own `%s` format strings survive untouched.
    ///
    /// Migrations 14 and 38 raise errors through `format('Workflow %s ...')`. Because `%` has
    /// no meaning to this renderer, they need no escaping and cannot be consumed by it.
    #[test]
    fn plpgsql_format_strings_pass_through() {
        for name in [
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
        ] {
            let s = source(name).unwrap();
            assert!(
                s.sql.contains("Workflow %s with queue %s"),
                "{name}: source"
            );
            let rendered = render(s.sql, VALUES).unwrap();
            assert!(
                rendered.contains("Workflow %s with queue %s"),
                "{name}: renderer consumed a plpgsql placeholder",
            );
        }
    }

    /// Single braces are literal, so JSON defaults survive.
    ///
    /// Migrations 14 and 38 declare `named_args JSON DEFAULT '{}'::JSON`.
    #[test]
    fn single_braces_are_literal() {
        assert_eq!(
            render("DEFAULT '{}'::JSON, x {{schema}}", VALUES).unwrap(),
            r#"DEFAULT '{}'::JSON, x "dbos""#,
        );
        for name in [
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
        ] {
            let rendered = render(source(name).unwrap().sql, VALUES).unwrap();
            assert!(
                rendered.contains("'{}'::JSON"),
                "{name}: JSON default mangled"
            );
        }
    }

    /// The migration-10 probe is bound separately and binds its schema rather than
    /// interpolating it.
    ///
    /// Keeping it out of [`SOURCES`] is what makes applying it by accident impossible —
    /// stronger than a flag on the item, and it matches how Go binds it. The runner
    /// special-cases migration 10 with this query, as Python and Java do.
    #[test]
    fn the_migration_ten_probe_binds_its_schema() {
        assert_eq!(MIGRATION_10_PK_PROBE.version, 10);
        assert!(
            !SOURCES.iter().any(|s| s.name == MIGRATION_10_PK_PROBE.name),
            "the probe must not be in the set the runner applies",
        );
        assert!(
            !MIGRATION_10_PK_PROBE.sql.contains("%s"),
            "a probe has no interpolation slots",
        );
        assert!(
            MIGRATION_10_PK_PROBE.sql.contains("$1"),
            "the probe takes the schema as a bind parameter",
        );
    }

    /// No migration compares against a bare, unquoted schema name.
    ///
    /// Migration 10's PostgreSQL file used to: its `DO` block matched `n.nspname = '%s'`, a
    /// string comparison needing the unquoted name, while the `ALTER` beside it needed the
    /// quoted identifier. Rendering the quoted form into both produced valid-looking SQL
    /// whose guard silently never matched — making the one migration whose entire purpose is
    /// idempotence run unconditionally. That file has been replaced by a runner check.
    ///
    /// Asserted so a migration that reintroduces a bare-name comparison is caught here
    /// rather than at runtime.
    /// No migration declares `INTEGER`; the corpus uses `INT4` throughout.
    ///
    /// The two spell the same 32-bit type on PostgreSQL, but CockroachDB aliases `INTEGER` to
    /// `INT8` (`default_int_size` is 8). Declaring `INTEGER` therefore produces a *different
    /// schema per backend*, which breaks the one thing that actually has to hold — and it did:
    /// eight columns across migrations 1, 6, and 21 came out `int8` on CockroachDB and `int4`
    /// on PostgreSQL until this was fixed. `INT4` is unambiguous on both.
    ///
    /// This also covers function signatures, where `ALTER FUNCTION` and `DROP FUNCTION` must
    /// name the same types the `CREATE` used, or they silently fail to match.
    #[test]
    fn no_migration_declares_integer() {
        for s in SOURCES
            .iter()
            .chain(std::iter::once(&MIGRATION_10_PK_PROBE))
        {
            let sql = s
                .sql
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<String>()
                .to_ascii_uppercase();
            assert!(
                !sql.contains("INTEGER"),
                "{}: declares INTEGER; use INT4, which is 32 bits on both backends",
                s.name,
            );
        }
    }

    /// Every placeholder in the corpus is one the renderer knows.
    ///
    /// A typo renders as an error rather than silently leaving `{{shcema}}` in the SQL.
    #[test]
    fn every_placeholder_is_known() {
        for s in SOURCES
            .iter()
            .chain(std::iter::once(&MIGRATION_10_PK_PROBE))
        {
            for frag in s.sql.split("{{").skip(1) {
                let name = frag.split("}}").next().unwrap_or_default();
                assert!(
                    matches!(name, "schema" | "concurrently"),
                    "{}: unknown placeholder {{{{{name}}}}}",
                    s.name,
                );
            }
        }
    }

    #[test]
    fn unknown_and_unterminated_placeholders_are_rejected() {
        assert_eq!(
            render("SELECT {{nope}}", VALUES),
            Err(RenderError::Unknown {
                name: "nope".to_owned(),
                offset: 7,
            }),
        );
        assert_eq!(
            render("SELECT {{schema", VALUES),
            Err(RenderError::Unterminated { offset: 7 }),
        );
    }

    #[test]
    fn concurrently_renders_empty_for_cockroach() {
        let s = source("23_create_partial_forked_from_index.sql").unwrap();
        let crdb = render(
            s.sql,
            Placeholders {
                schema: SCHEMA,
                concurrently: "",
            },
        )
        .unwrap();
        assert!(!crdb.contains("CONCURRENTLY"));
        assert!(
            render(s.sql, VALUES)
                .unwrap()
                .contains("CREATE INDEX CONCURRENTLY")
        );
    }

    #[test]
    fn quoting_handles_embedded_quotes() {
        assert_eq!(quote_identifier("dbos"), r#""dbos""#);
        assert_eq!(quote_identifier(r#"a"b"#), r#""a""b""#);
    }

    /// Dollar-quoted blocks contain semicolons, so a runner that splits statements on `;`
    /// destroys them. Asserted here so the property is visible at the corpus level, where
    /// the runner will have to honour it.
    #[test]
    fn dollar_quoted_files_contain_semicolons_inside_their_blocks() {
        // Migration 10's `DO` block used to be a fifth; it was replaced by a runner check.
        let expected = [
            "1_initial_dbos_schema_listen_notify.sql",
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
            "39_create_streams_trigger.sql",
        ];
        let found: Vec<&str> = SOURCES
            .iter()
            .filter(|s| s.sql.contains("$$"))
            .map(|s| s.name)
            .collect();
        assert_eq!(found.len(), expected.len());
        for name in expected {
            assert!(found.contains(&name), "{name} should carry a $$ block");
            let sql = source(name).unwrap().sql;
            let inside: String = sql.split("$$").skip(1).step_by(2).collect();
            assert!(
                inside.contains(';'),
                "{name}: expected semicolons inside its $$ block",
            );
        }
    }
}
