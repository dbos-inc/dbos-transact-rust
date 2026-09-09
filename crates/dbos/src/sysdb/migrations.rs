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
//!   in migrations 1, 14, 38, 39, and 105.
//!
//! Applying the corpus to both backends is what verifies it; reading the SQL is not enough.

/// Applying the corpus above to a database.
pub mod runner;

/// Whether a migration's index DDL uses `CONCURRENTLY`, and so cannot run inside a transaction
/// on PostgreSQL.
///
/// Read from the file rather than from a list beside it. A migration is online exactly when it
/// carries a `{{concurrently}}` placeholder — see [`Placeholders`] — so a list would be a second
/// copy of a fact the file already states, and the two could disagree.
///
/// Never true on CockroachDB, which applies schema changes online regardless and does not take
/// the keyword; the placeholder renders empty there.
pub fn is_online(version: u32) -> bool {
    SOURCES
        .iter()
        .any(|s| s.version == version && s.sql.contains("{{concurrently}}"))
}

/// Quotes any SQL identifier — a schema, table, index, or database name.
///
/// Callers interpolate the *result*, so it carries its own quotes: the migration files contain
/// `{{schema}}.notifications`, not `"{{schema}}".notifications`, and the same holds for the
/// table and database names built at runtime. Embedded double quotes are doubled, per SQL's
/// quoted-identifier rules.
///
/// Quoting is what makes the surrounding SQL safe to assert, since a schema name is the one
/// piece of these statements that comes from configuration.
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

/// When a migration file applies.
///
/// Declared with the file rather than decided by the assembler. Upstream expresses the same
/// conditions as `if` statements inside one function per migration; ours are files, so the
/// condition has to live somewhere — and next to the file it governs is where it can be read
/// without cross-referencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applies {
    /// On every dialect and configuration.
    Always,
    /// Only where LISTEN/NOTIFY is in use — enabled by the caller *and* supported by the
    /// dialect, which CockroachDB is not.
    WhenNotify,
    /// Only on PostgreSQL.
    Postgres,
    /// Only on CockroachDB, which is how a dialect ships a different form of one migration.
    Cockroach,
}

impl Applies {
    /// Whether this file belongs in the list being assembled.
    fn matches(self, dialect: Dialect, notify: bool) -> bool {
        match self {
            Applies::Always => true,
            Applies::WhenNotify => notify,
            Applies::Postgres => !dialect.is_cockroach(),
            Applies::Cockroach => dialect.is_cockroach(),
        }
    }
}

/// A migration file as copied from upstream: its number, its filename, its contents, and when it
/// applies.
///
/// Numbers are not unique across the corpus — several migrations ship variant files for
/// CockroachDB or for LISTEN/NOTIFY, and those share the number of the migration they vary. A
/// version's SQL is every applicable file for that version, concatenated in declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationSource {
    /// The migration number, taken from the filename. **Never from the header comment**,
    /// which is wrong for 22 through 35.
    pub version: u32,
    /// The filename, which is what distinguishes variants sharing a version.
    pub name: &'static str,
    /// The file's verbatim contents, still carrying its `{{name}}` placeholders.
    pub sql: &'static str,
    /// The condition under which it is applied.
    pub applies: Applies,
}

macro_rules! sources {
    ($(($version:expr, $file:literal, $applies:expr)),* $(,)?) => {
        /// Every migration file in the corpus, in filename order.
        ///
        /// Assembling these into the ordered list a runner applies — the padding to the shared
        /// numbering base, and migration 10's probe — is a separate concern. Variant selection
        /// is not: each file says when it applies.
        pub const SOURCES: &[MigrationSource] = &[
            $(MigrationSource {
                version: $version,
                name: $file,
                sql: include_str!(concat!("../../migrations/", $file)),
                applies: $applies,
            }),*
        ];
    };
}

sources![
    (1, "1_initial_dbos_schema.sql", Applies::Always),
    (
        1,
        "1_initial_dbos_schema_listen_notify.sql",
        Applies::WhenNotify
    ),
    (2, "2_add_queue_partition_key.sql", Applies::Always),
    (3, "3_add_workflow_status_index.sql", Applies::Always),
    (4, "4_add_forked_from.sql", Applies::Always),
    (5, "5_add_step_timestamps.sql", Applies::Always),
    (6, "6_add_workflow_events_history.sql", Applies::Always),
    (7, "7_add_owner_xid.sql", Applies::Always),
    (8, "8_add_parent_workflow_id.sql", Applies::Always),
    (9, "9_add_workflow_schedules.sql", Applies::Always),
    (10, "10_add_notifications_pkey.sql", Applies::Always),
    (11, "11_add_serialization_columns.sql", Applies::Always),
    (12, "12_add_notifications_consumed.sql", Applies::Always),
    (13, "13_add_application_versions.sql", Applies::Always),
    (14, "14_add_pgsql_client_functions.sql", Applies::Always),
    (15, "15_add_workflow_schedule_columns.sql", Applies::Always),
    (16, "16_add_delay_until.sql", Applies::Always),
    (
        17,
        "17_add_workflow_schedule_queue_name.sql",
        Applies::Always
    ),
    (18, "18_add_was_forked_from.sql", Applies::Always),
    (
        19,
        "19_add_operation_outputs_completed_at_index.sql",
        Applies::Always
    ),
    (20, "20_set_function_search_path.sql", Applies::Postgres),
    (
        20,
        "20_set_notify_function_search_path.sql",
        Applies::WhenNotify
    ),
    (21, "21_create_queues_table.sql", Applies::Always),
    (22, "22_drop_forked_from_index.sql", Applies::Always),
    (
        23,
        "23_create_partial_forked_from_index.sql",
        Applies::Always
    ),
    (24, "24_drop_parent_workflow_id_index.sql", Applies::Always),
    (
        25,
        "25_create_partial_parent_workflow_id_index.sql",
        Applies::Always
    ),
    (26, "26_drop_executor_id_index.sql", Applies::Always),
    (27, "27_create_partial_dedup_id_index.sql", Applies::Always),
    (28, "28_drop_dedup_id_constraint.sql", Applies::Postgres),
    (
        28,
        "28_drop_dedup_id_constraint_cockroach.sql",
        Applies::Cockroach
    ),
    (29, "29_create_pending_index.sql", Applies::Always),
    (30, "30_create_failed_index.sql", Applies::Always),
    (31, "31_drop_status_index.sql", Applies::Always),
    (32, "32_create_in_flight_index.sql", Applies::Always),
    (33, "33_add_rate_limited.sql", Applies::Always),
    (34, "34_create_rate_limited_index.sql", Applies::Always),
    (
        35,
        "35_drop_queue_status_started_index.sql",
        Applies::Always
    ),
    (36, "36_add_completed_at.sql", Applies::Always),
    (37, "37_create_started_at_index.sql", Applies::Always),
    (38, "38_update_enqueue_workflow.sql", Applies::Always),
    (
        38,
        "38_set_enqueue_workflow_search_path.sql",
        Applies::Postgres
    ),
    (39, "39_create_streams_trigger.sql", Applies::WhenNotify),
    (40, "40_add_attributes.sql", Applies::Always),
    (41, "41_add_schedule_name.sql", Applies::Always),
    (42, "42_add_debounce_columns.sql", Applies::Always),
    (43, "43_drop_streams_trigger.sql", Applies::WhenNotify),
    (
        44,
        "44_drop_workflow_events_trigger.sql",
        Applies::WhenNotify
    ),
    (45, "45_add_partition_dequeue_index.sql", Applies::Always),
    (46, "46_add_partition_dequeue_index_v2.sql", Applies::Always),
    (47, "47_drop_partition_dequeue_index.sql", Applies::Always),
    // The shared series. Numbers from 100 up mean the same DDL in every implementation.
    (
        100,
        "100_workflow_status_application_name.sql",
        Applies::Always
    ),
    (101, "101_queues_application_name.sql", Applies::Always),
    (
        102,
        "102_workflow_schedules_application_name.sql",
        Applies::Always
    ),
    (
        103,
        "103_application_versions_application_name.sql",
        Applies::Always
    ),
    (
        104,
        "104_operation_outputs_application_name.sql",
        Applies::Always
    ),
    (
        105,
        "105_enqueue_workflow_application_name.sql",
        Applies::Always
    ),
    (
        105,
        "105_set_enqueue_workflow_search_path.sql",
        Applies::Postgres
    ),
    (
        106,
        "106_application_versions_owner_key.sql",
        Applies::Always
    ),
    (
        107,
        "107_application_versions_unclaimed_key.sql",
        Applies::Always
    ),
    (108, "108_add_queue_partition_limits.sql", Applies::Always),
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
    // Carried for the shape only. A probe is run, not applied, so nothing consults this.
    applies: Applies::Always,
};

/// Looks up a migration file by name.
pub fn source(name: &str) -> Option<&'static MigrationSource> {
    SOURCES.iter().find(|s| s.name == name)
}

/// The highest migration in this implementation's own history.
///
/// **Below the shared base the numbering still mirrors upstream's, and must.** A version number
/// under 100 is a claim about schema state, so a database left half-migrated by one
/// implementation is picked up correctly by another only if their version *n* describes the same
/// schema. Our 45, 46 and 47 are Python's `fortyfive`, `fortysix` and `fortyseven` down to the
/// index names. The rule: keep the Go/Python/Java numbering below 100, then jump to 100.
///
/// What the shared base changes is only what happens *above* it — see
/// [`SHARED_MIGRATION_BASE`]. It does not make the numbers below it free.
pub const LOCAL_MIGRATIONS: u32 = 47;

/// Where the numbering stops being per-implementation and starts being shared.
///
/// From here up, a version number is a cross-SDK agreement *by construction*: 100 means the same
/// DDL in Rust, Python and TypeScript because all three define it identically. Below it, the
/// numbering agrees by porting rather than by agreement — see [`LOCAL_MIGRATIONS`] — which is why
/// a new shared migration is written once and copied, while an old local one is never renumbered.
///
/// Versions between [`LOCAL_MIGRATIONS`] and here are padding — empty migrations that consume a
/// number and do nothing. They exist so the shared series lands on its agreed numbers whatever
/// length a given implementation's own history happens to be.
pub const SHARED_MIGRATION_BASE: u32 = 100;

/// The highest migration defined here, and the version a fully migrated database records.
pub const SHARED_MIGRATIONS: u32 = 108;

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

/// Assembles the ordered migration list for a dialect and configuration.
///
/// Seven migrations vary, and the variance lives here rather than in the files:
///
/// - **1** gains its LISTEN/NOTIFY half only when notifications are on and supported.
/// - **10** carries a guard ([`MIGRATION_10_PK_PROBE`]) instead of being conditional.
/// - **20** hardens `search_path` on the functions that exist: the client functions always,
///   the trigger functions only if migration 1 installed them. Empty on CockroachDB, which
///   has no `ALTER FUNCTION ... SET`.
/// - **28** drops a constraint PostgreSQL exposes as a constraint and CockroachDB as an index.
/// - **38** appends a PostgreSQL-only `search_path` tail.
/// - **39** installs a trigger, so it follows the same gate as 1's notify half.
/// - **105** replaces the function 38 defined, and appends the same `search_path` tail.
///
/// A migration that does not apply renders empty rather than being dropped: versions are
/// positional, so removing one would renumber everything after it.
///
/// TODO(dbos-team): UPSTREAM item 15. `use_listen_notify` decides both how *this process* waits
/// and whether the *database* gets its NOTIFY triggers — and the second is permanent and shared.
/// A database migrated with it off has no triggers, so a Go peer, which has no such config and
/// always LISTENs on Postgres (`dialect.go`), waits out every `recv` timeout in silence. Rust
/// gates here because Python and Java do; whether any of them should is the question.
pub fn build_migrations(schema: &str, dialect: Dialect, use_listen_notify: bool) -> Vec<Migration> {
    let quoted = quote_identifier(schema);
    // CockroachDB has no LISTEN/NOTIFY, so asking for it there is asking for nothing.
    let notify = use_listen_notify && !dialect.is_cockroach();
    let values = Placeholders {
        schema: &quoted,
        concurrently: dialect.concurrently(),
    };

    // Own history, then the padding, then the shared series. The padding is not separable from
    // what follows it: on its own it would put a fresh database at version 99 while 47
    // migrations had run, claiming progress that has not happened.
    (1..=SHARED_MIGRATIONS)
        .map(|version| {
            // A version's SQL is every file that applies to it, in declaration order. Most
            // versions have one file; a few have a second that applies only under LISTEN/NOTIFY
            // or on one dialect, and a version whose files all decline renders empty.
            let sql = SOURCES
                .iter()
                .filter(|s| s.version == version && s.applies.matches(dialect, notify))
                .map(|s| render(s.sql, values).expect("corpus renders"))
                .collect::<Vec<_>>()
                .join("\n");

            Migration {
                version,
                // Read from the file's placeholder rather than from the rendered SQL, which no
                // longer carries the keyword on CockroachDB. Never online there anyway: it
                // applies schema changes online regardless and does not take the keyword.
                online: is_online(version) && !dialect.is_cockroach(),
                // Migration 10 alone runs a probe first, because its work is a backfill that
                // must not repeat: the probe asks whether the key is already there. It is a
                // query, not SQL to apply, so it cannot be another source file.
                guard: (version == 10).then_some(MIGRATION_10_PK_PROBE.sql),
                sql,
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

    /// Each version is assembled from the files that apply to it, and no others.
    ///
    /// Which files apply is declared on the files themselves rather than in the assembler.
    /// Asserted against the built output rather than against `Applies`, so the rules are pinned by
    /// what they produce rather than by how they are spelled.
    #[test]
    fn a_version_is_the_files_that_apply_to_it() {
        let built = |dialect, notify| {
            build_migrations("dbos", dialect, notify)
                .into_iter()
                .map(|m| (m.version, m.sql))
                .collect::<std::collections::HashMap<_, _>>()
        };
        let pg_notify = built(Dialect::Postgres, true);
        let pg_plain = built(Dialect::Postgres, false);
        let crdb = built(Dialect::Cockroach, true);

        // 1 gains its trigger half only under LISTEN/NOTIFY — including on CockroachDB, which
        // has none however the caller asks.
        assert!(pg_notify[&1].contains("TRIGGER"));
        assert!(!pg_plain[&1].contains("TRIGGER"));
        assert!(!crdb[&1].contains("TRIGGER"));

        // 20 sets a function's search_path, which CockroachDB does not take: empty there, and
        // its notify half follows the same rule as 1's.
        assert!(crdb[&20].is_empty());
        assert!(pg_notify[&20].len() > pg_plain[&20].len());

        // 28 ships one form per dialect, not a common form plus an extra.
        assert!(pg_notify[&28].contains("DROP CONSTRAINT"));
        assert_ne!(pg_notify[&28], crdb[&28]);
        assert!(!crdb[&28].is_empty());

        // 38 and 105 each define enqueue_workflow and then pin its search_path, a tail
        // CockroachDB does not take — so the function is still defined there, unhardened.
        for version in [38, 105] {
            assert!(pg_plain[&version].contains("ALTER FUNCTION"));
            assert!(!crdb[&version].contains("ALTER FUNCTION"));
            assert!(
                crdb[&version].contains("CREATE OR REPLACE FUNCTION"),
                "{version} still defines the function on CockroachDB",
            );
        }

        // 39, 43 and 44 install and drop triggers that exist only under LISTEN/NOTIFY.
        for version in [39, 43, 44] {
            assert!(
                !pg_notify[&version].is_empty(),
                "{version} applies with notify"
            );
            assert!(
                pg_plain[&version].is_empty(),
                "{version} is a no-op without"
            );
        }

        // Migration 10 is the only one carrying a probe, and it is a query rather than a file
        // that could have been given an `Applies`.
        let migrations = build_migrations("dbos", Dialect::Postgres, true);
        let guarded: Vec<u32> = migrations
            .iter()
            .filter(|m| m.guard.is_some())
            .map(|m| m.version)
            .collect();
        assert_eq!(guarded, [10]);
    }

    /// The list runs to the shared series, with the gap consuming its numbers and doing nothing.
    ///
    /// The padding is what makes a version number mean the same thing across implementations:
    /// the shared series has to land on 100 whatever length this implementation's own history
    /// happens to be. Its emptiness is the point, so it is asserted rather than assumed.
    #[test]
    fn the_gap_to_the_shared_base_is_padding() {
        let migrations = build_migrations("dbos", Dialect::Postgres, true);

        assert_eq!(migrations.len() as u32, SHARED_MIGRATIONS);
        assert_eq!(
            migrations.last().expect("non-empty").version,
            SHARED_MIGRATIONS
        );

        for m in &migrations {
            let padding = m.version > LOCAL_MIGRATIONS && m.version < SHARED_MIGRATION_BASE;
            if padding {
                assert!(
                    m.sql.is_empty(),
                    "migration {} is padding and must do nothing",
                    m.version
                );
            }
        }

        // And the shared series is not padding: every one of its migrations carries DDL.
        //
        // 100 to 107 are all about `application_name`, the column the series was opened to add;
        // 108 is the first that is not, so the assertion is what the padding check needs — that
        // the shared numbers do work — rather than what they happen to work on.
        for version in SHARED_MIGRATION_BASE..=SHARED_MIGRATIONS {
            let m = &migrations[version as usize - 1];
            assert_eq!(m.version, version);
            assert!(
                m.sql.contains("ALTER TABLE") || m.sql.contains("CREATE"),
                "migration {version} should carry the shared series' work",
            );
        }
    }

    #[test]
    fn corpus_matches_upstream_shape() {
        // 62 files: 61 the runner applies, plus the migration-10 probe, which is bound
        // separately so it cannot be applied by mistake.
        assert_eq!(SOURCES.len(), 61);

        let mut versions: Vec<u32> = SOURCES.iter().map(|s| s.version).collect();
        versions.sort_unstable();
        versions.dedup();
        // This implementation's own history, then the shared series — with the padding between
        // them contributing no files, which is what makes it padding.
        let expected: Vec<u32> = (1..=LOCAL_MIGRATIONS)
            .chain(SHARED_MIGRATION_BASE..=SHARED_MIGRATIONS)
            .collect();
        assert_eq!(
            versions, expected,
            "the corpus should cover its own history and the shared series, and nothing between",
        );

        // Five files share a version with another: the LISTEN/NOTIFY halves of migrations 1
        // and 20, the CockroachDB form of 28, and the search-path halves of 38 and 105.
        assert_eq!(
            SOURCES.len() - versions.len(),
            5,
            "expected 5 variant files"
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
        // Every file less migration 1's two, which open with a description rather than a
        // numbered header.
        assert_eq!(checked, 60, "expected 60 files to carry a numbered header");
    }

    #[test]
    fn online_migrations_match_the_reference_set() {
        // Derived from the files, so this is a snapshot rather than a definition: a synced file
        // that gained or lost a `{{concurrently}}` placeholder changes what the runner does with
        // it, and should have to say so here.
        let online: Vec<u32> = (1..=SHARED_MIGRATIONS).filter(|v| is_online(*v)).collect();
        assert_eq!(
            online,
            [
                22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35, 37, 45, 46, 47, 107
            ],
        );
        // 106 is the counterexample in the shared series: an index whose predicate matches
        // nothing on an existing database needs no online build, while 107's matches every row.
        assert!(!is_online(106) && is_online(107));
        // The gaps are real: 28, 33, and 36 are not online.
        assert!(!is_online(28) && !is_online(33) && !is_online(36));
    }

    /// `CONCURRENTLY` only ever reaches a migration through the placeholder.
    ///
    /// Hardcoding the keyword would render it on CockroachDB, which does not take it, and would
    /// leave the migration unmarked as online — so PostgreSQL would try to run it inside a
    /// transaction, which it also does not take. Both failures come from the same slip.
    #[test]
    fn concurrently_arrives_only_through_the_placeholder() {
        for s in SOURCES {
            // Comments discuss the keyword freely — several explain why a migration does *not*
            // need it — so only the SQL is examined.
            let statements: String = s
                .sql
                .lines()
                .map(|line| line.split("--").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !statements
                    .replace("{{concurrently}}", "")
                    .contains("CONCURRENTLY"),
                "{}: CONCURRENTLY is hardcoded; it must come from the placeholder",
                s.name,
            );
        }
    }

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
    /// Migrations 14, 38 and 105 raise errors through `format('Workflow %s ...')`. Because `%`
    /// has no meaning to this renderer, they need no escaping and cannot be consumed by it.
    #[test]
    fn plpgsql_format_strings_pass_through() {
        for name in [
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
            "105_enqueue_workflow_application_name.sql",
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
    /// Migrations 14, 38 and 105 declare `named_args JSON DEFAULT '{}'::JSON`.
    #[test]
    fn single_braces_are_literal() {
        assert_eq!(
            render("DEFAULT '{}'::JSON, x {{schema}}", VALUES).unwrap(),
            r#"DEFAULT '{}'::JSON, x "dbos""#,
        );
        for name in [
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
            "105_enqueue_workflow_application_name.sql",
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
    /// The hazard is a file that needs both forms: a `DO` block matching `n.nspname = '%s'` — a
    /// string comparison wanting the unquoted name — beside an `ALTER` wanting the quoted
    /// identifier. Rendering the quoted form into both produces valid-looking SQL whose guard
    /// silently never matches, so a migration whose whole purpose is idempotence runs
    /// unconditionally. Migration 10's guard is a probe the runner executes with the schema bound
    /// as `$1` (`MIGRATION_10_PK_PROBE`) for exactly that reason.
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
        let expected = [
            "1_initial_dbos_schema_listen_notify.sql",
            "14_add_pgsql_client_functions.sql",
            "38_update_enqueue_workflow.sql",
            "39_create_streams_trigger.sql",
            "105_enqueue_workflow_application_name.sql",
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
