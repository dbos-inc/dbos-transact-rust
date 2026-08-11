-- Migration 106: Half of the key that replaces version_name's global uniqueness
-- from migration 13. A version name is unique per application, not per database:
-- two applications sharing a system database may each register "v1.2.0".
--
-- This half covers the claimed rows; migration 107 covers the unclaimed ones,
-- which count as a single owner between them. The old table-wide UNIQUE stays in
-- place for now and is the stricter of the two, so it is what actually holds
-- until it is dropped — which cannot happen until every SDK reaching a shared
-- database is past 107.
--
-- No CONCURRENTLY: the predicate matches no rows on an existing database, since
-- migration 103 added application_name as NULL everywhere.

CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
    ON {{schema}}."application_versions" ("application_name", "version_name")
    WHERE "application_name" IS NOT NULL;
