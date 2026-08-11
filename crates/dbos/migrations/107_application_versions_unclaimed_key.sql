-- Migration 107: The other half of the key described in migration 106, treating
-- the unclaimed rows as one owner. Together the two indexes say what the retiring
-- table-wide UNIQUE said, but scoped per application.
--
-- Online, unlike 106: every row on an existing database is unclaimed, so this is
-- the index that has to build over all of them.

CREATE UNIQUE INDEX {{concurrently}} IF NOT EXISTS "uq_application_versions_unclaimed_version"
    ON {{schema}}."application_versions" ("version_name")
    WHERE "application_name" IS NULL;
