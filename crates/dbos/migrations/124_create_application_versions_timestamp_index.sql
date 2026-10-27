-- Migration 124: Index application_versions on version_timestamp, newest first,
-- so a latest-version lookup with no application name reads one index entry
-- instead of sorting the whole version history. Online.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_application_versions_timestamp" ON {{schema}}."application_versions" ("version_timestamp" DESC);
