-- Migration 43: Drop the per-row streams NOTIFY trigger installed by migration 39. Stream
-- writes are coalesced and pushed by the notifier off the write path instead, so the trigger
-- is pure overhead on every insert.
-- Applies on every PostgreSQL database, whether or not this process uses notifications: a
-- database migrated while notifications were on has the trigger even if they are off now.
-- IF EXISTS makes it a no-op where migration 39 never created it. CockroachDB never had it.

DROP TRIGGER IF EXISTS dbos_streams_trigger ON {{schema}}.streams;
DROP FUNCTION IF EXISTS {{schema}}.streams_function();
