-- Migration 43: Drop the per-row streams NOTIFY trigger installed by migration 39. Stream
-- writes are coalesced and pushed by the notifier off the write path instead, so the trigger
-- is pure overhead on every insert.
-- Applies only when notifications are enabled; otherwise migration 39 never created it.

DROP TRIGGER IF EXISTS dbos_streams_trigger ON {{schema}}.streams;
DROP FUNCTION IF EXISTS {{schema}}.streams_function();
