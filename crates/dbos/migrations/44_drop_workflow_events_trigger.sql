-- Migration 44: Drop the per-row workflow_events NOTIFY trigger installed by migration 1.
-- Events are coalesced and pushed by the notifier off the write path. After this only the
-- notifications trigger remains.
-- Applies only when notifications are enabled; otherwise migration 1 never created it.

DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON {{schema}}.workflow_events;
DROP FUNCTION IF EXISTS {{schema}}.workflow_events_function();
