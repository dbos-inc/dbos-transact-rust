-- Migration 20 (LISTEN/NOTIFY half): pin search_path on the trigger functions installed by
-- migration 1's LISTEN/NOTIFY half. Appended to 20_set_function_search_path.sql only when
-- notifications are enabled, because without them these functions do not exist.

ALTER FUNCTION {{schema}}.notifications_function() SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION {{schema}}.workflow_events_function() SET search_path = pg_catalog, pg_temp;
