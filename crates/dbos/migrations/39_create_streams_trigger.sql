-- Migration 39: Notify a blocked stream reader as soon as a value is written,
-- mirroring the notifications/workflow_events triggers from migration 1. Gated
-- on LISTEN/NOTIFY support; deployments without it (e.g. CockroachDB) use the
-- polling fallback and skip this migration.

CREATE OR REPLACE FUNCTION {{schema}}.streams_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_streams_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION {{schema}}.streams_function() SET search_path = pg_catalog, pg_temp;

DROP TRIGGER IF EXISTS dbos_streams_trigger ON {{schema}}.streams;
CREATE TRIGGER dbos_streams_trigger
AFTER INSERT ON {{schema}}.streams
FOR EACH ROW EXECUTE FUNCTION {{schema}}.streams_function();
