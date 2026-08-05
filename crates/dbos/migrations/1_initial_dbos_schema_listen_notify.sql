-- Create notification function
CREATE OR REPLACE FUNCTION {{schema}}.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create notification trigger
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON {{schema}}.notifications
FOR EACH ROW EXECUTE FUNCTION {{schema}}.notifications_function();

-- Create events function
CREATE OR REPLACE FUNCTION {{schema}}.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create events trigger
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON {{schema}}.workflow_events
FOR EACH ROW EXECUTE FUNCTION {{schema}}.workflow_events_function();

