-- Migration 11: Add serialization column to workflow and event tables
-- Stores serialization format/metadata for workflow data.

ALTER TABLE {{schema}}.workflow_status ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE {{schema}}.notifications ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE {{schema}}.workflow_events ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE {{schema}}.workflow_events_history ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE {{schema}}.operation_outputs ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE {{schema}}.streams ADD COLUMN serialization TEXT DEFAULT NULL;
