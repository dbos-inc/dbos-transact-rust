-- Migration 109: Move workflow payloads out of workflow_status into their own
-- tables, so a status update no longer rewrites a large payload. Writers put a
-- workflow's inputs in workflow_input and its outcome in workflow_output;
-- readers fall back to the legacy workflow_status columns for rows written
-- before the move. retention_timestamp orders the retention sweep's rounds.

CREATE TABLE IF NOT EXISTS {{schema}}."workflow_input" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE TABLE IF NOT EXISTS {{schema}}."workflow_output" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE INDEX IF NOT EXISTS "idx_workflow_input_retention"
    ON {{schema}}."workflow_input" ("retention_timestamp");

CREATE INDEX IF NOT EXISTS "idx_workflow_output_retention"
    ON {{schema}}."workflow_output" ("retention_timestamp");
