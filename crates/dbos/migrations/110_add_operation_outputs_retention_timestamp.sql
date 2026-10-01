-- Migration 110: The same retention stamp as migration 109's tables, on the
-- steps.

ALTER TABLE {{schema}}."operation_outputs"
    ADD COLUMN IF NOT EXISTS "retention_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint;
