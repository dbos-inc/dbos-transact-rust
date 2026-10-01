-- Migration 111: Index the steps' retention stamp. Online: operation_outputs
-- is the largest table on an existing database.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_operation_outputs_retention"
    ON {{schema}}."operation_outputs" ("retention_timestamp");
