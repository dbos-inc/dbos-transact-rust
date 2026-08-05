-- Migration 47: Drop the v1 partition-dequeue index created in migration 45, superseded by
-- the v2 index in migration 46. Only v2 survives.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_workflow_status_partition_dequeue";
