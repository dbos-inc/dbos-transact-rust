-- Migration 22: Drop the non-partial index on forked_from in preparation for
-- recreating it as a partial index (only on non-NULL values).

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_workflow_status_forked_from";
