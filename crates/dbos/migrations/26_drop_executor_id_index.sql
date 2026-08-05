-- Migration 26: Drop the index on executor_id. The recovery query that used
-- this index no longer relies on it.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."workflow_status_executor_id_index";
