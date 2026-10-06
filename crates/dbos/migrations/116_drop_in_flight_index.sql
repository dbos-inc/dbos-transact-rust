-- Migration 116: Drop idx_workflow_status_in_flight, which migration 115's v2
-- supersedes. Online.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_workflow_status_in_flight";
