-- Migration 114: Drop idx_notifications, which covers the same columns in the
-- same order as idx_workflow_topic and so only costs writes. Online.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_notifications";
