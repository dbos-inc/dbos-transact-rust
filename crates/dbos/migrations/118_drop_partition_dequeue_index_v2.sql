-- Migration 118: Drop idx_workflow_status_partition_dequeue_v2, which migration
-- 117's v3 supersedes. Online.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_workflow_status_partition_dequeue_v2";
