-- Migration 117: Partitioned-queue dequeue index, v3. Migration 46's key with
-- application_name as an INCLUDE column, matching migration 115. Online.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_partition_dequeue_v3" ON {{schema}}."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") INCLUDE ("application_name") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
