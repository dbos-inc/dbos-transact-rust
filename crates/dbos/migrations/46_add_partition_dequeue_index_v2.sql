-- Migration 46: Partitioned-queue dequeue index, v2. The trailing workflow_uuid totalizes the
-- dequeue order, which the v1 index in migration 45 left ambiguous.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2" ON {{schema}}."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
