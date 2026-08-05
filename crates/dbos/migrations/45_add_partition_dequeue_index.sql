-- Migration 45: Partitioned-queue dequeue index. Extends idx_workflow_status_in_flight with
-- queue_partition_key, so a lookup scoped to one partition stays selective when many
-- partitions are active. Superseded by the v2 index in migration 46 and dropped in 47.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_partition_dequeue" ON {{schema}}."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
