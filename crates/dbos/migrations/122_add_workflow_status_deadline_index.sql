-- Migration 122: Partial index for the workflow timeouts monitor.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_deadline" ON {{schema}}."workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
