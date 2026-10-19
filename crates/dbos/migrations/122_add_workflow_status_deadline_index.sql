-- Migration 122: A partial index over unsettled workflows' deadlines, for the
-- workflow-timeout sweep, which cancels every unsettled workflow whose deadline
-- has passed, oldest deadline first. The predicate's status list has to match
-- the sweep's literals for the planner to use the index. Online.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_deadline" ON {{schema}}."workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
