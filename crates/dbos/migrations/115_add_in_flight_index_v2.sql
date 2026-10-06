-- Migration 115: The in-flight index from migration 32, with application_name
-- carried as an INCLUDE column so counts scoped to one application run
-- index-only. The key is unchanged, so the dequeue plans as before; the planner
-- still cannot BitmapOr on application_name, so an ownership predicate is a
-- filter on the index entries, not an index condition. Online.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_workflow_status_in_flight_v2" ON {{schema}}."workflow_status" ("queue_name", "status", "priority", "created_at") INCLUDE ("application_name") WHERE "status" IN ('ENQUEUED', 'PENDING');
