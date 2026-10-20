-- Migration 123: Add creator_xid to workflow_status, the token of the attempt
-- that created the row. owner_xid becomes the token of the execution that owns
-- the workflow now, and is cleared on every hand-off; creator_xid never
-- changes, so a start that finds the row already there can tell its own retried
-- insert from another caller's. A nullable column with a NULL default is
-- catalog-only, so no CONCURRENTLY is needed.

ALTER TABLE {{schema}}."workflow_status" ADD COLUMN IF NOT EXISTS "creator_xid" TEXT DEFAULT NULL;
