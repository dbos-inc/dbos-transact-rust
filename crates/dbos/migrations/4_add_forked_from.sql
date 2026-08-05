-- Migration 4: Add forked_from column to workflow_status table
-- This enables tracking workflow fork lineage

ALTER TABLE {{schema}}.workflow_status
ADD COLUMN forked_from TEXT;

CREATE INDEX "idx_workflow_status_forked_from" ON {{schema}}."workflow_status" ("forked_from");

