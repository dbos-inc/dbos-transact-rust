-- Migration 119: The step-completion index from migration 19, with
-- application_name as an INCLUDE column, matching migration 115. Online:
-- operation_outputs is the largest table on an existing database.

CREATE INDEX {{concurrently}} IF NOT EXISTS "idx_operation_outputs_completed_at_function_name_v2" ON {{schema}}."operation_outputs" ("completed_at_epoch_ms", "function_name") INCLUDE ("application_name");
