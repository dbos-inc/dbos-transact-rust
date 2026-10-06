-- Migration 120: Drop idx_operation_outputs_completed_at_function_name, which
-- migration 119's v2 supersedes. Online.

DROP INDEX {{concurrently}} IF EXISTS {{schema}}."idx_operation_outputs_completed_at_function_name";
