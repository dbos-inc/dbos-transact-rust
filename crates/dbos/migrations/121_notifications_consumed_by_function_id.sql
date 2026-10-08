-- Migration 121: Add consumed_by_function_id to notifications, the step of the
-- recv that consumed each message, so a rewind can delete the messages the
-- discarded part of a run consumed. A nullable column with no default is
-- catalog-only, and nothing here builds an index, so no CONCURRENTLY is needed.

ALTER TABLE {{schema}}."notifications" ADD COLUMN IF NOT EXISTS "consumed_by_function_id" INT4;
