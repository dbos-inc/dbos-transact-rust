-- Migration 112: Drop the operation_outputs -> workflow_status foreign key.
-- Steps are reclaimed by the retention sweep now, and deleted explicitly with
-- their workflow, so the cascade that used to do it is gone. Dropped under both
-- names: TypeScript created it under the knex name, every other SDK under the
-- PostgreSQL default, and a database may carry either.

ALTER TABLE {{schema}}."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_foreign";

ALTER TABLE {{schema}}."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_fkey";
