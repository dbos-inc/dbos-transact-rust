-- Migration 10: probe whether the notifications primary key already exists.
-- The runner executes this before 10_add_notifications_pkey.sql and skips the ALTER when it
-- returns a row. This is not a migration and must never be applied as one.
-- Takes the schema as a bind parameter ($1) rather than an interpolated slot.

SELECT 1 FROM pg_constraint c
JOIN pg_class cl ON c.conrelid = cl.oid
JOIN pg_namespace n ON cl.relnamespace = n.oid
WHERE n.nspname = $1
  AND cl.relname = 'notifications'
  AND c.contype = 'p';
