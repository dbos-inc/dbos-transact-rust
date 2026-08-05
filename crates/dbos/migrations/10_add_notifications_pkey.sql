-- Migration 10: Add primary key to notifications table.
-- An earlier version of DBOS created this table without one. Migration 1 has created it
-- inline ever since, so this backfills the key only for databases created by those older
-- versions, and will do nothing on a database this implementation migrated itself.
-- Idempotence is the runner's job, not this file's: it runs 10_check_notifications_pkey.sql
-- first and skips the statement below when the key already exists. That check is done in the
-- runner rather than in SQL because CockroachDB has no DO block to do it with.

ALTER TABLE {{schema}}.notifications ADD CONSTRAINT notifications_pkey PRIMARY KEY (message_uuid);
