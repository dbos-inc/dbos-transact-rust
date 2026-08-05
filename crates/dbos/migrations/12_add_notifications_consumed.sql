-- Migration 12: Add consumed column to notifications and index for unconsumed lookups.

ALTER TABLE {{schema}}.notifications ADD COLUMN consumed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications" ON {{schema}}.notifications (destination_uuid, topic);
