-- Migration 103: Add application_name to application_versions.
--
-- NULL means unclaimed: any application may read and claim the row. One table per migration,
-- so a blocked table does not hold the others' locks.
--
-- Part of the shared 100-series, which every implementation defines identically. Unlike the
-- migrations below 100, the version number here is a cross-SDK agreement: 1xx must mean the
-- same DDL in Rust, Python and TypeScript, because they migrate the same database.
ALTER TABLE {{schema}}."application_versions" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
