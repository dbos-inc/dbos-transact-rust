-- Migration 105 (Postgres-only tail): pin search_path on the new
-- enqueue_workflow overload, matching the hardening applied in migrations 20
-- and 38. Skipped on CockroachDB, which does not support ALTER FUNCTION ... SET.

ALTER FUNCTION {{schema}}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT, TEXT
) SET search_path = pg_catalog, pg_temp;
