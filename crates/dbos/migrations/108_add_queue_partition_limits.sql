-- Migration 108: Add the per-partition limit columns to queues. A queue is
-- partitioned by any one of them being set, and each limit then applies per
-- partition rather than to the queue as a whole. NULL means the limit is not
-- set. ADD COLUMN with a constant default is catalog-only, and nothing here
-- builds an index, so no CONCURRENTLY is needed.

ALTER TABLE {{schema}}."queues" ADD COLUMN IF NOT EXISTS "partition_concurrency" INT4 DEFAULT NULL;
ALTER TABLE {{schema}}."queues" ADD COLUMN IF NOT EXISTS "partition_worker_concurrency" INT4 DEFAULT NULL;
ALTER TABLE {{schema}}."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_max" INT4 DEFAULT NULL;
ALTER TABLE {{schema}}."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_period_sec" DOUBLE PRECISION DEFAULT NULL;
