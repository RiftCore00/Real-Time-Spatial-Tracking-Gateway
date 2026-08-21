-- Migration: Time-Series Compaction for location_events
-- Issue #257: Implement time-series data compaction with automated downsampling,
-- retention policies, and continuous aggregates.
--
-- This migration:
--   1. Migrates location_events to a partitioned table (by day on timestamp)
--   2. Creates 1-minute downsample, 1-hour aggregate, and 1-day aggregate tables
--   3. Creates helper functions for compaction and querying
--
-- Prerequisites:
--   - PostgreSQL 16+ (for declarative partitioning)
--   - Optional: TimescaleDB extension for continuous aggregates
--
-- Usage:
--   psql -d spatial_tracking -f migration-compaction.sql

-- ============================================================
-- 0. Optionally enable TimescaleDB (skip if not installed)
-- ============================================================
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- 1. Migrate location_events to partitioned table
-- ============================================================

-- Create new partitioned table
CREATE TABLE IF NOT EXISTS location_events_new (
  id          UUID            NOT NULL DEFAULT gen_random_uuid(),
  client_id   TEXT            NOT NULL,
  room_id     TEXT            NOT NULL,
  latitude    DOUBLE PRECISION NOT NULL,
  longitude   DOUBLE PRECISION NOT NULL,
  altitude    DOUBLE PRECISION,
  accuracy    DOUBLE PRECISION,
  speed       DOUBLE PRECISION,
  timestamp   TIMESTAMPTZ     NOT NULL,
  created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Migrate data if old table exists and is not yet partitioned
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'location_events'
    AND table_type = 'BASE TABLE'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'location_events'
    AND relkind = 'p'
  ) THEN
    -- Copy data to new partitioned table
    INSERT INTO location_events_new
    SELECT id, client_id, room_id, latitude, longitude, altitude, accuracy, speed, timestamp, created_at
    FROM location_events;

    -- Drop old table and rename new
    DROP TABLE location_events;
    ALTER TABLE location_events_new RENAME TO location_events;
  ELSIF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'location_events'
  ) THEN
    -- No existing table, just rename the new one
    ALTER TABLE location_events_new RENAME TO location_events;
  ELSE
    -- Already partitioned, drop the unused new table
    DROP TABLE IF EXISTS location_events_new;
  END IF;
END $$;

-- Create indexes on the partitioned table
CREATE INDEX IF NOT EXISTS location_events_room_id_timestamp_idx
  ON location_events (room_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS location_events_lat_lon_idx
  ON location_events (latitude, longitude);

-- ============================================================
-- 2. Create partition creation helper
-- ============================================================

CREATE OR REPLACE FUNCTION create_daily_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  start_date DATE;
  end_date DATE;
BEGIN
  partition_name := 'location_events_' || to_char(partition_date, 'YYYY_MM_DD');
  start_date := partition_date;
  end_date := partition_date + INTERVAL '1 day';

  -- Only create if it does not exist
  IF NOT EXISTS (
    SELECT 1 FROM pg_class WHERE relname = partition_name
  ) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF location_events FOR VALUES FROM (%L) TO (%L)',
      partition_name, start_date, end_date
    );
  END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 3. Auto-create partitions for the next 30 days
-- ============================================================

DO $$
DECLARE
  i INT;
  d DATE;
BEGIN
  FOR i IN 0..30 LOOP
    d := CURRENT_DATE + (i || ' days')::INTERVAL;
    PERFORM create_daily_partition(d);
  END LOOP;
END $$;

-- ============================================================
-- 4. Create 1-minute downsample table
-- ============================================================

CREATE TABLE IF NOT EXISTS location_events_1m (
  id            UUID            NOT NULL DEFAULT gen_random_uuid(),
  room_id       TEXT            NOT NULL,
  bucket_start  TIMESTAMPTZ     NOT NULL,
  bucket_end    TIMESTAMPTZ     NOT NULL,
  latitude      DOUBLE PRECISION NOT NULL,
  longitude     DOUBLE PRECISION NOT NULL,
  altitude      DOUBLE PRECISION,
  max_speed     DOUBLE PRECISION,
  point_count   INTEGER         NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room_id, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_1m_room_bucket
  ON location_events_1m (room_id, bucket_start DESC);

-- ============================================================
-- 5. Create 1-hour aggregate table
-- ============================================================

CREATE TABLE IF NOT EXISTS location_events_1h (
  id              UUID            NOT NULL DEFAULT gen_random_uuid(),
  room_id         TEXT            NOT NULL,
  bucket_start    TIMESTAMPTZ     NOT NULL,
  bucket_end      TIMESTAMPTZ     NOT NULL,
  latitude        DOUBLE PRECISION NOT NULL,
  longitude       DOUBLE PRECISION NOT NULL,
  altitude        DOUBLE PRECISION,
  avg_speed       DOUBLE PRECISION,
  max_speed       DOUBLE PRECISION,
  min_speed       DOUBLE PRECISION,
  total_distance  DOUBLE PRECISION DEFAULT 0,
  point_count     INTEGER         NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room_id, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_1h_room_bucket
  ON location_events_1h (room_id, bucket_start DESC);

-- ============================================================
-- 6. Create 1-day aggregate table
-- ============================================================

CREATE TABLE IF NOT EXISTS location_events_1d (
  id              UUID            NOT NULL DEFAULT gen_random_uuid(),
  room_id         TEXT            NOT NULL,
  bucket_start    TIMESTAMPTZ     NOT NULL,
  bucket_end      TIMESTAMPTZ     NOT NULL,
  latitude        DOUBLE PRECISION NOT NULL,
  longitude       DOUBLE PRECISION NOT NULL,
  altitude        DOUBLE PRECISION,
  avg_speed       DOUBLE PRECISION,
  max_speed       DOUBLE PRECISION,
  min_speed       DOUBLE PRECISION,
  total_distance  DOUBLE PRECISION DEFAULT 0,
  point_count     INTEGER         NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room_id, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_1d_room_bucket
  ON location_events_1d (room_id, bucket_start DESC);

-- ============================================================
-- 7. Create partition pruning helper for old raw data
-- ============================================================

CREATE OR REPLACE FUNCTION drop_old_raw_partitions(retention_days INT)
RETURNS TABLE(dropped_partition TEXT, dropped_rows BIGINT) AS $$
DECLARE
  r RECORD;
  cutoff_date DATE;
  part_date DATE;
  row_count BIGINT;
BEGIN
  cutoff_date := CURRENT_DATE - (retention_days || ' days')::INTERVAL;

  FOR r IN
    SELECT inhrelid::regclass::text AS partition_name
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'location_events'
    AND child.relname LIKE 'location_events_%\_%\_%\_%\_%\_%'
  LOOP
    -- Extract date from partition name
    BEGIN
      part_date := to_date(
        replace(r.partition_name, 'location_events_', ''),
        'YYYY_MM_DD'
      );

      IF part_date < cutoff_date THEN
        -- Count rows before dropping
        EXECUTE format('SELECT count(*) FROM %I', r.partition_name) INTO row_count;

        -- Detach and drop the partition
        EXECUTE format(
          'ALTER TABLE location_events DETACH PARTITION %I',
          r.partition_name
        );
        EXECUTE format('DROP TABLE %I', r.partition_name);

        dropped_partition := r.partition_name;
        dropped_rows := row_count;
        RETURN NEXT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Skip partitions with non-standard naming
      CONTINUE;
    END;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 8. Create partition auto-creation trigger
-- ============================================================

CREATE OR REPLACE FUNCTION auto_create_partition_trigger()
RETURNS TRIGGER AS $$
DECLARE
  partition_date DATE;
BEGIN
  partition_date := date_trunc('day', NEW.timestamp)::DATE;
  PERFORM create_daily_partition(partition_date);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS auto_partition_trigger ON location_events;

CREATE TRIGGER auto_partition_trigger
  BEFORE INSERT ON location_events
  FOR EACH ROW
  EXECUTE FUNCTION auto_create_partition_trigger();

-- ============================================================
-- 9. Create compaction status tracking table
-- ============================================================

CREATE TABLE IF NOT EXISTS compaction_status (
  id              INTEGER         PRIMARY KEY DEFAULT 1,
  last_run        TIMESTAMPTZ,
  next_run        TIMESTAMPTZ,
  last_result     JSONB,
  updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

INSERT INTO compaction_status (id, last_run, next_run, updated_at)
VALUES (1, NULL, NULL, NOW())
ON CONFLICT (id) DO NOTHING;
