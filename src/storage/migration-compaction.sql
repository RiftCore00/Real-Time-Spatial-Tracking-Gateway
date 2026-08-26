-- Migration: Time-Series Compaction for location_events
-- Setup declarative partitioning and tiered downsampling tables.
--
-- This migration:
--   1. Renames the existing location_events table to location_events_old.
--   2. Recreates location_events with PARTITION BY RANGE (timestamp).
--   3. Creates tiered downsample tables (location_events_1m, location_events_1h, location_events_1d)
--      all partitioned by RANGE (bucket).
--   4. Dynamically creates daily partitions for all dates present in location_events_old,
--      migrates the existing data into the partitioned location_events table,
--      and drops location_events_old upon completion.
--   5. Sets up partition management helpers, auto-partition trigger, and compaction status tracking.
--
-- Prerequisites:
--   - PostgreSQL 16+ (declarative partitioning)
--   - Optional: PostGIS extension for geometry types
--
-- Usage:
--   psql -d spatial_tracking -f migration-compaction.sql

-- ============================================================
-- 0. Optional Extensions
-- ============================================================

DO $$
BEGIN
  -- Attempt to enable PostGIS if available; ignore error if not installed
  BEGIN
    CREATE EXTENSION IF NOT EXISTS postgis;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
END $$;

-- ============================================================
-- 1. Rename existing location_events table to location_events_old
-- ============================================================

DO $$
BEGIN
  -- Only rename if location_events exists as a standard unpartitioned base table
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'location_events' AND table_type = 'BASE TABLE'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_class WHERE relname = 'location_events' AND relkind = 'p'
  ) THEN
    ALTER TABLE location_events RENAME TO location_events_old;
    RAISE NOTICE 'Renamed existing unpartitioned location_events table to location_events_old';
  END IF;
END $$;

-- ============================================================
-- 2. Recreate location_events with PARTITION BY RANGE (timestamp)
-- ============================================================

CREATE TABLE IF NOT EXISTS location_events (
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

CREATE INDEX IF NOT EXISTS location_events_room_id_timestamp_idx
  ON location_events (room_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS location_events_lat_lon_idx
  ON location_events (latitude, longitude);

-- ============================================================
-- 3. Create Tiered Downsample Tables (Partitioned by RANGE)
-- ============================================================

-- 3a. location_events_1m: 1-minute downsampled data
CREATE TABLE IF NOT EXISTS location_events_1m (
  room_id     TEXT            NOT NULL,
  bucket      TIMESTAMPTZ     NOT NULL,
  lat         NUMERIC,
  lon         NUMERIC,
  altitude    DOUBLE PRECISION,
  accuracy    DOUBLE PRECISION,
  speed       DOUBLE PRECISION,
  point_count INT             NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket);

CREATE INDEX IF NOT EXISTS location_events_1m_room_bucket_idx
  ON location_events_1m (room_id, bucket DESC);

-- 3b. location_events_1h: 1-hour aggregate data (PostGIS geometry or fallback text)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'geometry') THEN
    CREATE TABLE IF NOT EXISTS location_events_1h (
      room_id         TEXT            NOT NULL,
      bucket          TIMESTAMPTZ     NOT NULL,
      lat             NUMERIC,
      lon             NUMERIC,
      point_count     INT             NOT NULL DEFAULT 0,
      distance        DOUBLE PRECISION DEFAULT 0,
      avg_speed       DOUBLE PRECISION,
      max_speed       DOUBLE PRECISION,
      min_speed       DOUBLE PRECISION,
      route_geometry  geometry
    ) PARTITION BY RANGE (bucket);
  ELSE
    CREATE TABLE IF NOT EXISTS location_events_1h (
      room_id         TEXT            NOT NULL,
      bucket          TIMESTAMPTZ     NOT NULL,
      lat             NUMERIC,
      lon             NUMERIC,
      point_count     INT             NOT NULL DEFAULT 0,
      distance        DOUBLE PRECISION DEFAULT 0,
      avg_speed       DOUBLE PRECISION,
      max_speed       DOUBLE PRECISION,
      min_speed       DOUBLE PRECISION,
      route_geometry  TEXT
    ) PARTITION BY RANGE (bucket);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS location_events_1h_room_bucket_idx
  ON location_events_1h (room_id, bucket DESC);

-- 3c. location_events_1d: 1-day aggregate data
CREATE TABLE IF NOT EXISTS location_events_1d (
  room_id     TEXT            NOT NULL,
  bucket      TIMESTAMPTZ     NOT NULL,
  point_count INT             NOT NULL DEFAULT 0,
  distance    DOUBLE PRECISION DEFAULT 0
) PARTITION BY RANGE (bucket);

CREATE INDEX IF NOT EXISTS location_events_1d_room_bucket_idx
  ON location_events_1d (room_id, bucket DESC);

-- ============================================================
-- 4. Partition Helper Functions
-- ============================================================

-- Daily partition creator for location_events
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

-- Partition creator for 1m downsample table
CREATE OR REPLACE FUNCTION create_1m_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  start_date DATE;
  end_date DATE;
BEGIN
  partition_name := 'location_events_1m_' || to_char(partition_date, 'YYYY_MM_DD');
  start_date := partition_date;
  end_date := partition_date + INTERVAL '1 day';

  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF location_events_1m FOR VALUES FROM (%L) TO (%L)',
      partition_name, start_date, end_date
    );
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Partition creator for 1h aggregate table
CREATE OR REPLACE FUNCTION create_1h_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  start_date DATE;
  end_date DATE;
BEGIN
  partition_name := 'location_events_1h_' || to_char(partition_date, 'YYYY_MM');
  start_date := date_trunc('month', partition_date)::DATE;
  end_date := (date_trunc('month', partition_date) + INTERVAL '1 month')::DATE;

  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF location_events_1h FOR VALUES FROM (%L) TO (%L)',
      partition_name, start_date, end_date
    );
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Partition creator for 1d aggregate table
CREATE OR REPLACE FUNCTION create_1d_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  start_date DATE;
  end_date DATE;
BEGIN
  partition_name := 'location_events_1d_' || to_char(partition_date, 'YYYY');
  start_date := date_trunc('year', partition_date)::DATE;
  end_date := (date_trunc('year', partition_date) + INTERVAL '1 year')::DATE;

  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF location_events_1d FOR VALUES FROM (%L) TO (%L)',
      partition_name, start_date, end_date
    );
  END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. Data Migration from location_events_old & Partition Provisioning
-- ============================================================

DO $$
DECLARE
  day_record RECORD;
  migrated_count BIGINT := 0;
  i INT;
BEGIN
  -- If location_events_old exists, migrate its data
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'location_events_old'
  ) THEN
    -- Dynamically create daily partitions for each distinct day in location_events_old
    FOR day_record IN
      SELECT DISTINCT date_trunc('day', timestamp)::DATE AS part_date
      FROM location_events_old
      WHERE timestamp IS NOT NULL
      ORDER BY part_date
    LOOP
      PERFORM create_daily_partition(day_record.part_date);
    END LOOP;

    -- Migrate all rows into the new partitioned location_events table
    INSERT INTO location_events (
      id,
      client_id,
      room_id,
      latitude,
      longitude,
      altitude,
      accuracy,
      speed,
      timestamp,
      created_at
    )
    SELECT
      id,
      client_id,
      room_id,
      latitude,
      longitude,
      altitude,
      accuracy,
      speed,
      timestamp,
      created_at
    FROM location_events_old;

    GET DIAGNOSTICS migrated_count = ROW_COUNT;
    RAISE NOTICE 'Successfully migrated % rows from location_events_old to location_events', migrated_count;

    -- Drop location_events_old after successful data migration
    DROP TABLE location_events_old;
    RAISE NOTICE 'Dropped table location_events_old';
  END IF;

  -- Pre-provision partitions for today and the next 30 days for location_events
  FOR i IN 0..30 LOOP
    PERFORM create_daily_partition(CURRENT_DATE + (i || ' days')::INTERVAL);
  END LOOP;

  -- Pre-provision downsample partitions
  FOR i IN 0..7 LOOP
    PERFORM create_1m_partition(CURRENT_DATE + (i || ' days')::INTERVAL);
  END LOOP;
  PERFORM create_1h_partition(CURRENT_DATE);
  PERFORM create_1h_partition((date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')::DATE);
  PERFORM create_1d_partition(CURRENT_DATE);
  PERFORM create_1d_partition((date_trunc('year', CURRENT_DATE) + INTERVAL '1 year')::DATE);
END $$;

-- ============================================================
-- 6. Trigger for Auto-Creating Future Partitions
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

DROP TRIGGER IF EXISTS auto_partition_trigger ON location_events;

CREATE TRIGGER auto_partition_trigger
  BEFORE INSERT ON location_events
  FOR EACH ROW
  EXECUTE FUNCTION auto_create_partition_trigger();

-- ============================================================
-- 7. Partition Retention & Drop Helper
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
    SELECT child.relname AS partition_name
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'location_events'
    AND child.relkind = 'r'
  LOOP
    BEGIN
      part_date := to_date(
        replace(r.partition_name, 'location_events_', ''),
        'YYYY_MM_DD'
      );

      IF part_date < cutoff_date THEN
        EXECUTE format('SELECT count(*) FROM %I', r.partition_name) INTO row_count;
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
      CONTINUE;
    END;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 8. Compaction Status Tracking Table
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
