-- Event sourcing schema (issue #246).
-- The events table is the immutable, append-only source of truth. It is
-- range-partitioned by day; daily partitions are created automatically by
-- PostgresEventStore (create_events_daily_partition) — this file documents
-- the same DDL applied at runtime and can be applied out-of-band like
-- 001_create_locations.sql.
--
-- Coexists with the legacy location_events / locations tables: those keep
-- storing raw fixes, while `events` records every domain state change.

CREATE TABLE IF NOT EXISTS events (
  event_id       UUID         NOT NULL,
  event_type     VARCHAR(100) NOT NULL,
  aggregate_id   VARCHAR(255) NOT NULL,
  aggregate_type VARCHAR(50)  NOT NULL,
  sequence       BIGINT       NOT NULL,
  payload        JSONB        NOT NULL,
  metadata       JSONB,
  timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  causation_id   UUID,
  correlation_id UUID
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS events_aggregate_seq_idx ON events (aggregate_id, sequence);
CREATE INDEX IF NOT EXISTS events_correlation_idx   ON events (correlation_id);
CREATE INDEX IF NOT EXISTS events_event_id_idx      ON events (event_id);

CREATE OR REPLACE FUNCTION create_events_daily_partition(partition_date DATE)
RETURNS VOID AS $func$
DECLARE
  partition_name TEXT;
BEGIN
  partition_name := 'events_' || to_char(partition_date, 'YYYY_MM_DD');
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
      partition_name, partition_date, partition_date + INTERVAL '1 day'
    );
  END IF;
END;
$func$ LANGUAGE plpgsql;

DO $$
DECLARE d DATE;
BEGIN
  FOR i IN -1..7 LOOP
    d := CURRENT_DATE + (i || ' days')::INTERVAL;
    PERFORM create_events_daily_partition(d);
  END LOOP;
END $$;

-- One snapshot per aggregate, written asynchronously every 1000 events so
-- reconstruction loads a snapshot plus the delta instead of full replay.
CREATE TABLE IF NOT EXISTS snapshots (
  aggregate_id   VARCHAR(255) PRIMARY KEY,
  aggregate_type VARCHAR(50)  NOT NULL,
  sequence       BIGINT       NOT NULL,
  state          JSONB        NOT NULL,
  timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── Materialised read models maintained by ProjectionManager ────────────────

CREATE TABLE IF NOT EXISTS vehicle_current_state (
  client_id           VARCHAR(255) PRIMARY KEY,
  rooms               TEXT[]       NOT NULL DEFAULT '{}',
  latitude            DOUBLE PRECISION,
  longitude           DOUBLE PRECISION,
  altitude            DOUBLE PRECISION,
  accuracy            DOUBLE PRECISION,
  speed               DOUBLE PRECISION,
  location_updated_at TIMESTAMPTZ,
  last_event_id       UUID,
  last_sequence       BIGINT,
  updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS vehicle_current_state_rooms_idx
  ON vehicle_current_state USING GIN (rooms);

CREATE TABLE IF NOT EXISTS room_membership (
  room_id    VARCHAR(255) NOT NULL,
  client_id  VARCHAR(255) NOT NULL,
  joined_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room_id, client_id)
);

CREATE TABLE IF NOT EXISTS geofence_violations (
  event_id       UUID PRIMARY KEY,
  client_id      VARCHAR(255) NOT NULL,
  fence_id       VARCHAR(255),
  room_id        VARCHAR(255),
  violation_type VARCHAR(10)  NOT NULL,
  occurred_at    TIMESTAMPTZ  NOT NULL,
  correlation_id UUID
);

CREATE INDEX IF NOT EXISTS geofence_violations_client_time_idx
  ON geofence_violations (client_id, occurred_at);

CREATE TABLE IF NOT EXISTS message_sequence (
  seq_key       VARCHAR(320) PRIMARY KEY,
  sequence      BIGINT       NOT NULL DEFAULT 0,
  last_event_id UUID,
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
  name          VARCHAR(100) PRIMARY KEY,
  last_event_id UUID,
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
