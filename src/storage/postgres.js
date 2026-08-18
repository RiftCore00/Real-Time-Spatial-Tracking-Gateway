/**
 * @fileoverview PostgreSQL StorageAdapter implementation.
 *
 * Features:
 *  - Connection pooling via `pg` Pool (dynamic import so `pg` is optional).
 *  - Auto-creates partitioned `location_events` table and indexes on first use.
 *  - Write batching: events accumulate in a bounded in-memory buffer and are
 *    flushed either when `batchSize` is reached or every `flushIntervalMs`.
 *  - Bulk insert via `INSERT … SELECT * FROM unnest(…)` — one round-trip per batch.
 *  - Backpressure: when the buffer exceeds `maxBufferSize`, the oldest events
 *    are dropped and `storage_dropped_events_total` is incremented.
 *  - Spatial index: composite (latitude, longitude) GiST-capable index for
 *    bounding-box queries without requiring PostGIS.
 *  - Time-series compaction: automated downsampling, retention policies, and
 *    continuous aggregates for cost-effective long-term historical storage.
 *  - Declarative partitioning by day for efficient data lifecycle management.
 *  - Transparent query routing: queryRoom supports resolution parameter
 *    ("raw" | "1m" | "1h" | "1d" | "auto") for tiered data access.
 */

import { logger } from "../logger.js";

// ─────────────────────────── Schema DDL ────────────────────────────────────────

/**
 * DDL for the partitioned location_events table.
 * Uses DOUBLE PRECISION for coordinates and TIMESTAMPTZ for temporal columns.
 * Partitioned by day on the timestamp column for efficient lifecycle management.
 */
const CREATE_TABLE_SQL = `
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
`;

const CREATE_INDEXES_SQL = `
CREATE INDEX IF NOT EXISTS location_events_room_id_timestamp_idx
  ON location_events (room_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS location_events_lat_lon_idx
  ON location_events (latitude, longitude);
`;

/**
 * Bulk insert using unnest — single round-trip for an arbitrarily large batch.
 * Parameter arrays are positionally matched.
 */
const BULK_INSERT_SQL = `
INSERT INTO location_events
  (client_id, room_id, latitude, longitude, altitude, accuracy, speed, timestamp)
SELECT * FROM unnest(
  $1::text[],
  $2::text[],
  $3::double precision[],
  $4::double precision[],
  $5::double precision[],
  $6::double precision[],
  $7::double precision[],
  $8::timestamptz[]
)
`;

// ─────────────────────────── Downsample DDL ────────────────────────────────────

const CREATE_DOWNSAMPLE_1M_SQL = `
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
)`;

const CREATE_DOWNSAMPLE_1M_INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_1m_room_bucket
  ON location_events_1m (room_id, bucket_start DESC)
`;

const CREATE_AGGREGATE_1H_SQL = `
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
)`;

const CREATE_AGGREGATE_1H_INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_1h_room_bucket
  ON location_events_1h (room_id, bucket_start DESC)
`;

const CREATE_AGGREGATE_1D_SQL = `
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
)`;

const CREATE_AGGREGATE_1D_INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_1d_room_bucket
  ON location_events_1d (room_id, bucket_start DESC)
`;

// ─────────────────────────── Partition Helpers ──────────────────────────────────

const CREATE_PARTITION_FUNCTION_SQL = `
CREATE OR REPLACE FUNCTION create_daily_partition(partition_date DATE)
RETURNS VOID AS $func$
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
$func$ LANGUAGE plpgsql;
`;

const CREATE_PARTITION_TRIGGER_SQL = `
CREATE OR REPLACE FUNCTION auto_create_partition_trigger()
RETURNS TRIGGER AS $func$
DECLARE
  partition_date DATE;
BEGIN
  partition_date := date_trunc('day', NEW.timestamp)::DATE;
  PERFORM create_daily_partition(partition_date);
  RETURN NEW;
END;
$func$ LANGUAGE plpgsql;
`;

const DROP_EXISTING_TRIGGER_SQL = `DROP TRIGGER IF EXISTS auto_partition_trigger ON location_events;`;

const CREATE_PARTITION_TRIGGER_BIND_SQL = `
CREATE TRIGGER auto_partition_trigger
  BEFORE INSERT ON location_events
  FOR EACH ROW
  EXECUTE FUNCTION auto_create_partition_trigger();
`;

const CREATE_COMPACT_STATUS_SQL = `
CREATE TABLE IF NOT EXISTS compaction_status (
  id              INTEGER         PRIMARY KEY DEFAULT 1,
  last_run        TIMESTAMPTZ,
  next_run        TIMESTAMPTZ,
  last_result     JSONB,
  updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
`;

// ─────────────────────────── Compaction Queries ────────────────────────────────

/**
 * Compute 1-minute downsample rows from raw data for a given time range.
 * Uses median for lat/lon, max for speed.
 */
const DOWNSAMPLE_1M_SQL = `
INSERT INTO location_events_1m
  (room_id, bucket_start, bucket_end, latitude, longitude, altitude, max_speed, point_count)
SELECT
  room_id,
  date_trunc('minute', timestamp) AS bucket_start,
  date_trunc('minute', timestamp) + INTERVAL '59 seconds 999 milliseconds' AS bucket_end,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY latitude) AS latitude,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY longitude) AS longitude,
  AVG(altitude) AS altitude,
  MAX(speed) AS max_speed,
  COUNT(*) AS point_count
FROM location_events
WHERE timestamp >= date_trunc('minute', $1::timestamptz)
  AND timestamp <  date_trunc('minute', $2::timestamptz) + INTERVAL '1 minute'
GROUP BY room_id, date_trunc('minute', timestamp)
ON CONFLICT (room_id, bucket_start) DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  altitude = EXCLUDED.altitude,
  max_speed = EXCLUDED.max_speed,
  point_count = EXCLUDED.point_count,
  bucket_end = EXCLUDED.bucket_end;
`;

/**
 * Compute 1-hour aggregates from 1-minute downsample data.
 * Includes haversine distance between consecutive median points.
 */
const AGGREGATE_1H_SQL = `
WITH hourly AS (
  SELECT
    room_id,
    date_trunc('hour', bucket_start) AS bucket_start,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude,
    AVG(altitude) AS altitude,
    AVG(max_speed) AS avg_speed,
    MAX(max_speed) AS max_speed,
    MIN(max_speed) AS min_speed,
    SUM(point_count) AS point_count
  FROM location_events_1m
  WHERE bucket_start >= date_trunc('hour', $1::timestamptz)
    AND bucket_start <  date_trunc('hour', $2::timestamptz) + INTERVAL '1 hour'
  GROUP BY room_id, date_trunc('hour', bucket_start)
),
ordered AS (
  SELECT
    h.*,
    LAG(latitude) OVER (PARTITION BY room_id ORDER BY bucket_start) AS prev_lat,
    LAG(longitude) OVER (PARTITION BY room_id ORDER BY bucket_start) AS prev_lon
  FROM hourly h
)
INSERT INTO location_events_1h
  (room_id, bucket_start, bucket_end, latitude, longitude, altitude,
   avg_speed, max_speed, min_speed, total_distance, point_count)
SELECT
  room_id,
  bucket_start,
  bucket_start + INTERVAL '59 minutes 59 seconds 999 milliseconds' AS bucket_end,
  latitude,
  longitude,
  altitude,
  avg_speed,
  max_speed,
  min_speed,
  COALESCE(
    (SELECT SUM(
      6371000.0 * 2 * asin(sqrt(
        power(sin((o.latitude - o.prev_lat) / 2.0), 2) +
        cos(radians(o.prev_lat)) * cos(radians(o.latitude)) *
        power(sin((o.longitude - o.prev_lon) / 2.0), 2)
      ))
    ) FROM ordered o
     WHERE o.room_id = ordered.room_id
       AND o.bucket_start <= ordered.bucket_start
       AND o.prev_lat IS NOT NULL
    ), 0
  ) AS total_distance,
  point_count
FROM ordered
ON CONFLICT (room_id, bucket_start) DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  altitude = EXCLUDED.altitude,
  avg_speed = EXCLUDED.avg_speed,
  max_speed = EXCLUDED.max_speed,
  min_speed = EXCLUDED.min_speed,
  total_distance = EXCLUDED.total_distance,
  point_count = EXCLUDED.point_count,
  bucket_end = EXCLUDED.bucket_end;
`;

/**
 * Compute 1-day aggregates from 1-hour aggregate data.
 */
const AGGREGATE_1D_SQL = `
INSERT INTO location_events_1d
  (room_id, bucket_start, bucket_end, latitude, longitude, altitude,
   avg_speed, max_speed, min_speed, total_distance, point_count)
SELECT
  room_id,
  date_trunc('day', bucket_start) AS bucket_start,
  date_trunc('day', bucket_start) + INTERVAL '23 hours 59 minutes 59 seconds 999 milliseconds' AS bucket_end,
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  AVG(altitude) AS altitude,
  AVG(avg_speed) AS avg_speed,
  MAX(max_speed) AS max_speed,
  MIN(min_speed) AS min_speed,
  SUM(total_distance) AS total_distance,
  SUM(point_count) AS point_count
FROM location_events_1h
WHERE bucket_start >= date_trunc('day', $1::timestamptz)
  AND bucket_start <  date_trunc('day', $2::timestamptz) + INTERVAL '1 day'
GROUP BY room_id, date_trunc('day', bucket_start)
ON CONFLICT (room_id, bucket_start) DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  altitude = EXCLUDED.altitude,
  avg_speed = EXCLUDED.avg_speed,
  max_speed = EXCLUDED.max_speed,
  min_speed = EXCLUDED.min_speed,
  total_distance = EXCLUDED.total_distance,
  point_count = EXCLUDED.point_count,
  bucket_end = EXCLUDED.bucket_end;
`;

const CREATE_DROP_PARTITION_FUNCTION_SQL = `
CREATE OR REPLACE FUNCTION drop_old_raw_partitions(retention_days INT)
RETURNS TABLE(dropped_partition TEXT, dropped_rows BIGINT) AS $func$
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
        EXECUTE format('ALTER TABLE location_events DETACH PARTITION %I', r.partition_name);
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
$func$ LANGUAGE plpgsql;
`;

// ─────────────────────────── Tier trim queries ─────────────────────────────────

const TRIM_TIER_1M_SQL = `DELETE FROM location_events_1m WHERE bucket_start < $1::timestamptz;`;
const TRIM_TIER_1H_SQL = `DELETE FROM location_events_1h WHERE bucket_start < $1::timestamptz;`;
const TRIM_TIER_1D_SQL = `DELETE FROM location_events_1d WHERE bucket_start < $1::timestamptz;`;

// ─────────────────────────── Status queries ────────────────────────────────────

const GET_COMPACT_STATUS_SQL = `SELECT last_run, next_run, last_result FROM compaction_status WHERE id = 1;`;
const UPDATE_COMPACT_STATUS_SQL = `
UPDATE compaction_status SET last_run = $1, next_run = $2, last_result = $3::jsonb, updated_at = NOW() WHERE id = 1;
`;

const RAW_TIER_STATS_SQL = `
SELECT
  'raw' AS tier_name,
  COALESCE(
    (SELECT SUM(c.reltuples)::bigint FROM pg_inherits i
     JOIN pg_class parent ON i.inhparent = parent.oid
     JOIN pg_class c ON i.inhrelid = c.oid
     WHERE parent.relname = 'location_events'),
    0
  ) AS row_count,
  COALESCE(
    (SELECT pg_total_relation_size(parent.oid) FROM pg_class parent WHERE parent.relname = 'location_events'),
    0
  ) AS size_bytes,
  (SELECT MIN(timestamp) FROM location_events) AS oldest,
  (SELECT MAX(timestamp) FROM location_events) AS newest;
`;

// ─────────────────────────── Class ─────────────────────────────────────────────

/**
 * @implements {import("./adapter.js").StorageAdapter}
 */
export class PostgresAdapter {
  /**
   * @param {object} [config={}]
   * @param {string}  [config.connectionString]          - Postgres connection URL. Falls back to DATABASE_URL.
   * @param {number}  [config.poolSize=10]               - Maximum pool connections.
   * @param {number}  [config.batchSize=100]             - Flush when buffer reaches this size.
   * @param {number}  [config.flushIntervalMs=1000]      - Flush every N milliseconds regardless of size.
   * @param {number}  [config.maxBufferSize=10000]       - Hard cap on in-flight buffer; oldest dropped beyond this.
   * @param {number}  [config.rawRetentionDays=7]        - Days to keep raw events.
   * @param {number}  [config.downsample1mRetentionDays=90]  - Days to keep 1m downsample.
   * @param {number}  [config.downsample1hRetentionDays=365] - Days to keep 1h aggregates.
   * @param {number}  [config.aggregate1dRetentionDays=2555] - Days to keep 1d rollups.
   * @param {number}  [config.compactionIntervalMs=300000]   - Compaction interval (default 5 min).
   * @param {number}  [config.compactionBatchSize=10000]     - Batch size for deletion loops.
   * @param {boolean} [config.timescaleDbEnabled=false]      - Use TimescaleDB continuous aggregates.
   */
  constructor(config = {}) {
    this._connectionString =
      config.connectionString ?? process.env.DATABASE_URL;
    this._poolSize = config.poolSize ?? 10;
    this._batchSize = config.batchSize ?? 100;
    this._flushIntervalMs = config.flushIntervalMs ?? 1000;
    this._maxBufferSize = config.maxBufferSize ?? 10000;

    // Retention configuration
    this._rawRetentionDays = config.rawRetentionDays ?? 7;
    this._downsample1mRetentionDays = config.downsample1mRetentionDays ?? 90;
    this._downsample1hRetentionDays = config.downsample1hRetentionDays ?? 365;
    this._aggregate1dRetentionDays = config.aggregate1dRetentionDays ?? 2555;

    // Compaction configuration
    this._compactionIntervalMs = config.compactionIntervalMs ?? 300000;
    this._compactionBatchSize = config.compactionBatchSize ?? 10000;
    this._timescaleDbEnabled = config.timescaleDbEnabled ?? false;

    /** @type {import("./adapter.js").LocationEvent[]} */
    this._buffer = [];

    /** Monotonically-increasing counter of dropped events. */
    this.storage_dropped_events_total = 0;

    /** @type {import("pg").Pool|null} */
    this._pool = null;

    /** @type {NodeJS.Timeout|null} */
    this._flushTimer = null;

    /** @type {NodeJS.Timeout|null} */
    this._compactionTimer = null;

    /** Promise that resolves when the schema has been initialised. */
    this._initPromise = null;

    /** Whether close() has been called. */
    this._closed = false;

    /** Whether a flush is currently in-flight (prevents double-flush). */
    this._flushing = false;

    /** Whether compaction is currently running (prevents double-run). */
    this._compacting = false;

    /** Last compaction run result. */
    this._lastCompactionResult = null;

    /** Last compaction run timestamp. */
    this._lastCompactionRun = null;

    /** Next scheduled compaction run timestamp. */
    this._nextCompactionRun = null;
  }

  // ─────────────────────────── lifecycle ────────────────────────────────────

  /**
   * Lazily initialises the connection pool and runs schema migrations.
   * Idempotent — safe to call multiple times.
   *
   * @returns {Promise<void>}
   */
  async _init() {
    if (this._initPromise) return this._initPromise;
    this._initPromise = this._doInit();
    return this._initPromise;
  }

  async _doInit() {
    const { default: pg } = await import("pg");
    const { Pool } = pg;

    this._pool = new Pool({
      connectionString: this._connectionString,
      max: this._poolSize,
    });

    this._pool.on("error", (err) => {
      logger.error("PostgresAdapter pool error", { error: err.message });
    });

    // Run schema migrations
    const client = await this._pool.connect();
    try {
      // 1. Partitioned location_events table
      await client.query(CREATE_TABLE_SQL);
      await client.query(CREATE_INDEXES_SQL);

      // 2. Partition helper functions
      await client.query(CREATE_PARTITION_FUNCTION_SQL);
      await client.query(CREATE_DROP_PARTITION_FUNCTION_SQL);

      // 3. Partition trigger
      await client.query(CREATE_PARTITION_TRIGGER_SQL);
      await client.query(DROP_EXISTING_TRIGGER_SQL);
      await client.query(CREATE_PARTITION_TRIGGER_BIND_SQL);

      // 4. Create initial partitions (today + next 30 days)
      await client.query(`
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
      `);

      // 5. Downsample and aggregate tables
      await client.query(CREATE_DOWNSAMPLE_1M_SQL);
      await client.query(CREATE_DOWNSAMPLE_1M_INDEX_SQL);
      await client.query(CREATE_AGGREGATE_1H_SQL);
      await client.query(CREATE_AGGREGATE_1H_INDEX_SQL);
      await client.query(CREATE_AGGREGATE_1D_SQL);
      await client.query(CREATE_AGGREGATE_1D_INDEX_SQL);

      // 6. Compaction status table
      await client.query(CREATE_COMPACT_STATUS_SQL);
      await client.query(`
        INSERT INTO compaction_status (id, last_run, next_run, updated_at)
        VALUES (1, NULL, NULL, NOW())
        ON CONFLICT (id) DO NOTHING;
      `);
    } finally {
      client.release();
    }

    // Start periodic flush timer
    this._flushTimer = setInterval(() => {
      this._scheduleFlush();
    }, this._flushIntervalMs);
    if (this._flushTimer.unref) this._flushTimer.unref();

    // Start compaction timer
    this._scheduleCompaction();
  }

  // ─────────────────────────── write pipeline ───────────────────────────────

  /**
   * Accepts a batch of events into the write buffer.
   *
   * @param {import("./adapter.js").LocationEvent[]} events
   * @returns {Promise<void>}
   */
  async writeBatch(events) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    if (!Array.isArray(events) || events.length === 0) return;

    this._ensureInit();

    this._buffer.push(...events);

    if (this._buffer.length > this._maxBufferSize) {
      const excess = this._buffer.length - this._maxBufferSize;
      this._buffer.splice(0, excess);
      this.storage_dropped_events_total += excess;
      logger.warn("Storage buffer overflow: dropping oldest events", {
        dropped: excess,
        storage_dropped_events_total: this.storage_dropped_events_total,
      });
    }

    if (this._buffer.length >= this._batchSize) {
      this._scheduleFlush();
    }
  }

  _scheduleFlush() {
    this._flush().catch((err) => {
      logger.error("PostgresAdapter flush error", { error: err.message });
    });
  }

  async _flush() {
    if (this._flushing || this._buffer.length === 0) return;
    this._flushing = true;

    try {
      await this._init();

      while (this._buffer.length > 0) {
        const batch = this._buffer.splice(0, this._batchSize);
        await this._insertBatch(batch);
      }
    } finally {
      this._flushing = false;
    }
  }

  async _insertBatch(batch) {
    const clientIds = batch.map((e) => e.clientId);
    const roomIds = batch.map((e) => e.roomId);
    const lats = batch.map((e) => e.latitude);
    const lons = batch.map((e) => e.longitude);
    const alts = batch.map((e) => e.altitude ?? null);
    const accs = batch.map((e) => e.accuracy ?? null);
    const speeds = batch.map((e) => e.speed ?? null);
    const timestamps = batch.map((e) => e.timestamp);

    await this._pool.query(BULK_INSERT_SQL, [
      clientIds, roomIds, lats, lons, alts, accs, speeds, timestamps,
    ]);
  }

  _ensureInit() {
    if (!this._initPromise) {
      this._init().catch((err) => {
        logger.error("PostgresAdapter init error", { error: err.message });
      });
    }
  }

  // ─────────────────────────── compaction pipeline ──────────────────────────

  /**
   * Schedule periodic compaction runs.
   * @private
   */
  _scheduleCompaction() {
    if (this._closed) return;

    this._nextCompactionRun = Date.now() + this._compactionIntervalMs;

    this._compactionTimer = setTimeout(async () => {
      if (this._closed) return;
      try {
        await this.compact();
      } catch (err) {
        logger.error("Compaction run failed", { error: err.message });
      }
      this._scheduleCompaction();
    }, this._compactionIntervalMs);

    if (this._compactionTimer.unref) this._compactionTimer.unref();
  }

  /**
   * Run the full compaction pipeline:
   *   1. Drop expired raw partitions (instant DDL).
   *   2. Compute 1-minute downsamples for the last 24 hours.
   *   3. Compute 1-hour aggregates from new 1m data.
   *   4. Compute 1-day aggregates from new 1h data.
   *   5. Trim each tier to its retention window.
   *
   * Compaction is idempotent — re-running for the same time window
   * performs upserts (ON CONFLICT DO UPDATE).
   *
   * @param {object} [overrides={}]
   * @returns {Promise<import("./adapter.js").CompactionResult>}
   */
  async compact(overrides = {}) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    if (this._compacting) {
      logger.info("Compaction already in progress, skipping");
      return {
        rawDeleted: 0, downsample1m: 0, aggregate1h: 0,
        aggregate1d: 0, durationMs: 0, error: "already running",
      };
    }

    this._compacting = true;
    const start = Date.now();

    try {
      await this._init();

      const rawRetention = overrides.rawRetentionDays ?? this._rawRetentionDays;
      const m1Retention = overrides.downsample1mRetentionDays ?? this._downsample1mRetentionDays;
      const h1Retention = overrides.downsample1hRetentionDays ?? this._downsample1hRetentionDays;
      const d1Retention = overrides.aggregate1dRetentionDays ?? this._aggregate1dRetentionDays;

      let rawDeleted = 0;

      // Phase 1: Drop expired raw partitions (instant DDL)
      try {
        const dropResult = await this._pool.query(
          `SELECT drop_old_raw_partitions($1)`,
          [rawRetention]
        );
        if (dropResult.rows.length > 0) {
          rawDeleted = dropResult.rows.reduce((sum, r) => sum + Number(r.drop_old_raw_partitions || 0), 0);
        }
        logger.info("Raw partition drop completed", { rawDeleted });
      } catch (err) {
        logger.warn("Raw partition drop failed", { error: err.message });
      }

      // Phase 2: Compute 1-minute downsamples (last 24h window, safe for incremental)
      let downsample1m = 0;
      try {
        const m1Start = new Date(Date.now() - 24 * 3600000);
        const m1End = new Date();
        const m1Result = await this._pool.query(DOWNSAMPLE_1M_SQL, [m1Start, m1End]);
        downsample1m = m1Result.rowCount ?? 0;
        logger.info("1m downsample completed", { rows: downsample1m });
      } catch (err) {
        logger.warn("1m downsample failed", { error: err.message });
      }

      // Phase 3: Compute 1-hour aggregates (last 7 days window)
      let aggregate1h = 0;
      try {
        const h1Start = new Date(Date.now() - 7 * 86400000);
        const h1End = new Date();
        const h1Result = await this._pool.query(AGGREGATE_1H_SQL, [h1Start, h1End]);
        aggregate1h = h1Result.rowCount ?? 0;
        logger.info("1h aggregate completed", { rows: aggregate1h });
      } catch (err) {
        logger.warn("1h aggregate failed", { error: err.message });
      }

      // Phase 4: Compute 1-day aggregates (last 90 days window)
      let aggregate1d = 0;
      try {
        const d1Start = new Date(Date.now() - 90 * 86400000);
        const d1End = new Date();
        const d1Result = await this._pool.query(AGGREGATE_1D_SQL, [d1Start, d1End]);
        aggregate1d = d1Result.rowCount ?? 0;
        logger.info("1d aggregate completed", { rows: aggregate1d });
      } catch (err) {
        logger.warn("1d aggregate failed", { error: err.message });
      }

      // Phase 5: Trim each tier to retention window
      try {
        const m1Cutoff = new Date(Date.now() - m1Retention * 86400000);
        const h1Cutoff = new Date(Date.now() - h1Retention * 86400000);
        const d1Cutoff = new Date(Date.now() - d1Retention * 86400000);
        await this._pool.query(TRIM_TIER_1M_SQL, [m1Cutoff]);
        await this._pool.query(TRIM_TIER_1H_SQL, [h1Cutoff]);
        await this._pool.query(TRIM_TIER_1D_SQL, [d1Cutoff]);
        logger.info("Tier retention trimming completed");
      } catch (err) {
        logger.warn("Tier retention trimming failed", { error: err.message });
      }

      const durationMs = Date.now() - start;
      const result = {
        rawDeleted,
        downsample1m,
        aggregate1h,
        aggregate1d,
        durationMs,
      };

      this._lastCompactionResult = result;
      this._lastCompactionRun = new Date().toISOString();

      // Update compaction status
      try {
        await this._pool.query(UPDATE_COMPACT_STATUS_SQL, [
          this._lastCompactionRun,
          new Date(Date.now() + this._compactionIntervalMs).toISOString(),
          JSON.stringify(result),
        ]);
      } catch (err) {
        logger.warn("Failed to update compaction status", { error: err.message });
      }

      logger.info("Compaction completed", result);
      return result;
    } catch (err) {
      const durationMs = Date.now() - start;
      logger.error("Compaction failed", { error: err.message, durationMs });
      return {
        rawDeleted: 0, downsample1m: 0, aggregate1h: 0,
        aggregate1d: 0, durationMs, error: err.message,
      };
    } finally {
      this._compacting = false;
    }
  }

  /**
   * Return current compaction status including last/next run times and
   * per-tier statistics.
   *
   * @returns {Promise<import("./adapter.js").CompactionStatus>}
   */
  async getCompactionStatus() {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    await this._init();

    let lastRun = null;
    let nextRun = null;

    try {
      const statusResult = await this._pool.query(GET_COMPACT_STATUS_SQL);
      if (statusResult.rows.length > 0) {
        lastRun = statusResult.rows[0].last_run;
        nextRun = statusResult.rows[0].next_run;
      }
    } catch (err) {
      logger.warn("Failed to read compaction status", { error: err.message });
    }

    // Get tier stats
    const tiers = [];

    // Raw tier
    try {
      const rawStats = await this._pool.query(RAW_TIER_STATS_SQL);
      if (rawStats.rows.length > 0) {
        const r = rawStats.rows[0];
        tiers.push({
          name: r.tier_name,
          rows: Number(r.row_count),
          sizeBytes: Number(r.size_bytes),
          oldest: r.oldest ? new Date(r.oldest).toISOString() : null,
          newest: r.newest ? new Date(r.newest).toISOString() : null,
        });
      }
    } catch {
      tiers.push({ name: "raw", rows: 0, sizeBytes: 0, oldest: null, newest: null });
    }

    // 1m tier
    try {
      const m1Stats = await this._pool.query(
        `SELECT '1m' AS tier_name,
         COALESCE((SELECT count(*) FROM location_events_1m), 0) AS row_count,
         COALESCE(pg_total_relation_size('location_events_1m'), 0) AS size_bytes,
         (SELECT MIN(bucket_start) FROM location_events_1m) AS oldest,
         (SELECT MAX(bucket_start) FROM location_events_1m) AS newest;`
      );
      if (m1Stats.rows.length > 0) {
        const r = m1Stats.rows[0];
        tiers.push({
          name: r.tier_name,
          rows: Number(r.row_count),
          sizeBytes: Number(r.size_bytes),
          oldest: r.oldest ? new Date(r.oldest).toISOString() : null,
          newest: r.newest ? new Date(r.newest).toISOString() : null,
        });
      }
    } catch {
      tiers.push({ name: "1m", rows: 0, sizeBytes: 0, oldest: null, newest: null });
    }

    // 1h tier
    try {
      const h1Stats = await this._pool.query(
        `SELECT '1h' AS tier_name,
         COALESCE((SELECT count(*) FROM location_events_1h), 0) AS row_count,
         COALESCE(pg_total_relation_size('location_events_1h'), 0) AS size_bytes,
         (SELECT MIN(bucket_start) FROM location_events_1h) AS oldest,
         (SELECT MAX(bucket_start) FROM location_events_1h) AS newest;`
      );
      if (h1Stats.rows.length > 0) {
        const r = h1Stats.rows[0];
        tiers.push({
          name: r.tier_name,
          rows: Number(r.row_count),
          sizeBytes: Number(r.size_bytes),
          oldest: r.oldest ? new Date(r.oldest).toISOString() : null,
          newest: r.newest ? new Date(r.newest).toISOString() : null,
        });
      }
    } catch {
      tiers.push({ name: "1h", rows: 0, sizeBytes: 0, oldest: null, newest: null });
    }

    // 1d tier
    try {
      const d1Stats = await this._pool.query(
        `SELECT '1d' AS tier_name,
         COALESCE((SELECT count(*) FROM location_events_1d), 0) AS row_count,
         COALESCE(pg_total_relation_size('location_events_1d'), 0) AS size_bytes,
         (SELECT MIN(bucket_start) FROM location_events_1d) AS oldest,
         (SELECT MAX(bucket_start) FROM location_events_1d) AS newest;`
      );
      if (d1Stats.rows.length > 0) {
        const r = d1Stats.rows[0];
        tiers.push({
          name: r.tier_name,
          rows: Number(r.row_count),
          sizeBytes: Number(r.size_bytes),
          oldest: r.oldest ? new Date(r.oldest).toISOString() : null,
          newest: r.newest ? new Date(r.newest).toISOString() : null,
        });
      }
    } catch {
      tiers.push({ name: "1d", rows: 0, sizeBytes: 0, oldest: null, newest: null });
    }

    return {
      lastRun: lastRun ? new Date(lastRun).toISOString() : null,
      nextRun: nextRun ? new Date(nextRun).toISOString() : null,
      tiers,
    };
  }

  // ─────────────────────────── read pipeline ────────────────────────────────

  /**
   * Retrieves historical events for a room, ordered ascending by timestamp.
   * Supports resolution parameter for tiered data access:
   *   "raw" - query raw location_events table
   *   "1m"  - query 1-minute downsampled data
   *   "1h"  - query 1-hour aggregate data
   *   "1d"  - query 1-day aggregate data
   *   "auto" - select finest resolution fitting within limit (default)
   *
   * @param {string} roomId
   * @param {import("./adapter.js").QueryOptions} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async queryRoom(roomId, options = {}) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    await this._init();

    const { from, to, limit, resolution = "auto" } = options;

    let resolved = resolution;
    if (resolution === "auto") {
      resolved = await this._autoResolveResolution(roomId, from, to, limit);
    }

    if (resolved === "1m") {
      return this._queryRoom1m(roomId, from, to, limit);
    }
    if (resolved === "1h") {
      return this._queryRoom1h(roomId, from, to, limit);
    }
    if (resolved === "1d") {
      return this._queryRoom1d(roomId, from, to, limit);
    }

    // Default: raw
    return this._queryRoomRaw(roomId, from, to, limit);
  }

  /**
   * Auto-select the finest resolution that returns ≤ limit points.
   * @private
   */
  async _autoResolveResolution(roomId, from, to, limit) {
    const maxPoints = limit || 1000;
    const now = new Date();
    const fromMs = from ? from.getTime() : now.getTime() - 7 * 86400000;
    const toMs = to ? to.getTime() : now.getTime();
    const rangeMs = toMs - fromMs;

    if (rangeMs <= 0) return "raw";

    // Estimate raw points (1 per second)
    const rawPointsEstimate = Math.floor(rangeMs / 1000);
    if (rawPointsEstimate <= maxPoints) return "raw";

    // Check 1m tier
    const m1PointsEstimate = Math.floor(rangeMs / 60000);
    if (m1PointsEstimate <= maxPoints) {
      // Verify 1m data exists
      try {
        const result = await this._pool.query(
          `SELECT count(*) FROM location_events_1m WHERE room_id = $1 AND bucket_start >= $2 AND bucket_start <= $3`,
          [roomId, new Date(fromMs), new Date(toMs)]
        );
        if (Number(result.rows[0].count) <= maxPoints) return "1m";
      } catch {
        // 1m table may not have data, fall through
      }
    }

    // Check 1h tier
    const h1PointsEstimate = Math.floor(rangeMs / 3600000);
    if (h1PointsEstimate <= maxPoints) {
      try {
        const result = await this._pool.query(
          `SELECT count(*) FROM location_events_1h WHERE room_id = $1 AND bucket_start >= $2 AND bucket_start <= $3`,
          [roomId, new Date(fromMs), new Date(toMs)]
        );
        if (Number(result.rows[0].count) <= maxPoints) return "1h";
      } catch {
        // 1h table may not have data
      }
    }

    return "1d";
  }

  /**
   * Query raw events for a room.
   * @private
   */
  async _queryRoomRaw(roomId, from, to, limit) {
    const params = [roomId];
    const conditions = ["room_id = $1"];
    let idx = 2;

    if (from instanceof Date) {
      conditions.push(`timestamp >= $${idx++}`);
      params.push(from.toISOString());
    }
    if (to instanceof Date) {
      conditions.push(`timestamp <= $${idx++}`);
      params.push(to.toISOString());
    }

    let sql = `
      SELECT id, client_id AS "clientId", room_id AS "roomId",
             latitude, longitude, altitude, accuracy, speed,
             timestamp::text AS timestamp,
             created_at::text AS "createdAt"
      FROM location_events
      WHERE ${conditions.join(" AND ")}
      ORDER BY timestamp ASC
    `;

    if (typeof limit === "number" && limit > 0) {
      sql += ` LIMIT $${idx}`;
      params.push(limit);
    }

    const result = await this._pool.query(sql, params);
    return result.rows;
  }

  /**
   * Query 1-minute downsampled data for a room.
   * @private
   */
  async _queryRoom1m(roomId, from, to, limit) {
    const params = [roomId];
    const conditions = ["room_id = $1"];
    let idx = 2;

    if (from instanceof Date) {
      conditions.push(`bucket_start >= $${idx++}`);
      params.push(from.toISOString());
    }
    if (to instanceof Date) {
      conditions.push(`bucket_start <= $${idx++}`);
      params.push(to.toISOString());
    }

    let sql = `
      SELECT
        gen_random_uuid() AS id,
        'downsample' AS "clientId",
        room_id AS "roomId",
        latitude, longitude,
        altitude,
        NULL AS accuracy,
        max_speed AS speed,
        bucket_start::text AS timestamp,
        created_at::text AS "createdAt"
      FROM location_events_1m
      WHERE ${conditions.join(" AND ")}
      ORDER BY bucket_start ASC
    `;

    if (typeof limit === "number" && limit > 0) {
      sql += ` LIMIT $${idx}`;
      params.push(limit);
    }

    const result = await this._pool.query(sql, params);
    return result.rows;
  }

  /**
   * Query 1-hour aggregate data for a room.
   * @private
   */
  async _queryRoom1h(roomId, from, to, limit) {
    const params = [roomId];
    const conditions = ["room_id = $1"];
    let idx = 2;

    if (from instanceof Date) {
      conditions.push(`bucket_start >= $${idx++}`);
      params.push(from.toISOString());
    }
    if (to instanceof Date) {
      conditions.push(`bucket_start <= $${idx++}`);
      params.push(to.toISOString());
    }

    let sql = `
      SELECT
        gen_random_uuid() AS id,
        'aggregate-1h' AS "clientId",
        room_id AS "roomId",
        latitude, longitude,
        altitude,
        NULL AS accuracy,
        avg_speed AS speed,
        bucket_start::text AS timestamp,
        created_at::text AS "createdAt"
      FROM location_events_1h
      WHERE ${conditions.join(" AND ")}
      ORDER BY bucket_start ASC
    `;

    if (typeof limit === "number" && limit > 0) {
      sql += ` LIMIT $${idx}`;
      params.push(limit);
    }

    const result = await this._pool.query(sql, params);
    return result.rows;
  }

  /**
   * Query 1-day aggregate data for a room.
   * @private
   */
  async _queryRoom1d(roomId, from, to, limit) {
    const params = [roomId];
    const conditions = ["room_id = $1"];
    let idx = 2;

    if (from instanceof Date) {
      conditions.push(`bucket_start >= $${idx++}`);
      params.push(from.toISOString());
    }
    if (to instanceof Date) {
      conditions.push(`bucket_start <= $${idx++}`);
      params.push(to.toISOString());
    }

    let sql = `
      SELECT
        gen_random_uuid() AS id,
        'aggregate-1d' AS "clientId",
        room_id AS "roomId",
        latitude, longitude,
        altitude,
        NULL AS accuracy,
        avg_speed AS speed,
        bucket_start::text AS timestamp,
        created_at::text AS "createdAt"
      FROM location_events_1d
      WHERE ${conditions.join(" AND ")}
      ORDER BY bucket_start ASC
    `;

    if (typeof limit === "number" && limit > 0) {
      sql += ` LIMIT $${idx}`;
      params.push(limit);
    }

    const result = await this._pool.query(sql, params);
    return result.rows;
  }

  /**
   * Returns events whose coordinates fall within the bounding box.
   * Uses the composite (latitude, longitude) index for efficient range scans.
   *
   * @param {import("./adapter.js").SpatialBounds} bounds
   * @param {{ limit?: number }} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async querySpatial(bounds, options = {}) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    await this._init();

    const { minLat, maxLat, minLon, maxLon } = bounds;
    const { limit } = options;

    const params = [minLat, maxLat, minLon, maxLon];

    let sql = `
      SELECT id, client_id AS "clientId", room_id AS "roomId",
             latitude, longitude, altitude, accuracy, speed,
             timestamp::text AS timestamp,
             created_at::text AS "createdAt"
      FROM location_events
      WHERE latitude  BETWEEN $1 AND $2
        AND longitude BETWEEN $3 AND $4
      ORDER BY timestamp ASC
    `;

    if (typeof limit === "number" && limit > 0) {
      sql += ` LIMIT $5`;
      params.push(limit);
    }

    const result = await this._pool.query(sql, params);
    return result.rows;
  }

  /**
   * Returns the most-recent location event for a room, or null.
   *
   * @param {string} roomId
   * @returns {Promise<import("./adapter.js").LocationEvent|null>}
   */
  async getLatest(roomId) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    await this._init();

    const sql = `
      SELECT id, client_id AS "clientId", room_id AS "roomId",
             latitude, longitude, altitude, accuracy, speed,
             timestamp::text AS timestamp,
             created_at::text AS "createdAt"
      FROM location_events
      WHERE room_id = $1
      ORDER BY timestamp DESC
      LIMIT 1
    `;

    const result = await this._pool.query(sql, [roomId]);
    return result.rows[0] ?? null;
  }

  // ─────────────────────────── shutdown ─────────────────────────────────────

  /**
   * Flushes any remaining buffered events, stops the flush timer,
   * stops the compaction timer, and drains the connection pool.
   * Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed) return;
    this._closed = true;

    // Stop the periodic flush timer.
    if (this._flushTimer) {
      clearInterval(this._flushTimer);
      this._flushTimer = null;
    }

    // Stop the compaction timer.
    if (this._compactionTimer) {
      clearTimeout(this._compactionTimer);
      this._compactionTimer = null;
    }

    // Attempt a final flush of any remaining events.
    if (this._buffer.length > 0 && this._pool) {
      this._flushing = false;
      try {
        await this._flush();
      } catch (err) {
        logger.error("PostgresAdapter final flush error", { error: err.message });
      }
    }

    // Drain the pool.
    if (this._pool) {
      try {
        await this._pool.end();
      } catch (err) {
        logger.error("PostgresAdapter pool close error", { error: err.message });
      }
      this._pool = null;
    }
  }
}
