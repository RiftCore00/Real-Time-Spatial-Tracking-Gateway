/**
 * @fileoverview PostgreSQL StorageAdapter implementation.
 *
 * Features:
 *  - Connection pooling via `pg` Pool (dynamic import so `pg` is optional).
 *  - Auto-creates the `location_events` table and indexes on first use.
 *  - Write batching: events accumulate in a bounded in-memory buffer and are
 *    flushed either when `batchSize` is reached or every `flushIntervalMs`.
 *  - Bulk insert via `INSERT … SELECT * FROM unnest(…)` — one round-trip per batch.
 *  - Backpressure: when the buffer exceeds `maxBufferSize`, the oldest events
 *    are dropped and `storage_dropped_events_total` is incremented.
 *  - Spatial index: composite (latitude, longitude) GiST-capable index for
 *    bounding-box queries without requiring PostGIS.
 */

import { logger } from "../logger.js";

/**
 * DDL executed once on startup to ensure the schema exists.
 * Uses DOUBLE PRECISION for coordinates and TIMESTAMPTZ for temporal columns
 * as required by the issue specification.
 */
const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS location_events (
  id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id   TEXT            NOT NULL,
  room_id     TEXT            NOT NULL,
  latitude    DOUBLE PRECISION NOT NULL,
  longitude   DOUBLE PRECISION NOT NULL,
  altitude    DOUBLE PRECISION,
  accuracy    DOUBLE PRECISION,
  speed       DOUBLE PRECISION,
  timestamp   TIMESTAMPTZ     NOT NULL,
  created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
`;

const CREATE_INDEXES_SQL = `
CREATE INDEX IF NOT EXISTS location_events_room_id_timestamp_idx
  ON location_events (room_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS location_events_lat_lon_idx
  ON location_events (latitude, longitude);
`;

/**
 * Bulk insert using unnest — single round-trip for an arbitrarily large batch.
 *
 * Parameter arrays are positionally matched:
 *   $1 = client_id[], $2 = room_id[], $3 = latitude[], $4 = longitude[],
 *   $5 = altitude[], $6 = accuracy[], $7 = speed[], $8 = timestamp[]
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

/**
 * @implements {import("./adapter.js").StorageAdapter}
 */
export class PostgresAdapter {
  /**
   * @param {object} [config={}]
   * @param {string}  [config.connectionString]   - Postgres connection URL. Falls back to DATABASE_URL.
   * @param {number}  [config.poolSize=10]         - Maximum pool connections.
   * @param {number}  [config.batchSize=100]       - Flush when buffer reaches this size.
   * @param {number}  [config.flushIntervalMs=1000]- Flush every N milliseconds regardless of size.
   * @param {number}  [config.maxBufferSize=10000] - Hard cap on in-flight buffer; oldest dropped beyond this.
   */
  constructor(config = {}) {
    this._connectionString =
      config.connectionString ?? process.env.DATABASE_URL;
    this._poolSize = config.poolSize ?? 10;
    this._batchSize = config.batchSize ?? 100;
    this._flushIntervalMs = config.flushIntervalMs ?? 1000;
    this._maxBufferSize = config.maxBufferSize ?? 10000;

    /** @type {import("./adapter.js").LocationEvent[]} */
    this._buffer = [];

    /** Monotonically-increasing counter of dropped events. */
    this.storage_dropped_events_total = 0;

    /** @type {import("pg").Pool|null} */
    this._pool = null;

    /** @type {NodeJS.Timeout|null} */
    this._flushTimer = null;

    /** Promise that resolves when the schema has been initialised. */
    this._initPromise = null;

    /** Whether close() has been called. */
    this._closed = false;

    /** Whether a flush is currently in-flight (prevents double-flush). */
    this._flushing = false;
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
    // Dynamic import keeps pg optional when the memory adapter is used.
    const { default: pg } = await import("pg");
    const { Pool } = pg;

    this._pool = new Pool({
      connectionString: this._connectionString,
      max: this._poolSize,
    });

    // Surface connection errors without crashing the process.
    this._pool.on("error", (err) => {
      logger.error("PostgresAdapter pool error", { error: err.message });
    });

    // Run schema migration.
    const client = await this._pool.connect();
    try {
      await client.query(CREATE_TABLE_SQL);
      await client.query(CREATE_INDEXES_SQL);
    } finally {
      client.release();
    }

    // Start the periodic flush timer.
    this._flushTimer = setInterval(() => {
      this._scheduleFlush();
    }, this._flushIntervalMs);

    // Node should not wait for the timer to exit.
    if (this._flushTimer.unref) this._flushTimer.unref();
  }

  // ─────────────────────────── write pipeline ───────────────────────────────

  /**
   * Accepts a batch of events into the write buffer.
   *
   * This method is intentionally non-blocking: it enqueues events and returns
   * immediately.  The flush happens asynchronously so the WebSocket broadcast
   * pipeline is never stalled by I/O.
   *
   * Backpressure enforcement: when the buffer exceeds `maxBufferSize`, the
   * oldest events are evicted and counted in `storage_dropped_events_total`.
   *
   * @param {import("./adapter.js").LocationEvent[]} events
   * @returns {Promise<void>}
   */
  async writeBatch(events) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    if (!Array.isArray(events) || events.length === 0) return;

    // Ensure pool + schema are ready (no-op after first call).
    // We do NOT await here so writeBatch returns immediately.
    // The flush that actually writes to PG will await _init internally.
    this._ensureInit();

    // Append to buffer.
    this._buffer.push(...events);

    // Enforce hard cap: drop oldest events if buffer is too large.
    if (this._buffer.length > this._maxBufferSize) {
      const excess = this._buffer.length - this._maxBufferSize;
      this._buffer.splice(0, excess);
      this.storage_dropped_events_total += excess;
      logger.warn("Storage buffer overflow: dropping oldest events", {
        dropped: excess,
        storage_dropped_events_total: this.storage_dropped_events_total,
      });
    }

    // Flush immediately if we've hit the batch threshold.
    if (this._buffer.length >= this._batchSize) {
      this._scheduleFlush();
    }
  }

  /**
   * Kicks off an async flush without blocking the caller.
   * @private
   */
  _scheduleFlush() {
    // Run flush async; do not propagate errors to caller.
    this._flush().catch((err) => {
      logger.error("PostgresAdapter flush error", { error: err.message });
    });
  }

  /**
   * Flushes the current buffer contents to Postgres.
   * Concurrent flushes are serialised by the `_flushing` flag.
   *
   * @private
   * @returns {Promise<void>}
   */
  async _flush() {
    if (this._flushing || this._buffer.length === 0) return;
    this._flushing = true;

    try {
      // Ensure the pool is ready before we try to write.
      await this._init();

      // Drain the buffer one batch at a time.
      while (this._buffer.length > 0) {
        const batch = this._buffer.splice(0, this._batchSize);
        await this._insertBatch(batch);
      }
    } finally {
      this._flushing = false;
    }
  }

  /**
   * Executes a single bulk INSERT for the given batch.
   *
   * @private
   * @param {import("./adapter.js").LocationEvent[]} batch
   * @returns {Promise<void>}
   */
  async _insertBatch(batch) {
    const clientIds = batch.map((e) => e.clientId);
    const roomIds   = batch.map((e) => e.roomId);
    const lats      = batch.map((e) => e.latitude);
    const lons      = batch.map((e) => e.longitude);
    const alts      = batch.map((e) => e.altitude  ?? null);
    const accs      = batch.map((e) => e.accuracy  ?? null);
    const speeds    = batch.map((e) => e.speed     ?? null);
    const timestamps = batch.map((e) => e.timestamp);

    await this._pool.query(BULK_INSERT_SQL, [
      clientIds,
      roomIds,
      lats,
      lons,
      alts,
      accs,
      speeds,
      timestamps,
    ]);
  }

  /**
   * Fires off the init promise without awaiting it, so the first writeBatch
   * call does not block.
   * @private
   */
  _ensureInit() {
    if (!this._initPromise) {
      this._init().catch((err) => {
        logger.error("PostgresAdapter init error", { error: err.message });
      });
    }
  }

  // ─────────────────────────── read pipeline ────────────────────────────────

  /**
   * Retrieves historical events for a room, ordered ascending by timestamp.
   *
   * @param {string} roomId
   * @param {import("./adapter.js").QueryOptions} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async queryRoom(roomId, options = {}) {
    if (this._closed) throw new Error("PostgresAdapter is closed");
    await this._init();

    const { from, to, limit } = options;
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
   * Flushes any remaining buffered events, stops the flush timer, and drains
   * the connection pool.  Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed) return;
    this._closed = true;

    // Stop the periodic timer.
    if (this._flushTimer) {
      clearInterval(this._flushTimer);
      this._flushTimer = null;
    }

    // Attempt a final flush of any remaining events.
    if (this._buffer.length > 0 && this._pool) {
      this._flushing = false; // reset flag so flush can proceed
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
