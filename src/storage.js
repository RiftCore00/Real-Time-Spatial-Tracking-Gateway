/**
 * @fileoverview Storage adapter for persisting `location_update` events.
 *
 * `createStorageAdapter(config)` picks a backend:
 *   - No connection string available → `NullAdapter` (no-op). This is the
 *     default when `DATABASE_URL` is not set, so the gateway degrades
 *     gracefully instead of failing to start.
 *   - A connection string (`config.url`, `config.connectionString`, or the
 *     `DATABASE_URL` env var) → `PostgresAdapter`, backed by the `postgres`
 *     npm package (not an ORM).
 *
 * The interface is intentionally minimal: every adapter implements
 * `async saveLocation(clientId, roomId, payload)` and `async close()`.
 * Connection pooling tuning and migration tooling are out of scope here —
 * `postgres()` already pools connections with sane defaults, and the schema
 * is provisioned via db/migrations/001_create_locations.sql (applied once,
 * out-of-band, the same way any other SQL migration would be).
 */

import postgres from "postgres";

/**
 * No-op adapter used when no database is configured. Every write is silently
 * discarded so the gateway can run without a persistence dependency.
 */
export class NullAdapter {
  async saveLocation(_clientId, _roomId, _payload) {}
  async close() {}
}

/**
 * PostgreSQL-backed adapter. Writes one row per location update to the
 * `locations` table (see db/migrations/001_create_locations.sql).
 */
export class PostgresAdapter {
  /**
   * @param {object} config
   * @param {string} config.connectionString - Postgres connection string.
   */
  constructor({ connectionString }) {
    this.sql = postgres(connectionString);
  }

  /**
   * Persists a single location update.
   *
   * @param {string} clientId
   * @param {string} roomId
   * @param {object} payload - The validated `location_update` payload.
   * @param {number} payload.latitude
   * @param {number} payload.longitude
   * @param {number} [payload.altitude]
   * @param {number} [payload.accuracy]
   * @param {number} [payload.speed]
   * @param {string} [payload.timestamp] - ISO 8601 string; defaults to now.
   * @returns {Promise<void>}
   */
  async saveLocation(clientId, roomId, payload) {
    const recordedAt = payload.timestamp ?? new Date().toISOString();
    await this.sql`
      INSERT INTO locations
        (client_id, room_id, latitude, longitude, altitude, accuracy, speed, recorded_at)
      VALUES
        (${clientId}, ${roomId}, ${payload.latitude}, ${payload.longitude},
         ${payload.altitude ?? null}, ${payload.accuracy ?? null}, ${payload.speed ?? null}, ${recordedAt})
    `;
  }

  /**
   * Closes the connection pool. Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    await this.sql.end({ timeout: 5 });
  }
}

/**
 * @param {object} [config={}]
 * @param {string} [config.url] - Postgres connection string.
 * @param {string} [config.connectionString] - Alias for `config.url`.
 * @returns {NullAdapter|PostgresAdapter}
 */
export function createStorageAdapter(config = {}) {
  const connectionString = config.url ?? config.connectionString ?? process.env.DATABASE_URL;
  if (!connectionString) {
    return new NullAdapter();
  }
  return new PostgresAdapter({ connectionString });
}
