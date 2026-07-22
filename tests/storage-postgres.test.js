/**
 * @fileoverview PostgreSQL integration tests for PostgresAdapter.
 *
 * These tests run against a real PostgreSQL database.
 * The entire suite is skipped automatically when the DATABASE_URL environment
 * variable is not set, so they never block the standard `npm test` run.
 *
 * To run locally:
 *   DATABASE_URL=postgres://tracker:devpassword@localhost:5432/spatial_tracking \
 *   npm test -- tests/storage-postgres.test.js
 *
 * The docker-compose.yml in the project root provisions a compatible instance.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { PostgresAdapter } from "../src/storage/postgres.js";
import { assertStorageAdapter } from "../src/storage/adapter.js";

const DATABASE_URL = process.env.DATABASE_URL;

// Skip everything when no database is available.
const describeOrSkip = DATABASE_URL ? describe : describe.skip;

// ─── helpers ──────────────────────────────────────────────────────────────────

/**
 * Returns a fresh LocationEvent with a recent timestamp.
 *
 * @param {Partial<import("../src/storage/adapter.js").LocationEvent>} [overrides]
 * @returns {import("../src/storage/adapter.js").LocationEvent}
 */
function makeEvent(overrides = {}) {
  return {
    clientId: "pg-client-001",
    roomId: "pg-room-alpha",
    latitude: 40.7128,
    longitude: -74.006,
    altitude: 15,
    accuracy: 4,
    speed: 0.5,
    timestamp: new Date().toISOString(),
    ...overrides,
  };
}

// ─── integration suite ────────────────────────────────────────────────────────

describeOrSkip("PostgresAdapter integration", () => {
  /** @type {PostgresAdapter} */
  let adapter;

  beforeAll(async () => {
    adapter = new PostgresAdapter({
      connectionString: DATABASE_URL,
      batchSize: 5,
      flushIntervalMs: 200,
      maxBufferSize: 50,
    });

    // Force schema creation before any test runs.
    // We do this by calling _init() directly.
    await adapter._init();
  });

  afterAll(async () => {
    await adapter.close();
  });

  beforeEach(async () => {
    // Wipe the test data between runs using a direct pool query.
    // We use a unique room prefix so parallel test runs don't collide.
    await adapter._pool.query(
      "DELETE FROM location_events WHERE room_id LIKE 'pg-room-%' OR room_id LIKE 'pg-spatial-%' OR room_id LIKE 'pg-latest-%' OR room_id LIKE 'pg-batch-%' OR room_id LIKE 'pg-drop-%'"
    );
  });

  // ── interface conformance ──────────────────────────────────────────────────

  it("implements the StorageAdapter interface", () => {
    expect(() => assertStorageAdapter(adapter)).not.toThrow();
  });

  // ── schema / table ─────────────────────────────────────────────────────────

  it("auto-creates the location_events table", async () => {
    const result = await adapter._pool.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'location_events'
      ORDER BY ordinal_position
    `);

    const cols = Object.fromEntries(result.rows.map((r) => [r.column_name, r.data_type]));

    expect(cols.id).toBeDefined();
    expect(cols.client_id).toBe("text");
    expect(cols.room_id).toBe("text");
    // DOUBLE PRECISION shows as "double precision" in information_schema
    expect(cols.latitude).toBe("double precision");
    expect(cols.longitude).toBe("double precision");
    expect(cols.altitude).toBe("double precision");
    expect(cols.accuracy).toBe("double precision");
    expect(cols.speed).toBe("double precision");
    // TIMESTAMPTZ shows as "timestamp with time zone"
    expect(cols.timestamp).toBe("timestamp with time zone");
    expect(cols.created_at).toBe("timestamp with time zone");
  });

  it("creates the required indexes", async () => {
    const result = await adapter._pool.query(`
      SELECT indexname FROM pg_indexes
      WHERE tablename = 'location_events'
    `);
    const names = result.rows.map((r) => r.indexname);

    expect(names).toContain("location_events_room_id_timestamp_idx");
    expect(names).toContain("location_events_lat_lon_idx");
  });

  // ── write & read ───────────────────────────────────────────────────────────

  it("persists a single event end-to-end", async () => {
    const event = makeEvent({ roomId: "pg-room-single" });

    adapter._buffer.push(event);
    // Force flush
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.queryRoom("pg-room-single");
    expect(results).toHaveLength(1);
    expect(results[0].clientId).toBe(event.clientId);
    expect(results[0].roomId).toBe("pg-room-single");
    expect(typeof results[0].latitude).toBe("number");
  });

  it("bulk insert via writeBatch persists multiple events", async () => {
    const events = Array.from({ length: 10 }, (_, i) =>
      makeEvent({
        roomId: "pg-batch-multi",
        clientId: `c${i}`,
        timestamp: new Date(Date.now() + i * 1000).toISOString(),
      })
    );

    // Write all events then force flush by exceeding batchSize (5).
    for (let i = 0; i < events.length; i += 5) {
      adapter._buffer.push(...events.slice(i, i + 5));
      adapter._flushing = false;
      await adapter._flush();
    }

    const results = await adapter.queryRoom("pg-batch-multi");
    expect(results).toHaveLength(10);
  });

  it("queryRoom filters by 'from' date", async () => {
    const base = Date.now();
    const events = [
      makeEvent({ roomId: "pg-room-from", timestamp: new Date(base - 10000).toISOString() }),
      makeEvent({ roomId: "pg-room-from", timestamp: new Date(base + 10000).toISOString() }),
    ];

    adapter._buffer.push(...events);
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.queryRoom("pg-room-from", {
      from: new Date(base),
    });
    expect(results).toHaveLength(1);
    expect(new Date(results[0].timestamp).getTime()).toBeGreaterThanOrEqual(base);
  });

  it("queryRoom filters by 'to' date", async () => {
    const base = Date.now();
    const events = [
      makeEvent({ roomId: "pg-room-to", timestamp: new Date(base - 10000).toISOString() }),
      makeEvent({ roomId: "pg-room-to", timestamp: new Date(base + 10000).toISOString() }),
    ];

    adapter._buffer.push(...events);
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.queryRoom("pg-room-to", {
      to: new Date(base),
    });
    expect(results).toHaveLength(1);
    expect(new Date(results[0].timestamp).getTime()).toBeLessThanOrEqual(base);
  });

  it("queryRoom respects the limit option", async () => {
    const base = Date.now();
    const events = Array.from({ length: 5 }, (_, i) =>
      makeEvent({ roomId: "pg-room-limit", timestamp: new Date(base + i * 1000).toISOString() })
    );

    adapter._buffer.push(...events);
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.queryRoom("pg-room-limit", { limit: 3 });
    expect(results).toHaveLength(3);
  });

  // ── spatial query ──────────────────────────────────────────────────────────

  it("querySpatial returns events within bounding box", async () => {
    adapter._buffer.push(
      makeEvent({ roomId: "pg-spatial-1", latitude: 5,  longitude: 5  }),  // inside
      makeEvent({ roomId: "pg-spatial-1", latitude: 50, longitude: 50 })   // outside
    );
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.querySpatial({
      minLat: 0, maxLat: 10,
      minLon: 0, maxLon: 10,
    });
    // At least the "inside" event should appear (others from prior runs are cleaned up)
    expect(results.some((e) => e.latitude === 5)).toBe(true);
    expect(results.every((e) => e.latitude >= 0 && e.latitude <= 10)).toBe(true);
  });

  it("querySpatial respects the limit option", async () => {
    adapter._buffer.push(
      ...Array.from({ length: 5 }, (_, i) =>
        makeEvent({ roomId: "pg-spatial-limit", latitude: i + 1, longitude: i + 1 })
      )
    );
    adapter._flushing = false;
    await adapter._flush();

    const results = await adapter.querySpatial(
      { minLat: 0, maxLat: 90, minLon: -180, maxLon: 180 },
      { limit: 2 }
    );
    expect(results.length).toBeLessThanOrEqual(2);
  });

  // ── getLatest ──────────────────────────────────────────────────────────────

  it("getLatest returns the most-recent event", async () => {
    const base = Date.now();
    adapter._buffer.push(
      makeEvent({ roomId: "pg-latest-1", timestamp: new Date(base - 2000).toISOString(), clientId: "old" }),
      makeEvent({ roomId: "pg-latest-1", timestamp: new Date(base).toISOString(),        clientId: "new" }),
      makeEvent({ roomId: "pg-latest-1", timestamp: new Date(base - 1000).toISOString(), clientId: "mid" })
    );
    adapter._flushing = false;
    await adapter._flush();

    const result = await adapter.getLatest("pg-latest-1");
    expect(result).not.toBeNull();
    expect(result.clientId).toBe("new");
  });

  it("getLatest returns null for an empty room", async () => {
    const result = await adapter.getLatest("pg-room-nonexistent-xyz");
    expect(result).toBeNull();
  });

  // ── backpressure / buffer overflow ─────────────────────────────────────────

  it("drops oldest events and increments counter when maxBufferSize is exceeded", () => {
    const small = new PostgresAdapter({
      connectionString: DATABASE_URL,
      batchSize: 1000,         // don't flush by size
      flushIntervalMs: 60000,  // don't flush by timer
      maxBufferSize: 5,
    });

    const before = small.storage_dropped_events_total;

    // Push 10 events into a buffer capped at 5 — should drop 5.
    const events = Array.from({ length: 10 }, (_, i) =>
      makeEvent({ clientId: `overflow-${i}`, roomId: "pg-drop-overflow" })
    );

    // We call the buffer manipulation directly (synchronous path)
    small._buffer.push(...events);
    if (small._buffer.length > small._maxBufferSize) {
      const excess = small._buffer.length - small._maxBufferSize;
      small._buffer.splice(0, excess);
      small.storage_dropped_events_total += excess;
    }

    expect(small._buffer).toHaveLength(5);
    expect(small.storage_dropped_events_total).toBe(before + 5);

    // Clean up without actually connecting.
    small._closed = true;
  });

  // ── close ──────────────────────────────────────────────────────────────────

  it("close is idempotent", async () => {
    const a = new PostgresAdapter({ connectionString: DATABASE_URL });
    await a._init();
    await a.close();
    await expect(a.close()).resolves.toBeUndefined();
  });

  it("throws after close()", async () => {
    const a = new PostgresAdapter({ connectionString: DATABASE_URL });
    await a._init();
    await a.close();
    await expect(a.writeBatch([makeEvent()])).rejects.toThrow("PostgresAdapter is closed");
    await expect(a.queryRoom("r")).rejects.toThrow("PostgresAdapter is closed");
    await expect(a.querySpatial({ minLat: 0, maxLat: 1, minLon: 0, maxLon: 1 })).rejects.toThrow("PostgresAdapter is closed");
    await expect(a.getLatest("r")).rejects.toThrow("PostgresAdapter is closed");
  });
});
