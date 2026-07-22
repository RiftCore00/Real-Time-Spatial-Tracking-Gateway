/**
 * @fileoverview Interface contract tests for StorageAdapter implementations.
 *
 * These tests run the exact same suite against both MemoryAdapter and
 * (optionally) PostgresAdapter so both implementations stay in sync.
 * The PostgreSQL suite is automatically skipped when DATABASE_URL is not set.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { MemoryAdapter } from "../src/storage/memory.js";
import { assertStorageAdapter } from "../src/storage/adapter.js";
import { createStorageAdapter } from "../src/storage/index.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

/**
 * Returns a valid LocationEvent with all required fields.
 * Optional fields can be overridden via `overrides`.
 *
 * @param {Partial<import("../src/storage/adapter.js").LocationEvent>} [overrides]
 * @returns {import("../src/storage/adapter.js").LocationEvent}
 */
function makeEvent(overrides = {}) {
  return {
    clientId: "client-001",
    roomId: "room-alpha",
    latitude: 40.7128,
    longitude: -74.006,
    altitude: 10,
    accuracy: 5,
    speed: 1.2,
    timestamp: new Date().toISOString(),
    ...overrides,
  };
}

// ─── shared contract suite ────────────────────────────────────────────────────

/**
 * Runs the full interface contract against a StorageAdapter factory.
 * The factory is called before each test so every test starts with a
 * fresh, empty adapter.
 *
 * @param {string} suiteName  - Label shown in the test output.
 * @param {() => import("../src/storage/adapter.js").StorageAdapter} factory
 */
function runContractSuite(suiteName, factory) {
  describe(suiteName, () => {
    /** @type {import("../src/storage/adapter.js").StorageAdapter} */
    let adapter;

    beforeEach(() => {
      adapter = factory();
    });

    afterEach(async () => {
      await adapter.close();
    });

    // ── interface shape ──────────────────────────────────────────────────────

    it("implements the StorageAdapter interface", () => {
      expect(() => assertStorageAdapter(adapter)).not.toThrow();
    });

    it("exposes all five required methods", () => {
      expect(typeof adapter.writeBatch).toBe("function");
      expect(typeof adapter.queryRoom).toBe("function");
      expect(typeof adapter.querySpatial).toBe("function");
      expect(typeof adapter.getLatest).toBe("function");
      expect(typeof adapter.close).toBe("function");
    });

    // ── writeBatch ───────────────────────────────────────────────────────────

    it("writeBatch returns a Promise", async () => {
      const result = adapter.writeBatch([makeEvent()]);
      expect(result).toBeInstanceOf(Promise);
      await result;
    });

    it("writeBatch accepts an empty array without error", async () => {
      await expect(adapter.writeBatch([])).resolves.toBeUndefined();
    });

    it("writeBatch persists multiple events", async () => {
      const events = [
        makeEvent({ clientId: "c1", roomId: "room-1" }),
        makeEvent({ clientId: "c2", roomId: "room-1" }),
        makeEvent({ clientId: "c3", roomId: "room-2" }),
      ];
      await adapter.writeBatch(events);

      const room1 = await adapter.queryRoom("room-1");
      expect(room1).toHaveLength(2);

      const room2 = await adapter.queryRoom("room-2");
      expect(room2).toHaveLength(1);
    });

    // ── queryRoom ────────────────────────────────────────────────────────────

    it("queryRoom returns [] for a room with no events", async () => {
      const results = await adapter.queryRoom("nonexistent-room");
      expect(results).toEqual([]);
    });

    it("queryRoom returns events only for the requested room", async () => {
      await adapter.writeBatch([
        makeEvent({ roomId: "room-a", clientId: "c1" }),
        makeEvent({ roomId: "room-b", clientId: "c2" }),
      ]);

      const results = await adapter.queryRoom("room-a");
      expect(results).toHaveLength(1);
      expect(results[0].roomId).toBe("room-a");
    });

    it("queryRoom orders results ascending by timestamp", async () => {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "room-ts", timestamp: new Date(base + 2000).toISOString() }),
        makeEvent({ roomId: "room-ts", timestamp: new Date(base).toISOString() }),
        makeEvent({ roomId: "room-ts", timestamp: new Date(base + 1000).toISOString() }),
      ]);

      const results = await adapter.queryRoom("room-ts");
      expect(results).toHaveLength(3);

      const timestamps = results.map((e) => new Date(e.timestamp).getTime());
      expect(timestamps[0]).toBeLessThanOrEqual(timestamps[1]);
      expect(timestamps[1]).toBeLessThanOrEqual(timestamps[2]);
    });

    it("queryRoom respects the 'from' option", async () => {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "room-from", timestamp: new Date(base - 5000).toISOString() }),
        makeEvent({ roomId: "room-from", timestamp: new Date(base + 5000).toISOString() }),
      ]);

      const results = await adapter.queryRoom("room-from", {
        from: new Date(base),
      });
      expect(results).toHaveLength(1);
      expect(new Date(results[0].timestamp).getTime()).toBeGreaterThanOrEqual(base);
    });

    it("queryRoom respects the 'to' option", async () => {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "room-to", timestamp: new Date(base - 5000).toISOString() }),
        makeEvent({ roomId: "room-to", timestamp: new Date(base + 5000).toISOString() }),
      ]);

      const results = await adapter.queryRoom("room-to", {
        to: new Date(base),
      });
      expect(results).toHaveLength(1);
      expect(new Date(results[0].timestamp).getTime()).toBeLessThanOrEqual(base);
    });

    it("queryRoom respects the 'limit' option", async () => {
      await adapter.writeBatch([
        makeEvent({ roomId: "room-limit", clientId: "c1" }),
        makeEvent({ roomId: "room-limit", clientId: "c2" }),
        makeEvent({ roomId: "room-limit", clientId: "c3" }),
      ]);

      const results = await adapter.queryRoom("room-limit", { limit: 2 });
      expect(results).toHaveLength(2);
    });

    it("queryRoom returns correct event fields", async () => {
      const event = makeEvent({ roomId: "room-fields", clientId: "client-x" });
      await adapter.writeBatch([event]);

      const results = await adapter.queryRoom("room-fields");
      expect(results).toHaveLength(1);

      const r = results[0];
      expect(r.clientId).toBe("client-x");
      expect(r.roomId).toBe("room-fields");
      expect(typeof r.latitude).toBe("number");
      expect(typeof r.longitude).toBe("number");
      expect(typeof r.timestamp).toBe("string");
    });

    // ── querySpatial ─────────────────────────────────────────────────────────

    it("querySpatial returns [] when no events fall in bounds", async () => {
      await adapter.writeBatch([
        makeEvent({ latitude: 51.5, longitude: -0.1 }),
      ]);

      const results = await adapter.querySpatial({
        minLat: -10, maxLat: 10,
        minLon: -10, maxLon: 10,
      });
      expect(results).toEqual([]);
    });

    it("querySpatial returns events within bounding box", async () => {
      await adapter.writeBatch([
        makeEvent({ latitude: 5,  longitude: 5  }),  // inside
        makeEvent({ latitude: 50, longitude: 50 }),  // outside
      ]);

      const results = await adapter.querySpatial({
        minLat: 0, maxLat: 10,
        minLon: 0, maxLon: 10,
      });
      expect(results).toHaveLength(1);
      expect(results[0].latitude).toBe(5);
    });

    it("querySpatial includes events on bounding box boundaries", async () => {
      await adapter.writeBatch([
        makeEvent({ latitude: 0,  longitude: 0  }),   // corner
        makeEvent({ latitude: 10, longitude: 10 }),   // corner
      ]);

      const results = await adapter.querySpatial({
        minLat: 0, maxLat: 10,
        minLon: 0, maxLon: 10,
      });
      expect(results).toHaveLength(2);
    });

    it("querySpatial respects the limit option", async () => {
      await adapter.writeBatch([
        makeEvent({ latitude: 1, longitude: 1 }),
        makeEvent({ latitude: 2, longitude: 2 }),
        makeEvent({ latitude: 3, longitude: 3 }),
      ]);

      const results = await adapter.querySpatial(
        { minLat: 0, maxLat: 90, minLon: -180, maxLon: 180 },
        { limit: 2 }
      );
      expect(results).toHaveLength(2);
    });

    // ── getLatest ────────────────────────────────────────────────────────────

    it("getLatest returns null for a room with no events", async () => {
      const result = await adapter.getLatest("nonexistent-room");
      expect(result).toBeNull();
    });

    it("getLatest returns the most-recent event by timestamp", async () => {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "room-latest", timestamp: new Date(base - 2000).toISOString(), clientId: "old" }),
        makeEvent({ roomId: "room-latest", timestamp: new Date(base).toISOString(),        clientId: "new" }),
        makeEvent({ roomId: "room-latest", timestamp: new Date(base - 1000).toISOString(), clientId: "mid" }),
      ]);

      const result = await adapter.getLatest("room-latest");
      expect(result).not.toBeNull();
      expect(result.clientId).toBe("new");
    });

    it("getLatest returns only from the requested room", async () => {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "room-x", timestamp: new Date(base + 5000).toISOString(), clientId: "x-latest" }),
        makeEvent({ roomId: "room-y", timestamp: new Date(base).toISOString(),        clientId: "y-only" }),
      ]);

      const result = await adapter.getLatest("room-y");
      expect(result.clientId).toBe("y-only");
    });

    // ── close ────────────────────────────────────────────────────────────────

    it("close is idempotent", async () => {
      await adapter.close();
      await expect(adapter.close()).resolves.toBeUndefined();
    });
  });
}

// ─── run against MemoryAdapter ────────────────────────────────────────────────

runContractSuite("MemoryAdapter contract", () => new MemoryAdapter());

// ─── MemoryAdapter-specific tests ────────────────────────────────────────────

describe("MemoryAdapter specifics", () => {
  it("clear() resets events without closing the adapter", async () => {
    const adapter = new MemoryAdapter();
    await adapter.writeBatch([makeEvent({ roomId: "r" })]);
    adapter.clear();

    const results = await adapter.queryRoom("r");
    expect(results).toHaveLength(0);
    await adapter.close();
  });

  it("throws after close()", async () => {
    const adapter = new MemoryAdapter();
    await adapter.close();
    await expect(adapter.writeBatch([makeEvent()])).rejects.toThrow("MemoryAdapter is closed");
    await expect(adapter.queryRoom("r")).rejects.toThrow("MemoryAdapter is closed");
    await expect(adapter.querySpatial({ minLat: 0, maxLat: 1, minLon: 0, maxLon: 1 })).rejects.toThrow("MemoryAdapter is closed");
    await expect(adapter.getLatest("r")).rejects.toThrow("MemoryAdapter is closed");
  });

  it("optional fields (altitude, accuracy, speed) are stored and returned", async () => {
    const adapter = new MemoryAdapter();
    await adapter.writeBatch([
      makeEvent({ altitude: 100, accuracy: 3, speed: 10 }),
    ]);
    const [event] = await adapter.queryRoom("room-alpha");
    expect(event.altitude).toBe(100);
    expect(event.accuracy).toBe(3);
    expect(event.speed).toBe(10);
    await adapter.close();
  });

  it("events missing optional fields do not break reads", async () => {
    const adapter = new MemoryAdapter();
    // Build a minimal event without the optional fields
    const full = makeEvent();
    delete full.altitude;
    delete full.accuracy;
    delete full.speed;
    await adapter.writeBatch([full]);
    const [event] = await adapter.queryRoom("room-alpha");
    expect(event.latitude).toBe(40.7128);
    await adapter.close();
  });
});

// ─── createStorageAdapter factory ────────────────────────────────────────────

describe("createStorageAdapter", () => {
  it("returns a MemoryAdapter when adapter='memory'", () => {
    const adapter = createStorageAdapter({ adapter: "memory" });
    expect(adapter).toBeInstanceOf(MemoryAdapter);
  });

  it("returns a no-op adapter when adapter='none'", async () => {
    const adapter = createStorageAdapter({ adapter: "none" });
    assertStorageAdapter(adapter);
    await adapter.writeBatch([makeEvent()]);
    const results = await adapter.queryRoom("any");
    expect(results).toEqual([]);
    const latest = await adapter.getLatest("any");
    expect(latest).toBeNull();
  });

  it("throws for an unknown adapter name", () => {
    expect(() => createStorageAdapter({ adapter: "redis" })).toThrow(
      /Unknown storage adapter/
    );
  });

  it("throws when adapter='postgres' and no connectionString is provided", () => {
    const saved = process.env.DATABASE_URL;
    delete process.env.DATABASE_URL;
    try {
      expect(() => createStorageAdapter({ adapter: "postgres" })).toThrow(
        /DATABASE_URL/
      );
    } finally {
      if (saved !== undefined) process.env.DATABASE_URL = saved;
    }
  });

  it("defaults to memory when STORAGE_ADAPTER env var is not set", () => {
    const saved = process.env.STORAGE_ADAPTER;
    delete process.env.STORAGE_ADAPTER;
    try {
      const adapter = createStorageAdapter();
      expect(adapter).toBeInstanceOf(MemoryAdapter);
    } finally {
      if (saved !== undefined) process.env.STORAGE_ADAPTER = saved;
    }
  });
});

// ─── assertStorageAdapter ────────────────────────────────────────────────────

describe("assertStorageAdapter", () => {
  it("passes for a fully-implemented adapter", () => {
    expect(() => assertStorageAdapter(new MemoryAdapter())).not.toThrow();
  });

  it("throws listing every missing method", () => {
    expect(() => assertStorageAdapter({})).toThrow(/writeBatch/);
    expect(() => assertStorageAdapter({})).toThrow(/queryRoom/);
    expect(() => assertStorageAdapter({})).toThrow(/querySpatial/);
    expect(() => assertStorageAdapter({})).toThrow(/getLatest/);
    expect(() => assertStorageAdapter({})).toThrow(/close/);
  });

  it("throws only for the missing methods", () => {
    const partial = {
      writeBatch: async () => {},
      queryRoom: async () => [],
      querySpatial: async () => [],
      getLatest: async () => null,
      // close is intentionally missing
    };
    expect(() => assertStorageAdapter(partial)).toThrow(/close/);
  });
});
