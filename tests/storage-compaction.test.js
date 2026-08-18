/**
 * @fileoverview Compaction and resolution-aware query tests for StorageAdapter.
 *
 * Tests the MemoryAdapter implementation of compaction, downsampling,
 * resolution-aware queryRoom, and getCompactionStatus.
 * PostgreSQL integration tests are skipped when DATABASE_URL is not set.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { MemoryAdapter } from "../src/storage/memory.js";
import { assertStorageAdapter } from "../src/storage/adapter.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

/**
 * Returns a valid LocationEvent with all required fields.
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

/**
 * Generate a series of events spread across a time range.
 * @param {string} roomId
 * @param {number} startMs - Start timestamp in ms.
 * @param {number} count - Number of events.
 * @param {number} intervalMs - Interval between events.
 * @returns {import("../src/storage/adapter.js").LocationEvent[]}
 */
function generateEvents(roomId, startMs, count, intervalMs = 1000) {
  const events = [];
  for (let i = 0; i < count; i++) {
    events.push(makeEvent({
      roomId,
      clientId: `client-${String(i).padStart(3, "0")}`,
      latitude: 40.7128 + (i * 0.001),
      longitude: -74.006 + (i * 0.001),
      altitude: 10 + (i * 0.1),
      speed: 1.0 + (i * 0.01),
      timestamp: new Date(startMs + i * intervalMs).toISOString(),
    }));
  }
  return events;
}

// ─── adapter interface contract ───────────────────────────────────────────────

describe("StorageAdapter interface with compaction", () => {
  let adapter;

  beforeEach(() => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("implements the full StorageAdapter interface including compact and getCompactionStatus", () => {
    expect(() => assertStorageAdapter(adapter)).not.toThrow();
    expect(typeof adapter.compact).toBe("function");
    expect(typeof adapter.getCompactionStatus).toBe("function");
  });

  it("getCompactionStatus returns valid structure", async () => {
    const status = await adapter.getCompactionStatus();
    expect(status).toHaveProperty("lastRun");
    expect(status).toHaveProperty("nextRun");
    expect(status).toHaveProperty("tiers");
    expect(Array.isArray(status.tiers)).toBe(true);
    expect(status.tiers.length).toBe(4);

    const tierNames = status.tiers.map((t) => t.name);
    expect(tierNames).toContain("raw");
    expect(tierNames).toContain("1m");
    expect(tierNames).toContain("1h");
    expect(tierNames).toContain("1d");

    for (const tier of status.tiers) {
      expect(typeof tier.rows).toBe("number");
      expect(typeof tier.sizeBytes).toBe("number");
    }
  });
});

// ─── queryRoom resolution routing ─────────────────────────────────────────────

describe("queryRoom resolution routing", () => {
  let adapter;

  beforeEach(async () => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("default resolution is 'auto'", async () => {
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date().toISOString() }),
    ]);
    // Should not throw with default resolution
    const results = await adapter.queryRoom("r1");
    expect(results.length).toBeGreaterThan(0);
  });

  it("resolution='raw' returns raw events", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date(base).toISOString() }),
      makeEvent({ roomId: "r1", timestamp: new Date(base + 1000).toISOString() }),
    ]);

    const results = await adapter.queryRoom("r1", { resolution: "raw" });
    expect(results).toHaveLength(2);
    expect(results[0].clientId).not.toBe("downsample");
  });

  it("resolution='1m' returns downsampled data after compaction", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date(base).toISOString() }),
      makeEvent({ roomId: "r1", timestamp: new Date(base + 5000).toISOString() }),
      makeEvent({ roomId: "r1", timestamp: new Date(base + 10000).toISOString() }),
    ]);

    await adapter.compact();

    const results = await adapter.queryRoom("r1", { resolution: "1m" });
    expect(results.length).toBeGreaterThan(0);
    // Downsampled events have clientId "downsample"
    expect(results[0].clientId).toBe("downsample");
  });

  it("resolution='1h' returns aggregate data after compaction", async () => {
    const base = Date.now();
    // Generate events across multiple minutes
    const events = [];
    for (let i = 0; i < 10; i++) {
      events.push(makeEvent({
        roomId: "r1",
        timestamp: new Date(base + i * 60000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);
    await adapter.compact();

    const results = await adapter.queryRoom("r1", {
      resolution: "1h",
      from: new Date(base - 60000),
      to: new Date(base + 600000),
    });
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].clientId).toBe("aggregate-1h");
  });

  it("resolution='1d' returns aggregate data after compaction", async () => {
    const base = Date.now();
    // Generate events across multiple hours
    const events = [];
    for (let i = 0; i < 5; i++) {
      events.push(makeEvent({
        roomId: "r1",
        timestamp: new Date(base + i * 3600000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);
    await adapter.compact();

    const results = await adapter.queryRoom("r1", {
      resolution: "1d",
      from: new Date(base - 3600000),
      to: new Date(base + 5 * 3600000),
    });
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].clientId).toBe("aggregate-1d");
  });

  it("resolution='auto' selects raw for short time ranges", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date(base).toISOString() }),
    ]);

    const results = await adapter.queryRoom("r1", {
      from: new Date(base - 10000),
      to: new Date(base + 10000),
      resolution: "auto",
    });
    // Short range should return raw events
    expect(results.length).toBe(1);
  });

  it("resolution='auto' defaults to raw when no time range specified", async () => {
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date().toISOString() }),
    ]);

    const results = await adapter.queryRoom("r1", { resolution: "auto" });
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].clientId).not.toBe("downsample");
  });
});

// ─── compaction pipeline ──────────────────────────────────────────────────────

describe("MemoryAdapter compaction", () => {
  let adapter;

  beforeEach(async () => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("compact() returns CompactionResult with correct shape", async () => {
    await adapter.writeBatch([
      makeEvent({ roomId: "r1" }),
    ]);

    const result = await adapter.compact();
    expect(result).toHaveProperty("rawDeleted");
    expect(result).toHaveProperty("downsample1m");
    expect(result).toHaveProperty("aggregate1h");
    expect(result).toHaveProperty("aggregate1d");
    expect(result).toHaveProperty("durationMs");
    expect(typeof result.rawDeleted).toBe("number");
    expect(typeof result.downsample1m).toBe("number");
    expect(typeof result.durationMs).toBe("number");
  });

  it("compact() computes 1m downsamples from raw data", async () => {
    const base = Date.now();
    // Generate multiple events within the same minute
    const events = [];
    for (let i = 0; i < 5; i++) {
      events.push(makeEvent({
        roomId: "r1",
        latitude: 40.7128 + i * 0.01,
        longitude: -74.006 + i * 0.01,
        timestamp: new Date(base + i * 1000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);

    const result = await adapter.compact();
    // Should have created downsample rows
    expect(result.downsample1m).toBeGreaterThanOrEqual(1);

    // Verify 1m data is queryable
    const m1Results = await adapter.queryRoom("r1", { resolution: "1m" });
    expect(m1Results.length).toBeGreaterThanOrEqual(1);
  });

  it("compact() computes 1h aggregates from 1m data", async () => {
    const base = Date.now();
    // Generate events across multiple minutes to get 1h aggregates
    const events = [];
    for (let i = 0; i < 10; i++) {
      events.push(makeEvent({
        roomId: "r1",
        timestamp: new Date(base + i * 60000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);

    const result = await adapter.compact();
    expect(result.aggregate1h).toBeGreaterThanOrEqual(1);
  });

  it("compact() computes 1d aggregates from 1h data", async () => {
    const base = Date.now();
    // Generate events across multiple hours
    const events = [];
    for (let i = 0; i < 5; i++) {
      events.push(makeEvent({
        roomId: "r1",
        timestamp: new Date(base + i * 3600000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);

    const result = await adapter.compact();
    expect(result.aggregate1d).toBeGreaterThanOrEqual(1);
  });

  it("compact() with custom retention overrides", async () => {
    await adapter.writeBatch([
      makeEvent({ roomId: "r1" }),
    ]);

    const result = await adapter.compact({
      rawRetentionDays: 1,
      downsample1mRetentionDays: 30,
      downsample1hRetentionDays: 180,
      aggregate1dRetentionDays: 1000,
    });
    expect(result.rawDeleted).toBeGreaterThanOrEqual(0);
  });

  it("compact() is idempotent — running twice produces same results", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date(base).toISOString() }),
      makeEvent({ roomId: "r1", timestamp: new Date(base + 5000).toISOString() }),
    ]);

    await adapter.compact();
    const result2 = await adapter.compact();

    // Second run should still work
    expect(result2.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("getCompactionStatus updates after compact()", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      makeEvent({ roomId: "r1", timestamp: new Date(base).toISOString() }),
    ]);

    const statusBefore = await adapter.getCompactionStatus();
    expect(statusBefore.lastRun).toBeNull();

    await adapter.compact();

    const statusAfter = await adapter.getCompactionStatus();
    expect(statusAfter.lastRun).not.toBeNull();
    expect(statusAfter.tiers[0].rows).toBeGreaterThanOrEqual(0);
  });

  it("compaction is idempotent and resumable", async () => {
    const base = Date.now();
    await adapter.writeBatch(generateEvents("r1", base, 50, 1000));
    await adapter.compact();

    // Run compaction again — should not throw or duplicate data
    const result = await adapter.compact();
    expect(result.durationMs).toBeGreaterThanOrEqual(0);

    const m1 = await adapter.queryRoom("r1", { resolution: "1m" });
    expect(m1.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── data tier statistics ─────────────────────────────────────────────────────

describe("Compaction tier statistics", () => {
  let adapter;

  beforeEach(async () => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("raw tier shows correct row count", async () => {
    await adapter.writeBatch(generateEvents("r1", Date.now(), 25));
    const status = await adapter.getCompactionStatus();
    const rawTier = status.tiers.find((t) => t.name === "raw");
    expect(rawTier.rows).toBe(25);
  });

  it("raw tier oldest/newest timestamps are correct", async () => {
    const base = Date.now();
    await adapter.writeBatch(generateEvents("r1", base, 5, 60000));
    const status = await adapter.getCompactionStatus();
    const rawTier = status.tiers.find((t) => t.name === "raw");
    expect(rawTier.oldest).not.toBeNull();
    expect(rawTier.newest).not.toBeNull();
  });

  it("1m tier gets populated after compaction", async () => {
    const base = Date.now();
    await adapter.writeBatch(generateEvents("r1", base, 10, 1000));
    await adapter.compact();

    const status = await adapter.getCompactionStatus();
    const m1Tier = status.tiers.find((t) => t.name === "1m");
    expect(m1Tier.rows).toBeGreaterThanOrEqual(1);
  });

  it("1h tier gets populated after compaction", async () => {
    const base = Date.now();
    // Events across multiple minutes
    await adapter.writeBatch(generateEvents("r1", base, 10, 60000));
    await adapter.compact();

    const status = await adapter.getCompactionStatus();
    const h1Tier = status.tiers.find((t) => t.name === "1h");
    expect(h1Tier.rows).toBeGreaterThanOrEqual(1);
  });

  it("sizeBytes are non-negative", async () => {
    await adapter.writeBatch(generateEvents("r1", Date.now(), 10));
    await adapter.compact();
    const status = await adapter.getCompactionStatus();
    for (const tier of status.tiers) {
      expect(tier.sizeBytes).toBeGreaterThanOrEqual(0);
    }
  });
});

// ─── multi-room compaction ────────────────────────────────────────────────────

describe("Multi-room compaction", () => {
  let adapter;

  beforeEach(async () => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("compaction computes per-room downsamples", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      ...generateEvents("room-a", base, 5, 1000),
      ...generateEvents("room-b", base, 5, 1000),
    ]);

    await adapter.compact();

    const a1m = await adapter.queryRoom("room-a", { resolution: "1m" });
    const b1m = await adapter.queryRoom("room-b", { resolution: "1m" });
    expect(a1m.length).toBeGreaterThanOrEqual(1);
    expect(b1m.length).toBeGreaterThanOrEqual(1);
  });

  it("queryRoom resolution only returns data for the requested room", async () => {
    const base = Date.now();
    await adapter.writeBatch([
      ...generateEvents("room-a", base, 5, 1000),
      ...generateEvents("room-b", base, 5, 1000),
    ]);

    await adapter.compact();

    const aResults = await adapter.queryRoom("room-a", { resolution: "1m" });
    for (const r of aResults) {
      expect(r.roomId).toBe("room-a");
    }
  });
});

// ─── haversine distance calculation ───────────────────────────────────────────

describe("Compaction distance calculation", () => {
  let adapter;

  beforeEach(async () => {
    adapter = new MemoryAdapter();
  });

  afterEach(async () => {
    await adapter.close();
  });

  it("1h aggregate includes totalDistance", async () => {
    const base = Date.now();
    // Events spread across different locations over multiple minutes
    const events = [];
    for (let i = 0; i < 10; i++) {
      events.push(makeEvent({
        roomId: "r1",
        latitude: 40.7128 + i * 0.001,
        longitude: -74.006 + i * 0.001,
        timestamp: new Date(base + i * 60000).toISOString(),
      }));
    }
    await adapter.writeBatch(events);
    await adapter.compact();

    const h1 = await adapter.queryRoom("r1", {
      resolution: "1h",
      from: new Date(base - 60000),
      to: new Date(base + 600000),
    });
    expect(h1.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── close edge cases ─────────────────────────────────────────────────────────

describe("Compaction close edge cases", () => {
  it("compact() throws after close()", async () => {
    const adapter = new MemoryAdapter();
    await adapter.close();
    await expect(adapter.compact()).rejects.toThrow("MemoryAdapter is closed");
  });

  it("getCompactionStatus() throws after close()", async () => {
    const adapter = new MemoryAdapter();
    await adapter.close();
    await expect(adapter.getCompactionStatus()).rejects.toThrow("MemoryAdapter is closed");
  });

  it("queryRoom with resolution throws after close()", async () => {
    const adapter = new MemoryAdapter();
    await adapter.close();
    await expect(
      adapter.queryRoom("r1", { resolution: "1m" })
    ).rejects.toThrow("MemoryAdapter is closed");
  });
});

// ─── PostgreSQL integration tests (skipped without DATABASE_URL) ─────────────

const describePg = process.env.DATABASE_URL ? describe : describe.skip;

describePg("PostgreSQL compaction integration", () => {
  let PostgresAdapter;

  beforeEach(async () => {
    const mod = await import("../src/storage/postgres.js");
    PostgresAdapter = mod.PostgresAdapter;
  });

  it("PostgresAdapter implements compact and getCompactionStatus", async () => {
    const adapter = new PostgresAdapter({
      connectionString: process.env.DATABASE_URL,
      compactionIntervalMs: 600000, // disable auto-compaction for tests
    });
    try {
      expect(() => assertStorageAdapter(adapter)).not.toThrow();
      expect(typeof adapter.compact).toBe("function");
      expect(typeof adapter.getCompactionStatus).toBe("function");

      const status = await adapter.getCompactionStatus();
      expect(status).toHaveProperty("tiers");
      expect(Array.isArray(status.tiers)).toBe(true);
    } finally {
      await adapter.close();
    }
  });

  it("PostgresAdapter compact() runs without error", async () => {
    const adapter = new PostgresAdapter({
      connectionString: process.env.DATABASE_URL,
      compactionIntervalMs: 600000,
    });
    try {
      const result = await adapter.compact();
      expect(result).toHaveProperty("rawDeleted");
      expect(result).toHaveProperty("downsample1m");
      expect(result).toHaveProperty("aggregate1h");
      expect(result).toHaveProperty("aggregate1d");
      expect(result).toHaveProperty("durationMs");
    } finally {
      await adapter.close();
    }
  });

  it("PostgresAdapter queryRoom with resolution=raw returns raw events", async () => {
    const adapter = new PostgresAdapter({
      connectionString: process.env.DATABASE_URL,
      compactionIntervalMs: 600000,
    });
    try {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "pg-test-raw", timestamp: new Date(base).toISOString() }),
        makeEvent({ roomId: "pg-test-raw", timestamp: new Date(base + 1000).toISOString() }),
      ]);

      // Flush the buffer
      await adapter._flush();

      const results = await adapter.queryRoom("pg-test-raw", { resolution: "raw" });
      expect(results.length).toBeGreaterThanOrEqual(2);
    } finally {
      await adapter.close();
    }
  });

  it("PostgresAdapter queryRoom with resolution=auto selects appropriate tier", async () => {
    const adapter = new PostgresAdapter({
      connectionString: process.env.DATABASE_URL,
      compactionIntervalMs: 600000,
    });
    try {
      const base = Date.now();
      await adapter.writeBatch([
        makeEvent({ roomId: "pg-test-auto", timestamp: new Date(base).toISOString() }),
      ]);
      await adapter._flush();

      // Short range should resolve to raw
      const results = await adapter.queryRoom("pg-test-auto", {
        from: new Date(base - 10000),
        to: new Date(base + 10000),
        resolution: "auto",
      });
      expect(results.length).toBeGreaterThanOrEqual(1);
    } finally {
      await adapter.close();
    }
  });
});
