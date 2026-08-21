/**
 * @fileoverview Unit tests for src/storage.js — the storage-adapter issue's
 * factory, `NullAdapter`, and `PostgresAdapter`.
 *
 * The `postgres` package is mocked throughout: these are unit tests, not
 * integration tests, so nothing here opens a real network connection.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

const sqlTag = vi.fn(async () => []);
sqlTag.end = vi.fn().mockResolvedValue(undefined);
const postgresFactory = vi.fn(() => sqlTag);

vi.mock("postgres", () => ({
  default: (...args) => postgresFactory(...args),
}));

const { createStorageAdapter, NullAdapter, PostgresAdapter } = await import("../src/storage.js");

const PAYLOAD = {
  latitude: 40.7128,
  longitude: -74.006,
  altitude: 15,
  accuracy: 4,
  speed: 0.5,
  timestamp: "2026-08-21T12:00:00.000Z",
};

beforeEach(() => {
  sqlTag.mockClear();
  sqlTag.mockImplementation(async () => []);
  sqlTag.end.mockClear();
  postgresFactory.mockClear();
});

describe("createStorageAdapter", () => {
  it("with no DATABASE_URL returns a NullAdapter with a no-op saveLocation", async () => {
    const prevUrl = process.env.DATABASE_URL;
    delete process.env.DATABASE_URL;
    try {
      const adapter = createStorageAdapter();
      expect(adapter).toBeInstanceOf(NullAdapter);
      await expect(adapter.saveLocation("client-1", "room-1", PAYLOAD)).resolves.toBeUndefined();
      expect(postgresFactory).not.toHaveBeenCalled();
    } finally {
      if (prevUrl !== undefined) process.env.DATABASE_URL = prevUrl;
    }
  });

  it("createStorageAdapter({ url: 'postgres://...' }) returns a PostgresAdapter", () => {
    const adapter = createStorageAdapter({ url: "postgres://user:pass@localhost:5432/db" });
    expect(adapter).toBeInstanceOf(PostgresAdapter);
    expect(postgresFactory).toHaveBeenCalledWith("postgres://user:pass@localhost:5432/db");
  });

  it("accepts connectionString as an alias for url", () => {
    const adapter = createStorageAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });
    expect(adapter).toBeInstanceOf(PostgresAdapter);
  });

  it("falls back to the DATABASE_URL env var when config is omitted", () => {
    const prevUrl = process.env.DATABASE_URL;
    process.env.DATABASE_URL = "postgres://user:pass@localhost:5432/db";
    try {
      const adapter = createStorageAdapter();
      expect(adapter).toBeInstanceOf(PostgresAdapter);
    } finally {
      if (prevUrl !== undefined) process.env.DATABASE_URL = prevUrl;
      else delete process.env.DATABASE_URL;
    }
  });
});

describe("NullAdapter", () => {
  it("saveLocation resolves without doing anything, for any input", async () => {
    const adapter = new NullAdapter();
    await expect(adapter.saveLocation("a", "r1", PAYLOAD)).resolves.toBeUndefined();
    await expect(adapter.saveLocation("b", "r2", {})).resolves.toBeUndefined();
  });

  it("close() resolves", async () => {
    await expect(new NullAdapter().close()).resolves.toBeUndefined();
  });
});

describe("PostgresAdapter.saveLocation (postgres package mocked)", () => {
  it("inserts a row with clientId, roomId, and the payload fields", async () => {
    const adapter = new PostgresAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });

    await adapter.saveLocation("client-1", "room-1", PAYLOAD);

    expect(sqlTag).toHaveBeenCalledTimes(1);
    const values = sqlTag.mock.calls[0].slice(1);
    expect(values).toEqual([
      "client-1",
      "room-1",
      PAYLOAD.latitude,
      PAYLOAD.longitude,
      PAYLOAD.altitude,
      PAYLOAD.accuracy,
      PAYLOAD.speed,
      PAYLOAD.timestamp,
    ]);
  });

  it("defaults recorded_at to now when the payload omits timestamp", async () => {
    const adapter = new PostgresAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });
    const rest = { ...PAYLOAD };
    delete rest.timestamp;

    await adapter.saveLocation("client-1", "room-1", rest);

    const recordedAt = sqlTag.mock.calls[0].slice(1).at(-1);
    expect(() => new Date(recordedAt).toISOString()).not.toThrow();
  });

  it("passes null for missing optional fields (altitude, accuracy, speed)", async () => {
    const adapter = new PostgresAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });

    await adapter.saveLocation("client-1", "room-1", { latitude: 1, longitude: 2 });

    const values = sqlTag.mock.calls[0].slice(1);
    expect(values.slice(4, 7)).toEqual([null, null, null]);
  });

  it("propagates a query rejection to the caller", async () => {
    const adapter = new PostgresAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });
    sqlTag.mockImplementation(async () => {
      throw new Error("connection refused");
    });

    await expect(adapter.saveLocation("client-1", "room-1", PAYLOAD)).rejects.toThrow("connection refused");
  });

  it("close() ends the connection pool", async () => {
    const adapter = new PostgresAdapter({ connectionString: "postgres://user:pass@localhost:5432/db" });
    await adapter.close();
    expect(sqlTag.end).toHaveBeenCalledTimes(1);
  });
});
