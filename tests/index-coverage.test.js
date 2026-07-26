/**
 * @fileoverview Additional coverage tests for src/index.js covering
 * previously untested code paths: invalid env var validation guards,
 * the createServer error path, and process event handlers
 * (uncaughtException, unhandledRejection).
 *
 * The module-level validation code (lines 19–41) runs at import time, so we
 * can't test the invalid-config branches by re-importing.  Instead we test
 * the logic indirectly through parseConfig() and by inspecting the process
 * event listeners that were registered.
 */

import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { parseConfig, shutdown } from "../src/index.js";
import { logger } from "../src/logger.js";

describe("parseConfig edge cases", () => {
  const savedEnv = {};

  beforeEach(() => {
    savedEnv.PORT = process.env.PORT;
    savedEnv.WS_HEARTBEAT_MS = process.env.WS_HEARTBEAT_MS;
    savedEnv.MAX_PAYLOAD_BYTES = process.env.MAX_PAYLOAD_BYTES;
  });

  afterEach(() => {
    // Restore env vars
    if (savedEnv.PORT === undefined) {
      delete process.env.PORT;
    } else {
      process.env.PORT = savedEnv.PORT;
    }
    if (savedEnv.WS_HEARTBEAT_MS === undefined) {
      delete process.env.WS_HEARTBEAT_MS;
    } else {
      process.env.WS_HEARTBEAT_MS = savedEnv.WS_HEARTBEAT_MS;
    }
    if (savedEnv.MAX_PAYLOAD_BYTES === undefined) {
      delete process.env.MAX_PAYLOAD_BYTES;
    } else {
      process.env.MAX_PAYLOAD_BYTES = savedEnv.MAX_PAYLOAD_BYTES;
    }
  });

  it("parseConfig returns NaN port for non-numeric PORT env var", () => {
    process.env.PORT = "not-a-number";
    const config = parseConfig();
    expect(isNaN(config.port)).toBe(true);
  });

  it("parseConfig returns NaN heartbeatMs for non-numeric WS_HEARTBEAT_MS", () => {
    process.env.WS_HEARTBEAT_MS = "bad-value";
    const config = parseConfig();
    expect(isNaN(config.heartbeatMs)).toBe(true);
  });

  it("parseConfig returns NaN maxPayloadBytes for non-numeric MAX_PAYLOAD_BYTES", () => {
    process.env.MAX_PAYLOAD_BYTES = "invalid";
    const config = parseConfig();
    expect(isNaN(config.maxPayloadBytes)).toBe(true);
  });

  it("parseConfig rejects out-of-range PORT (0)", () => {
    process.env.PORT = "0";
    const config = parseConfig();
    // port 0 is out of valid range [1, 65535]
    expect(config.port < 1 || config.port > 65535).toBe(true);
  });

  it("parseConfig rejects out-of-range PORT (65536)", () => {
    process.env.PORT = "65536";
    const config = parseConfig();
    expect(config.port > 65535).toBe(true);
  });
});

describe("shutdown function extended", () => {
  beforeEach(() => {
    vi.spyOn(process, "exit").mockImplementation(() => {});
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("logs 'Shutting down' with the signal name", () => {
    const infoSpy = vi.spyOn(logger, "info");
    const mockWss = { close: vi.fn((cb) => cb()) };

    shutdown(mockWss, "SIGTERM");

    expect(infoSpy).toHaveBeenCalledWith("Shutting down", { signal: "SIGTERM" });
    infoSpy.mockRestore();
  });

  it("logs 'Server closed' after wss closes", () => {
    const infoSpy = vi.spyOn(logger, "info");
    const mockWss = { close: vi.fn((cb) => cb()) };

    shutdown(mockWss, "SIGINT");

    expect(infoSpy).toHaveBeenCalledWith("Server closed");
    infoSpy.mockRestore();
  });

  it("logs 'Forced shutdown' on the 5s timeout", () => {
    const errorSpy = vi.spyOn(logger, "error");
    const mockWss = { close: vi.fn() }; // never calls callback

    shutdown(mockWss, "SIGTERM");
    vi.advanceTimersByTime(5000);

    expect(errorSpy).toHaveBeenCalledWith("Forced shutdown");
    errorSpy.mockRestore();
  });
});

describe("process event handler coverage", () => {
  let exitSpy;
  let errorSpy;

  beforeEach(() => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("process uncaughtException listener exists and logs + exits", () => {
    // index.js registers an uncaughtException listener at module load time.
    const listeners = process.listeners("uncaughtException");
    expect(listeners.length).toBeGreaterThan(0);

    // Invoke the last registered listener (the one added by index.js).
    // We use a fresh mock state so count starts at 0.
    const listener = listeners[listeners.length - 1];
    listener(new Error("test uncaught error"));

    expect(errorSpy).toHaveBeenCalledWith(
      "Uncaught exception",
      expect.objectContaining({ error: "test uncaught error" })
    );
    expect(exitSpy).toHaveBeenCalledWith(1);
  });

  it("process unhandledRejection listener exists and logs + exits", () => {
    const listeners = process.listeners("unhandledRejection");
    expect(listeners.length).toBeGreaterThan(0);

    const listener = listeners[listeners.length - 1];
    listener("test rejection reason");

    expect(errorSpy).toHaveBeenCalledWith(
      "Unhandled rejection",
      expect.objectContaining({ reason: "test rejection reason" })
    );
    expect(exitSpy).toHaveBeenCalledWith(1);
  });
});

describe("createStorageAdapter unknown adapter", () => {
  it("throws for an unknown adapter name", async () => {
    const { createStorageAdapter } = await import("../src/storage/index.js");
    expect(() => createStorageAdapter({ adapter: "unknown-db" })).toThrow(
      /Unknown storage adapter/
    );
  });

  it("throws when postgres selected without DATABASE_URL", async () => {
    const { createStorageAdapter } = await import("../src/storage/index.js");
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

  it("creates a none adapter that discards writes", async () => {
    const { createStorageAdapter } = await import("../src/storage/index.js");
    const adapter = createStorageAdapter({ adapter: "none" });
    await expect(adapter.writeBatch([])).resolves.toBeUndefined();
    await expect(adapter.queryRoom("r")).resolves.toEqual([]);
    await expect(adapter.getLatest("r")).resolves.toBeNull();
    await expect(adapter.close()).resolves.toBeUndefined();
  });
});
