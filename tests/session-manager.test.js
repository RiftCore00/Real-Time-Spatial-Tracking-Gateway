/**
 * @fileoverview Unit tests for the encrypted session resumption store.
 *
 * Covers AES-256-GCM round-trips, every `load()` rejection path, sliding TTL,
 * key rotation, debounced saves, both storage backends, the 16 KB blob budget
 * and the issue-18 metrics.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { randomBytes } from "node:crypto";
import { SessionManager } from "../src/session-manager.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

const KEY_A = randomBytes(32).toString("base64");
const KEY_B = randomBytes(32).toString("base64");

/** Keeps expected background failures out of the test output. */
const silentLogger = { error: () => {} };

/** @type {SessionManager[]} */
let managers = [];

/**
 * Builds a manager and registers it for teardown so no timer outlives a test.
 *
 * @param {object} [options]
 * @returns {SessionManager}
 */
function newManager(options = {}) {
  const manager = new SessionManager({
    encryptionKey: KEY_A,
    logger: silentLogger,
    ...options,
  });
  managers.push(manager);
  return manager;
}

/**
 * Fake node-redis v4 client recording every call.
 *
 * @returns {{ entries: Map<string, { value: string, opts: object }>, set: Function, get: Function, del: Function }}
 */
function makeFakeRedis() {
  const entries = new Map();
  return {
    entries,
    set: vi.fn(async (key, value, opts) => {
      entries.set(key, { value, opts });
      return "OK";
    }),
    get: vi.fn(async (key) => entries.get(key)?.value ?? null),
    del: vi.fn(async (key) => (entries.delete(key) ? 1 : 0)),
  };
}

/**
 * @param {object} [overrides]
 * @returns {import("../src/session-manager.js").SessionState}
 */
function makeState(overrides = {}) {
  return {
    clientId: "client-001",
    protocolVersion: 3,
    authIdentity: { sub: "device-123", iss: "fleet-auth" },
    rooms: [
      {
        roomId: "fleet-alpha",
        highestAckedSeq: 42,
        highestReceivedSeq: 45,
        geofenceInsideSet: ["fence-1", "fence-3"],
      },
    ],
    rateLimitState: { messageWindow: [1001, 1002, 1003], connectionWindow: [900] },
    metadata: {
      ip: "10.0.0.1",
      userAgent: "FleetApp/2.3",
      connectedAt: 1700000000000,
      lastActivityAt: 1700000009000,
    },
    ...overrides,
  };
}

/**
 * Session state for a client subscribed to 50 rooms with full per-room state.
 *
 * @returns {import("../src/session-manager.js").SessionState}
 */
function makeFiftyRoomState() {
  const now = 1700000000000;
  const rooms = Array.from({ length: 50 }, (_, i) => ({
    roomId: `fleet-region-${i}-vehicles`,
    highestAckedSeq: 100000 + i * 7,
    highestReceivedSeq: 100010 + i * 7,
    geofenceInsideSet: [`fence-${i}-depot`, `fence-${i}-zone-a`, `fence-${i}-zone-b`],
  }));
  return {
    clientId: "client-fleet-050",
    protocolVersion: 3,
    authIdentity: { sub: "device-abcdef123456", iss: "fleet-auth", aud: "gateway", exp: now },
    rooms,
    rateLimitState: {
      messageWindow: Array.from({ length: 100 }, (_, i) => now - i * 9),
      connectionWindow: Array.from({ length: 10 }, (_, i) => now - i * 1000),
    },
    metadata: {
      ip: "10.42.0.17",
      userAgent: "FleetApp/2.3 (iOS 17.4)",
      connectedAt: now - 60000,
      lastActivityAt: now,
    },
  };
}

/**
 * Flips one ciphertext byte, leaving the blob structurally valid.
 *
 * @param {string} blob
 * @returns {string}
 */
function corruptCiphertext(blob) {
  const parts = blob.split(".");
  const ciphertext = Buffer.from(parts[2], "base64");
  ciphertext[0] ^= 0xff;
  parts[2] = ciphertext.toString("base64");
  return parts.join(".");
}

/**
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

afterEach(async () => {
  for (const manager of managers) await manager.close();
  managers = [];
  vi.useRealTimers();
  vi.restoreAllMocks();
});

// ─── key material ─────────────────────────────────────────────────────────────

describe("SessionManager key material", () => {
  it("throws when encryptionKey is missing", () => {
    expect(() => new SessionManager({})).toThrow(/encryptionKey is required/);
  });

  it("throws when the base64 key is not 32 bytes", () => {
    expect(() => new SessionManager({ encryptionKey: randomBytes(16).toString("base64") })).toThrow(
      /must decode to 32 bytes, got 16/
    );
  });

  it("throws when a key map entry is not a string", () => {
    expect(() => new SessionManager({ encryptionKey: { v1: 42 } })).toThrow(
      /must be a non-empty base64 string/
    );
  });

  it("throws when keyId is absent from the key map", () => {
    expect(() => new SessionManager({ encryptionKey: { v1: KEY_A }, keyId: "v9" })).toThrow(
      /keyId "v9" is absent/
    );
  });

  it("throws when keyId contains the blob separator", () => {
    expect(() => new SessionManager({ encryptionKey: KEY_A, keyId: "v.1" })).toThrow(
      /must not contain/
    );
  });

  it("throws on malformed JSON key material", () => {
    expect(() => new SessionManager({ encryptionKey: '{"v1": ' })).toThrow(/does not parse/);
  });

  it("accepts a JSON string key map", async () => {
    const manager = newManager({ encryptionKey: JSON.stringify({ v1: KEY_A, v2: KEY_B }) });
    const blob = await manager.save("client-001", makeState());
    expect(blob.startsWith("v1.")).toBe(true);
    await expect(manager.load(blob)).resolves.toEqual(makeState());
  });
});

// ─── save / load round trip ───────────────────────────────────────────────────

describe("SessionManager save/load", () => {
  it("round-trips state through encryption and compression", async () => {
    const manager = newManager();
    const state = makeState();
    const blob = await manager.save("client-001", state);

    await expect(manager.load(blob)).resolves.toEqual(state);
  });

  it("produces a keyId.iv.ciphertext.tag blob", async () => {
    const manager = newManager({ keyId: "v7", encryptionKey: { v7: KEY_A } });
    const blob = await manager.save("client-001", makeState());
    const parts = blob.split(".");

    expect(parts).toHaveLength(4);
    expect(parts[0]).toBe("v7");
    expect(Buffer.from(parts[1], "base64")).toHaveLength(12);
    expect(Buffer.from(parts[2], "base64").length).toBeGreaterThan(0);
    expect(Buffer.from(parts[3], "base64")).toHaveLength(16);
  });

  it("never stores plaintext state", async () => {
    const redis = makeFakeRedis();
    const manager = newManager({ redis });
    const blob = await manager.save("client-001", makeState());

    expect(blob).not.toContain("fleet-alpha");
    expect(redis.entries.get("session:client-001").value).not.toContain("device-123");
  });

  it("uses a fresh IV for every save", async () => {
    const manager = newManager();
    const first = await manager.save("client-001", makeState());
    const second = await manager.save("client-001", makeState());

    expect(first.split(".")[1]).not.toBe(second.split(".")[1]);
    await expect(manager.load(first)).resolves.toEqual(makeState());
  });

  it("rejects a non-object state and an empty clientId", async () => {
    const manager = newManager();
    await expect(manager.save("client-001", null)).rejects.toThrow(/state must be an object/);
    await expect(manager.save("", makeState())).rejects.toThrow(/clientId/);
  });
});

// ─── load failure paths ───────────────────────────────────────────────────────

describe("SessionManager load failures", () => {
  it("returns null for a corrupted ciphertext byte", async () => {
    const manager = newManager();
    const blob = await manager.save("client-001", makeState());

    await expect(manager.load(corruptCiphertext(blob))).resolves.toBeNull();
    expect(manager.metrics.session_resumption_total.decrypt_failed).toBe(1);
  });

  it("returns null for a tampered auth tag", async () => {
    const manager = newManager();
    const parts = (await manager.save("client-001", makeState())).split(".");
    const tag = Buffer.from(parts[3], "base64");
    tag[15] ^= 0x01;
    parts[3] = tag.toString("base64");

    await expect(manager.load(parts.join("."))).resolves.toBeNull();
  });

  it("returns null for truncated and garbage blobs", async () => {
    const manager = newManager();
    const blob = await manager.save("client-001", makeState());

    for (const bad of [
      "",
      "garbage",
      "v1.only.three",
      "v1.a.b.c.d",
      blob.split(".").slice(0, 3).join("."),
      blob.slice(0, blob.length - 4),
    ]) {
      await expect(manager.load(bad)).resolves.toBeNull();
    }
  });

  it("returns null instead of throwing for non-string input", async () => {
    const manager = newManager();
    for (const bad of [undefined, null, 42, {}, []]) {
      await expect(manager.load(bad)).resolves.toBeNull();
    }
  });

  it("returns null when the blob was sealed with a different key", async () => {
    const redis = makeFakeRedis();
    const writer = newManager({ redis, encryptionKey: KEY_A });
    const reader = newManager({ redis, encryptionKey: KEY_B });

    const blob = await writer.save("client-001", makeState());

    await expect(reader.load(blob)).resolves.toBeNull();
    expect(reader.metrics.session_resumption_total.decrypt_failed).toBe(1);
    expect(reader.metrics.session_resumption_total.expired).toBe(0);
  });

  it("returns null once the TTL has expired", async () => {
    const manager = newManager({ ttlMs: 60 });
    const blob = await manager.save("client-001", makeState());

    await sleep(90);

    await expect(manager.load(blob)).resolves.toBeNull();
    expect(manager.metrics.session_resumption_total.expired).toBe(1);
    expect(manager.metrics.session_resumption_total.decrypt_failed).toBe(0);
  });

  it("returns null for a deleted session", async () => {
    const manager = newManager();
    const blob = await manager.save("client-001", makeState());

    await manager.delete("client-001");

    await expect(manager.load(blob)).resolves.toBeNull();
    expect(manager.metrics.session_resumption_total.expired).toBe(1);
  });

  it("counts a storage read error as expired", async () => {
    const redis = makeFakeRedis();
    const manager = newManager({ redis });
    const blob = await manager.save("client-001", makeState());
    redis.get.mockRejectedValueOnce(new Error("connection lost"));

    await expect(manager.load(blob)).resolves.toBeNull();
    expect(manager.metrics.session_resumption_total.expired).toBe(1);
  });
});

// ─── sliding TTL ──────────────────────────────────────────────────────────────

describe("SessionManager sliding TTL", () => {
  it("refreshes the expiry on every save", async () => {
    vi.useFakeTimers();
    const manager = newManager({ ttlMs: 1000 });
    await manager.save("client-001", makeState());

    await vi.advanceTimersByTimeAsync(700);
    const refreshed = await manager.save("client-001", makeState());
    await vi.advanceTimersByTimeAsync(700);

    // 1400 ms after the first save, but only 700 ms after the second.
    await expect(manager.load(refreshed)).resolves.toEqual(makeState());
  });

  it("lets a session that is not re-saved lapse", async () => {
    vi.useFakeTimers();
    const manager = newManager({ ttlMs: 1000 });
    const stale = await manager.save("client-stale", makeState({ clientId: "client-stale" }));

    await vi.advanceTimersByTimeAsync(700);
    const fresh = await manager.save("client-001", makeState());
    await vi.advanceTimersByTimeAsync(700);

    await expect(manager.load(stale)).resolves.toBeNull();
    await expect(manager.load(fresh)).resolves.toEqual(makeState());
  });

  it("sends PX on every redis write", async () => {
    const redis = makeFakeRedis();
    const manager = newManager({ redis, ttlMs: 45000 });

    await manager.save("client-001", makeState());
    await manager.save("client-001", makeState());

    expect(redis.set).toHaveBeenCalledTimes(2);
    for (const call of redis.set.mock.calls) {
      expect(call[0]).toBe("session:client-001");
      expect(call[2]).toEqual({ PX: 45000 });
    }
  });
});

// ─── key rotation ─────────────────────────────────────────────────────────────

describe("SessionManager key rotation", () => {
  it("loads v1 blobs after rotating to v2 and seals new blobs with v2", async () => {
    const redis = makeFakeRedis();
    const before = newManager({ redis, encryptionKey: { v1: KEY_A }, keyId: "v1" });
    const after = newManager({ redis, encryptionKey: { v1: KEY_A, v2: KEY_B }, keyId: "v2" });

    const oldBlob = await before.save("client-001", makeState());
    expect(oldBlob.startsWith("v1.")).toBe(true);

    await expect(after.load(oldBlob)).resolves.toEqual(makeState());

    const newBlob = await after.save("client-001", makeState());
    expect(newBlob.startsWith("v2.")).toBe(true);
    await expect(after.load(newBlob)).resolves.toEqual(makeState());

    // The pre-rotation manager has no v2 key, so it cannot open the new blob.
    await expect(before.load(newBlob)).resolves.toBeNull();
  });

  it("falls back to the other keys when the blob prefix is unknown", async () => {
    const redis = makeFakeRedis();
    const writer = newManager({ redis, encryptionKey: { v1: KEY_A }, keyId: "v1" });
    const reader = newManager({ redis, encryptionKey: { v2: KEY_B, v1: KEY_A }, keyId: "v2" });

    const blob = await writer.save("client-001", makeState());
    const relabelled = ["v99", ...blob.split(".").slice(1)].join(".");

    await expect(reader.load(relabelled)).resolves.toEqual(makeState());
  });
});

// ─── debounced saves ──────────────────────────────────────────────────────────

describe("SessionManager debouncedSave", () => {
  let redis;
  let manager;

  beforeEach(() => {
    vi.useFakeTimers();
    redis = makeFakeRedis();
    manager = newManager({ redis });
  });

  it("coalesces rapid calls into a single save of the newest state", async () => {
    const provider = vi.fn(() => makeState({ protocolVersion: 9 }));

    await manager.debouncedSave("client-001", () => makeState({ protocolVersion: 1 }));
    await manager.debouncedSave("client-001", () => makeState({ protocolVersion: 2 }));
    await manager.debouncedSave("client-001", provider);

    expect(redis.set).not.toHaveBeenCalled();
    expect(provider).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(500);

    expect(redis.set).toHaveBeenCalledTimes(1);
    expect(provider).toHaveBeenCalledTimes(1);
    const blob = redis.set.mock.calls[0][1];
    await expect(manager.load(blob)).resolves.toEqual(makeState({ protocolVersion: 9 }));
  });

  it("does not fire before the debounce window elapses", async () => {
    await manager.debouncedSave("client-001", () => makeState());

    await vi.advanceTimersByTimeAsync(499);
    expect(redis.set).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(redis.set).toHaveBeenCalledTimes(1);
  });

  it("honours a custom debounceMs", async () => {
    const fast = newManager({ redis, debounceMs: 50 });
    await fast.debouncedSave("client-001", () => makeState());

    await vi.advanceTimersByTimeAsync(50);
    expect(redis.set).toHaveBeenCalledTimes(1);
  });

  it("keeps separate windows per client", async () => {
    await manager.debouncedSave("client-a", () => makeState({ clientId: "client-a" }));
    await manager.debouncedSave("client-b", () => makeState({ clientId: "client-b" }));

    await vi.advanceTimersByTimeAsync(500);

    expect(redis.set).toHaveBeenCalledTimes(2);
    expect(redis.entries.has("session:client-a")).toBe(true);
    expect(redis.entries.has("session:client-b")).toBe(true);
  });

  it("flush stores immediately and cancels the pending timer", async () => {
    await manager.debouncedSave("client-001", () => makeState());

    const blob = await manager.flush("client-001");

    expect(redis.set).toHaveBeenCalledTimes(1);
    expect(typeof blob).toBe("string");

    await vi.advanceTimersByTimeAsync(500);
    expect(redis.set).toHaveBeenCalledTimes(1);
  });

  it("flush returns null when nothing is pending", async () => {
    await expect(manager.flush("client-001")).resolves.toBeNull();
    expect(redis.set).not.toHaveBeenCalled();
  });

  it("flushAll stores every pending session", async () => {
    for (const id of ["c1", "c2", "c3"]) {
      await manager.debouncedSave(id, () => makeState({ clientId: id }));
    }

    const blobs = await manager.flushAll();

    expect(blobs).toHaveLength(3);
    expect(redis.set).toHaveBeenCalledTimes(3);
    expect(await manager.flushAll()).toEqual([]);
  });

  it("swallows and logs a save failure raised inside the timer", async () => {
    await manager.debouncedSave("client-001", () => {
      throw new Error("state gone");
    });

    await vi.advanceTimersByTimeAsync(500);

    expect(redis.set).not.toHaveBeenCalled();
  });

  it("rejects an invalid stateProvider", async () => {
    await expect(manager.debouncedSave("client-001", "nope")).rejects.toThrow(/stateProvider/);
  });

  it("delete cancels a pending debounced save", async () => {
    await manager.debouncedSave("client-001", () => makeState());
    await manager.delete("client-001");

    await vi.advanceTimersByTimeAsync(500);

    expect(redis.set).not.toHaveBeenCalled();
    expect(redis.del).toHaveBeenCalledWith("session:client-001");
  });

  it("close clears pending timers", async () => {
    await manager.debouncedSave("client-001", () => makeState());
    await manager.close();
    await manager.close();

    await vi.advanceTimersByTimeAsync(500);

    expect(redis.set).not.toHaveBeenCalled();
  });
});

// ─── storage backends ─────────────────────────────────────────────────────────

describe("SessionManager storage backends", () => {
  it("works without redis using the in-memory fallback", async () => {
    const manager = newManager({ redis: null });
    const blob = await manager.save("client-001", makeState());

    await expect(manager.load(blob)).resolves.toEqual(makeState());

    await manager.delete("client-001");
    await expect(manager.load(blob)).resolves.toBeNull();
  });

  it("sweeps expired in-memory entries on a periodic timer", async () => {
    vi.useFakeTimers();
    const manager = newManager({ ttlMs: 1000 });
    await manager.save("client-001", makeState());
    expect(manager._memory.size).toBe(1);

    await vi.advanceTimersByTimeAsync(1100);

    expect(manager._memory.size).toBe(0);
  });

  it("drives a redis client with session: keys and PX TTL", async () => {
    const redis = makeFakeRedis();
    const manager = newManager({ redis, ttlMs: 1234 });

    const blob = await manager.save("client-001", makeState());
    expect(redis.set).toHaveBeenCalledWith("session:client-001", blob, { PX: 1234 });

    await manager.load(blob);
    expect(redis.get).toHaveBeenCalledWith("session:client-001");

    await manager.delete("client-001");
    expect(redis.del).toHaveBeenCalledWith("session:client-001");
    expect(redis.entries.size).toBe(0);
  });
});

// ─── blob size budget ─────────────────────────────────────────────────────────

describe("SessionManager blob size", () => {
  it("keeps a 50-room session under 16 KB", async () => {
    const manager = newManager();
    const state = makeFiftyRoomState();

    const blob = await manager.save(state.clientId, state);

    expect(Buffer.byteLength(blob, "utf8")).toBeLessThan(16384);
    expect(manager.metrics.session_state_size_bytes).toBe(Buffer.byteLength(blob, "utf8"));
    await expect(manager.load(blob)).resolves.toEqual(state);
  });

  it("rejects a state whose blob exceeds 16 KB", async () => {
    const manager = newManager();
    const state = makeState({ metadata: { payload: randomBytes(24 * 1024).toString("base64") } });

    await expect(manager.save("client-001", state)).rejects.toThrow(/over the 16384 byte limit/);
  });
});

// ─── metrics ──────────────────────────────────────────────────────────────────

describe("SessionManager metrics", () => {
  it("starts every counter at zero", () => {
    const manager = newManager();

    expect(manager.metrics).toEqual({
      session_resumption_total: {
        success: 0,
        decrypt_failed: 0,
        expired: 0,
        mismatch: 0,
        new_session: 0,
      },
      session_state_size_bytes: 0,
    });
  });

  it("counts each load outcome", async () => {
    const manager = newManager({ ttlMs: 60 });
    const blob = await manager.save("client-001", makeState());

    await manager.load(blob);
    await manager.load(blob);
    await manager.load("garbage");
    await sleep(90);
    await manager.load(blob);

    expect(manager.metrics.session_resumption_total).toMatchObject({
      success: 2,
      decrypt_failed: 1,
      expired: 1,
    });
  });

  it("records server-side outcomes through recordResumption", () => {
    const manager = newManager();

    manager.recordResumption("mismatch");
    manager.recordResumption("new_session");
    manager.recordResumption("new_session");

    expect(manager.metrics.session_resumption_total.mismatch).toBe(1);
    expect(manager.metrics.session_resumption_total.new_session).toBe(2);
  });

  it("throws on an unknown resumption result", () => {
    const manager = newManager();
    expect(() => manager.recordResumption("bogus")).toThrow(/unknown resumption result/);
  });

  it("tracks the last observed blob size", async () => {
    const manager = newManager();
    await manager.save("client-001", makeState());
    const small = manager.metrics.session_state_size_bytes;

    await manager.save("client-001", makeFiftyRoomState());

    expect(small).toBeGreaterThan(0);
    expect(manager.metrics.session_state_size_bytes).toBeGreaterThan(small);
  });

  it("returns a snapshot that cannot mutate internal counters", () => {
    const manager = newManager();
    const snapshot = manager.metrics;

    snapshot.session_resumption_total.success = 99;
    snapshot.session_state_size_bytes = 99;

    expect(manager.metrics.session_resumption_total.success).toBe(0);
    expect(manager.metrics.session_state_size_bytes).toBe(0);
  });
});
