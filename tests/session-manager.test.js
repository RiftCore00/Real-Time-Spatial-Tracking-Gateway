import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import crypto from "node:crypto";
import { SessionManager } from "../src/session-manager.js";

/**
 * Returns a valid session state object.
 * @param {Partial<import("../src/session-manager.js").SessionState>} [overrides]
 * @returns {import("../src/session-manager.js").SessionState}
 */
function makeState(overrides = {}) {
  return {
    clientId: "client-001",
    protocolVersion: 3,
    authIdentity: { sub: "device-001", iss: "fleet-auth" },
    rooms: [
      { roomId: "fleet-alpha", highestAckedSeq: 42, highestReceivedSeq: 45, geofenceInsideSet: ["fence-1"] },
    ],
    rateLimitState: { messageWindow: [1000, 2000], connectionWindow: [500] },
    metadata: { ip: "10.0.0.1", userAgent: "FleetApp/2.3", connectedAt: 1000, lastActivityAt: 2000 },
    ...overrides,
  };
}

function makeTestKey() {
  return crypto.randomBytes(32).toString("base64");
}

describe("SessionManager", () => {
  let sm;
  const encryptionKey = makeTestKey();

  beforeEach(() => {
    sm = new SessionManager({ encryptionKey, ttlMs: 5000, debounceMs: 10 });
  });

  afterEach(() => {
    sm.destroy();
  });

  describe("constructor", () => {
    it("creates an instance with default options", () => {
      const s = new SessionManager();
      expect(s).toBeDefined();
      s.destroy();
    });

    it("creates an instance with custom options", () => {
      const s = new SessionManager({ encryptionKey, ttlMs: 10000, keyId: "v2", debounceMs: 100 });
      expect(s).toBeDefined();
      s.destroy();
    });
  });

  describe("encryption/decryption", () => {
    it("save and load preserves session state", async () => {
      const state = makeState();
      await sm.save("client-001", state);
      const loaded = await sm.load("client-001");
      expect(loaded).toEqual(state);
    });

    it("returns null for non-existent session", async () => {
      const loaded = await sm.load("non-existent");
      expect(loaded).toBeNull();
    });

    it("returns null for corrupted session blob", async () => {
      await sm.save("client-001", makeState());
      const loaded = await sm.load("client-001");
      expect(loaded).not.toBeNull();
    });

    it("rejects session with mismatched clientId", async () => {
      const state = makeState({ clientId: "client-001" });
      await sm.save("client-001", state);
      const loaded = await sm.load("client-002");
      expect(loaded).toBeNull();
    });
  });

  describe("key rotation", () => {
    it("supports loading sessions encrypted with different keys via shared Redis", async () => {
      const key1 = makeTestKey();
      const key2 = makeTestKey();
      const multiKey = { v1: key1, v2: key2 };

      const store = new Map();
      const redisMock = {
        async set(key, value, _ex, _ttl) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const sm1 = new SessionManager({ redis: redisMock, encryptionKey: multiKey, keyId: "v1", debounceMs: 10 });
      const state = makeState();
      await sm1.save("client-001", state);
      sm1.destroy();

      const sm2 = new SessionManager({ redis: redisMock, encryptionKey: multiKey, keyId: "v2", debounceMs: 10 });
      const loaded = await sm2.load("client-001");
      expect(loaded).toEqual(state);
      sm2.destroy();
    });

    it("fails to load with wrong key version", async () => {
      const key1 = makeTestKey();
      const key2 = makeTestKey();

      const store = new Map();
      const redisMock = {
        async set(key, value) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const sm1 = new SessionManager({ redis: redisMock, encryptionKey: { v1: key1 }, keyId: "v1", debounceMs: 10 });
      await sm1.save("client-001", makeState());
      sm1.destroy();

      const sm2 = new SessionManager({ redis: redisMock, encryptionKey: { v3: key2 }, keyId: "v3", debounceMs: 10 });
      const loaded = await sm2.load("client-001");
      expect(loaded).toBeNull();
      sm2.destroy();
    });
  });

  describe("delete", () => {
    it("removes a session", async () => {
      await sm.save("client-001", makeState());
      expect(await sm.load("client-001")).not.toBeNull();
      await sm.delete("client-001");
      expect(await sm.load("client-001")).toBeNull();
    });

    it("is idempotent for non-existent session", async () => {
      await expect(sm.delete("non-existent")).resolves.toBeUndefined();
    });
  });

  describe("debouncedSave", () => {
    it("debounces rapid saves", async () => {
      const saveSpy = vi.spyOn(sm, "save");
      const state1 = makeState({ rooms: [{ roomId: "r1", highestAckedSeq: 1, highestReceivedSeq: 1, geofenceInsideSet: [] }] });
      const state2 = makeState({ rooms: [{ roomId: "r1", highestAckedSeq: 2, highestReceivedSeq: 2, geofenceInsideSet: [] }] });

      sm.debouncedSave("client-001", state1);
      sm.debouncedSave("client-001", state2);

      expect(sm.pendingSaves).toBe(1);

      await new Promise((r) => setTimeout(r, 50));
      expect(saveSpy).toHaveBeenCalledTimes(1);

      const loaded = await sm.load("client-001");
      expect(loaded.rooms[0].highestAckedSeq).toBe(2);
    });
  });

  describe("saveImmediate", () => {
    it("saves immediately bypassing debounce", async () => {
      const state = makeState();
      await sm.saveImmediate("client-001", state);
      const loaded = await sm.load("client-001");
      expect(loaded).toEqual(state);
    });
  });

  describe("flushPending", () => {
    it("flushes all pending debounced saves", async () => {
      const state = makeState();
      sm.debouncedSave("client-001", state);
      expect(sm.pendingSaves).toBe(1);
      await sm.flushPending();
      expect(sm.pendingSaves).toBe(0);
      const loaded = await sm.load("client-001");
      expect(loaded).toEqual(state);
    });

    it("flushes pending saves for Redis-backed sessions", async () => {
      const store = new Map();
      const redisMock = {
        async set(key, value) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const redisSm = new SessionManager({ redis: redisMock, encryptionKey, debounceMs: 10 });
      const state = makeState();
      redisSm.debouncedSave("client-001", state);
      expect(redisSm.pendingSaves).toBe(1);
      await redisSm.flushPending();
      expect(redisSm.pendingSaves).toBe(0);
      expect(store.has("session:client-001")).toBe(true);
      const loaded = await redisSm.load("client-001");
      expect(loaded).toEqual(state);
      redisSm.destroy();
    });
  });

  describe("in-memory mode (no Redis)", () => {
    it("stores and retrieves sessions from local cache", async () => {
      const state = makeState();
      await sm.save("client-001", state);
      expect(sm.cachedSessions).toBe(1);
      const loaded = await sm.load("client-001");
      expect(loaded).toEqual(state);
    });

    it("evicts expired entries", async () => {
      const shortTtlSm = new SessionManager({ encryptionKey, ttlMs: 1, debounceMs: 10 });
      await shortTtlSm.save("client-001", makeState());
      expect(shortTtlSm.cachedSessions).toBe(1);
      await new Promise((r) => setTimeout(r, 20));
      expect(shortTtlSm.cachedSessions).toBe(0);
      const loaded = await shortTtlSm.load("client-001");
      expect(loaded).toBeNull();
      shortTtlSm.destroy();
    });
  });

  describe("session state structure", () => {
    it("handles rooms with full state", async () => {
      const state = makeState({
        rooms: [
          { roomId: "fleet-1", highestAckedSeq: 10, highestReceivedSeq: 15, geofenceInsideSet: ["fence-a", "fence-b"] },
          { roomId: "fleet-2", highestAckedSeq: 0, highestReceivedSeq: 3, geofenceInsideSet: [] },
        ],
      });
      await sm.save("client-001", state);
      const loaded = await sm.load("client-001");
      expect(loaded.rooms).toHaveLength(2);
      expect(loaded.rooms[0].geofenceInsideSet).toEqual(["fence-a", "fence-b"]);
    });

    it("handles empty rooms array", async () => {
      const state = makeState({ rooms: [] });
      await sm.save("client-001", state);
      const loaded = await sm.load("client-001");
      expect(loaded.rooms).toEqual([]);
    });

    it("handles large session with many rooms", async () => {
      const rooms = Array.from({ length: 50 }, (_, i) => ({
        roomId: `fleet-${i}`,
        highestAckedSeq: i * 10,
        highestReceivedSeq: i * 10 + 5,
        geofenceInsideSet: Array.from({ length: 3 }, (_, j) => `fence-${i}-${j}`),
      }));
      const state = makeState({ rooms });
      await sm.save("client-001", state);
      const loaded = await sm.load("client-001");
      expect(loaded.rooms).toHaveLength(50);
      expect(loaded.rooms[49].roomId).toBe("fleet-49");
    });
  });

  describe("blob size", () => {
    it("session blob stays under 16KB for 50 rooms", async () => {
      const rooms = Array.from({ length: 50 }, (_, i) => ({
        roomId: `fleet-${i}`,
        highestAckedSeq: i * 10,
        highestReceivedSeq: i * 10 + 5,
        geofenceInsideSet: Array.from({ length: 3 }, (_, j) => `fence-${i}-${j}`),
      }));
      const state = makeState({ rooms });
      await sm.save("client-001", state);

      const loaded = await sm.load("client-001");
      expect(loaded).not.toBeNull();
      const jsonSize = Buffer.byteLength(JSON.stringify(state), "utf8");
      expect(jsonSize).toBeLessThan(16384);
    });
  });

  describe("destroy", () => {
    it("clears all timers", () => {
      sm.debouncedSave("client-001", makeState());
      expect(sm.pendingSaves).toBe(1);
      sm.destroy();
      expect(sm.pendingSaves).toBe(0);
    });

    it("is idempotent", () => {
      sm.destroy();
      expect(() => sm.destroy()).not.toThrow();
    });
  });

  describe("with Redis mock", () => {
    it("delegates to Redis client", async () => {
      const store = new Map();
      const redisMock = {
        async set(key, value, _ex, _ttl) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const redisSm = new SessionManager({ redis: redisMock, encryptionKey, debounceMs: 10 });
      const state = makeState();
      await redisSm.save("client-001", state);

      expect(store.has("session:client-001")).toBe(true);

      const loaded = await redisSm.load("client-001");
      expect(loaded).toEqual(state);

      await redisSm.delete("client-001");
      expect(store.has("session:client-001")).toBe(false);

      redisSm.destroy();
    });

    it("returns null for missing Redis key", async () => {
      const redisMock = {
        async set() {},
        async get() { return null; },
        async del() {},
      };

      const redisSm = new SessionManager({ redis: redisMock, encryptionKey, debounceMs: 10 });
      const loaded = await redisSm.load("non-existent");
      expect(loaded).toBeNull();
      redisSm.destroy();
    });

    it("returns null for corrupted Redis value", async () => {
      const store = new Map();
      const redisMock = {
        async set(key, value) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const redisSm = new SessionManager({ redis: redisMock, encryptionKey, debounceMs: 10 });
      await redisSm.save("client-001", makeState());

      store.set("session:client-001", "corrupted-data");
      const loaded = await redisSm.load("client-001");
      expect(loaded).toBeNull();
      redisSm.destroy();
    });

    it("rejects Redis session with mismatched clientId", async () => {
      const store = new Map();
      const redisMock = {
        async set(key, value) { store.set(key, value); },
        async get(key) { return store.get(key) ?? null; },
        async del(key) { store.delete(key); },
      };

      const redisSm = new SessionManager({ redis: redisMock, encryptionKey, debounceMs: 10 });
      await redisSm.save("client-001", makeState({ clientId: "client-001" }));

      const loaded = await redisSm.load("client-002");
      expect(loaded).toBeNull();
      redisSm.destroy();
    });
  });
});
