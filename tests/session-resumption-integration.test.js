import { describe, it, expect, beforeEach, afterEach } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";
import { SessionManager } from "../src/session-manager.js";

const TEST_SECRET = "test-secret-session-resumption";
const KEY_V1 = Buffer.alloc(32, 7).toString("base64");
const KEY_V2 = Buffer.alloc(32, 9).toString("base64");

/** Signs the HS256 token the gateway authenticates with. */
function makeToken(clientId, claims = {}) {
  return jwt.sign({ sub: clientId, ...claims }, TEST_SECRET, { expiresIn: 60 });
}

/** In-memory stand-in for node-redis v4 with PX expiry and call counters. */
function createFakeRedis() {
  const entries = new Map();
  const calls = { get: 0, set: 0, del: 0 };
  return {
    entries,
    calls,
    async set(key, value, opts) {
      calls.set++;
      entries.set(key, { value, expiresAt: Date.now() + (opts?.PX ?? 60000) });
    },
    async get(key) {
      calls.get++;
      const entry = entries.get(key);
      if (!entry) return null;
      if (entry.expiresAt <= Date.now()) {
        entries.delete(key);
        return null;
      }
      return entry.value;
    },
    async del(key) {
      calls.del++;
      entries.delete(key);
    },
  };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Polls `fn` until it returns something truthy. */
async function waitFor(fn, { timeout = 1000, interval = 10, label = "condition" } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const result = await fn();
    if (result) return result;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await sleep(interval);
  }
}

/** Waits for the first buffered frame of a given type. */
function waitForFrame(ws, type, options = {}) {
  return waitFor(() => ws.frames.find((frame) => frame.type === type), {
    label: `${type} frame`,
    ...options,
  });
}

function waitClose(ws) {
  return new Promise((resolve) => {
    ws.once("close", (code, reason) => resolve({ code, reason: reason?.toString() ?? "" }));
  });
}

/** Session state in the shape the gateway captures. */
function makeState(clientId, roomIds = [], overrides = {}) {
  return {
    clientId,
    protocolVersion: 1,
    authIdentity: { sub: clientId },
    rooms: roomIds.map((roomId) => ({
      roomId,
      highestAckedSeq: 0,
      highestReceivedSeq: 0,
      geofenceInsideSet: [],
    })),
    rateLimitState: { messageWindow: [], connectionWindow: [] },
    metadata: {
      ip: "127.0.0.1",
      userAgent: "vitest",
      connectedAt: Date.now(),
      lastActivityAt: Date.now(),
    },
    ...overrides,
  };
}

describe("session resumption (integration)", () => {
  /** @type {Array<object>} */
  const gateways = [];
  /** @type {Array<WebSocket>} */
  const sockets = [];

  /**
   * Starts a gateway on an ephemeral port with a fake-redis backed manager.
   * `inspector` shares the store and keys so tests can decrypt saved blobs.
   */
  function startGateway({
    encryptionKey = KEY_V1,
    keyId = "v1",
    ttlMs = 60000,
    debounceMs = 50,
    instanceId = "instance-1",
    redis = createFakeRedis(),
    sessionManager,
    withSessions = true,
  } = {}) {
    const manager =
      sessionManager ??
      (withSessions ? new SessionManager({ redis, encryptionKey, keyId, ttlMs, debounceMs }) : null);
    const inspector = withSessions ? new SessionManager({ redis, encryptionKey, keyId, ttlMs }) : null;
    const server = createServer({
      port: 0,
      heartbeatMs: 60000,
      maxPayloadBytes: 4096,
      instanceId,
      ...(manager ? { sessionManager: manager } : {}),
    });
    const gateway = { server, port: server.wss.address().port, redis, manager, inspector, instanceId };
    gateways.push(gateway);
    return gateway;
  }

  /** Opens a client socket that buffers every frame it receives. */
  function connect(port, token, { sessionId, cookie } = {}) {
    let url = `ws://localhost:${port}/?token=${token}`;
    if (sessionId) url += `&session_id=${encodeURIComponent(sessionId)}`;
    const options = cookie ? { headers: { Cookie: cookie } } : undefined;

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, options);
      ws.frames = [];
      ws.upgradeHeaders = null;
      sockets.push(ws);
      ws.on("upgrade", (res) => {
        ws.upgradeHeaders = res.headers;
      });
      ws.on("message", (data) => ws.frames.push(JSON.parse(data.toString())));
      ws.once("open", () => resolve(ws));
      ws.once("error", reject);
    });
  }

  /** Closes a client and waits until the gateway has released the connection. */
  async function disconnect(gateway, ws) {
    const before = gateway.server.wss.clients.size;
    const closed = waitClose(ws);
    ws.close();
    await closed;
    await waitFor(() => gateway.server.wss.clients.size < before, { label: "server-side close" });
  }

  /** Waits for a stored blob whose decrypted state satisfies `predicate`. */
  function waitForSavedState(gateway, clientId, predicate = () => true) {
    return waitFor(
      async () => {
        const entry = gateway.redis.entries.get(`session:${clientId}`);
        if (!entry) return null;
        const state = await gateway.inspector.load(entry.value);
        return state && predicate(state) ? { blob: entry.value, state } : null;
      },
      { label: `saved session for ${clientId}` }
    );
  }

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
  });

  afterEach(async () => {
    for (const ws of sockets) {
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.terminate();
    }
    sockets.length = 0;

    for (const gateway of gateways) {
      for (const client of gateway.server.wss.clients) client.terminate();
      await new Promise((resolve) => gateway.server.wss.close(resolve));
      if (gateway.manager) await gateway.manager.close();
      if (gateway.inspector) await gateway.inspector.close();
    }
    gateways.length = 0;
    delete process.env.AUTH_SECRET;
  });

  it("resumes rooms and sequence numbers, and the client is a member again", async () => {
    const gateway = startGateway();

    const a = await connect(gateway.port, makeToken("client-a"));
    a.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await waitForFrame(a, "room_joined");

    const b = await connect(gateway.port, makeToken("client-b"));
    b.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await waitForFrame(b, "room_joined");

    b.send(JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 2 } }));
    await waitForFrame(a, "location_update");

    a.send(JSON.stringify({ type: "reconnect", roomId: "fleet-1", lastSeq: 1 }));
    await waitForFrame(a, "replay_complete");

    const { blob } = await waitForSavedState(
      gateway,
      "client-a",
      (state) => state.rooms[0]?.highestAckedSeq === 1
    );

    await disconnect(gateway, a);
    await waitFor(() => gateway.server.rooms.getRoomSize("fleet-1") === 1, {
      label: "membership cleanup",
    });

    const resumed = await connect(gateway.port, makeToken("client-a"), { sessionId: blob });
    const frame = await waitForFrame(resumed, "session_resumed");

    expect(frame.payload.rooms).toEqual([
      { roomId: "fleet-1", highestAckedSeq: 1, highestReceivedSeq: 1 },
    ]);
    expect(frame.payload.currentSeqPerRoom).toEqual({ "fleet-1": 1 });
    expect(gateway.server.rooms.getRoomSize("fleet-1")).toBe(2);

    b.send(JSON.stringify({ type: "location_update", payload: { latitude: 3, longitude: 4 } }));
    const broadcast = await waitForFrame(resumed, "location_update");
    expect(broadcast.payload.latitude).toBe(3);
  });

  it("accepts the session blob from the JWT sid claim", async () => {
    const gateway = startGateway();
    const blob = await gateway.manager.save("client-sid", makeState("client-sid", ["fleet-sid"]));

    const ws = await connect(gateway.port, makeToken("client-sid", { sid: blob }));
    const frame = await waitForFrame(ws, "session_resumed");

    expect(frame.payload.rooms.map((room) => room.roomId)).toEqual(["fleet-sid"]);
    expect(gateway.server.rooms.getRoomSize("fleet-sid")).toBe(1);
  });

  it("builds its own manager from an encryption key and the injected store", async () => {
    const redis = createFakeRedis();
    const server = createServer({
      port: 0,
      heartbeatMs: 60000,
      maxPayloadBytes: 4096,
      sessionEncryptionKey: KEY_V1,
      sessionTtlMs: 60000,
      instanceId: "instance-own",
      redis,
    });
    const gateway = {
      server,
      port: server.wss.address().port,
      redis,
      manager: server.sessionManager,
      inspector: new SessionManager({ redis, encryptionKey: KEY_V1, ttlMs: 60000 }),
    };
    gateways.push(gateway);

    expect(server.sessionManager).toBeInstanceOf(SessionManager);
    expect(server.instanceId).toBe("instance-own");

    const ws = await connect(gateway.port, makeToken("client-own"));
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-own" }));
    await waitForFrame(ws, "room_joined");

    const { state } = await waitForSavedState(gateway, "client-own");
    expect(state.rooms.map((room) => room.roomId)).toEqual(["fleet-own"]);
  });

  it("reads SESSION_ENCRYPTION_KEY from the environment and works without a store", async () => {
    process.env.SESSION_ENCRYPTION_KEY = KEY_V1;
    let server;
    try {
      server = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096 });
    } finally {
      delete process.env.SESSION_ENCRYPTION_KEY;
    }
    const gateway = {
      server,
      port: server.wss.address().port,
      redis: null,
      manager: server.sessionManager,
      inspector: null,
    };
    gateways.push(gateway);

    expect(server.sessionManager).toBeInstanceOf(SessionManager);
    expect(server.instanceId).toMatch(/^[0-9a-f-]{36}$/);

    const first = await connect(gateway.port, makeToken("client-env"));
    first.send(JSON.stringify({ type: "join_room", roomId: "fleet-env" }));
    await waitForFrame(first, "room_joined");
    await disconnect(gateway, first);

    const second = await connect(gateway.port, makeToken("client-env"), {
      cookie: `GW_AFFINITY=${server.instanceId}`,
    });
    const frame = await waitForFrame(second, "session_resumed");
    expect(frame.payload.rooms.map((room) => room.roomId)).toEqual(["fleet-env"]);
  });

  it("treats an expired session as a new session", async () => {
    const gateway = startGateway({ ttlMs: 60 });
    const blob = await gateway.manager.save("client-exp", makeState("client-exp", ["fleet-1"]));
    await sleep(120);

    const ws = await connect(gateway.port, makeToken("client-exp"), { sessionId: blob });
    await sleep(150);

    expect(ws.frames.find((f) => f.type === "session_resumed")).toBeUndefined();
    expect(gateway.manager.metrics.session_resumption_total.expired).toBe(1);
    expect(gateway.server.rooms.getRoomSize("fleet-1")).toBe(0);

    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await waitForFrame(ws, "room_joined");
  });

  it("treats a corrupted session blob as a new session", async () => {
    const gateway = startGateway();
    const blob = await gateway.manager.save("client-bad", makeState("client-bad", ["fleet-1"]));
    const parts = blob.split(".");
    parts[2] = (parts[2][0] === "A" ? "B" : "A") + parts[2].slice(1);
    const tampered = parts.join(".");

    const ws = await connect(gateway.port, makeToken("client-bad"), { sessionId: tampered });
    await sleep(150);

    expect(ws.frames.find((f) => f.type === "session_resumed")).toBeUndefined();
    expect(gateway.manager.metrics.session_resumption_total.decrypt_failed).toBe(1);

    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await waitForFrame(ws, "room_joined");
  });

  it("ignores a session blob issued for another identity", async () => {
    const gateway = startGateway();
    const blob = await gateway.manager.save("client-one", makeState("client-one", ["fleet-1"]));

    const ws = await connect(gateway.port, makeToken("client-two"), { sessionId: blob });
    await sleep(150);

    expect(ws.frames.find((f) => f.type === "session_resumed")).toBeUndefined();
    expect(gateway.manager.metrics.session_resumption_total.mismatch).toBe(1);
    expect(gateway.server.rooms.getClientRooms("client-two").size).toBe(0);
  });

  it("persists a debounced save to the store within 1s of join_room", async () => {
    const gateway = startGateway({ debounceMs: 500 });

    const ws = await connect(gateway.port, makeToken("client-debounce"));
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-9" }));
    await waitForFrame(ws, "room_joined");

    const startedAt = Date.now();
    const { state } = await waitForSavedState(gateway, "client-debounce");

    expect(Date.now() - startedAt).toBeLessThan(1000);
    expect(state.rooms.map((room) => room.roomId)).toEqual(["fleet-9"]);
    expect(state.authIdentity).toEqual({ sub: "client-debounce" });
  });

  it("sets GW_AFFINITY on upgrade and restores from the local cache without a store read", async () => {
    const gateway = startGateway({ instanceId: "instance-7" });

    const first = await connect(gateway.port, makeToken("client-sticky"));
    const cookies = first.upgradeHeaders["set-cookie"] ?? [];
    expect(cookies.join(";")).toContain("GW_AFFINITY=instance-7");
    expect(cookies.join(";")).toContain("HttpOnly");
    expect(cookies.join(";")).toContain("SameSite=Lax");

    first.send(JSON.stringify({ type: "join_room", roomId: "fleet-sticky" }));
    await waitForFrame(first, "room_joined");
    await disconnect(gateway, first);

    const readsBefore = gateway.redis.calls.get;
    const second = await connect(gateway.port, makeToken("client-sticky"), {
      cookie: "GW_AFFINITY=instance-7",
    });
    const frame = await waitForFrame(second, "session_resumed");

    expect(frame.payload.rooms.map((room) => room.roomId)).toEqual(["fleet-sticky"]);
    expect(gateway.redis.calls.get).toBe(readsBefore);
    expect(gateway.server.rooms.getRoomSize("fleet-sticky")).toBe(1);
  });

  it("migrates a client on demand and resumes from the handed-over blob", async () => {
    const gateway = startGateway();

    const ws = await connect(gateway.port, makeToken("client-migrate"));
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-migrate" }));
    await waitForFrame(ws, "room_joined");

    const closed = waitClose(ws);
    const res = await fetch(`http://localhost:${gateway.port}/admin/v1/clients/client-migrate/migrate`, {
      method: "POST",
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });

    const { code, reason } = await closed;
    expect(code).toBe(4100);

    const frame = ws.frames.find((f) => f.type === "migrate");
    const sessionId = frame ? frame.payload.session_id : reason;
    expect(sessionId.split(".")).toHaveLength(4);

    await waitFor(() => gateway.server.wss.clients.size === 0, { label: "server-side close" });

    const resumed = await connect(gateway.port, makeToken("client-migrate"), { sessionId });
    const handshake = await waitForFrame(resumed, "session_resumed");
    expect(handshake.payload.rooms.map((room) => room.roomId)).toEqual(["fleet-migrate"]);
  });

  it("returns 404 from the migrate endpoint for an unknown client", async () => {
    const gateway = startGateway();
    const res = await fetch(`http://localhost:${gateway.port}/admin/v1/clients/nobody/migrate`, {
      method: "POST",
    });
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "Not Found" });
  });

  it("opens a v1 blob after rotation and seals new saves with v2", async () => {
    const redis = createFakeRedis();
    const legacy = new SessionManager({ redis, encryptionKey: KEY_V1, keyId: "v1" });
    const blob = await legacy.save("client-rot", makeState("client-rot", ["fleet-rot"]));
    expect(blob.startsWith("v1.")).toBe(true);
    await legacy.close();

    const gateway = startGateway({
      redis,
      encryptionKey: { v1: KEY_V1, v2: KEY_V2 },
      keyId: "v2",
    });

    const ws = await connect(gateway.port, makeToken("client-rot"), { sessionId: blob });
    const frame = await waitForFrame(ws, "session_resumed");
    expect(frame.payload.rooms.map((room) => room.roomId)).toEqual(["fleet-rot"]);

    const rotated = await waitFor(
      () => {
        const entry = redis.entries.get("session:client-rot");
        return entry && entry.value.startsWith("v2.") ? entry.value : null;
      },
      { label: "v2 blob" }
    );
    expect(rotated.startsWith("v2.")).toBe(true);
  });

  it("saves every live session on saveAllSessions()", async () => {
    const gateway = startGateway({ debounceMs: 5000 });

    const first = await connect(gateway.port, makeToken("client-s1"));
    first.send(JSON.stringify({ type: "join_room", roomId: "fleet-s1" }));
    await waitForFrame(first, "room_joined");

    const second = await connect(gateway.port, makeToken("client-s2"));
    second.send(JSON.stringify({ type: "join_room", roomId: "fleet-s2" }));
    await waitForFrame(second, "room_joined");

    const blobs = await gateway.server.saveAllSessions();

    expect([...blobs.keys()].sort()).toEqual(["client-s1", "client-s2"]);
    expect(gateway.redis.entries.has("session:client-s1")).toBe(true);
    expect(gateway.redis.entries.has("session:client-s2")).toBe(true);

    const state = await gateway.inspector.load(blobs.get("client-s2"));
    expect(state.rooms.map((room) => room.roomId)).toEqual(["fleet-s2"]);
  });

  it("reports resumption counters on GET /metrics", async () => {
    const gateway = startGateway();

    const first = await connect(gateway.port, makeToken("client-metrics"));
    first.send(JSON.stringify({ type: "join_room", roomId: "fleet-metrics" }));
    await waitForFrame(first, "room_joined");
    const { blob } = await waitForSavedState(gateway, "client-metrics");
    await disconnect(gateway, first);

    const resumed = await connect(gateway.port, makeToken("client-metrics"), { sessionId: blob });
    await waitForFrame(resumed, "session_resumed");

    const res = await fetch(`http://localhost:${gateway.port}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/plain; version=0.0.4; charset=utf-8");
    const body = await res.text();
    expect(body).toContain('session_resumption_total{result="new_session"} 1');
    expect(body).toContain('session_resumption_total{result="success"} 1');
    expect(body).toContain('session_resumption_total{result="mismatch"} 0');
    const sizeMatch = body.match(/session_state_size_bytes (\d+)/);
    expect(sizeMatch).not.toBeNull();
    expect(parseInt(sizeMatch[1], 10)).toBeGreaterThan(0);
  });

  it("omits session counters from /metrics when resumption is disabled", async () => {
    const gateway = startGateway({ withSessions: false });
    expect(gateway.server.sessionManager).toBeNull();

    const res = await fetch(`http://localhost:${gateway.port}/metrics`);
    expect(res.status).toBe(200);
    const body = await res.text();
    expect(body).toContain("gateway_connections_active");
    expect(body).not.toContain("session_resumption_total");
  });
});
