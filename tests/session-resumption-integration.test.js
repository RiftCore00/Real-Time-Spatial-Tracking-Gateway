import { describe, it, expect, beforeEach, afterEach } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key";

/** Sign a JWT for a client. */
function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

/** Collect the next N messages from a WebSocket. */
function nextMessages(ws, n = 1, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const msgs = [];
    const timeout = setTimeout(() => reject(new Error("Timeout waiting for messages")), timeoutMs);
    ws.on("message", function handler(data) {
      msgs.push(JSON.parse(data.toString()));
      if (msgs.length === n) {
        clearTimeout(timeout);
        ws.off("message", handler);
        resolve(msgs);
      }
    });
  });
}

/** Collect messages for a duration, returning all received. */
function collectMessages(ws, durationMs = 500) {
  return new Promise((resolve) => {
    const msgs = [];
    ws.on("message", (data) => {
      msgs.push(JSON.parse(data.toString()));
    });
    setTimeout(() => resolve(msgs), durationMs);
  });
}

/** Wait for the WS close event. */
function waitClose(ws) {
  return new Promise((resolve) => ws.once("close", resolve));
}

/** Close a list of sockets and wait for them all. */
async function closeAll(...sockets) {
  sockets.forEach((ws) => ws.readyState === WebSocket.OPEN && ws.close());
  await Promise.all(sockets.map(waitClose));
}

/**
 * Open a WS connection and set up a message listener BEFORE the open event.
 * Returns { ws, messages } where messages is a Promise resolving to the next N messages.
 */
function connectWithListener(port, token, extraParams = {}, n = 1) {
  const params = new URLSearchParams();
  if (token) params.set("token", token);
  for (const [k, v] of Object.entries(extraParams)) {
    params.set(k, v);
  }
  const qs = params.toString();
  const url = `ws://localhost:${port}/${qs ? `?${qs}` : ""}`;

  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    let collectedMsgs = [];

    const messages = new Promise((msgResolve, msgReject) => {
      const timeout = setTimeout(() => msgReject(new Error("Timeout waiting for messages")), 3000);
      ws.on("message", function handler(data) {
        collectedMsgs.push(JSON.parse(data.toString()));
        if (collectedMsgs.length === n) {
          clearTimeout(timeout);
          ws.off("message", handler);
          msgResolve(collectedMsgs);
        }
      });
    });

    ws.once("open", () => resolve({ ws, messages }));
    ws.once("error", reject);
  });
}

describe("Session Resumption Integration", () => {
  let server;
  let port;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096 });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    for (const client of server.wss.clients) {
      client.terminate();
    }
    await new Promise((resolve) => server.wss.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  it("saves session state on room join and disconnect", async () => {
    const { ws: ws1, messages: j1 } = await connectWithListener(port, makeToken("client-resume-1"), {}, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "fleet-resume" }));
    await j1;

    ws1.close();
    await waitClose(ws1);
    await new Promise((r) => setTimeout(r, 100));

    const loaded = await server.sessionManager.load("client-resume-1");
    expect(loaded).not.toBeNull();
    expect(loaded.clientId).toBe("client-resume-1");
    expect(loaded.rooms).toHaveLength(1);
    expect(loaded.rooms[0].roomId).toBe("fleet-resume");
  });

  it("restores session on reconnect with valid session_id", async () => {
    const { ws: ws1, messages: j1 } = await connectWithListener(port, makeToken("client-resume-2"), {}, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "fleet-session" }));
    await j1;

    ws1.close();
    await waitClose(ws1);
    await new Promise((r) => setTimeout(r, 100));

    const { ws: ws2, messages: resumed } = await connectWithListener(port, makeToken("client-resume-2"), { session_id: "client-resume-2" }, 1);
    const msgs = await resumed;
    expect(msgs[0].type).toBe("session_resumed");
    expect(msgs[0].payload.rooms).toContain("fleet-session");

    await closeAll(ws2);
  });

  it("treats expired session as new session (no session_resumed sent)", async () => {
    const sm = server.sessionManager;
    await sm.save("client-expired", {
      clientId: "client-expired",
      protocolVersion: 3,
      authIdentity: { sub: "client-expired" },
      rooms: [{ roomId: "fleet-expired", highestAckedSeq: 0, highestReceivedSeq: 0, geofenceInsideSet: [] }],
      rateLimitState: { messageWindow: [], connectionWindow: [] },
      metadata: { ip: "127.0.0.1", userAgent: "", connectedAt: Date.now(), lastActivityAt: Date.now() },
    });
    await sm.delete("client-expired");

    const ws = new WebSocket(`ws://localhost:${port}/?token=${makeToken("client-expired")}&session_id=client-expired`);
    const msgs = await collectMessages(ws, 500);
    const resumedMsgs = msgs.filter((m) => m.type === "session_resumed");
    expect(resumedMsgs).toHaveLength(0);
    ws.close();
    await waitClose(ws);
  });

  it("rejects session with identity mismatch (no session_resumed sent)", async () => {
    await server.sessionManager.save("client-mismatch", {
      clientId: "client-mismatch",
      protocolVersion: 3,
      authIdentity: { sub: "wrong-identity" },
      rooms: [{ roomId: "fleet-mismatch", highestAckedSeq: 0, highestReceivedSeq: 0, geofenceInsideSet: [] }],
      rateLimitState: { messageWindow: [], connectionWindow: [] },
      metadata: { ip: "127.0.0.1", userAgent: "", connectedAt: Date.now(), lastActivityAt: Date.now() },
    });

    const ws = new WebSocket(`ws://localhost:${port}/?token=${makeToken("client-different")}&session_id=client-mismatch`);
    const msgs = await collectMessages(ws, 500);
    const resumedMsgs = msgs.filter((m) => m.type === "session_resumed");
    expect(resumedMsgs).toHaveLength(0);
    ws.close();
    await waitClose(ws);
  });

  it("saves session state on room leave", async () => {
    const { ws: ws1, messages: j1 } = await connectWithListener(port, makeToken("client-leave-1"), {}, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "fleet-leave" }));
    await j1;

    const l1 = nextMessages(ws1, 1);
    ws1.send(JSON.stringify({ type: "leave_room", roomId: "fleet-leave" }));
    await l1;

    ws1.close();
    await waitClose(ws1);
    await new Promise((r) => setTimeout(r, 100));

    const loaded = await server.sessionManager.load("client-leave-1");
    expect(loaded).not.toBeNull();
    expect(loaded.rooms).toHaveLength(0);
  });

  it("debounces session saves during rapid state changes", async () => {
    const { ws, messages: j1 } = await connectWithListener(port, makeToken("client-debounce"), {}, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "room-1" }));
    await j1;

    const j2 = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "room-2" }));
    await j2;

    const j3 = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "room-3" }));
    await j3;

    await new Promise((r) => setTimeout(r, 600));

    const loaded = await server.sessionManager.load("client-debounce");
    expect(loaded).not.toBeNull();
    expect(loaded.rooms).toHaveLength(3);

    ws.close();
    await waitClose(ws);
  });

  it("returns session_resumed with correct sequence numbers", async () => {
    const { ws: ws1, messages: j1 } = await connectWithListener(port, makeToken("client-seq-1"), {}, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "fleet-seq" }));
    await j1;

    ws1.close();
    await waitClose(ws1);
    await new Promise((r) => setTimeout(r, 100));

    const { ws: ws2, messages: resumed } = await connectWithListener(port, makeToken("client-seq-1"), { session_id: "client-seq-1" }, 1);
    const msgs = await resumed;
    expect(msgs[0].type).toBe("session_resumed");
    expect(msgs[0].payload.currentSeqPerRoom).toBeDefined();
    expect(msgs[0].payload.currentSeqPerRoom[0].roomId).toBe("fleet-seq");

    await closeAll(ws2);
  });

  it("session metrics increment correctly", async () => {
    const { ws: ws1, messages: j1 } = await connectWithListener(port, makeToken("client-metrics-1"), {}, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "fleet-metrics" }));
    await j1;

    ws1.close();
    await waitClose(ws1);
    await new Promise((r) => setTimeout(r, 100));

    const { ws: ws2, messages: resumed } = await connectWithListener(port, makeToken("client-metrics-1"), { session_id: "client-metrics-1" }, 1);
    const msgs = await resumed;
    expect(msgs[0].type).toBe("session_resumed");

    await closeAll(ws2);
  });

  it("handles session without session_id gracefully", async () => {
    const ws = new WebSocket(`ws://localhost:${port}/?token=${makeToken("client-no-session")}`);
    await new Promise((resolve) => ws.once("open", resolve));
    const msgs = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-no-session" }));
    await msgs;

    expect(server.rooms.getRoomSize("fleet-no-session")).toBe(1);
    ws.close();
    await waitClose(ws);
  });

  it("saves session on graceful shutdown via flushPending", async () => {
    const ws = new WebSocket(`ws://localhost:${port}/?token=${makeToken("client-shutdown")}`);
    await new Promise((resolve) => ws.once("open", resolve));
    const msgs = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-shutdown" }));
    await msgs;

    await server.sessionManager.flushPending();
    const loaded = await server.sessionManager.load("client-shutdown");
    expect(loaded).not.toBeNull();
    expect(loaded.rooms).toHaveLength(1);

    ws.close();
    await waitClose(ws);
  });
});
