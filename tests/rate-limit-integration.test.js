import { describe, it, expect, beforeEach, afterEach } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key";
const LIMIT = 3;

/** Sign a JWT for a client. */
function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

/** Open a WS connection with an optional token and wait for it to be ready. */
function connect(port, token) {
  const url = token
    ? `ws://localhost:${port}/?token=${token}`
    : `ws://localhost:${port}/`;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once("open", () => resolve(ws));
    ws.once("error", reject);
  });
}

/** Collect the next N messages from a WebSocket. */
function nextMessages(ws, n = 1) {
  return new Promise((resolve) => {
    const msgs = [];
    ws.on("message", function handler(data) {
      msgs.push(JSON.parse(data.toString()));
      if (msgs.length === n) {
        ws.off("message", handler);
        resolve(msgs);
      }
    });
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

describe("rate limiter enforcement — end-to-end (integration)", () => {
  let server;
  let port;
  let ws;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({
      port: 0,
      heartbeatMs: 60000,
      maxPayloadBytes: 4096,
      maxMessagesPerSecond: LIMIT,
    });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    // Terminate all remaining clients before closing the server
    for (const client of server.wss.clients) {
      client.terminate();
    }
    await new Promise((resolve) => server.wss.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  it("allows exactly LIMIT messages without rate-limit errors, then blocks the (LIMIT+1)th", async () => {
    ws = await connect(port, makeToken("rl-client-1"));

    // Send LIMIT valid messages back-to-back — all should succeed (not be rate-limit errors)
    const validMsg = JSON.stringify({ type: "join_room", roomId: "rl-room" });

    // First message: expect room_joined
    const first = nextMessages(ws, 1);
    ws.send(validMsg);
    const [firstResp] = await first;
    expect(firstResp.type).not.toBe("error");

    // Messages 2..LIMIT — each sends join_room again (re-join is allowed)
    for (let i = 2; i <= LIMIT; i++) {
      const pending = nextMessages(ws, 1);
      ws.send(validMsg);
      const [resp] = await pending;
      // None of these should be rate-limit errors
      expect(resp).not.toMatchObject({
        type: "error",
        payload: { message: "Rate limit exceeded" },
      });
    }

    // (LIMIT+1)th message — must trigger the rate limit
    const overLimit = nextMessages(ws, 1);
    ws.send(validMsg);
    const [errorResp] = await overLimit;

    expect(errorResp).toEqual({
      type: "error",
      payload: { message: "Rate limit exceeded" },
    });

    // Connection must remain open after the rate-limit error
    expect(ws.readyState).toBe(WebSocket.OPEN);

    await closeAll(ws);
  });

  it("connection stays functional after a rate-limit error (can still receive messages)", async () => {
    ws = await connect(port, makeToken("rl-client-2"));

    const validMsg = JSON.stringify({ type: "join_room", roomId: "rl-room-2" });

    // Exhaust the limit
    for (let i = 0; i < LIMIT; i++) {
      const pending = nextMessages(ws, 1);
      ws.send(validMsg);
      await pending;
    }

    // Trigger the rate-limit error
    const errorPending = nextMessages(ws, 1);
    ws.send(validMsg);
    const [errorResp] = await errorPending;
    expect(errorResp).toEqual({
      type: "error",
      payload: { message: "Rate limit exceeded" },
    });

    // Connection is still open
    expect(ws.readyState).toBe(WebSocket.OPEN);

    // Sending another message while still rate-limited continues to produce the error
    // (not a disconnect) — confirming the connection is alive and server is responsive
    const stillLimited = nextMessages(ws, 1);
    ws.send(validMsg);
    const [secondError] = await stillLimited;
    expect(secondError).toEqual({
      type: "error",
      payload: { message: "Rate limit exceeded" },
    });
    expect(ws.readyState).toBe(WebSocket.OPEN);

    await closeAll(ws);
  });

  it("independent clients have separate rate limit counters", async () => {
    const wsA = await connect(port, makeToken("rl-client-a"));
    const wsB = await connect(port, makeToken("rl-client-b"));

    const validMsg = JSON.stringify({ type: "join_room", roomId: "rl-room-shared" });

    // Exhaust client A's limit
    for (let i = 0; i < LIMIT; i++) {
      const pending = nextMessages(wsA, 1);
      wsA.send(validMsg);
      await pending;
    }

    // Client A is now rate-limited
    const aLimited = nextMessages(wsA, 1);
    wsA.send(validMsg);
    const [aResp] = await aLimited;
    expect(aResp).toEqual({ type: "error", payload: { message: "Rate limit exceeded" } });

    // Client B should be unaffected — first message goes through fine
    const bPending = nextMessages(wsB, 1);
    wsB.send(validMsg);
    const [bResp] = await bPending;
    expect(bResp).not.toMatchObject({ type: "error", payload: { message: "Rate limit exceeded" } });

    await closeAll(wsA, wsB);
  });
});
