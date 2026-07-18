import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";
import { logger } from "../src/logger.js";

const TEST_SECRET = "test-secret-key";

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

describe("createServer", () => {
  let server;
  let port;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096 });
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

  it("accepts a WebSocket connection with a valid token", async () => {
    const ws = await connect(port, makeToken("client-a"));
    expect(ws.readyState).toBe(WebSocket.OPEN);
    await closeAll(ws);
  });

  it("rejects connection with close code 4001 when token is missing", async () => {
    const ws = new WebSocket(`ws://localhost:${port}/`);
    const code = await new Promise((resolve) => ws.once("close", (c) => resolve(c)));
    expect(code).toBe(4001);
  });

  it("sends error frame for invalid JSON message", async () => {
    const ws = await connect(port, makeToken("client-b"));
    const pending = nextMessages(ws, 1);
    ws.send("not-json");
    const [msg] = await pending;
    expect(msg.type).toBe("error");
    await closeAll(ws);
  });

  it("sends room_joined confirmation on join_room", async () => {
    const ws = await connect(port, makeToken("client-c"));
    const pending = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    const [msg] = await pending;
    expect(msg.type).toBe("room_joined");
    expect(msg.payload.roomId).toBe("fleet-1");
    await closeAll(ws);
  });

  it("sends room_left confirmation on leave_room", async () => {
    const ws = await connect(port, makeToken("client-d"));
    const j = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await j;

    const pending = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "leave_room", roomId: "fleet-1" }));
    const [msg] = await pending;
    expect(msg.type).toBe("room_left");
    expect(msg.payload.roomId).toBe("fleet-1");
    await closeAll(ws);
  });

  it("broadcasts location_update to other room members", async () => {
    const ws1 = await connect(port, makeToken("sender-1"));
    const ws2 = await connect(port, makeToken("receiver-1"));

    const j1 = nextMessages(ws1, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "broadcast-room" }));
    await j1;

    const j2 = nextMessages(ws2, 1);
    ws2.send(JSON.stringify({ type: "join_room", roomId: "broadcast-room" }));
    await j2;

    const pending = nextMessages(ws2, 1);
    ws1.send(JSON.stringify({
      type: "location_update",
      payload: { latitude: 40.7128, longitude: -74.006 },
    }));

    const [msg] = await pending;
    expect(msg.type).toBe("location_update");
    expect(msg.payload.latitude).toBe(40.7128);
    await closeAll(ws1, ws2);
  });

  it("does not broadcast location_update back to sender", async () => {
    const ws1 = await connect(port, makeToken("sender-2"));
    const ws2 = await connect(port, makeToken("receiver-2"));

    const j1 = nextMessages(ws1, 1);
    ws1.send(JSON.stringify({ type: "join_room", roomId: "no-echo-room" }));
    await j1;

    const j2 = nextMessages(ws2, 1);
    ws2.send(JSON.stringify({ type: "join_room", roomId: "no-echo-room" }));
    await j2;

    let senderReceived = false;
    ws1.on("message", () => { senderReceived = true; });

    const pending = nextMessages(ws2, 1);
    ws1.send(JSON.stringify({
      type: "location_update",
      payload: { latitude: 10, longitude: 20 },
    }));
    await pending;

    await new Promise((r) => setTimeout(r, 50));
    expect(senderReceived).toBe(false);
    await closeAll(ws1, ws2);
  });

  it("cleans up room membership on disconnect", async () => {
    const ws = await connect(port, makeToken("cleanup-client"));
    const pending = nextMessages(ws, 1);
    ws.send(JSON.stringify({ type: "join_room", roomId: "cleanup-room" }));
    await pending;

    expect(server.rooms.getRoomSize("cleanup-room")).toBe(1);

    ws.close();
    await waitClose(ws);
    await new Promise((r) => setTimeout(r, 50));

    expect(server.rooms.getRoomSize("cleanup-room")).toBe(0);
  });

  it("logs error on ws error event", async () => {
    const errorSpy = vi.spyOn(logger, "error");
    const ws = await connect(port, makeToken("error-client"));

    // Allow connection to be fully established
    await new Promise((r) => setTimeout(r, 50));

    // Get the server-side WebSocket and emit an error on it
    const serverWs = Array.from(server.wss.clients)[0];
    const testError = new Error("socket hang up");
    serverWs.emit("error", testError);

    // Allow a brief moment for the error handler to process
    await new Promise((r) => setTimeout(r, 10));

    expect(errorSpy).toHaveBeenCalledWith(
      "WebSocket error",
      expect.objectContaining({ error: "socket hang up" })
    );

    errorSpy.mockRestore();
    await closeAll(ws);
  });

  it("closes with 4000 on malformed request URL", () => {
    function makeReq(url) {
      return { url, socket: { remoteAddress: "1.1.1.1" } };
    }

    function makeWs() {
      return { isAlive: false, close: vi.fn(), send: vi.fn(), on: vi.fn() };
    }

    const ws = makeWs();
    // Pass a URL that will fail URL parsing (invalid IPv6 bracket)
    server.wss.emit("connection", ws, makeReq("http://["));

    expect(ws.close).toHaveBeenCalledWith(4000, expect.any(String));
  });
});
