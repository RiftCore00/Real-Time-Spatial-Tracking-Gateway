/**
 * @fileoverview Integration coverage for the storage-adapter wiring inside
 * server.js: a valid `location_update` must trigger `adapter.saveLocation()`
 * fire-and-forget, and a storage failure must be logged without crashing the
 * server or affecting the client's broadcast.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";
import { logger } from "../src/logger.js";

const TEST_SECRET = "test-secret-storage";

function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

function connect(port, token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
    ws.once("open", () => resolve(ws));
    ws.once("error", reject);
  });
}

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

function waitClose(ws) {
  return new Promise((resolve) => ws.once("close", resolve));
}

async function closeAll(...sockets) {
  sockets.forEach((ws) => ws.readyState === WebSocket.OPEN && ws.close());
  await Promise.all(sockets.map(waitClose));
}

/** Waits until `fn()` returns truthy, polling every 5ms. */
async function waitFor(fn, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = fn();
    if (result) return result;
    if (Date.now() > deadline) throw new Error("timed out waiting for condition");
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Minimal fake StorageAdapter — only saveLocation is exercised. */
function makeFakeAdapter({ rejects } = {}) {
  return {
    saveLocation: vi.fn(() =>
      rejects ? Promise.reject(new Error(rejects)) : Promise.resolve()
    ),
    writeBatch: vi.fn().mockResolvedValue(),
    queryRoom: vi.fn().mockResolvedValue([]),
    querySpatial: vi.fn().mockResolvedValue([]),
    getLatest: vi.fn().mockResolvedValue(null),
    close: vi.fn().mockResolvedValue(),
    compact: vi.fn().mockResolvedValue({}),
    getCompactionStatus: vi.fn().mockResolvedValue({}),
  };
}

describe("server.js storage adapter wiring", () => {
  let server;
  let port;
  let adapter;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    adapter = makeFakeAdapter();
    server = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096, storageAdapter: adapter });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    delete process.env.AUTH_SECRET;
    await new Promise((resolve) => server.wss.close(resolve));
  });

  it("calls adapter.saveLocation with clientId, roomId, and the payload after broadcasting", async () => {
    const ws = await connect(port, makeToken("loc-client"));
    ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-1" }));
    await nextMessages(ws, 1); // room_joined

    const payload = { latitude: 10, longitude: 20, altitude: 5, accuracy: 3, speed: 1 };
    ws.send(JSON.stringify({ type: "location_update", payload }));

    await waitFor(() => adapter.saveLocation.mock.calls.length > 0);

    expect(adapter.saveLocation).toHaveBeenCalledWith("loc-client", "fleet-1", payload);

    await closeAll(ws);
  });

  it("does not call saveLocation when the client has not joined any room", async () => {
    const ws = await connect(port, makeToken("loc-client-2"));
    ws.send(JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 2 } }));

    // Give the message loop a tick to process before asserting the negative.
    await new Promise((r) => setTimeout(r, 20));
    expect(adapter.saveLocation).not.toHaveBeenCalled();

    await closeAll(ws);
  });

  it("logs a storage failure but still broadcasts to other room members", async () => {
    delete process.env.AUTH_SECRET;
    process.env.AUTH_SECRET = TEST_SECRET;
    const failingAdapter = makeFakeAdapter({ rejects: "connection refused" });
    const failingServer = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096, storageAdapter: failingAdapter });
    const failingPort = failingServer.wss.address().port;
    const errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});

    try {
      const sender = await connect(failingPort, makeToken("sender"));
      const receiver = await connect(failingPort, makeToken("receiver"));

      sender.send(JSON.stringify({ type: "join_room", roomId: "fleet-2" }));
      await nextMessages(sender, 1);
      receiver.send(JSON.stringify({ type: "join_room", roomId: "fleet-2" }));
      await nextMessages(receiver, 1);

      const payload = { latitude: 33, longitude: 44 };
      const received = nextMessages(receiver, 1);
      sender.send(JSON.stringify({ type: "location_update", payload }));

      // The receiver still gets the broadcast even though storage rejects.
      const [msg] = await received;
      expect(msg.type).toBe("location_update");
      expect(msg.payload).toMatchObject(payload);

      await waitFor(() =>
        errorSpy.mock.calls.some((call) => call[0] === "Failed to persist location")
      );

      // The sender's socket is still open and usable — storage failure didn't crash anything.
      expect(sender.readyState).toBe(WebSocket.OPEN);

      await closeAll(sender, receiver);
    } finally {
      errorSpy.mockRestore();
      await new Promise((resolve) => failingServer.wss.close(resolve));
    }
  });
});
