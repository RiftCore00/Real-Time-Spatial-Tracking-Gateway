import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret";

function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

function makeReq(ip = "1.1.1.1") {
  return { url: `/?token=${makeToken("test-client")}`, socket: { remoteAddress: ip } };
}

describe("binary WebSocket frame support (issue 29)", () => {
  let server;
  let messageHandler;
  let ws;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({ port: 0 });
    ws = {
      isAlive: false,
      close: vi.fn(),
      send: vi.fn(),
      on: vi.fn((event, handler) => {
        if (event === "message") messageHandler = handler;
      }),
    };
    server.wss.emit("connection", ws, makeReq());
  });

  afterEach(async () => {
    delete process.env.AUTH_SECRET;
    await new Promise((res) => server.wss.close(res));
  });

  it("accepts a valid JSON join_room sent as a binary Buffer", () => {
    const msg = JSON.stringify({ type: "join_room", roomId: "test-room" });
    const buf = Buffer.from(msg, "utf8");
    messageHandler(buf, true);
    expect(ws.send).toHaveBeenCalledWith(
      expect.stringContaining("room_joined")
    );
  });

  it("accepts a valid location_update sent as a binary Buffer", () => {
    // Join a room first (text)
    messageHandler(Buffer.from(JSON.stringify({ type: "join_room", roomId: "r1" }), "utf8"), true);
    ws.send.mockClear();

    const msg = JSON.stringify({ type: "location_update", payload: { latitude: 10, longitude: 20 } });
    messageHandler(Buffer.from(msg, "utf8"), true);
    // no error response means it was processed
    const errorSent = ws.send.mock.calls.some(c => c[0].includes("error"));
    expect(errorSent).toBe(false);
  });

  it("rejects invalid JSON in a binary frame with error response", () => {
    const buf = Buffer.from("not-json", "utf8");
    messageHandler(buf, true);
    const errorSent = ws.send.mock.calls.some(c => c[0].includes('"type":"error"'));
    expect(errorSent).toBe(true);
  });

  it("processes text frames normally (isBinary=false)", () => {
    const msg = JSON.stringify({ type: "join_room", roomId: "text-room" });
    messageHandler(Buffer.from(msg), false);
    expect(ws.send).toHaveBeenCalledWith(expect.stringContaining("room_joined"));
  });
});
