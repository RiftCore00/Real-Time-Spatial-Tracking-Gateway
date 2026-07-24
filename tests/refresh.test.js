import { describe, it, expect, beforeEach, afterEach } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-refresh";

function makeToken(clientId, expiresIn = 60) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn });
}

function connect(port, token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
    ws.once("open", () => resolve(ws));
    ws.once("error", reject);
  });
}

function nextMessage(ws) {
  return new Promise((resolve) => {
    ws.once("message", (data) => resolve(JSON.parse(data.toString())));
  });
}

describe("Token Refresh", () => {
  let server;
  let port;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({ port: 0 });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    for (const client of server.wss.clients) {
      client.terminate();
    }
    await new Promise((resolve) => server.wss.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  it("successfully refreshes token", async () => {
    const ws = await connect(port, makeToken("client-1"));
    const newToken = makeToken("client-1", 120);
    
    ws.send(JSON.stringify({ type: "token_refresh", token: newToken }));
    const msg = await nextMessage(ws);
    
    expect(msg.type).toBe("token_refresh_ok");
    ws.close();
  });

  it("updates identity on refresh", async () => {
    const ws = await connect(port, makeToken("client-old"));
    const newToken = makeToken("client-new");
    
    ws.send(JSON.stringify({ type: "token_refresh", token: newToken }));
    await nextMessage(ws);
    
    // Join a room and see if it uses the new identity
    ws.send(JSON.stringify({ type: "join_room", roomId: "room-1" }));
    const msg = await nextMessage(ws);
    expect(msg.type).toBe("room_joined");
    
    expect(server.rooms.getClientRooms("client-new")).toContain("room-1");
    expect(server.rooms.getClientRooms("client-old")).not.toContain("room-1");
    
    ws.close();
  });

  it("returns error for invalid refresh token", async () => {
    const ws = await connect(port, makeToken("client-1"));
    ws.send(JSON.stringify({ type: "token_refresh", token: "invalid-token" }));
    const msg = await nextMessage(ws);
    
    expect(msg.type).toBe("error");
    expect(msg.payload.message).toBeTruthy();
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.close();
  });
});
