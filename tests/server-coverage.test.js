/**
 * @fileoverview Additional unit tests for server.js covering previously
 * untested code paths: the heartbeat pong handler, the zombie-termination
 * branch of the heartbeat interval, and the wss "close" event that clears
 * the interval and closes the HTTP server.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key-coverage";

function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

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

function waitClose(ws) {
  return new Promise((resolve) => ws.once("close", (code) => resolve(code)));
}

describe("server heartbeat and close coverage", () => {
  let server;
  let port;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    // Short heartbeat so tests don't wait long
    server = createServer({ port: 0, heartbeatMs: 100, maxPayloadBytes: 4096 });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    for (const client of server.wss.clients) {
      client.terminate();
    }
    await new Promise((resolve) => server.wss.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  it("heartbeat pong handler sets isAlive = true", async () => {
    const ws = await connect(port, makeToken("heartbeat-client"));

    // Allow connection to be fully established
    await new Promise((r) => setTimeout(r, 30));

    // Get the server-side WebSocket
    const serverWs = Array.from(server.wss.clients)[0];
    expect(serverWs).toBeDefined();

    // Simulate what happens when a pong arrives: isAlive is reset to false
    // by the heartbeat interval, then set back to true by the pong handler
    serverWs.isAlive = false;
    // Emit a pong event to trigger the heartbeat() handler
    serverWs.emit("pong");

    expect(serverWs.isAlive).toBe(true);

    ws.close();
    await waitClose(ws);
  });

  it("heartbeat interval terminates zombie connections (isAlive=false)", async () => {
    const ws = await connect(port, makeToken("zombie-client"));

    await new Promise((r) => setTimeout(r, 30));

    const serverWs = Array.from(server.wss.clients)[0];
    expect(serverWs).toBeDefined();

    // Mark as a zombie — it will not have responded to pong
    serverWs.isAlive = false;

    // Wait for the heartbeat interval to fire (100ms) and terminate the connection
    const closeCode = await new Promise((resolve) => {
      ws.once("close", (code) => resolve(code));
    });

    // The connection should have been terminated
    expect([1006, 1000, 1001]).toContain(closeCode);
  });

  it("heartbeat interval calls ping on alive connections", async () => {
    const ws = await connect(port, makeToken("ping-client"));

    await new Promise((r) => setTimeout(r, 30));

    const serverWs = Array.from(server.wss.clients)[0];
    expect(serverWs).toBeDefined();

    // Ensure client is marked alive before the interval fires
    serverWs.isAlive = true;

    // Spy on the ping method
    const pingSpy = vi.spyOn(serverWs, "ping");

    // Wait for the heartbeat interval to fire (100ms configured)
    await new Promise((r) => setTimeout(r, 150));

    // The server should have called ping on the live connection
    expect(pingSpy).toHaveBeenCalled();

    ws.close();
    await waitClose(ws);
  });

  it("wss close event clears the heartbeat interval", async () => {
    // Create a separate server instance to close
    const s = createServer({ port: 0, heartbeatMs: 100, maxPayloadBytes: 1024 });

    const clearSpy = vi.spyOn(global, "clearInterval");

    // Close the wss — this should trigger the wss "close" event handler
    await new Promise((resolve) => s.wss.close(resolve));

    // clearInterval should have been called (for the heartbeat interval)
    expect(clearSpy).toHaveBeenCalled();
    clearSpy.mockRestore();
  });
});
