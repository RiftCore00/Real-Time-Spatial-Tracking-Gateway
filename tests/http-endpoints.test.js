import { describe, it, expect, beforeEach, afterEach } from "vitest";
import http from "node:http";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key";

function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

function httpGet(port, path) {
  return new Promise((resolve, reject) => {
    const req = http.get(`http://localhost:${port}${path}`, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() });
      });
    });
    req.on("error", reject);
  });
}

describe("HTTP endpoints", () => {
  let server;
  let port;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    server = createServer({ port: 0, heartbeatMs: 60000, maxPayloadBytes: 4096 });
    port = server.httpServer.address().port;
  });

  afterEach(async () => {
    for (const client of server.wss.clients) {
      client.terminate();
    }
    server.wss.close();
    await new Promise((resolve) => server.httpServer.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  describe("GET /healthz", () => {
    it("returns 200 with status ok and uptime", async () => {
      const res = await httpGet(port, "/healthz");
      expect(res.status).toBe(200);
      expect(res.headers["content-type"]).toContain("application/json");
      const body = JSON.parse(res.body);
      expect(body.status).toBe("ok");
      expect(typeof body.uptime).toBe("number");
      expect(body.uptime).toBeGreaterThanOrEqual(0);
    });

    it("returns 503 during shutdown", async () => {
      server.markShuttingDown();
      const res = await httpGet(port, "/healthz");
      expect(res.status).toBe(503);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("shutting down");
    });
  });

  describe("GET /readyz", () => {
    it("returns 200 with ready status when initialized", async () => {
      const res = await httpGet(port, "/readyz");
      expect(res.status).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("ready");
      expect(typeof body.connections).toBe("number");
      expect(typeof body.rooms).toBe("number");
    });

    it("returns 503 during shutdown", async () => {
      server.markShuttingDown();
      const res = await httpGet(port, "/readyz");
      expect(res.status).toBe(503);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("not ready");
      expect(body.reason).toBe("server is shutting down");
    });

    it("reports active connections and rooms", async () => {
      const token = makeToken("ready-client");
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      await new Promise((resolve) => ws.once("open", resolve));

      const joinMsg = JSON.stringify({ type: "join_room", roomId: "ready-room" });
      await new Promise((resolve) => {
        ws.once("message", resolve);
        ws.send(joinMsg);
      });

      const res = await httpGet(port, "/readyz");
      const body = JSON.parse(res.body);
      expect(body.connections).toBeGreaterThanOrEqual(1);
      expect(body.rooms).toBeGreaterThanOrEqual(1);

      ws.close();
      await new Promise((resolve) => ws.once("close", resolve));
    });
  });

  describe("GET /metrics", () => {
    it("returns 200 with Prometheus content type", async () => {
      const res = await httpGet(port, "/metrics");
      expect(res.status).toBe(200);
      expect(res.headers["content-type"]).toBe("text/plain; version=0.0.4; charset=utf-8");
    });

    it("contains all required metric names", async () => {
      const res = await httpGet(port, "/metrics");
      expect(res.body).toContain("gateway_connections_active");
      expect(res.body).toContain("gateway_rooms_active");
      expect(res.body).toContain("gateway_messages_total");
      expect(res.body).toContain("gateway_rate_limit_rejections_total");
      expect(res.body).toContain("gateway_auth_failures_total");
      expect(res.body).toContain("gateway_heap_used_bytes");
      expect(res.body).toContain("gateway_event_loop_lag_ms");
    });

    it("contains valid Prometheus TYPE declarations", async () => {
      const res = await httpGet(port, "/metrics");
      expect(res.body).toContain("# TYPE gateway_connections_active gauge");
      expect(res.body).toContain("# TYPE gateway_rooms_active gauge");
      expect(res.body).toContain("# TYPE gateway_messages_total counter");
      expect(res.body).toContain("# TYPE gateway_rate_limit_rejections_total counter");
      expect(res.body).toContain("# TYPE gateway_auth_failures_total counter");
      expect(res.body).toContain("# TYPE gateway_heap_used_bytes gauge");
      expect(res.body).toContain("# TYPE gateway_event_loop_lag_ms gauge");
    });

    it("tracks active connections", async () => {
      const res0 = await httpGet(port, "/metrics");
      expect(res0.body).toContain("gateway_connections_active 0");

      const token = makeToken("metrics-client");
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      await new Promise((resolve) => ws.once("open", resolve));

      const res1 = await httpGet(port, "/metrics");
      expect(res1.body).toContain("gateway_connections_active 1");

      ws.close();
      await new Promise((resolve) => ws.once("close", resolve));
    });

    it("tracks message counts", async () => {
      const token = makeToken("metrics-msg-client");
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      await new Promise((resolve) => ws.once("open", resolve));

      const joinMsg = JSON.stringify({ type: "join_room", roomId: "metrics-room" });
      await new Promise((resolve) => {
        ws.once("message", resolve);
        ws.send(joinMsg);
      });

      const res = await httpGet(port, "/metrics");
      expect(res.body).toContain('gateway_messages_total{type="join_room"} 1');
      expect(res.body).toContain('gateway_messages_total{type="leave_room"} 0');
      expect(res.body).toContain('gateway_messages_total{type="location_update"} 0');

      ws.close();
      await new Promise((resolve) => ws.once("close", resolve));
    });

    it("tracks heap usage", async () => {
      const res = await httpGet(port, "/metrics");
      const match = res.body.match(/gateway_heap_used_bytes (\d+)/);
      expect(match).not.toBeNull();
      const heapUsed = parseInt(match[1], 10);
      expect(heapUsed).toBeGreaterThan(0);
    });
  });

  describe("WebSocket upgrade on shared port", () => {
    it("allows WebSocket connections on the same port as HTTP", async () => {
      const token = makeToken("shared-port-client");
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      await new Promise((resolve) => ws.once("open", resolve));
      expect(ws.readyState).toBe(WebSocket.OPEN);
      ws.close();
      await new Promise((resolve) => ws.once("close", resolve));
    });

    it("does not intercept HTTP requests as WebSocket upgrades", async () => {
      const httpRes = await httpGet(port, "/healthz");
      expect(httpRes.status).toBe(200);
      const body = JSON.parse(httpRes.body);
      expect(body.status).toBe("ok");

      const token = makeToken("coexist-client");
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      await new Promise((resolve) => ws.once("open", resolve));
      expect(ws.readyState).toBe(WebSocket.OPEN);

      ws.close();
      await new Promise((resolve) => ws.once("close", resolve));
    });
  });

  describe("HTTP error handling", () => {
    it("returns 405 for non-GET requests", async () => {
      const res = await new Promise((resolve, reject) => {
        const req = http.request(`http://localhost:${port}/healthz`, { method: "POST" }, (r) => {
          const chunks = [];
          r.on("data", (c) => chunks.push(c));
          r.on("end", () => resolve({ status: r.statusCode, body: Buffer.concat(chunks).toString() }));
        });
        req.on("error", reject);
        req.end();
      });
      expect(res.status).toBe(405);
    });

    it("returns 404 for unknown paths", async () => {
      const res = await httpGet(port, "/unknown");
      expect(res.status).toBe(404);
    });
  });
});
