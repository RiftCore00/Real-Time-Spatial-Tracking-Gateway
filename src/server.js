import http from "node:http";
import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";

export function createServer({ port, heartbeatMs, maxPayloadBytes, connRateLimit, maxConnectionsPerIp } = {}) {
  const rooms = new RoomManager();
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP = maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);

  const metrics = {
    messages: { location_update: 0, join_room: 0, leave_room: 0 },
    authFailures: 0,
    rateLimitRejections: { connection: 0 },
    eventLoopLagMs: 0,
  };

  let isReady = false;
  let isShuttingDown = false;

  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const httpServer = http.createServer((req, res) => {
    if (req.method !== "GET") {
      res.writeHead(405);
      res.end("Method Not Allowed");
      return;
    }

    const pathname = new URL(req.url, `http://${req.headers.host ?? "localhost"}`).pathname;

    if (pathname === "/healthz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "shutting down" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
    } else if (pathname === "/readyz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "not ready", reason: "server is shutting down" }));
        return;
      }
      if (!isReady) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "not ready", reason: "initializing" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        status: "ready",
        connections: wss.clients.size,
        rooms: rooms.roomCount,
      }));
    } else if (pathname === "/metrics") {
      const mem = process.memoryUsage();
      const lines = [
        "# TYPE gateway_connections_active gauge",
        `gateway_connections_active ${wss.clients.size}`,
        "# TYPE gateway_rooms_active gauge",
        `gateway_rooms_active ${rooms.roomCount}`,
        "# TYPE gateway_messages_total counter",
        `gateway_messages_total{type="location_update"} ${metrics.messages.location_update}`,
        `gateway_messages_total{type="join_room"} ${metrics.messages.join_room}`,
        `gateway_messages_total{type="leave_room"} ${metrics.messages.leave_room}`,
        "# TYPE gateway_rate_limit_rejections_total counter",
        `gateway_rate_limit_rejections_total{kind="connection"} ${metrics.rateLimitRejections.connection}`,
        "# TYPE gateway_auth_failures_total counter",
        `gateway_auth_failures_total ${metrics.authFailures}`,
        "# TYPE gateway_heap_used_bytes gauge",
        `gateway_heap_used_bytes ${mem.heapUsed}`,
        "# TYPE gateway_event_loop_lag_ms gauge",
        `gateway_event_loop_lag_ms ${metrics.eventLoopLagMs}`,
      ];
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(lines.join("\n") + "\n");
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  httpServer.on("upgrade", (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  function heartbeat() {
    this.isAlive = true;
  }

  wss.on("connection", (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;

    const ip = req.socket.remoteAddress;

    if (!connRateLimiter.check(ip)) {
      logger.warn("Connection rate limit exceeded", { ip });
      metrics.rateLimitRejections.connection++;
      ws.close(4029, "Connection rate limit exceeded");
      return;
    }

    const currentCount = ipConnectionCount.get(ip) ?? 0;
    if (currentCount >= MAX_CONNS_PER_IP) {
      logger.warn("Max connections per IP exceeded", { ip });
      metrics.rateLimitRejections.connection++;
      ws.close(4029, "Too many connections from this IP");
      return;
    }
    ipConnectionCount.set(ip, currentCount + 1);
    ws._trackedIp = ip;

    let url;
    try {
      url = new URL(req.url, "http://localhost");
    } catch {
      logger.warn("Invalid request URL", { clientId, url: req.url });
      ws.close(4000, "Invalid request URL");
      return;
    }

    const token = url.searchParams.get("token");
    const authResult = verifyConnection(token);

    if (!authResult.ok) {
      logger.warn("Authentication failed", { clientId, reason: authResult.error });
      metrics.authFailures++;
      ws.close(4001, authResult.error);
      return;
    }

    const actualClientId = authResult.clientId ?? clientId;
    logger.info("Client connected", { clientId: actualClientId, ip });

    ws.on("pong", heartbeat);

    ws.on("message", (raw) => {
      const validation = validateMessage(raw.toString());

      if (!validation.ok) {
        logger.warn("Validation failed", { clientId: actualClientId, error: validation.error });
        ws.send(JSON.stringify({ type: "error", payload: { message: validation.error } }));
        return;
      }

      const msg = validation.data;

      switch (msg.type) {
        case "join_room": {
          rooms.join(actualClientId, msg.roomId, ws);
          metrics.messages.join_room++;
          logger.info("Client joined room", { clientId: actualClientId, roomId: msg.roomId });
          ws.send(JSON.stringify({ type: "room_joined", payload: { roomId: msg.roomId } }));
          break;
        }
        case "leave_room": {
          rooms.leave(actualClientId, msg.roomId);
          metrics.messages.leave_room++;
          logger.info("Client left room", { clientId: actualClientId, roomId: msg.roomId });
          ws.send(JSON.stringify({ type: "room_left", payload: { roomId: msg.roomId } }));
          break;
        }
        case "location_update": {
          metrics.messages.location_update++;
          const roomIds = rooms.getClientRooms(actualClientId);
          for (const roomId of roomIds) {
            rooms.broadcast(roomId, {
              type: "location_update",
              payload: { clientId: actualClientId, ...msg.payload },
            }, actualClientId);
          }
          break;
        }
      }
    });

    ws.on("close", (code, reason) => {
      rooms.disconnect(actualClientId);
      const trackedIp = ws._trackedIp;
      if (trackedIp) {
        const count = ipConnectionCount.get(trackedIp) ?? 1;
        if (count <= 1) {
          ipConnectionCount.delete(trackedIp);
        } else {
          ipConnectionCount.set(trackedIp, count - 1);
        }
      }
      logger.info("Client disconnected", {
        clientId: actualClientId,
        code,
        reason: reason?.toString() ?? "unknown",
      });
    });

    ws.on("error", (err) => {
      logger.error("WebSocket error", { clientId: actualClientId, error: err.message });
    });
  });

  const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        logger.warn("Terminating zombie connection", { clientId: ws._clientId ?? "unknown" });
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, heartbeatMs ?? 30000);

  function measureLag() {
    const start = Date.now();
    setTimeout(() => {
      metrics.eventLoopLagMs = Date.now() - start;
    }, 0);
  }
  const lagInterval = setInterval(measureLag, 5000);
  measureLag();

  httpServer.on("close", () => {
    clearInterval(heartbeatInterval);
    clearInterval(lagInterval);
  });

  httpServer.listen(port ?? 8080);

  wss.address = () => httpServer.address();

  function markShuttingDown() {
    isShuttingDown = true;
    isReady = false;
  }

  isReady = true;

  return { wss, httpServer, rooms, metrics, ipConnectionCount, markShuttingDown };
}
