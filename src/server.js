import http from "node:http";
import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";
import { createRateLimiter } from "./rate-limiter.js";
import { SessionManager } from "./session-manager.js";

function safeSend(ws, data) {
  try {
    ws.send(typeof data === "string" ? data : JSON.stringify(data));
  } catch {
    // Silently ignore send errors (connection may have closed)
  }
}

export function createServer({ port, heartbeatMs, maxPayloadBytes, connRateLimit, maxConnectionsPerIp } = {}) {
  const rooms = new RoomManager();
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const rateLimiter = createRateLimiter();
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP = maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);

  const sessionManager = new SessionManager();

  const metrics = {
    messages: { location_update: 0, join_room: 0, leave_room: 0 },
    authFailures: 0,
    rateLimitRejections: { connection: 0 },
    eventLoopLagMs: 0,
    sessionResumption: { success: 0, decrypt_failed: 0, expired: 0, mismatch: 0, new_session: 0 },
  };

  let isShuttingDown = false;

  const httpServer = http.createServer((req, res) => {
    if (req.method !== "GET") {
      res.writeHead(405);
      res.end("Method Not Allowed");
      return;
    }

    const pathname = new URL(req.url, `http://${req.headers.host ?? "localhost"}`).pathname;

    if (pathname === "/health" || pathname === "/healthz") {
      if (isShuttingDown && pathname === "/healthz") {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "shutting down" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      if (pathname === "/healthz") {
        res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
      } else {
        res.end(JSON.stringify({ status: "OK" }));
      }
    } else if (pathname === "/readyz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "not ready", reason: "server is shutting down" }));
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
        "# TYPE session_resumption_total counter",
        `session_resumption_total{result="success"} ${metrics.sessionResumption.success}`,
        `session_resumption_total{result="decrypt_failed"} ${metrics.sessionResumption.decrypt_failed}`,
        `session_resumption_total{result="expired"} ${metrics.sessionResumption.expired}`,
        `session_resumption_total{result="mismatch"} ${metrics.sessionResumption.mismatch}`,
        `session_resumption_total{result="new_session"} ${metrics.sessionResumption.new_session}`,
        "# TYPE gateway_heap_used_bytes gauge",
        `gateway_heap_used_bytes ${mem.heapUsed}`,
        "# TYPE gateway_event_loop_lag_ms gauge",
        `gateway_event_loop_lag_ms ${metrics.eventLoopLagMs}`,
      ];
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(lines.join("\n") + "\n");
    } else {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not Found" }));
    }
  });

  httpServer.listen(port ?? 8080);

  const wss = new WebSocketServer({
    server: httpServer,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  function markShuttingDown() {
    isShuttingDown = true;
  }

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
    let sessionResumed = false;
    let restoredRooms = [];

    const sessionId = url.searchParams.get("session_id");
    if (sessionId) {
      sessionManager.load(sessionId).then((restored) => {
        if (restored && restored.clientId === actualClientId) {
          sessionResumed = true;
          restoredRooms = restored.rooms || [];
          for (const room of restoredRooms) {
            rooms.join(actualClientId, room.roomId, ws);
          }
          metrics.sessionResumption.success++;
          logger.info("Session resumed", { clientId: actualClientId, sessionId, rooms: restoredRooms.map((r) => r.roomId) });
          safeSend(ws, {
            type: "session_resumed",
            payload: {
              rooms: restoredRooms.map((r) => r.roomId),
              currentSeqPerRoom: restoredRooms.map((r) => ({ roomId: r.roomId, seq: r.highestReceivedSeq })),
            },
          });
        } else if (restored && restored.clientId !== actualClientId) {
          metrics.sessionResumption.mismatch++;
          logger.warn("Session identity mismatch", { clientId: actualClientId, sessionId });
        } else {
          metrics.sessionResumption.new_session++;
          logger.info("No valid session found", { clientId: actualClientId, sessionId });
        }
      }).catch(() => {
        metrics.sessionResumption.decrypt_failed++;
      });
    }

    logger.info("Client connected", { clientId: actualClientId, ip, sessionResumed });

    ws.on("pong", heartbeat);

    ws.on("message", (raw) => {
      if (!rateLimiter.check(actualClientId)) {
        safeSend(ws, { type: "error", payload: { message: "Rate limit exceeded" } });
        return;
      }

      const validation = validateMessage(raw.toString());

      if (!validation.ok) {
        logger.warn("Validation failed", { clientId: actualClientId, error: validation.error });
        safeSend(ws, { type: "error", payload: { message: validation.error } });
        return;
      }

      const msg = validation.data;

      switch (msg.type) {
        case "join_room": {
          rooms.join(actualClientId, msg.roomId, ws);
          metrics.messages.join_room++;
          logger.info("Client joined room", { clientId: actualClientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_joined", payload: { roomId: msg.roomId } });

          const currentRooms = rooms.getClientRooms(actualClientId);
          const roomStates = Array.from(currentRooms).map((roomId) => ({
            roomId,
            highestAckedSeq: 0,
            highestReceivedSeq: 0,
            geofenceInsideSet: [],
          }));
          sessionManager.debouncedSave(actualClientId, {
            clientId: actualClientId,
            protocolVersion: 3,
            authIdentity: authResult,
            rooms: roomStates,
            rateLimitState: { messageWindow: [], connectionWindow: [] },
            metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
          });
          break;
        }
        case "leave_room": {
          rooms.leave(actualClientId, msg.roomId);
          metrics.messages.leave_room++;
          logger.info("Client left room", { clientId: actualClientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } });

          const currentRooms = rooms.getClientRooms(actualClientId);
          const roomStates = Array.from(currentRooms).map((roomId) => ({
            roomId,
            highestAckedSeq: 0,
            highestReceivedSeq: 0,
            geofenceInsideSet: [],
          }));
          sessionManager.debouncedSave(actualClientId, {
            clientId: actualClientId,
            protocolVersion: 3,
            authIdentity: authResult,
            rooms: roomStates,
            rateLimitState: { messageWindow: [], connectionWindow: [] },
            metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
          });
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
      const currentRooms = rooms.getClientRooms(actualClientId);
      const roomStates = Array.from(currentRooms).map((roomId) => ({
        roomId,
        highestAckedSeq: 0,
        highestReceivedSeq: 0,
        geofenceInsideSet: [],
      }));
      sessionManager.save(actualClientId, {
        clientId: actualClientId,
        protocolVersion: 3,
        authIdentity: authResult,
        rooms: roomStates,
        rateLimitState: { messageWindow: [], connectionWindow: [] },
        metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
      }).catch(() => {});

      rooms.disconnect(actualClientId);
      rateLimiter.remove(actualClientId);
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

  wss.on("close", () => {
    clearInterval(heartbeatInterval);
    httpServer.close();
  });

  return { wss, httpServer, markShuttingDown, rooms, sessionManager };
}
