import http from "node:http";
import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import jwt from "jsonwebtoken";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createRateLimiter } from "./rate-limiter.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";
import { VALIDATION_ERROR } from "./errors.js";
import { SessionManager } from "./session-manager.js";

export function createServer({
  port,
  heartbeatMs,
  maxPayloadBytes,
  connRateLimit,
  maxConnectionsPerIp,
  maxRoomSize,
  maxRoomsPerClient,
  maxMembersPerRoom,
  maxRooms,
  ringBufferSize: _ringBufferSize,
  deduplicationWindowMs: _deduplicationWindowMs,
  maxBufferBytes: _maxBufferBytes,
  maxDedupEntries: _maxDedupEntries,
  ackWindowSize: _ackWindowSize,
  maxMessagesPerSecond,
} = {}) {
  let isShuttingDown = false;

  const metrics = {
    messages: { location_update: 0, join_room: 0, leave_room: 0, ack: 0, nack: 0 },
    rateLimitRejections: { connection: 0 },
    authFailures: 0,
    sessionResumption: { success: 0, decrypt_failed: 0, expired: 0, mismatch: 0, new_session: 0 },
    eventLoopLagMs: 0,
  };

  const sessionManager = new SessionManager({
    encryptionKey: process.env.SESSION_ENCRYPTION_KEY || undefined,
  });

  const effectiveMaxRoomSize = maxRoomSize ?? (Number(process.env.MAX_ROOM_SIZE) || undefined);

  const httpServer = http.createServer((req, res) => {
    if (req.method !== "GET") {
      res.writeHead(405, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Method Not Allowed" }));
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
        `gateway_messages_total{type="ack"} ${metrics.messages.ack}`,
        `gateway_messages_total{type="nack"} ${metrics.messages.nack}`,
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

  const rooms = new RoomManager({
    maxRoomSize: effectiveMaxRoomSize,
    maxRoomsPerClient,
    maxMembersPerRoom,
    maxRooms,
    ringBufferSize: _ringBufferSize,
    deduplicationWindowMs: _deduplicationWindowMs,
    maxBufferBytes: _maxBufferBytes,
    maxDedupEntries: _maxDedupEntries,
    ackWindowSize: _ackWindowSize,
  });
  const rateLimiter = createRateLimiter(maxMessagesPerSecond);
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP = maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);

  function heartbeat() {
    this.isAlive = true;
  }

  function sendError(ws, message, code) {
    ws.send(JSON.stringify({ type: "error", payload: { message, code } }));
  }

  function safeSend(ws, data) {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(typeof data === "string" ? data : JSON.stringify(data));
      }
    } catch {
      // ignore send errors
    }
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
    let authResult;
    verifyConnection(token).then((result) => {
      authResult = result;

      if (!authResult.ok) {
        logger.warn("Authentication failed", { clientId, reason: authResult.error });
        metrics.authFailures++;
        ws.close(4001, authResult.error);
        return;
      }

      const identity = { clientId: authResult.clientId ?? clientId };
      ws._clientId = identity.clientId;
      logger.info("Client connected", { clientId: identity.clientId, ip });

      ws.on("pong", heartbeat);

      const sessionId = url.searchParams.get("session_id");
      if (sessionId) {
        sessionManager.load(sessionId).then((savedSession) => {
          if (!savedSession) {
            metrics.sessionResumption.new_session++;
            return;
          }
          const savedIdentity = savedSession.authIdentity?.sub ?? savedSession.authIdentity?.clientId;
          if (savedIdentity !== identity.clientId) {
            metrics.sessionResumption.mismatch++;
            return;
          }
          if (savedSession.rooms && savedSession.rooms.length > 0) {
            for (const roomState of savedSession.rooms) {
              rooms.join(identity.clientId, roomState.roomId, ws);
            }
            const roomIds = savedSession.rooms.map((r) => r.roomId);
            metrics.sessionResumption.success++;
            safeSend(ws, {
              type: "session_resumed",
              payload: {
                sessionId,
                rooms: roomIds,
                currentSeqPerRoom: savedSession.rooms.map((r) => ({
                  roomId: r.roomId,
                  highestAckedSeq: r.highestAckedSeq,
                  highestReceivedSeq: r.highestReceivedSeq,
                })),
              },
            });
          }
        }).catch(() => {});
      }

      ws.on("message", (raw) => {
        if (!rateLimiter.check(identity.clientId)) {
          logger.warn("Message rate limit exceeded", { clientId: identity.clientId });
          safeSend(ws, { type: "error", payload: { message: "Rate limit exceeded" } });
          return;
        }

        const validation = validateMessage(raw.toString());

        if (!validation.ok) {
          logger.warn("Validation failed", { clientId: identity.clientId, error: validation.error });
          sendError(ws, validation.error, validation.code ?? VALIDATION_ERROR);
          return;
        }

        const msg = validation.data;

        switch (msg.type) {
          case "join_room": {
            metrics.messages.join_room++;
            const joinResult = rooms.join(identity.clientId, msg.roomId, ws);
            if (joinResult && joinResult.type === "error") {
              logger.warn("Join rejected", { clientId: identity.clientId, roomId: msg.roomId, code: joinResult.payload.code });
              safeSend(ws, joinResult);
              break;
            }
            if (joinResult && joinResult.ok === false) {
              logger.warn("Room is full", { clientId: identity.clientId, roomId: msg.roomId });
              safeSend(ws, { type: "error", payload: { message: "Room is full", code: "ROOM_FULL" } });
              break;
            }
            logger.info("Client joined room", { clientId: identity.clientId, roomId: msg.roomId });
            safeSend(ws, { type: "room_joined", payload: { roomId: msg.roomId } });

            const currentRooms = rooms.getClientRooms(identity.clientId);
            const roomStates = Array.from(currentRooms).map((roomId) => ({
              roomId,
              highestAckedSeq: 0,
              highestReceivedSeq: 0,
              geofenceInsideSet: [],
            }));
            sessionManager.debouncedSave(identity.clientId, {
              clientId: identity.clientId,
              protocolVersion: 3,
              authIdentity: authResult,
              rooms: roomStates,
              rateLimitState: { messageWindow: [], connectionWindow: [] },
              metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
            });
            break;
          }
          case "leave_room": {
            metrics.messages.leave_room++;
            rooms.leave(identity.clientId, msg.roomId);
            logger.info("Client left room", { clientId: identity.clientId, roomId: msg.roomId });
            safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } });

            const currentRooms = rooms.getClientRooms(identity.clientId);
            const roomStates = Array.from(currentRooms).map((roomId) => ({
              roomId,
              highestAckedSeq: 0,
              highestReceivedSeq: 0,
              geofenceInsideSet: [],
            }));
            sessionManager.debouncedSave(identity.clientId, {
              clientId: identity.clientId,
              protocolVersion: 3,
              authIdentity: authResult,
              rooms: roomStates,
              rateLimitState: { messageWindow: [], connectionWindow: [] },
              metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
            });
            break;
          }
          case "reconnect": {
            const clientRooms = rooms.getClientRooms(identity.clientId);
            if (!clientRooms.has(msg.roomId)) {
              safeSend(ws, { type: "error", payload: { message: "Must join room before reconnecting" } });
              break;
            }
            const replayResult = rooms.handleReconnect(msg.roomId, msg.lastSeq, msg.highestAckedSeq);
            safeSend(ws, replayResult);
            break;
          }
          case "ack": {
            metrics.messages.ack++;
            rooms.ack(identity.clientId, msg.roomId, msg.seq);
            break;
          }
          case "nack": {
            metrics.messages.nack++;
            logger.warn("NACK received", { clientId: identity.clientId, roomId: msg.roomId, seq: msg.seq, reason: msg.reason });
            rooms.nack(identity.clientId, msg.roomId, msg.seq);
            break;
          }
          case "token_refresh": {
            try {
              const decoded = jwt.decode(msg.token, { complete: true });
              if (!decoded) {
                safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
                break;
              }
              verifyConnection(msg.token).then((refreshResult) => {
                if (!refreshResult.ok) {
                  safeSend(ws, { type: "error", payload: { message: refreshResult.error } });
                  return;
                }
                const oldClientId = identity.clientId;
                const newClientId = refreshResult.clientId;
                authResult = refreshResult;
                identity.clientId = newClientId;
                ws._clientId = newClientId;
                rooms.updateClientId(oldClientId, newClientId);
                rateLimiter.remove(oldClientId);
                safeSend(ws, { type: "token_refresh_ok", payload: { clientId: newClientId } });
              }).catch(() => {
                safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
              });
            } catch {
              safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
            }
            break;
          }
          case "location_update": {
            metrics.messages.location_update++;
            const roomIds = rooms.getClientRooms(identity.clientId);
            for (const roomId of roomIds) {
              rooms.broadcast(roomId, {
                type: "location_update",
                payload: { clientId: identity.clientId, ...msg.payload },
              }, identity.clientId);
            }
            break;
          }
        }
      });

      ws.on("close", (code, reason) => {
        const currentRooms = rooms.getClientRooms(identity.clientId);
        const roomStates = Array.from(currentRooms).map((roomId) => ({
          roomId,
          highestAckedSeq: 0,
          highestReceivedSeq: 0,
          geofenceInsideSet: [],
        }));
        sessionManager.save(identity.clientId, {
          clientId: identity.clientId,
          protocolVersion: 3,
          authIdentity: authResult,
          rooms: roomStates,
          rateLimitState: { messageWindow: [], connectionWindow: [] },
          metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
        }).catch(() => {});

        rooms.disconnect(identity.clientId);
        rateLimiter.remove(identity.clientId);
        const trackedIp = ws._trackedIp;
        if (trackedIp) {
          const count = ipConnectionCount.get(trackedIp) ?? 1;
          if (count <= 1) {
            ipConnectionCount.delete(trackedIp);
          } else {
            ipConnectionCount.set(trackedIp, count - 1);
          }
          connRateLimiter.cleanup(trackedIp);
        }
        logger.info("Client disconnected", {
          clientId: identity.clientId,
          code,
          reason: reason?.toString() ?? "unknown",
        });
      });

      ws.on("error", (err) => {
        logger.error("WebSocket error", { clientId: identity.clientId, error: err.message });
      });
    }).catch(() => {
      ws.close(4001, "Authentication failed");
    });
  });

  const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        logger.warn("Terminating zombie connection", {
          clientId: ws._clientId ?? ws._trackedIp ?? "unknown",
        });
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

  function markShuttingDown() {
    isShuttingDown = true;
  }

  return { wss, httpServer, rooms, sessionManager, ipConnectionCount, rateLimiter, markShuttingDown };
}
