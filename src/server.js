import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createRateLimiter } from "./rate-limiter.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";

/**
 * Creates and starts a WebSocket server for the spatial tracking gateway.
 *
 * @param {object} [options]
 * @param {number} [options.port=8080]                   - Port to listen on. Use 0 for ephemeral.
 * @param {number} [options.heartbeatMs=30000]           - Heartbeat interval in milliseconds.
 * @param {number} [options.maxPayloadBytes=1024]        - Max WebSocket frame payload in bytes.
 * @param {number} [options.maxMessagesPerSecond]        - Per-client message rate limit (msgs/sec).
 *                                                         Defaults to MAX_MESSAGES_PER_SECOND env var or 100.
 * @param {number} [options.connRateLimit]               - Max new connections per IP per minute.
 *                                                         Defaults to CONN_RATE_LIMIT env var or 30.
 * @param {number} [options.maxConnectionsPerIp]         - Max simultaneous connections per IP.
 *                                                         Defaults to MAX_CONNECTIONS_PER_IP env var or 10.
 * @returns {{ wss: WebSocketServer, rooms: RoomManager, ipConnectionCount: Map }}
 */
export function createServer({
  port,
  heartbeatMs,
  maxPayloadBytes,
  maxMessagesPerSecond,
  connRateLimit,
  maxConnectionsPerIp,
} = {}) {
  const wss = new WebSocketServer({
    port: port ?? 8080,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const rooms = new RoomManager();
  const rateLimiter = createRateLimiter(maxMessagesPerSecond);
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP =
    maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);

  function heartbeat() {
    this.isAlive = true;
  }

  wss.on("connection", (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;

    const ip = req.socket.remoteAddress;

    // Per-IP connection rate limit (new connections per minute)
    if (!connRateLimiter.check(ip)) {
      logger.warn("Connection rate limit exceeded", { ip });
      ws.close(4029, "Connection rate limit exceeded");
      return;
    }

    // Max simultaneous connections per IP
    const currentCount = ipConnectionCount.get(ip) ?? 0;
    if (currentCount >= MAX_CONNS_PER_IP) {
      logger.warn("Max connections per IP exceeded", { ip });
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
      ws.close(4001, authResult.error);
      return;
    }

    const actualClientId = authResult.clientId ?? clientId;
    logger.info("Client connected", { clientId: actualClientId, ip });

    ws.on("pong", heartbeat);

    ws.on("message", (raw, isBinary) => {
      // Per-client message rate limit
      if (!rateLimiter.check(actualClientId)) {
        logger.warn("Rate limit exceeded", { clientId: actualClientId });
        ws.send(JSON.stringify({ type: "error", payload: { message: "Rate limit exceeded" } }));
        return;
      }

      // Binary frames: decode as UTF-8 JSON, treat same as text
      const str = isBinary ? raw.toString("utf8") : raw.toString();
      const validation = validateMessage(str);

      if (!validation.ok) {
        logger.warn("Validation failed", { clientId: actualClientId, error: validation.error });
        ws.send(JSON.stringify({ type: "error", payload: { message: validation.error } }));
        return;
      }

      const msg = validation.data;

      switch (msg.type) {
        case "join_room": {
          rooms.join(actualClientId, msg.roomId, ws);
          logger.info("Client joined room", { clientId: actualClientId, roomId: msg.roomId });
          ws.send(JSON.stringify({ type: "room_joined", payload: { roomId: msg.roomId } }));
          break;
        }
        case "leave_room": {
          rooms.leave(actualClientId, msg.roomId);
          logger.info("Client left room", { clientId: actualClientId, roomId: msg.roomId });
          ws.send(JSON.stringify({ type: "room_left", payload: { roomId: msg.roomId } }));
          break;
        }
        case "location_update": {
          const roomIds = rooms.getClientRooms(actualClientId);
          for (const roomId of roomIds) {
            rooms.broadcast(
              roomId,
              { type: "location_update", payload: { clientId: actualClientId, ...msg.payload } },
              actualClientId,
            );
          }
          break;
        }
      }
    });

    ws.on("close", (code, reason) => {
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

  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        logger.warn("Terminating zombie connection", {});
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, heartbeatMs ?? 30000);

  wss.on("close", () => {
    clearInterval(interval);
  });

  return { wss, rooms, ipConnectionCount };
}
