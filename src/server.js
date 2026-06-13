import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";

export function createServer({ port, heartbeatMs, maxPayloadBytes, connRateLimit } = {}) {
  const wss = new WebSocketServer({
    port: port ?? 8080,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const rooms = new RoomManager();
  const connRateLimiter = createConnRateLimiter(connRateLimit);

  function heartbeat() {
    this.isAlive = true;
  }

  wss.on("connection", (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;

    const ip = req.socket.remoteAddress;

    if (!connRateLimiter.check(ip)) {
      logger.warn("Connection rate limit exceeded", { ip });
      ws.close(4029, "Connection rate limit exceeded");
      return;
    }

    const url = new URL(req.url, "http://localhost");
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

  return { wss, rooms };
}
