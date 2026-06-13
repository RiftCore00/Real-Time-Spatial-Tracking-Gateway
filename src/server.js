import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";

/**
 * Safely sends a JSON frame to a client, logging any send errors.
 * @param {import("ws").WebSocket} ws
 * @param {object} data
 * @param {string} clientId - Used for logging context.
 */
function safeSend(ws, data, clientId) {
  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    logger.error("Failed to send message", { clientId, error: err.message });
  }
}

export function createServer({ port, heartbeatMs, maxPayloadBytes } = {}) {
  const wss = new WebSocketServer({
    port: port ?? 8080,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const rooms = new RoomManager();

  function heartbeat() {
    this.isAlive = true;
  }

  wss.on("connection", (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;

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

    logger.info("Client connected", { clientId: actualClientId, ip: req.socket.remoteAddress });

    ws.on("pong", heartbeat);

    ws.on("message", (raw) => {
      const validation = validateMessage(raw.toString());

      if (!validation.ok) {
        logger.warn("Validation failed", { clientId: actualClientId, error: validation.error });
        safeSend(ws, { type: "error", payload: { message: validation.error } }, actualClientId);
        return;
      }

      const msg = validation.data;

      switch (msg.type) {
        case "join_room": {
          rooms.join(actualClientId, msg.roomId, ws);
          logger.info("Client joined room", { clientId: actualClientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_joined", payload: { roomId: msg.roomId } }, actualClientId);
          break;
        }
        case "leave_room": {
          rooms.leave(actualClientId, msg.roomId);
          logger.info("Client left room", { clientId: actualClientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } }, actualClientId);
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
