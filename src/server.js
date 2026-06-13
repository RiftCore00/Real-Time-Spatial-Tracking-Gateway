import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";

/** @this {import("ws").WebSocket} */
function heartbeat() {
  this.isAlive = true;
}

function safeSend(ws, data, clientId) {
  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    logger.error("Failed to send message", { clientId, error: err.message });
  }
}

function handleMessage(ws, clientId, rooms, raw) {
  const validation = validateMessage(raw.toString());

  if (!validation.ok) {
    logger.warn("Validation failed", { clientId, error: validation.error });
    safeSend(ws, { type: "error", payload: { message: validation.error } }, clientId);
    return;
  }

  const msg = validation.data;

  switch (msg.type) {
    case "join_room": {
      rooms.join(clientId, msg.roomId, ws);
      logger.info("Client joined room", { clientId, roomId: msg.roomId });
      safeSend(ws, { type: "room_joined", payload: { roomId: msg.roomId } }, clientId);
      break;
    }
    case "leave_room": {
      rooms.leave(clientId, msg.roomId);
      logger.info("Client left room", { clientId, roomId: msg.roomId });
      safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } }, clientId);
      break;
    }
    case "location_update": {
      const roomIds = rooms.getClientRooms(clientId);
      for (const roomId of roomIds) {
        rooms.broadcast(
          roomId,
          { type: "location_update", payload: { clientId, ...msg.payload } },
          clientId,
        );
      }
      break;
    }
  }
}

function handleConnection(ws, req, rooms) {
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
  ws.on("message", (raw) => handleMessage(ws, actualClientId, rooms, raw));

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
}

function setupHeartbeat(wss, intervalMs) {
  return setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        logger.warn("Terminating zombie connection", {});
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, intervalMs);
}

export function createServer({ port, heartbeatMs, maxPayloadBytes } = {}) {
  const wss = new WebSocketServer({
    port: port ?? 8080,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const rooms = new RoomManager();

  wss.on("connection", (ws, req) => handleConnection(ws, req, rooms));

  const interval = setupHeartbeat(wss, heartbeatMs ?? 30000);

  wss.on("close", () => {
    clearInterval(interval);
  });

  return { wss, rooms };
}
