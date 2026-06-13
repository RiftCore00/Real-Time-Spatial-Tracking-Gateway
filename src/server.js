import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";

/**
 * Marks a WebSocket connection as alive upon receiving a pong frame.
 * Used as the `pong` event handler; `this` refers to the WebSocket instance.
 * @this {import("ws").WebSocket}
 */
function heartbeat() {
  this.isAlive = true;
}

/**
 * Handles an incoming raw WebSocket message for a connected client.
 *
 * Validates the message, then dispatches to join_room, leave_room, or
 * location_update logic. Sends an error frame back if validation fails.
 *
 * @param {import("ws").WebSocket} ws - The client's WebSocket connection.
 * @param {string} clientId - The resolved client identifier.
 * @param {RoomManager} rooms - The shared room manager instance.
 * @param {Buffer|string} raw - The raw message data received from the client.
 */
function handleMessage(ws, clientId, rooms, raw) {
  const validation = validateMessage(raw.toString());

  if (!validation.ok) {
    logger.warn("Validation failed", { clientId, error: validation.error });
    try {
      ws.send(JSON.stringify({ type: "error", payload: { message: validation.error } }));
    } catch (err) {
      logger.error("Failed to send error frame", { clientId, error: err.message });
    }
    return;
  }

  const msg = validation.data;

  switch (msg.type) {
    case "join_room": {
      rooms.join(clientId, msg.roomId, ws);
      logger.info("Client joined room", { clientId, roomId: msg.roomId });
      try {
        ws.send(JSON.stringify({ type: "room_joined", payload: { roomId: msg.roomId } }));
      } catch (err) {
        logger.error("Failed to send room_joined", { clientId, error: err.message });
      }
      break;
    }
    case "leave_room": {
      rooms.leave(clientId, msg.roomId);
      logger.info("Client left room", { clientId, roomId: msg.roomId });
      try {
        ws.send(JSON.stringify({ type: "room_left", payload: { roomId: msg.roomId } }));
      } catch (err) {
        logger.error("Failed to send room_left", { clientId, error: err.message });
      }
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

/**
 * Handles a new WebSocket connection: authenticates the client, sets up
 * message/close/error event listeners, and registers heartbeat tracking.
 *
 * @param {import("ws").WebSocket} ws - The newly connected WebSocket.
 * @param {import("http").IncomingMessage} req - The HTTP upgrade request.
 * @param {RoomManager} rooms - The shared room manager instance.
 */
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

/**
 * Starts the heartbeat interval that pings all connected clients and
 * terminates any that have not responded since the last cycle.
 *
 * @param {import("ws").WebSocketServer} wss - The WebSocket server instance.
 * @param {number} intervalMs - Milliseconds between heartbeat checks.
 * @returns {ReturnType<typeof setInterval>} The interval handle (pass to clearInterval to stop).
 */
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

/**
 * Creates and starts the WebSocket tracking gateway server.
 *
 * @param {object} [options={}] - Server configuration options.
 * @param {number} [options.port=8080] - Port to listen on.
 * @param {number} [options.heartbeatMs=30000] - Heartbeat ping interval in milliseconds.
 * @param {number} [options.maxPayloadBytes=1024] - Maximum allowed incoming message size in bytes.
 * @returns {{ wss: import("ws").WebSocketServer, rooms: RoomManager }}
 *   The WebSocket server instance and the room manager.
 */
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
