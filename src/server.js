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

function safeSend(ws, data, clientId) {
  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    logger.error("Failed to send message", { clientId, error: err.message });
  }
}

function handleMessage(ws, clientId, rooms, raw) {
  const validation = validateMessage(raw.toString());

    const ip = req.socket.remoteAddress;

    if (!connRateLimiter.check(ip)) {
      logger.warn("Connection rate limit exceeded", { ip });
      ws.close(4029, "Connection rate limit exceeded");
      return;
    }

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

    logger.info("Client connected", { clientId: actualClientId, ip });

  const token = url.searchParams.get("token");
  const authResult = verifyConnection(token);

    ws.on("message", (raw) => {
      if (!rateLimiter.check(actualClientId)) {
        logger.warn("Rate limit exceeded", { clientId: actualClientId });
        ws.send(JSON.stringify({ type: "error", payload: { message: "Rate limit exceeded" } }));
        return;
      }

      const validation = validateMessage(raw.toString());

  const actualClientId = authResult.clientId ?? clientId;
  logger.info("Client connected", { clientId: actualClientId, ip: req.socket.remoteAddress });

  ws.on("pong", heartbeat);
  ws.on("message", (raw) => handleMessage(ws, actualClientId, rooms, raw));

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

  return { wss, rooms, ipConnectionCount };
}
