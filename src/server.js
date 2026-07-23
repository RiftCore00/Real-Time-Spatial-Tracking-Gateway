import http from "node:http";
import { WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import { RoomManager } from "./room-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";

export function createServer({ port, heartbeatMs, maxPayloadBytes, connRateLimit, maxConnectionsPerIp } = {}) {
  const server = http.createServer((req, res) => {
    let url;
    try {
      url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Bad Request" }));
      return;
    }

    if (req.method === "GET" && url.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "OK" }));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not Found" }));
  });

  const wss = new WebSocketServer({
    server,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  server.listen(port ?? 8080);

  const rooms = new RoomManager();
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP = maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);

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
        logger.warn("Terminating zombie connection", { clientId: ws._clientId ?? "unknown" });
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, heartbeatMs ?? 30000);

  wss.on("close", () => {
    clearInterval(interval);
    server.close();
  });

  return { wss, server, rooms, ipConnectionCount };
}
