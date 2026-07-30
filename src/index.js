import "dotenv/config";
import { createServer } from "./server.js";
import { logger } from "./logger.js";

/**
 * Parses environment variables into server configuration with integer coercion.
 *
 * @returns {{ port: number, heartbeatMs: number, maxPayloadBytes: number }}
 */
export function parseConfig() {
  const port = parseInt(process.env.PORT ?? "8080", 10);
  const heartbeatMs = parseInt(process.env.WS_HEARTBEAT_MS ?? "30000", 10);
  const maxPayloadBytes = parseInt(process.env.MAX_PAYLOAD_BYTES ?? "1024", 10);
  return { port, heartbeatMs, maxPayloadBytes };
}

const config = parseConfig();

if (isNaN(config.port) || config.port < 1 || config.port > 65535) {
  logger.error("Invalid PORT value", { PORT: process.env.PORT });
  process.exit(1);
}

if (isNaN(config.heartbeatMs) || config.heartbeatMs < 1) {
  logger.error("Invalid WS_HEARTBEAT_MS value", { WS_HEARTBEAT_MS: process.env.WS_HEARTBEAT_MS });
  process.exit(1);
}

if (isNaN(config.maxPayloadBytes) || config.maxPayloadBytes < 1) {
  logger.error("Invalid MAX_PAYLOAD_BYTES value", { MAX_PAYLOAD_BYTES: process.env.MAX_PAYLOAD_BYTES });
  process.exit(1);
}

let wss;
try {
  ({ wss } = createServer(config));
} catch (err) {
  logger.error("Failed to start server", { error: err.message });
  process.exit(1);
}

logger.info("Gateway started", config);

/**
 * Initiates a multi-phase graceful shutdown of the WebSocket server.
 *
 * Phase 1 (0ms):       Stop accepting new connections.
 * Phase 2 (100ms):     Notify all connected clients of impending shutdown.
 * Phase 3 (500–4000ms): Drain pending sends (poll bufferedAmount === 0).
 * Phase 4 (4000ms):    Close connections with WebSocket code 1001 "Going Away".
 * Phase 5 (>5000ms):   Force exit.
 *
 * @param {object} wss - The WebSocket server instance.
 * @param {string} signal - The OS signal that triggered the shutdown (e.g. "SIGTERM").
 * @returns {void}
 */
export function shutdown(wss, signal) {
  logger.info("shutdown: stopping accept", { signal });

  const clientCount = wss.clients ? wss.clients.size : 0;

  // Phase 2 — Notify clients (100ms)
  setTimeout(() => {
    logger.info("shutdown: notifying N clients", { clientCount });
    if (wss.clients) {
      const notification = JSON.stringify({ type: "server_shutting_down", payload: { reconnectIn: 5 } });
      for (const client of wss.clients) {
        try {
          client.send(notification);
        } catch {
          // Client may already be disconnected
        }
      }
    }
  }, 100);

  // Phase 3 — Drain pending sends (500ms–4000ms)
  setTimeout(() => {
    logger.info("shutdown: draining N clients", { clientCount });

    if (!wss.clients || wss.clients.size === 0) {
      closeConnections();
      return;
    }

    const deadline = Date.now() + 3500;
    const drainPoll = setInterval(() => {
      let allDrained = true;
      for (const client of wss.clients) {
        if (client.readyState === 1 && client.bufferedAmount > 0) {
          allDrained = false;
          break;
        }
      }
      if (allDrained || Date.now() >= deadline) {
        clearInterval(drainPoll);
        closeConnections();
      }
    }, 100);
  }, 500);

  // Phase 5 — Force exit (>5000ms)
  const forceExitTimer = setTimeout(() => {
    logger.error("shutdown: force exit");
    process.exit(1);
  }, 5000);

  function closeConnections() {
    const remaining = wss.clients ? wss.clients.size : 0;
    logger.info("shutdown: closing N clients", { clientCount: remaining });

    // Phase 4 — Send close frames with code 1001 "Going Away"
    if (wss.clients) {
      for (const client of wss.clients) {
        if (client.readyState === 1) {
          client.close(1001, "Going Away");
        }
      }
    }
  }

  // Phase 1 — Close the server (stops accepting new connections).
  // The callback fires once all connections are closed, completing the shutdown.
  wss.close(() => {
    clearTimeout(forceExitTimer);
    logger.info("Server closed");
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown(wss, "SIGTERM"));
process.on("SIGINT", () => shutdown(wss, "SIGINT"));

process.on("uncaughtException", (err) => {
  logger.error("Uncaught exception", { error: err.message });
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled rejection", { reason: String(reason) });
  process.exit(1);
});
