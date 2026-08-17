import "dotenv/config";
import { createServer } from "./server.js";
import { logger } from "./logger.js";

/**
 * Parses environment variables into server configuration with integer coercion.
 *
 * Session-resumption keys are only present when their variables are set, so an
 * unconfigured deployment keeps the historical three-key shape.
 *
 * @returns {{ port: number, heartbeatMs: number, maxPayloadBytes: number, sessionEncryptionKey?: string, sessionTtlMs?: number, instanceId?: string }}
 */
export function parseConfig() {
  const port = parseInt(process.env.PORT ?? "8080", 10);
  const heartbeatMs = parseInt(process.env.WS_HEARTBEAT_MS ?? "30000", 10);
  const maxPayloadBytes = parseInt(process.env.MAX_PAYLOAD_BYTES ?? "1024", 10);
  const config = { port, heartbeatMs, maxPayloadBytes };

  if (process.env.SESSION_ENCRYPTION_KEY) {
    config.sessionEncryptionKey = process.env.SESSION_ENCRYPTION_KEY;
  }
  if (process.env.SESSION_TTL_MS) {
    config.sessionTtlMs = parseInt(process.env.SESSION_TTL_MS, 10);
  }
  if (process.env.INSTANCE_ID) {
    config.instanceId = process.env.INSTANCE_ID;
  }
  return config;
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

if (config.sessionTtlMs !== undefined && (isNaN(config.sessionTtlMs) || config.sessionTtlMs < 1)) {
  logger.error("Invalid SESSION_TTL_MS value", { SESSION_TTL_MS: process.env.SESSION_TTL_MS });
  process.exit(1);
}

let wss;
let sessionManager;
let instanceId;
let saveAllSessions;
try {
  ({ wss, sessionManager, instanceId, saveAllSessions } = createServer(config));
} catch (err) {
  logger.error("Failed to start server", { error: err.message });
  process.exit(1);
}

// The encryption key never reaches the logs.
logger.info("Gateway started", {
  port: config.port,
  heartbeatMs: config.heartbeatMs,
  maxPayloadBytes: config.maxPayloadBytes,
  instanceId,
  sessionResumption: sessionManager != null,
});

/**
 * Initiates a multi-phase graceful shutdown of the WebSocket server.
 *
 * Phase 1 (0ms):       Stop accepting new connections.
 * Phase 2 (100ms):     Notify all connected clients of impending shutdown.
 * Phase 3 (500–4000ms): Drain pending sends (poll bufferedAmount === 0).
 * Phase 4 (4000ms):    Close connections with WebSocket code 1001 "Going Away".
 * Phase 5 (>5000ms):   Force exit.
 *
 * When session resumption is active, phase 2 first persists every live session
 * and hands each client its own fresh blob as `session_id`, so a client can
 * resume on another instance immediately.
 *
 * @param {object} wss - The WebSocket server instance.
 * @param {string} signal - The OS signal that triggered the shutdown (e.g. "SIGTERM").
 * @param {object} [options]
 * @param {() => Promise<Map<string, string>>} [options.saveAllSessions] - Persists live sessions.
 * @returns {void}
 */
export function shutdown(wss, signal, { saveAllSessions } = {}) {
  logger.info("shutdown: stopping accept", { signal });

  const clientCount = wss.clients ? wss.clients.size : 0;

  /** Sends `server_shutting_down`, adding a per-client blob when one exists. */
  function notifyClients(blobs) {
    const shared = JSON.stringify({ type: "server_shutting_down", payload: { reconnectIn: 5 } });
    for (const client of wss.clients) {
      const sessionId = blobs?.get(client._clientId);
      try {
        client.send(
          sessionId
            ? JSON.stringify({
                type: "server_shutting_down",
                payload: { reconnectIn: 5, session_id: sessionId },
              })
            : shared
        );
      } catch {
        // Client may already be disconnected
      }
    }
  }

  // Phase 2 — Notify clients (100ms)
  setTimeout(() => {
    logger.info("shutdown: notifying N clients", { clientCount });
    if (!wss.clients) return;

    if (typeof saveAllSessions !== "function") {
      notifyClients(null);
      return;
    }

    saveAllSessions()
      .then((blobs) => notifyClients(blobs))
      .catch((err) => {
        logger.error("shutdown: session save failed", { error: err.message });
        notifyClients(null);
      });
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

// Without a session manager there is nothing to persist, so the shared
// broadcast path stays in use.
const shutdownOptions = sessionManager ? { saveAllSessions } : {};

process.on("SIGTERM", () => shutdown(wss, "SIGTERM", shutdownOptions));
process.on("SIGINT", () => shutdown(wss, "SIGINT", shutdownOptions));

process.on("uncaughtException", (err) => {
  logger.error("Uncaught exception", { error: err.message });
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled rejection", { reason: String(reason) });
  process.exit(1);
});
