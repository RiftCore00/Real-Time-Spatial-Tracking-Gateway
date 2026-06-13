import "dotenv/config";
import { createServer } from "./server.js";
import { logger } from "./logger.js";

const port = parseInt(process.env.PORT ?? "8080", 10);
const heartbeatMs = parseInt(process.env.WS_HEARTBEAT_MS ?? "30000", 10);
const maxPayloadBytes = parseInt(process.env.MAX_PAYLOAD_BYTES ?? "1024", 10);

const { wss } = createServer({ port, heartbeatMs, maxPayloadBytes });

logger.info("Gateway started", { port, heartbeatMs, maxPayloadBytes });

/**
 * Initiates a graceful shutdown of the WebSocket server.
 *
 * Closes the server and waits for existing connections to finish. If the
 * server does not close within 5 seconds, a forced exit is triggered.
 *
 * @param {string} signal - The OS signal that triggered the shutdown (e.g. "SIGTERM").
 * @returns {void}
 */
function shutdown(signal) {
  logger.info("Shutting down", { signal });
  wss.close(() => {
    logger.info("Server closed");
    process.exit(0);
  });
  setTimeout(() => {
    logger.error("Forced shutdown");
    process.exit(1);
  }, 5000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
