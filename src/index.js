import "dotenv/config";
import { createServer } from "./server.js";
import { logger } from "./logger.js";

/**
 * Parses gateway configuration from environment variables.
 * @returns {{ port: number, heartbeatMs: number, maxPayloadBytes: number }}
 */
export function parseConfig() {
  return {
    port: parseInt(process.env.PORT ?? "8080", 10),
    heartbeatMs: parseInt(process.env.WS_HEARTBEAT_MS ?? "30000", 10),
    maxPayloadBytes: parseInt(process.env.MAX_PAYLOAD_BYTES ?? "1024", 10),
  };
}

/**
 * Initiates graceful shutdown of the WebSocket server.
 * @param {import('ws').WebSocketServer} wss
 * @param {string} signal
 */
export function shutdown(wss, signal) {
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

// Entry point — only runs when executed directly
const config = parseConfig();
const { wss } = createServer(config);

logger.info("Gateway started", config);

process.on("SIGTERM", () => shutdown(wss, "SIGTERM"));
process.on("SIGINT", () => shutdown(wss, "SIGINT"));
