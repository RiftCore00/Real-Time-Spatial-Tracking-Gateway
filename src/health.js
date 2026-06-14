import http from "node:http";
import { logger } from "./logger.js";

const startTime = Date.now();

export function startHealthServer(wss) {
  const port = parseInt(process.env.HEALTH_PORT ?? "9090", 10);

  if (isNaN(port) || port < 1 || port > 65535) {
    logger.error("Invalid HEALTH_PORT value", { HEALTH_PORT: process.env.HEALTH_PORT });
    return;
  }

  const server = http.createServer((req, res) => {
    if (req.url !== "/health" || req.method !== "GET") {
      res.writeHead(404);
      res.end();
      return;
    }

    const data = JSON.stringify({
      status: "ok",
      connections: wss.clients.size,
      uptime: Math.floor((Date.now() - startTime) / 1000),
      memory: process.memoryUsage(),
    });

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(data);
  });

  server.listen(port, () => {
    logger.info("Health check server started", { port });
  });

  return server;
}
