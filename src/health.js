import http from "node:http";

const startTime = Date.now();

export function startHealthServer(wss) {
  const port = parseInt(process.env.HEALTH_PORT ?? "9090", 10);

  const server = http.createServer((req, res) => {
    if (req.url !== "/health" || req.method !== "GET") {
      res.writeHead(404);
      res.end();
      return;
    }

    const mem = process.memoryUsage();
    const status = {
      status: "ok",
      uptime: Math.floor((Date.now() - startTime) / 1000),
      connections: wss.clients.size,
      memory: {
        rss: mem.rss,
        heapTotal: mem.heapTotal,
        heapUsed: mem.heapUsed,
      },
    };

    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(status));
  });

  server.listen(port, () => {
    console.log(`Health server listening on port ${port}`);
  });

  return server;
}
