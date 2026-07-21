## Title
Implement a co-located HTTP server with health check, readiness probe, Prometheus metrics exposition, and WebSocket upgrade protocol detection on a shared port

## Difficulty
10/10 — Expert. Estimated effort: 4–5 days for a senior engineer.

## Context
The gateway currently only binds a raw `WebSocketServer` on a port. There is no HTTP listener, no health check endpoint, and no metrics endpoint. This makes the service undeployable in any container orchestrator (Kubernetes, ECS, Nomad) that requires:

- **Liveness probe**: An HTTP endpoint that returns 200 when the process is alive and not deadlocked. Without it, the orchestrator cannot detect a stuck process and will not restart it.
- **Readiness probe**: An HTTP endpoint that returns 200 only when the service is ready to accept traffic (all subsystems initialized, auth configured, rate limiters functional). Without it, traffic is routed to the instance before it's ready, causing connection failures.
- **Prometheus metrics**: A `/metrics` endpoint exposing connection count, message rate, room sizes, rate limit rejections, memory usage, and event loop lag. Without it, there is no observability into the running system.

The `docker-compose.yml` includes a Postgres service and the `Dockerfile` exposes port 8080, but the application has no HTTP server. The README architecture diagram shows an "Observability" layer with "Structured logging" but no metrics endpoint.

The hard part is **protocol detection on a shared port**: WebSocket connections start with an HTTP GET request with `Upgrade: websocket` header. A naive HTTP server would intercept these upgrade requests and break WebSocket connectivity. The solution requires inspecting the incoming request to determine if it's a WebSocket upgrade (pass through to WSS) or a plain HTTP request (handle as health/metrics). This is the core algorithmic challenge.

## Problem statement
Implement an HTTP server that co-exists with the WebSocket server on the same port using protocol detection:

1. **Protocol detection**: Create an `http.createServer` as the primary listener. On each incoming request, check for the `Upgrade: websocket` header. If present, pass the socket to the `WebSocketServer` for upgrade. If absent, handle as an HTTP request.
2. **Health endpoint** (`GET /healthz`): Returns `200 { "status": "ok", "uptime": <seconds> }` when the process is alive. Always returns 200 unless the process is shutting down.
3. **Readiness endpoint** (`GET /readyz`): Returns `200 { "status": "ready", "connections": <n>, "rooms": <n> }` when all subsystems are initialized and the circuit breaker (if implemented) is closed. Returns `503 { "status": "not ready", "reason": "..." }` when not ready.
4. **Prometheus metrics** (`GET /metrics`): Returns Prometheus exposition format (text/plain) with:
   - `gateway_connections_active` (gauge)
   - `gateway_rooms_active` (gauge)
   - `gateway_messages_total` (counter, labeled by `type`)
   - `gateway_rate_limit_rejections_total` (counter, labeled by `kind`)
   - `gateway_auth_failures_total` (counter)
   - `gateway_heap_used_bytes` (gauge)
   - `gateway_event_loop_lag_ms` (gauge)
5. **Graceful shutdown integration**: When `shutdown()` is called, the HTTP server stops accepting new connections (both HTTP and WebSocket) simultaneously.

## Current behavior
`src/server.js` creates a `WebSocketServer` with `new WebSocketServer({ port })`. This directly binds a TCP listener. There is no HTTP server, no health endpoint, no metrics. The `index.js` shutdown function calls `wss.close()` which closes the underlying TCP listener.

`src/index.js` creates the server with `createServer({ port, heartbeatMs, maxPayloadBytes })` and the `wss` is the only listener.

## Required behavior
- `createServer()` returns `{ wss, httpServer, rooms, metrics }` where `httpServer` is the `http.Server` instance.
- WebSocket upgrade requests are correctly routed to the WSS.
- `/healthz`, `/readyz`, `/metrics` endpoints respond correctly.
- Prometheus metrics are exposed in standard exposition format.
- All existing tests pass (they create `WebSocketServer` on port 0 — you must ensure the HTTP server doesn't interfere with direct WSS creation in tests).
- Event loop lag is measured using a periodic `setTimeout` drift detector.

## Constraints
- Do not add new npm dependencies. Use Node.js built-in `http` module.
- The HTTP server must be the primary listener — the `WebSocketServer` must be attached via `noServer: true` mode and the upgrade handled manually.
- Metrics counters must be atomically incremented (no race conditions — Node.js single-threaded, but ensure no counter corruption during async operations).
- The event loop lag detector must not add more than 1ms of overhead per measurement cycle.
- Do not modify existing test files. New test files for HTTP endpoints are allowed.
- `/metrics` must return valid Prometheus exposition format (no library — format manually).

## Acceptance criteria
- [ ] `GET /healthz` returns `200` with `{ status: "ok", uptime: <number> }`
- [ ] `GET /readyz` returns `200` when server is initialized
- [ ] `GET /readyz` returns `503` during shutdown
- [ ] `GET /metrics` returns `200` with `Content-Type: text/plain; version=0.0.4; charset=utf-8`
- [ ] `/metrics` output contains `gateway_connections_active`, `gateway_rooms_active`, `gateway_messages_total`, `gateway_heap_used_bytes`, `gateway_event_loop_lag_ms`
- [ ] WebSocket connections still work correctly on the same port (all server.test.js tests pass)
- [ ] HTTP requests to non-upgrade paths do not interfere with WebSocket upgrade
- [ ] `npm run lint` passes
- [ ] New test file: HTTP endpoint tests for health, readiness, metrics

## Out of scope
- Changes to `room-manager.js`, `auth.js`, `validator.js`, `rate-limiter.js`, `logger.js`.
- Alerting or dashboard configuration (that's a Grafana/Datadog concern).
- Distributed tracing (OpenTelemetry integration).
- Admin API for runtime configuration changes.

## Hints and references
- The `ws` library supports `noServer: true` mode: `new WebSocketServer({ noServer: true })`. Then handle the `upgrade` event on the HTTP server: `httpServer.on("upgrade", (req, socket, head) => { wss.handleUpgrade(req, socket, head, (ws) => { wss.emit("connection", ws, req); }); })`.
- For event loop lag measurement: record `Date.now()` before and after a `setTimeout(0)`. The difference minus 0ms is the lag. Do this every 5 seconds. Store the result in a module-level variable.
- Prometheus format for a gauge: `# TYPE gateway_connections_active gauge\ngateway_connections_active 42\n`. For a counter: `# TYPE gateway_messages_total counter\ngateway_messages_total{type="location_update"} 12345\n`.
- The `uptime` in `/healthz` is `process.uptime()` — built into Node.js.
- Consider: the existing tests use `createServer({ port: 0 })` which creates a `WebSocketServer` directly. If you change `createServer` to return an HTTP server, the tests that do `server.wss.address().port` will break unless the HTTP server's port is used instead. Ensure the returned port is consistent.
