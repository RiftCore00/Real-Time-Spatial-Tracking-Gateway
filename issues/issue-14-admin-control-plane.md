## Title
Implement an administrative control plane API with real-time room/connection inspection, client management, dynamic configuration, and audit logging

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The gateway has zero runtime observability or control beyond structured logs. Operators cannot: list active rooms and their member counts, inspect a specific client's subscriptions and message rate, kick a misbehaving client, adjust rate limits dynamically, or drain a room for maintenance. The `README.md` mentions "Observability — Structured logging for connection events, message throughput, validation failures, and errors, enabling operational dashboards" (line 36) but logs are passive — no active control plane.

In a production fleet-tracking deployment, operations teams need to: identify the top 10 rooms by message volume, find clients exceeding rate limits, force-disconnect a compromised device, increase `MAX_MESSAGES_PER_SECOND` for a specific fleet during an event, or gracefully drain a room before deploying a geofence update (issue 12). None of this is possible today.

## Problem statement
Design and implement an administrative control plane exposed via a dedicated HTTP server (separate port from the WebSocket gateway) with:

1. **REST API for inspection** (read-only, safe for monitoring):
   - `GET /admin/v1/rooms` — list all rooms with `{ roomId, memberCount, messageRatePerSec, primaryInstance }` (primaryInstance from issue 11).
   - `GET /admin/v1/rooms/{roomId}` — room details: members `[{ clientId, ip, connectedAt, protocolVersion, lastMessageAt, messageCount }]`, slow consumers, geofences (issue 12).
   - `GET /admin/v1/clients` — paginated list of all clients with filters: `roomId`, `ip`, `protocolVersion`, `connectedSince`.
   - `GET /admin/v1/clients/{clientId}` — client detail: rooms, message rate, rate limit status, auth identity, geofence state.
   - `GET /admin/v1/stats` — global: connections, rooms, messages/sec, rate limit rejections, auth failures, memory, event loop lag, protocol version distribution.
   - `GET /admin/v1/geofences` — list all fences with `fenceId`, `name`, `roomId`, `vertexCount`, `clientInsideCount`.

2. **REST API for control** (mutating, requires elevated auth):
   - `POST /admin/v1/clients/{clientId}/disconnect` — force close with code 4001 "Administrative disconnect", optional `reason`.
   - `POST /admin/v1/rooms/{roomId}/drain` — initiate graceful drain: stop accepting new joins, send `room_draining` to members, wait for `drainTimeoutMs`, then force-disconnect remaining. Returns `drainId` for polling.
   - `POST /admin/v1/rooms/{roomId}/drain/{drainId}/status` — poll drain progress.
   - `PATCH /admin/v1/config/rate-limits` — dynamic rate limit updates: `{ maxMessagesPerSecond?: number, connRateLimit?: number, maxConnectionsPerIp?: number }` — applies to NEW connections/messages immediately, no restart.
   - `PATCH /admin/v1/config/room-limits` — dynamic room limits: `{ maxRoomsPerClient?, maxMembersPerRoom?, maxRooms? }`.
   - `POST /admin/v1/geofences/{fenceId}/disable` — temporarily disable a fence (stops evaluation, keeps definition).

3. **Real-time event stream** (Server-Sent Events):
   - `GET /admin/v1/events?types=connect,disconnect,join,leave,rate_limit,geofence` — SSE stream of structured admin events for dashboards/alerting.

4. **Authentication and authorization**:
   - Separate `ADMIN_API_KEY` env var (or mTLS client cert). All admin endpoints require `Authorization: Bearer <ADMIN_API_KEY>`.
   - Role-based access: `admin:read` for inspection, `admin:write` for control. API key format: `ak_live_<role>_<random>`.

5. **Audit logging**: Every control API call writes an immutable audit log entry (to storage adapter from issue 10) with `{ timestamp, adminIdentity, action, target, params, result }`.

6. **Integration with distributed mode (issue 11)**: Admin API on any instance can operate on global state — `disconnect` finds the instance hosting the client and forwards the command via Redis.

## Current behavior
- No admin HTTP server exists.
- `src/server.js` only has `/health` endpoint (line 36–40).
- No client/room inspection APIs.
- No dynamic configuration — all limits are startup-only env vars.
- No audit logging.

## Required behavior
- New module `src/admin-server.js` exporting `createAdminServer({ gateway, config })` where `gateway` provides access to `rooms`, `rateLimiter`, `connRateLimiter`, `geofenceEngine` (issue 12), `storageAdapter` (issue 10), `protocolRegistry` (issue 13), `distributedRoomManager` (issue 11).
- Admin HTTP server binds to `ADMIN_PORT` (default 8081), separate from WebSocket port.
- All endpoints return JSON with consistent envelope: `{ ok: true, data }` or `{ ok: false, error: { code, message } }`.
- Rate limit: admin API itself rate-limited to 100 req/min per API key (prevents DoS).
- CORS: configurable `ADMIN_CORS_ORIGIN` for browser-based dashboards.
- OpenAPI 3.1 spec generated at `/admin/v1/openapi.json` and Swagger UI at `/admin/v1/docs`.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`.
- Do not modify existing test files. New test files required.
- Add `fastify` (or `express` if you prefer, but `fastify` is faster) to `package.json` — only new dependency allowed for HTTP framework.
- Admin server must be able to run in the SAME process as the gateway (shared memory access to `RoomManager`, etc.) OR as a separate process (communicating via Redis). Design for both — the `gateway` parameter abstracts the data source.
- Dynamic config changes must be atomic and thread-safe (Node.js single-threaded, but consider async race conditions).
- Audit log entries must be written via `storageAdapter.writeBatch()` with a special `audit` room/type.
- The admin API must not block the WebSocket event loop — use `setImmediate` for any heavy computation (e.g., iterating 100K rooms for `/admin/v1/rooms`).

## Acceptance criteria
- [ ] `createAdminServer()` starts HTTP server on `ADMIN_PORT`
- [ ] `GET /admin/v1/stats` returns global stats in <50ms for 100K connections
- [ ] `GET /admin/v1/rooms` returns paginated room list with correct member counts
- [ ] `GET /admin/v1/rooms/{roomId}` returns member details including `lastMessageAt`
- [ ] `POST /admin/v1/clients/{clientId}/disconnect` terminates the client's WebSocket with code 4001
- [ ] `POST /admin/v1/rooms/{roomId}/drain` initiates drain, returns `drainId`, completes within `drainTimeoutMs`
- [ ] `PATCH /admin/v1/config/rate-limits` updates rate limits for new connections immediately
- [ ] `GET /admin/v1/events` streams SSE events for connect/disconnect/join/leave
- [ ] Admin API key auth: requests without valid `Authorization: Bearer ak_admin_write_...` return 401
- [ ] Role enforcement: `ak_admin_read_...` can GET but not POST/PATCH
- [ ] Audit log entries written to storage for every control action
- [ ] Distributed mode: `disconnect` called on instance 1 for client on instance 2 → command forwarded via Redis, client disconnected on instance 2
- [ ] `npm run lint` passes
- [ ] New test file: `tests/admin-server.test.js` with all above scenarios
- [ ] OpenAPI spec at `/admin/v1/openapi.json` is valid and complete

## Out of scope
- Dashboard UI (Swagger UI is sufficient for this issue).
- Alerting/notification integration (webhook, PagerDuty, etc.).
- Historical admin event queries (audit log in storage handles this).
- Multi-tenancy / organization hierarchy — single admin domain.
- Configuration persistence across restarts (env vars remain source of truth; dynamic changes are runtime-only).

## Hints and references
- Fastify schema validation: use `zod-to-json-schema` to convert Zod schemas to JSON Schema for Fastify's built-in validation.
- For SSE: `reply.raw.write('data: ' + JSON.stringify(event) + '\n\n')` in a long-held request. Track connections in a `Set` for broadcast.
- Dynamic rate limit: `rateLimiter` and `connRateLimiter` are closures over `Map`s. Expose `setLimit(newLimit)` method on each that updates the internal limit variable.
- For distributed disconnect: Redis channel `admin:command` with payload `{ type: "disconnect", clientId, targetInstanceId }`. Each instance subscribes, checks if it owns the client, executes if so.
- Audit log schema (storage adapter):
  ```js
  { type: "audit", payload: { timestamp, adminKeyId, action: "disconnect", target: "client:abc123", params: { reason: "compromised" }, result: "success" } }
  ```
- Pagination: `?page=1&pageSize=50` with `Link` header for next/prev.
- Event loop protection: for `/admin/v1/rooms` on 100K rooms, chunk the iteration: `for (const chunk of chunked(roomEntries, 1000)) { await setImmediatePromise(); results.push(...processChunk(chunk)); }`