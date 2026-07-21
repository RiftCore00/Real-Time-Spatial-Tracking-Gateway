## Title
Implement room membership DoS protection with per-client room limits, per-room capacity caps, total room count ceiling, and resource-pressure circuit breaker

## Difficulty
10/10 — Expert. Estimated effort: 3–4 days for a senior engineer.

## Context
`RoomManager.join()` in `src/room-manager.js` (lines 51–58) accepts any `clientId` into any `roomId` with no limits whatsoever. A single misbehaving client can call `join_room` for 100,000 different room IDs, creating 100,000 entries in `_rooms` and 100,000 entries in `_clientRooms`. Each room entry is a `Map<string, WebSocket>` (minimum ~100 bytes overhead per Map), and each client-room entry is a `Set<string>` that grows by one per join. At 100K rooms, this is ~10MB of pure overhead — and that's from a single client.

Conversely, a single room can accumulate unlimited members. A public "region-global" room could attract 50,000 clients, making every `broadcast()` call iterate 50,000 WebSocket objects — an O(N) event-loop-blocking operation that takes ~50ms per broadcast at 50K members (issue 4 addresses broadcast backpressure, but the root cause is the lack of room capacity limits).

The `project.md` describes the system as supporting "fleet tracking, asset monitoring, geofencing enforcement, and live mapping." In a real deployment, rooms correspond to fleet IDs, regions, or user groups — all of which have natural cardinality and membership bounds. The absence of these bounds makes the system trivially vulnerable to resource exhaustion.

## Problem statement
Implement a multi-layered resource accounting and DoS protection system for `RoomManager`:

1. **Per-client room limit**: A single client cannot join more than `maxRoomsPerClient` rooms (configurable, default 50). Excess `join_room` attempts return `{ type: "error", payload: { code: "ROOM_LIMIT_EXCEEDED", message: "..." } }`.
2. **Per-room member limit**: A single room cannot have more than `maxMembersPerRoom` clients (configurable, default 10,000). Excess joins return `{ type: "error", payload: { code: "ROOM_FULL", message: "..." } }`.
3. **Total room count ceiling**: The `RoomManager` cannot hold more than `maxRooms` rooms (configurable, default 10,000). When the ceiling is hit, new room creation (first `join` to a non-existent room) is rejected with `{ type: "error", payload: { code: "MAX_ROOMS_REACHED", message: "..." } }`.
4. **Resource-pressure circuit breaker**: When total memory usage of the `RoomManager` (estimated via `roomCount × avgRoomSize + clientCount × avgClientRoomCount`) exceeds a configurable threshold, the circuit breaker opens. While open, all `join_room` operations are rejected with `{ type: "error", payload: { code: "CIRCUIT_BREAKER_OPEN", message: "..." } }`. The breaker closes automatically when memory pressure subsides (configurable recovery threshold).
5. **Metrics exposure**: `RoomManager` exposes `stats` getter returning `{ roomCount, clientCount, totalMembers, circuitBreakerState }`.

## Current behavior
`src/room-manager.js` `join()` (lines 51–58):
```js
join(clientId, roomId, ws) {
  if (clientId == null) throw new TypeError("clientId is required");
  if (roomId == null) throw new TypeError("roomId is required");
  if (ws == null) throw new TypeError("ws is required");
  this._ensureRoom(roomId).set(clientId, ws);
  this._ensureClientRooms(clientId).add(roomId);
}
```
No limits. No resource accounting. No circuit breaker. A single client can join unlimited rooms, and a single room can hold unlimited members.

## Required behavior
- `RoomManager` constructor accepts `{ maxRoomsPerClient, maxMembersPerRoom, maxRooms, circuitBreaker: { enabled, memoryThresholdBytes, recoveryThresholdBytes } }` with sane defaults.
- `join()` checks all limits before adding membership and returns/throws structured error objects (not just `TypeError`).
- The circuit breaker uses `process.memoryUsage().heapUsed` to estimate memory pressure.
- All existing `room-manager.test.js`, `room-manager-extended.test.js`, `room-manager-additional.test.js` tests pass (they don't hit any limits with default settings).
- New tests for: limit enforcement, circuit breaker open/close transitions, metrics exposure.

## Constraints
- Do not change the `join(clientId, roomId, ws)` method signature — callers pass the same arguments.
- `join()` must throw `TypeError` for null/undefined required args (existing behavior) AND return/throw structured errors for limit violations.
- Circuit breaker must not add per-request overhead beyond a single `process.memoryUsage()` call (which is ~1μs in Node.js).
- Do not add new npm dependencies.
- Do not modify `server.js` or any test file (except adding new test files).
- The `maxMembersPerRoom` limit must be enforced atomically — no TOCTOU race between checking the count and adding the member (Node.js single-threaded, so this is naturally satisfied, but the code must not yield between check and insert).

## Acceptance criteria
- [ ] `join("c1", "r1", ws)` succeeds when client has < `maxRoomsPerClient` rooms
- [ ] `join("c1", "r2", ws)` throws/returns error when client already has `maxRoomsPerClient` rooms
- [ ] `join("c1", "r1", ws)` succeeds when room has < `maxMembersPerRoom` members
- [ ] `join("c2", "r1", ws)` throws/returns error when room already has `maxMembersPerRoom` members
- [ ] First join to a new room succeeds when `roomCount < maxRooms`
- [ ] First join to a new room fails when `roomCount === maxRooms` (existing room joins still work)
- [ ] Circuit breaker opens when `heapUsed > memoryThresholdBytes`
- [ ] Circuit breaker closes when `heapUsed < recoveryThresholdBytes`
- [ ] When circuit breaker is open, all `join()` calls are rejected
- [ ] `stats` getter returns `{ roomCount, clientCount, totalMembers, circuitBreakerState }`
- [ ] All 30 existing RoomManager tests pass unchanged
- [ ] `npm run lint` passes

## Out of scope
- Changes to `server.js`, `auth.js`, `validator.js`, `rate-limiter.js`, `index.js`, `logger.js`.
- Implementing per-room message rate limiting (that is a broadcast concern).
- Dynamic limit adjustment based on system load (static limits are sufficient for this issue).

## Hints and references
- `process.memoryUsage().heapUsed` returns the V8 heap usage in bytes. It's fast (~1μs) but not free — call it only on `join()`, not on every `broadcast()`.
- The circuit breaker pattern has three states: `CLOSED` (normal), `OPEN` (rejecting), `HALF_OPEN` (testing recovery). For this implementation, `CLOSED` and `OPEN` are sufficient — the breaker transitions directly from `OPEN` to `CLOSED` when memory drops below the recovery threshold.
- For `maxMembersPerRoom`, the check is simply `room.size >= maxMembersPerRoom` after `_ensureRoom(roomId)` — the room is guaranteed to exist at that point.
- For `maxRoomsPerClient`, check `_clientRooms.get(clientId)?.size ?? 0` before `_ensureClientRooms(clientId).add(roomId)`.
- For `maxRooms`, check `_rooms.size` before `_ensureRoom(roomId)` — but only when the room doesn't already exist: `!this._rooms.has(roomId) && this._rooms.size >= maxRooms`.
