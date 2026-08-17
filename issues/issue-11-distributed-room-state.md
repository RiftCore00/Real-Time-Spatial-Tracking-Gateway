## Title
Implement horizontally-scalable distributed room state with Redis-backed pub/sub, consistent hashing for room affinity, and cross-instance broadcast coordination

## Difficulty
10/10 — Expert. Estimated effort: 7–10 days for a senior engineer.

## Context
The current `RoomManager` in `src/room-manager.js` stores all room membership and client WebSocket references in-process (`Map<RoomId, Map<ClientId, WebSocket>>`). This architecture fundamentally prevents horizontal scaling: multiple gateway instances behind a load balancer cannot share room state. When client A (connected to instance 1) publishes a location update to room "fleet-alpha", only clients on instance 1 receive it — clients on instance 2, 3, etc. are completely isolated.

The `README.md` architecture diagram (lines 43–62) shows a single-process design ("Single-process, async I/O") but the project description targets "thousands of concurrent mobile or web clients" and "fleet tracking" — workloads that routinely exceed a single Node.js process's capacity (CPU-bound by event loop, memory-bound by V8 heap limits ~1.4GB). Kubernetes deployments with HPA require stateless pods that can scale out.

## Problem statement
Design and implement a distributed room state layer that enables N gateway instances to operate as a single logical broadcast domain:

1. **Room membership synchronization**: When a client joins/leaves a room on any instance, all instances must update their local view of that room's membership within <50ms.

2. **Cross-instance broadcast**: A `location_update` published on instance 1 must be fanned out to all members of that room across ALL instances, with <100ms end-to-end latency (publisher → subscriber across instances).

3. **Consistent hashing for room affinity**: Use consistent hashing (e.g., Ketama/Jump hash) to assign each room a "primary" instance. The primary is responsible for sequencing, deduplication, and ring buffer (from issue 6). Non-primary instances forward publish requests to the primary via Redis pub/sub.

4. **Instance discovery and failure detection**: Instances register in Redis with TTL heartbeats. When an instance fails, its rooms are re-assigned to surviving instances, and orphaned clients are notified to reconnect.

5. **Connection migration**: When a client reconnects to a different instance (load balancer redistribution), the new instance must fetch the client's room memberships from Redis and restore subscriptions seamlessly.

6. **Backpressure across instances**: The broadcast backpressure mechanism (issue 4) must work across the distributed layer — slow consumers on any instance must not block the primary's event loop.

## Current behavior
- `RoomManager` is purely in-memory (`src/room-manager.js` lines 11–18).
- `server.js` creates one `RoomManager` per process.
- No Redis dependency exists in `package.json`.
- No cross-instance communication mechanism.
- `docker-compose.yml` has no Redis service.

## Required behavior
- New `DistributedRoomManager` class (or extended `RoomManager`) that wraps a local `RoomManager` and adds Redis coordination.
- Constructor accepts `{ redisUrl, instanceId, hashRing: [...], localRoomManager }`.
- `join(clientId, roomId, ws)` registers membership in Redis (hash: `room:{roomId}:members` → `Set<instanceId:clientId>`) and locally.
- `leave(clientId, roomId)` removes from Redis and locally.
- `broadcast(roomId, message, excludeClientId)` on non-primary instances publishes to Redis channel `room:{roomId}:broadcast`; primary instance receives, sequences, deduplicates, stores in ring buffer, then re-publishes to all instances (or direct-delivers to local members and publishes to other instances).
- Redis pub/sub messages use a compact binary format (MessagePack or custom) — not JSON — to minimize bandwidth.
- Instance heartbeats: `SET instance:{instanceId}:heartbeat <timestamp> EX 10` refreshed every 3s.
- Room primary assignment: `hash(roomId) % hashRing.length` → `instanceId`. Hash ring stored in Redis `config:hash-ring` as JSON array of instance IDs, updated on membership changes.
- On instance failure (heartbeat TTL expiry), surviving instances detect via Redis keyspace notifications or polling, re-assign primaries for affected rooms, and publish `room:{roomId}:rebalance` events.
- `getClientRooms(clientId)` queries Redis for the client's full membership set (stored as `client:{clientId}:rooms` → `Set<roomId>`).

## Constraints
- Do not modify `validator.js`, `auth.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`.
- Do not modify existing test files. New test files for distributed behavior are required.
- Add `ioredis` to `package.json` (only new dependency allowed).
- Redis must be optional: when `REDIS_URL` is not set, the gateway operates in single-instance mode (current behavior).
- The local `RoomManager` API (`join`, `leave`, `broadcast`, `disconnect`, `getClientRooms`, `getRoomSize`, `stats`) must remain unchanged for callers.
- Message ordering: within a room, all subscribers must receive messages in the same sequence order (primary assigns sequence numbers).
- Deduplication (issue 6) must work globally — a duplicate sent to any instance is dropped.
- Maximum added latency for cross-instance broadcast: 50ms p99 over localhost Redis.
- Memory overhead per instance: ≤ 50MB for 100K rooms, 1M clients.

## Acceptance criteria
- [ ] `npm install` adds `ioredis` without breaking existing tests
- [ ] Single-instance mode (no `REDIS_URL`) passes all existing tests unchanged
- [ ] Multi-instance test: 3 gateway processes + Redis, client on instance 1 joins room, client on instance 2 joins same room, client on instance 1 publishes → client on instance 2 receives within 100ms
- [ ] Rebalance test: kill instance 1, verify instance 2 becomes primary for its rooms, clients on instance 1 reconnect to instance 2 and resume receiving broadcasts
- [ ] Membership sync test: client joins on instance 1, `getClientRooms` on instance 2 returns the room within 50ms
- [ ] Sequence ordering test: rapid publishes from multiple instances → all subscribers receive in same sequence order
- [ ] Deduplication test: same location_update sent to instance 1 and instance 2 within dedup window → only one broadcast delivered
- [ ] Backpressure test: slow consumer on instance 3 does not block primary's event loop (measured via event loop lag)
- [ ] `npm run lint` passes
- [ ] New test file: `tests/distributed-room-manager.test.js` with all above scenarios

## Out of scope
- Changes to `server.js` message routing logic (the `broadcast` call site stays the same — it calls `rooms.broadcast()`).
- Redis Cluster / Sentinel support (single Redis instance is sufficient for this issue).
- Persistent room state across full cluster restart (in-memory Redis is acceptable).
- Geo-distributed deployments (single data center assumed).
- Client-side SDK changes.

## Hints and references
- Consistent hashing: use `jump-hash` algorithm (Google's Jump Consistent Hash) — O(1), no ring state needed, just `instanceCount`. Or `hash-ring` npm package if you prefer Ketama.
- Redis pub/sub: `SUBSCRIBE room:fleet-alpha:broadcast` on each instance. Publish from primary: `PUBLISH room:fleet-alpha:broadcast <binary-payload>`.
- For binary encoding: `msgpackr` is fast and zero-copy, or implement a minimal TLV format (type: 1 byte, length: 2 bytes, value: bytes).
- Instance ID: generate UUID on startup, or use `HOSTNAME` + `PID` in container environments.
- Keyspace notifications for failure detection: `CONFIG SET notify-keyspace-events Ex` then `PSUBSCRIBE __keyevent@0__:expired` to catch `instance:*:heartbeat` expiry.
- For connection migration: store `client:{clientId}:rooms` as a Redis Set with TTL (e.g., 1 hour). On reconnect to new instance, `SMEMBERS client:{clientId}:rooms` → re-join each room locally.
- The `RoomManager` changes are additive — wrap the existing class, don't rewrite it. Use composition: `DistributedRoomManager` has a `localRoomManager` and delegates local operations.