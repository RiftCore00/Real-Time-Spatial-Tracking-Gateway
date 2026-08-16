## Title
Implement connection migration and session resumption with zero-downtime client handoff, encrypted session state transfer, and consistent client identity across gateway instances

## Difficulty
10/10 — Expert. Estimated effort: 6–8 days for a senior engineer.

## Context
Issue 11 (distributed room state) enables multiple gateway instances to share room membership and broadcast across instances. However, it does not solve **connection migration**: when a client's WebSocket connection is terminated (load balancer rebalance, instance scale-down, network glitch, TLS cert rotation) and the client reconnects to a *different* gateway instance, the new instance must instantly restore the client's full session state — room subscriptions, sequence numbers, ACK state (issue 16), geofence inside-set (issue 12), rate limit counters, and protocol version — without the client re-sending `join_room` for each room or losing its place in the message stream.

The `README.md` targets "mobile or web-based tracking platforms" (line 23) where clients frequently switch networks (WiFi → cellular, tower handoff) and reconnect. Kubernetes HPA scales gateway pods up/down, causing connection draining and rebalancing. Without session resumption, each reconnection causes: (1) client re-joins all rooms (N round-trips), (2) replay from last known seq (may have gaps if client didn't persist), (3) rate limit counters reset (bursts allowed), (4) geofence state lost (re-evaluation from scratch).

## Problem statement
Design and implement a session resumption protocol that enables zero-downtime client handoff between gateway instances:

1. **Encrypted session state blob**: On each significant state change (room join/leave, sequence advance, ACK update, geofence state change, rate limit window shift), the gateway serializes the client's session state and stores it in Redis with TTL (default 1 hour). State includes:
   - `clientId`, `protocolVersion`, `authIdentity` (JWT claims or mTLS deviceId)
   - `rooms`: `[{ roomId, highestAckedSeq, highestReceivedSeq, geofenceInsideSet: [...] }]`
   - `rateLimitState`: `{ messageWindow: [...], connectionWindow: [...] }`
   - `metadata`: `{ ip, userAgent, connectedAt, lastActivityAt }`
   - Encrypted with AEAD (AES-256-GCM) using a per-deployment key (`SESSION_ENCRYPTION_KEY` env var, 32 bytes base64). Key rotation supported via key ID prefix in blob.

2. **Session resumption handshake**: On WebSocket connection, client sends `?token=...&session_id=<blob>` (or `session_id` in JWT `sid` claim). Server:
   - Decrypts blob, verifies integrity, checks TTL.
   - Validates `clientId` matches auth identity (JWT `sub` or mTLS deviceId).
   - Restores `RoomManager` memberships locally (calls `join()` for each room with restored `highestAckedSeq`).
   - Restores rate limiter windows.
   - Responds with `{ type: "session_resumed", rooms: [...], currentSeqPerRoom: [...] }`.
   - Client compares `currentSeqPerRoom` with its local `highestAckedSeq` — if gap, sends `reconnect` for each room.

3. **Proactive session push (optional optimization)**: Before draining a connection (issue 5 graceful shutdown, or instance scale-down), the gateway pushes the latest session state to Redis. Client receives `server_shutting_down` with `session_id` — can immediately reconnect to another instance with full state.

4. **Session affinity via load balancer cookie**: For clients that don't support `session_id` (legacy), use a sticky session cookie (`GW_AFFINITY=<instanceId>`) so reconnections land on the same instance. Instance reads local session cache (in-memory Map) instead of Redis.

5. **Cross-instance session migration API (admin)**: `POST /admin/v1/clients/{clientId}/migrate` — forces session state sync to Redis, then disconnects client with `session_id` in close frame reason. Client reconnects (load balancer routes to new instance) and resumes.

6. **Security**: Session blob MUST be encrypted. If decryption fails (key rotation, corruption, tampering), fall back to full re-authentication (treat as new connection). No sensitive data (JWT, coordinates) in plaintext in Redis.

7. **Metrics**: `session_resumption_total{result="success|decrypt_failed|expired|mismatch|new_session"}`, `session_state_size_bytes`.

## Current behavior
- Issue 11: distributed room state syncs membership via Redis, but no per-client session blob.
- `server.js`: on connect, generates new `clientId` (uuid), no session restoration.
- `room-manager.js`: `join()` creates new membership, no `highestAckedSeq` restoration.
- Rate limiters: fresh windows on each connection.
- No `session_id` concept in protocol.

## Required behavior
- New module `src/session-manager.js` exporting `SessionManager` class.
- `SessionManager` constructor: `{ redis, encryptionKey, ttlMs: 3600000, keyId: "v1" }`.
- `sessionManager.save(clientId, state)` — encrypts, stores in Redis `session:{clientId}` with TTL.
- `sessionManager.load(sessionId)` — decrypts, returns state or null.
- `sessionManager.delete(clientId)` — removes from Redis (on explicit logout).
- Encryption: `crypto.subtle` (Web Crypto API) or `node:crypto` `createCipheriv`/`createDecipheriv` with AES-256-GCM. Key derived from `SESSION_ENCRYPTION_KEY` via HKDF if needed.
- `server.js` connection handler:
  - Extract `session_id` from query or JWT `sid`.
  - If present: `state = await sessionManager.load(session_id)`. If valid: restore.
  - Else: check sticky cookie `GW_AFFINITY`. If matches local instance: load from local cache.
  - Else: new session.
- On state change (join, leave, broadcast ACK, geofence change, rate limit update): debounce `sessionManager.save()` (max once per 500ms per client).
- On graceful shutdown (issue 5): `sessionManager.save()` for all clients before closing connections.
- `validator.js` (issue 13): add `session_resumed` server-to-client message type.
- Admin API (issue 14): `POST /admin/v1/clients/{clientId}/migrate` triggers save + disconnect.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`.
- Do not modify existing test files. New test files required.
- Use only Node.js built-in `crypto` for encryption. No new npm dependencies.
- Redis key TTL must be refreshed on each save (sliding window).
- Session blob size must be < 16KB (Redis limit for reasonable performance). Compress with `zlib.deflateSync` before encryption if needed.
- Debounced save: use a `Map<clientId, NodeJS.Timeout>` to coalesce rapid state changes.
- The `SessionManager` must work in single-instance mode (no Redis) — falls back to in-memory Map with TTL cleanup.
- Client identity binding: `state.authIdentity` must match current connection's auth (JWT `sub` or mTLS deviceId). If mismatch → reject session, force new auth.
- Key rotation: `SESSION_ENCRYPTION_KEY` can be a JSON object `{ "v1": "base64key1", "v2": "base64key2" }`. Blob format: `v1.<base64iv>.<base64ciphertext>.<base64tag>`. On load, try each key until one works.

## Acceptance criteria
- [ ] `SessionManager` encrypts/decrypts session blobs with AES-256-GCM
- [ ] Client connects with valid `session_id` → `session_resumed` received with correct room list and sequence numbers
- [ ] Client connects with expired `session_id` (TTL exceeded) → treated as new session
- [ ] Client connects with corrupted `session_id` → treated as new session
- [ ] Client connects with `session_id` for different `clientId` (identity mismatch) → rejected, new session
- [ ] Room join/leave triggers debounced session save (verify Redis updated within 1s)
- [ ] Graceful shutdown saves all sessions before closing connections
- [ ] Sticky cookie fallback: client with `GW_AFFINITY=instance-1` reconnects to instance-1 → local cache restore (no Redis round-trip)
- [ ] Admin migrate API: `POST /admin/v1/clients/{clientId}/migrate` → session saved, client disconnected with `session_id` in close reason
- [ ] Key rotation: add `v2` key, new sessions use `v2`, old `v1` sessions still load
- [ ] Metrics: `session_resumption_total` counters increment correctly
- [ ] Session blob size < 16KB for 50 rooms with full state
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/session-manager.test.js` with unit tests for encryption, save/load, debouncing
- [ ] New test file: `tests/session-resumption-integration.test.js` with full handshake scenarios (requires Redis)

## Out of scope
- Client-side session persistence (localStorage, IndexedDB) — server manages state.
- Session sharing across different deployments (multi-region) — single Redis cluster assumed.
- Session revocation list (for logout everywhere) — `delete(clientId)` is sufficient.
- WebSocket session resumption (RFC 8441) — application-layer only.

## Hints and references
- AES-256-GCM with Node.js `crypto`:
  ```js
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  const blob = `v1.${iv.toString('base64')}.${ciphertext.toString('base64')}.${tag.toString('base64')}`;
  ```
  Decrypt: parse blob, `createDecipheriv`, `setAuthTag`, `update` + `final`.
- HKDF for key derivation: `crypto.hkdfSync('sha256', masterKey, '', 'session-key', 32)`.
- Debounce pattern:
  ```js
  const timers = new Map();
  function debouncedSave(clientId, state) {
    const existing = timers.get(clientId);
    if (existing) clearTimeout(existing);
    timers.set(clientId, setTimeout(() => {
      timers.delete(clientId);
      sessionManager.save(clientId, state);
    }, 500));
  }
  ```
- Session state structure:
  ```js
  {
    clientId: "abc",
    protocolVersion: 3,
    authIdentity: { sub: "device-123", iss: "fleet-auth", ... },
    rooms: [
      { roomId: "fleet-alpha", highestAckedSeq: 42, highestReceivedSeq: 45, geofenceInsideSet: ["fence-1", "fence-3"] }
    ],
    rateLimitState: { messageWindow: [ts1, ts2, ...], connectionWindow: [ts1, ...] },
    metadata: { ip: "10.0.0.1", userAgent: "FleetApp/2.3", connectedAt: 1234567890, lastActivityAt: 1234567900 }
  }
  ```
- Integration with issue 11: `DistributedRoomManager` already syncs membership to Redis. `SessionManager` adds the per-client sequence/ACK/geoface/rate-limit state. They can share the same Redis connection.
- For sticky cookie: `res.cookie('GW_AFFINITY', instanceId, { httpOnly: true, secure: true, sameSite: 'lax', maxAge: 3600000 })` on HTTP upgrade response (requires `http` server access — see issue 9).