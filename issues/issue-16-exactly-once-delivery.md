## Title
Implement exactly-once delivery semantics with client acknowledgments, server-side deduplication, and idempotent replay for mission-critical geofence enforcement

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
Issue 6 implements server-side sequencing, ring-buffer replay, and broadcast deduplication — but it only guarantees **at-least-once** delivery from the server's perspective. The client receives messages with sequence numbers, but there is no acknowledgment mechanism. If a client processes a message (e.g., triggers a geofence entry alert) but crashes before persisting its last-seen sequence number, it will re-request replay on restart and re-process the same message — causing duplicate alerts, double-billing, or incorrect state.

The `README.md` targets "geofencing enforcement" (line 25) which requires **exactly-once** semantics: a geofence crossing must be processed exactly once, even across client crashes, network partitions, and server restarts. At-least-once with manual deduplication is insufficient — the client cannot reliably deduplicate because it doesn't know which messages it successfully processed vs. which it received but didn't persist.

## Problem statement
Design and implement an end-to-end exactly-once delivery protocol that:

1. **Client acknowledgments (ACKs)**: After processing a message, the client sends `{ type: "ack", roomId, seq }`. The server tracks the highest ACKed sequence per `(clientId, roomId)`.

2. **Server-side processed-set tracking**: The server maintains `Map<clientId, Map<roomId, number>>` of `highestAckedSeq`. On replay request (`reconnect` with `lastSeq`), the server only replays messages with `seq > highestAckedSeq` — not `seq > lastSeq`. This ensures messages the client acknowledged are never re-sent.

3. **Idempotent message processing**: Each message carries a globally unique `messageId` (UUID v7 — timestamp-ordered). The server's ring buffer stores `messageId`. Clients track processed `messageId`s in a local bounded set (e.g., LRU of 10K). Duplicate delivery (network retry, server re-broadcast) is detected by `messageId` and silently dropped by the client.

4. **Transactional outbox for server-side persistence**: When the server persists a location update to storage (issue 10), it must atomically: (a) write the event, (b) assign sequence number, (c) store in ring buffer. Use a database transaction or a transactional outbox pattern to ensure no message is sequenced but not persisted, or persisted but not sequenced.

5. **Server restart recovery**: On server restart, the `highestAckedSeq` per client must be recovered. Option A: persist ACKs to storage adapter (new `ack` table). Option B: clients re-send their `highestAckedSeq` on reconnect (included in `reconnect` payload). Option B is simpler and more robust — the client is the source of truth for what it processed.

6. **Flow control with ACK window**: The server must not send more than `W` unacknowledged messages per client (configurable, default 100). If the window is full, the server pauses sending to that client until ACKs arrive. This prevents overwhelming slow clients and bounds memory.

7. **Negative acknowledgments (NACKs) for corruption**: If a client detects a corrupted message (checksum mismatch, schema validation failure), it sends `{ type: "nack", roomId, seq, reason }`. Server logs, increments metric, and re-sends that specific message (bypassing the ACK window).

## Current behavior
- Issue 6: `reconnect` uses `lastSeq` (client-reported last received). No ACKs. No `messageId`. No processed-set tracking. Replay returns all messages `> lastSeq`.
- `room-manager.js`: `broadcast()` assigns sequence numbers but no `messageId`.
- `validator.js`: no `ack` or `nack` message types.
- No flow control based on unacknowledged messages.

## Required behavior
- `src/validator.js` (via issue 13 protocol versioning): add `ack` and `nack` message types to schema.
- `src/room-manager.js` (or new `DeliveryManager`):
  - `broadcast()` generates `messageId` (UUID v7: `crypto.randomUUID()` with timestamp prefix) and attaches to message envelope: `{ seq, messageId, payload, timestamp }`.
  - Tracks `unackedCount` per `(clientId, roomId)`. If `unackedCount >= ackWindowSize`, pauses delivery to that client.
  - On `ack` message: updates `highestAckedSeq`, decrements `unackedCount`, resumes delivery if paused.
  - On `nack` message: re-sends the specific `seq` (from ring buffer) to that client only.
  - `reconnect` payload extended: `{ type: "reconnect", roomId, lastSeq, highestAckedSeq }`. Server replays `max(lastSeq, highestAckedSeq) + 1` to `currentSeq`.
- `src/storage/adapter.js` (issue 10): new method `ackMessage(clientId, roomId, seq): Promise<void>` for Option A persistence. Or rely on client-reported `highestAckedSeq` (Option B).
- Client-side protocol (documented, not implemented): client maintains `processedMessageIds: LRUSet<messageId>` (max 10K). On message receipt: if `messageId in processedMessageIds` → drop. Else → process, add to set, send `ack`. On `reconnect`: send `highestAckedSeq` (max seq for which all prior messages are in `processedMessageIds`).

## Constraints
- Do not modify `auth.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`.
- Do not modify existing test files. New test files required.
- Add `uuid` v7 support — `crypto.randomUUID()` is v4. Implement UUID v7 (timestamp + random) or use `uuid` package v9+ which supports v7. Only new dependency if needed.
- The ACK window must be enforced per-client, not globally — one slow client must not block others.
- `unackedCount` tracking must be memory-bounded: on client disconnect, clean up. On server restart, client re-connects with `highestAckedSeq`.
- Ring buffer must store `messageId` alongside `seq` and `payload`.
- Latency overhead: ACK processing <0.1ms. Flow control check in `broadcast()` <0.05ms.
- The exactly-once guarantee is **per room per client**. Cross-room ordering is not guaranteed.

## Acceptance criteria
- [ ] `broadcast()` assigns UUID v7 `messageId` to each message
- [ ] Client sends `ack` for seq 5 → server updates `highestAckedSeq=5`, `unackedCount--`
- [ ] Client sends `reconnect` with `lastSeq=10, highestAckedSeq=8` → server replays seq 9, 10 (not 1-8)
- [ ] Client crashes after processing seq 5 but before persisting `highestAckedSeq` → on restart, sends `highestAckedSeq=4` → server replays 5, 6... → client detects duplicate via `messageId`, drops, sends `ack` for 5, 6...
- [ ] Flow control: `ackWindowSize=3`, server sends seq 1,2,3 → pauses. Client acks 1 → server sends 4. Client acks 2,3 → server sends 5,6.
- [ ] NACK: client sends `nack` for seq 7 → server re-sends seq 7 immediately (bypasses window)
- [ ] Ring buffer stores `{ seq, messageId, payload, timestamp }` — `getRingBuffer()` returns all fields
- [ ] Memory: 100K clients × 10 rooms × `ackWindowSize=100` → ≤ 50MB for tracking maps
- [ ] `npm run lint` passes
- [ ] All existing tests pass (ACKs are additive — clients that don't send ACKs still work with at-least-once)
- [ ] New test file: `tests/exactly-once-delivery.test.js` with all above scenarios + chaos testing (kill client, kill server, network partition simulation)

## Out of scope
- Client-side SDK implementation (protocol specification only).
- Distributed exactly-once across gateway instances (issue 11) — this issue assumes single-instance or that ACK state is synced via Redis.
- Persistent ACK storage (Option A) — Option B (client-reported) is sufficient for this issue.
- Message compression — `messageId` adds 36 bytes, acceptable.
- Dead letter queue for permanently unacknowledged messages — log and alert only.

## Hints and references
- UUID v7 format: 48-bit unix timestamp (ms) + 74 random bits + version/variant bits. Implementation:
  ```js
  function uuidv7() {
    const ts = Date.now();
    const tsBytes = Buffer.alloc(6);
    tsBytes.writeBigUInt64BE(BigInt(ts) << 16); // 48 bits at top
    const rand = crypto.randomBytes(10);
    rand[0] = (rand[0] & 0x0f) | 0x70; // version 7
    rand[1] = (rand[1] & 0x3f) | 0x80; // variant RFC4122
    return [...tsBytes.slice(2), ...rand].map(b => b.toString(16).padStart(2, '0')).join('').match(/.{8}/g).join('-');
  }
  ```
  Or use `uuid` package v9: `uuidv7()`.
- ACK window implementation: `Map<clientId, Map<roomId, { highestAckedSeq, unackedCount, paused }>>`. In `broadcast()`, check `if (state.unackedCount >= ackWindowSize) { state.paused = true; return; }` — skip this client, schedule a retry check when ACK arrives.
- For NACK re-send: `const msg = ringBuffer.find(e => e.seq === nackSeq); if (msg) sendToClient(clientId, msg);`
- Client `processedMessageIds`: LRU cache (e.g., `Map` + `Set` for O(1) eviction). On `messageId` check: if exists → drop. Else → add, if size > 10000 → delete oldest.
- Protocol versioning (issue 13): `ack`/`nack` are new in v2. v1 clients don't send ACKs — server treats them as `ackWindowSize = Infinity` (no flow control).
- Geofence enforcement exactly-once: the geofence event (`geofence_entry`) is a message like any other — gets `messageId`, client ACKs after persisting the alert. Duplicate delivery → duplicate `messageId` → dropped.