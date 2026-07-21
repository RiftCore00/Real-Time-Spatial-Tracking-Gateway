## Title
Implement server-side message sequencing, bounded ring-buffer replay, and reconnection handshake for gap-free location delivery

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The current protocol has no message ordering, no sequence numbers, and no replay mechanism. When a client disconnects (network handoff, app backgrounding, TCP reset) and reconnects, it has no way to know what messages it missed. In a fleet-tracking deployment, this means:

1. A vehicle driving through a cell tower handoff loses 5–30 seconds of location history.
2. A dispatcher viewing the fleet map sees the vehicle "teleport" from the last known position to the current one, with no intermediate points.
3. Geofence enforcement is violated silently — the vehicle may have crossed a boundary during the gap, but the server never delivered the crossing event.
4. There is no deduplication — if a client sends the same location_update twice (e.g., retry after timeout), it's broadcast twice to all subscribers.

The `project.md` describes the system as targeting "geofencing enforcement" and "fleet tracking" — both require reliable, ordered, gap-free delivery as a core correctness property.

## Problem statement
Implement a message sequencing and replay subsystem that:

1. **Sequence numbering**: Every broadcast message gets a monotonically increasing sequence number per room. The sequence is a 64-bit integer (safe for `Number.MAX_SAFE_INTEGER` at 9,007,199,254,740,991 — more than enough for centuries of updates).
2. **Bounded ring buffer per room**: Each room maintains a ring buffer of the last N broadcast messages (configurable, default 1,000). The buffer stores `{ seq, payload, timestamp }`.
3. **Reconnection handshake**: When a client reconnects, it sends `{ type: "reconnect", roomId: string, lastSeq: number }`. The server responds with:
   - If `lastSeq` is within the buffer range: `{ type: "replay", roomId: string, messages: [...], currentSeq: number }` containing all missed messages.
   - If `lastSeq` is too old (buffer has rotated past it): `{ type: "replay_gap", roomId: string, fromSeq: number, currentSeq: number }` — the client knows it has an unrecoverable gap.
   - If `lastSeq === currentSeq`: `{ type: "replay_complete", roomId: string }` — no messages missed.
4. **Deduplication**: Broadcast deduplicates messages by `(roomId, clientId, timestamp)` within a 5-second TTL window. Duplicate location_updates from the same client with the same timestamp are silently dropped.
5. **Client-side tracking**: Each client tracks the last sequence number they received per room. This is maintained server-side (not client-reported for the initial implementation) by recording the last seq delivered to each `(clientId, roomId)` pair.

## Current behavior
- No sequence numbers on any message. `broadcast()` in `room-manager.js` sends raw payloads.
- No ring buffer. Messages are fire-and-forget.
- No reconnection protocol. The only message types are `join_room`, `leave_room`, `location_update`.
- No deduplication. Identical messages are broadcast multiple times.
- The `messageSchema` in `validator.js` is a discriminated union of three types. Adding new types requires modifying the schema.

## Required behavior
- Every `broadcast()` call increments a per-room sequence counter and stores the message in a bounded ring buffer.
- The reconnection handshake types (`reconnect`, `replay`, `replay_gap`, `replay_complete`) are added to the Zod schema in `validator.js`.
- Ring buffer size is configurable per `RoomManager` instance.
- Deduplication window is configurable (default 5 seconds).
- Existing `location_update`, `join_room`, `leave_room` behavior is unchanged.
- `getRoomSeq(roomId)` returns the current sequence number for a room.
- `getRingBuffer(roomId)` returns the contents of the ring buffer (for testing).

## Constraints
- Ring buffer memory per room must be bounded: `bufferSize × averageMessageSize` must not exceed a configurable `maxBufferBytes` (default 5MB).
- Sequence numbers must be gap-free within the buffer — if the buffer is full, the oldest message is evicted and the gap is detectable via `replay_gap`.
- Deduplication must not add per-client state that grows unboundedly — use a time-bounded LRU or similar structure with a hard size cap.
- Do not add new npm dependencies.
- The existing 30 passing test suites must continue to pass unchanged (the new features are additive).
- New message types must be added to `validator.js`'s `messageSchema` discriminated union.

## Acceptance criteria
- [ ] `RoomManager` constructor accepts `{ ringBufferSize: number, deduplicationWindowMs: number, maxBufferBytes: number }` with sane defaults
- [ ] `broadcast()` stores each message in the ring buffer with a monotonically increasing sequence number
- [ ] `getRoomSeq(roomId)` returns the current sequence number (0 if room has no broadcasts)
- [ ] `getRingBuffer(roomId)` returns an array of `{ seq, payload, timestamp }` in order
- [ ] Ring buffer is bounded: when full, oldest messages are evicted and `getRingBuffer()` length ≤ `ringBufferSize`
- [ ] `reconnect` message type is accepted by `validateMessage()` in `validator.js`
- [ ] Server responds to `reconnect` with correct `replay`, `replay_gap`, or `replay_complete`
- [ ] Deduplication: sending the same `location_update` twice within the dedup window results in only one broadcast
- [ ] Deduplication state does not grow beyond `maxDedupEntries` (configurable, default 10,000)
- [ ] `maxBufferBytes` is enforced: ring buffer memory ≤ configured limit
- [ ] All existing tests pass (room-manager, validator, server, integration, etc.)
- [ ] New tests: replay delivers correct missed messages, gap detection works, dedup drops duplicates, ring buffer eviction works

## Out of scope
- Client-side implementation (sequence tracking, replay requests) — this is server-only.
- Persistent storage of ring buffers (in-memory only for this issue).
- Exactly-once delivery semantics across network partitions (this requires distributed consensus, which is out of scope).
- Historical trail / breadcrumb storage in a database.

## Hints and references
- A ring buffer can be implemented as a fixed-size `Array` with a head pointer and modular arithmetic. `buffer[seq % bufferSize] = entry`. Eviction is automatic — old entries are overwritten.
- For the sequence number, use a `Map<string, number>` of `roomId → currentSeq`. Increment on each `broadcast()`. This is a simple counter, not a complex data structure.
- The deduplication key `(roomId, clientId, timestamp)` can be stored in a `Map<string, number>` where the key is `${roomId}:${clientId}:${timestamp}` and the value is `Date.now()`. Periodic cleanup removes entries older than `deduplicationWindowMs`. Use a `Set` with TTL-based expiry if you want O(1) lookup.
- For the `replay` response, serialize the ring buffer entries from `lastSeq + 1` to `currentSeq`. If `lastSeq < currentSeq - bufferSize`, the gap is unrecoverable.
- Consider: what if a client sends `reconnect` before `join_room`? The server should reject it with an error frame — the client must join a room first.
- The Zod schema addition for `reconnect` is straightforward: add a new variant to the discriminated union in `validator.js`. The response types (`replay`, `replay_gap`, `replay_complete`) are server-to-client only and don't need schema validation (they're never received from clients).
