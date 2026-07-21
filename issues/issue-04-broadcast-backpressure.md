## Title
Implement backpressure-aware async broadcast with per-client drain queues to prevent event loop starvation in large rooms

## Difficulty
10/10 — Expert. Estimated effort: 4–5 days for a senior engineer.

## Context
`RoomManager.broadcast()` in `src/room-manager.js` (lines 97–115) serializes the message to JSON once, then iterates every member and calls `ws.send(data)` synchronously. In a fleet-tracking deployment, a single room (e.g., `fleet-alpha`) can contain 5,000 vehicles. Each vehicle publishes a location update every second. When vehicle X publishes, `broadcast()` must call `ws.send()` 4,999 times synchronously. At 10 updates/second across the fleet, that is 50,000 synchronous `ws.send()` calls per second — all blocking the Node.js event loop.

The `ws` library's `ws.send(data)` is not truly synchronous for TCP — it buffers into the kernel write buffer. When the kernel buffer fills (slow consumer), `ws.send()` internally queues the message. But the `RoomManager` has no awareness of this: it will keep pushing messages into the internal buffer of a slow client, growing memory without bound until the client is OOM-killed or the server runs out of memory.

There is no write backpressure detection, no per-client send queue with configurable high-water mark, and no mechanism to pause or drop messages for slow consumers. This is a correctness and stability issue, not just a performance optimization.

## Problem statement
Redesign `RoomManager.broadcast()` to be backpressure-aware and non-blocking, such that:

1. **Event loop is not blocked**: `broadcast()` must not iterate N clients synchronously. It must yield control back to the event loop periodically (e.g., every M sends).
2. **Slow consumers are detected**: When a client's internal send buffer exceeds a configurable high-water mark (e.g., 1MB), the client is flagged as slow. Subsequent broadcasts to that client are either queued with a bounded buffer or dropped (configurable policy).
3. **Slow consumers are eventually evicted**: If a slow client does not drain within a configurable timeout (e.g., 30 seconds), the connection is terminated.
4. **Message coalescing for location updates**: When a slow client is catching up, intermediate location updates can be coalesced — only the most recent location per client needs to be delivered. This requires the broadcast path to know the message type.
5. **Metrics are exposed**: The `RoomManager` must expose per-room and per-client send queue depths for observability.

## Current behavior
`src/room-manager.js` lines 97–115:
```js
broadcast(roomId, message, excludeClientId = null) {
  const room = this._rooms.get(roomId);
  if (!room) return;
  const data = typeof message === "string" ? message : JSON.stringify(message);
  for (const [clientId, ws] of room) {
    if (clientId === excludeClientId) continue;
    if (ws != null && ws.readyState === WebSocket.OPEN) {
      try { ws.send(data); } catch { }
    }
  }
}
```
Problems:
- Synchronous loop over all members — blocks event loop for O(N) sends.
- No backpressure detection — `ws.send()` can buffer indefinitely.
- No slow consumer handling — one slow client causes memory growth for the entire server.
- No message coalescing — a client receiving 100 location updates/sec behind a slow connection gets all 100, even though only the latest matters.

## Required behavior
- `broadcast()` must be non-blocking: it must process sends in batches (e.g., 100 per tick) using `setImmediate` or `queueMicrotask` batching.
- Each client connection must have an associated send queue with a configurable `highWaterMark` (bytes).
- When a client's send queue exceeds `highWaterMark`, the client is marked as "slow" and subsequent location_update broadcasts to that client coalesce — only the latest location is kept.
- A slow client that doesn't drain within `slowConsumerTimeout` ms is terminated via `ws.close(4000, "Slow consumer")`.
- `getRoomStats(roomId)` returns `{ memberCount, sendQueueDepths: { [clientId]: number }, slowConsumers: string[] }`.

## Constraints
- Do not change the `RoomManager` public API for `join`, `leave`, `disconnect`, `getClientRooms`, `getRoomSize` — existing callers must work unchanged.
- The new backpressure behavior must be opt-in via constructor options (defaulting to the current synchronous behavior for backward compatibility in tests).
- Do not add new npm dependencies.
- Do not modify any existing test file.
- The `ws` library's `bufferedAmount` property is available on WebSocket instances and returns the number of bytes queued in the outgoing buffer — use it for backpressure detection.
- Must not introduce memory leaks: slow consumer queues must be bounded and cleaned up on disconnect.

## Acceptance criteria
- [ ] `RoomManager` constructor accepts `{ backpressure: { enabled: boolean, highWaterMark: number, slowConsumerTimeout: number, batchSize: number } }` with sane defaults
- [ ] When `backpressure.enabled === false` (default), `broadcast()` behaves identically to current implementation
- [ ] When `backpressure.enabled === true`, `broadcast()` processes sends in batches of `batchSize` using `setImmediate` yielding between batches
- [ ] A client whose `ws.bufferedAmount > highWaterMark` is flagged as slow
- [ ] Slow clients receive only the latest location_update (message coalescing) — intermediate updates are dropped
- [ ] Slow clients are terminated after `slowConsumerTimeout` ms
- [ ] `getRoomStats(roomId)` returns correct member count, queue depths, and slow consumer list
- [ ] Disconnect cleanup removes slow consumer tracking state (no leak)
- [ ] Existing `room-manager.test.js` (11 tests), `room-manager-extended.test.js` (11 tests), `room-manager-additional.test.js` (8 tests) all pass unchanged
- [ ] Performance test: `broadcast()` to 10,000 mock clients completes without blocking the event loop for more than 50ms per batch (measured via `setImmediate` timing)

## Out of scope
- Changes to `server.js`, `validator.js`, `auth.js`, `rate-limiter.js`, `index.js`.
- Implementing a full pub/sub system with topic hierarchies.
- WebSocket compression (permessage-deflate).

## Hints and references
- `ws` WebSocket instances expose `bufferedAmount` (readonly) — the number of bytes of data queued for delivery. This is the canonical backpressure signal per the WebSocket spec (RFC 6455 §5.2).
- The `backpressureOptions` should be stored on the `RoomManager` instance, not as module-level globals, to support testing with different configurations.
- For message coalescing, maintain a `Map<string, object>` of latest pending messages per slow client. On each broadcast, overwrite the entry. A drain loop (triggered by `ws.on("drain")` or periodic timer) serializes and sends the latest entry.
- Consider: `ws.send()` returns `false` when the internal buffer is full (backpressure signal). But the `ws` library v8+ doesn't consistently expose this — check `bufferedAmount` instead.
