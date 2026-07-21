## Title
Implement connection draining with pending message flush for graceful shutdown — prevent data loss during deploys and SIGTERM

## Difficulty
9/10 — Expert. Estimated effort: 2–3 days for a senior engineer.

## Context
The current `shutdown()` function in `src/index.js` (lines 43–53) calls `wss.close()` and sets a 5-second force-exit timer. `wss.close()` stops accepting new connections but does **not** close existing client connections or wait for pending sends to complete. In a fleet-tracking deployment, at any given moment there may be hundreds of in-flight location broadcasts queued in WebSocket send buffers. When `wss.close()` is called, these pending bytes are silently dropped. The 5-second force exit then kills the process before clients can receive close frames.

This means every deploy (rolling update, Docker restart, Kubernetes pod eviction) causes:
1. **Silent data loss** of all pending location updates.
2. **No close frame sent** to clients — they must wait for TCP timeout (typically 30–120 seconds) to detect the disconnect, during which they appear as "connected" but receive nothing.
3. **No room membership cleanup events** — other clients in the same rooms don't know a member disconnected until the heartbeat reaper catches it.

For a fleet-tracking system, this means vehicles appear frozen on the map for up to 2 minutes after a deploy, and location history has gaps.

## Problem statement
Implement a multi-phase graceful shutdown that:

1. **Phase 1 — Stop accepting** (0–100ms): Close the HTTP upgrade listener so no new WebSocket connections are established. Existing connections remain open.
2. **Phase 2 — Notify clients** (100ms–500ms): Send a `{ type: "server_shutting_down", payload: { reconnectIn: N } }` message to all connected clients, giving them time to reconnect to another instance.
3. **Phase 3 — Drain pending sends** (500ms–4000ms): For each connected client, flush any buffered outgoing data. Use `ws.bufferedAmount === 0` as the drain signal. Clients that haven't drained within this window are terminated.
4. **Phase 4 — Close connections** (4000ms–5000ms): Send WebSocket close frames (code 1001 "Going Away") to all remaining clients. Wait for close acknowledgements.
5. **Phase 5 — Force exit** (>5000ms): If any clients are still connected, force `process.exit(1)`.

The shutdown must also emit structured log entries at each phase transition for observability.

## Current behavior
`src/index.js` lines 43–53:
```js
function shutdown(signal) {
  logger.info("Shutting down", { signal });
  wss.close(() => {
    logger.info("Server closed");
    process.exit(0);
  });
  setTimeout(() => {
    logger.error("Forced shutdown");
    process.exit(1);
  }, 5000);
}
```
- `wss.close()` stops accepting but doesn't close existing connections.
- No drain phase — pending sends are dropped.
- No client notification — clients don't know the server is shutting down.
- The 5-second timer fires unconditionally — even if clients have already disconnected.
- No structured log entries per phase.

## Required behavior
- `shutdown(wss, signal)` implements the 5-phase draining protocol.
- All connected clients receive a shutdown notification before connections are closed.
- Pending send buffers are drained with a bounded timeout.
- Clients receive proper close frames (code 1001).
- The process exits 0 on clean shutdown, 1 on forced.
- Structured log entries at each phase: "shutdown: stopping accept", "shutdown: notifying N clients", "shutdown: draining N clients", "shutdown: closing N clients", "shutdown: force exit".

## Constraints
- Do not change the `SIGTERM`/`SIGINT` signal handlers — they must still call `shutdown()`.
- Do not add new npm dependencies.
- Do not modify `server.js` — `shutdown()` receives the `wss` instance and operates on it.
- Do not modify existing test files. The existing `shutdown` tests in `index.test.js` must still pass (they test the current behavior with a mock WSS — ensure backward compatibility).
- The total shutdown timeline must not exceed 5 seconds (the existing force-exit budget).
- Must handle: clients that never respond to close frames, clients with broken TCP connections, clients that reconnect during shutdown.

## Acceptance criteria
- [ ] `index.test.js` existing tests pass: `shutdown()` calls `wss.close()` and `process.exit(0)`, force exit after 5s if not closed
- [ ] New test: `shutdown()` sends shutdown notification to all connected clients before closing
- [ ] New test: `shutdown()` waits for `bufferedAmount === 0` before closing each client
- [ ] New test: `shutdown()` sends close frame with code 1001
- [ ] New test: total shutdown time does not exceed 5 seconds even with unresponsive clients
- [ ] New test: structured log entries emitted at each phase transition
- [ ] `npm run lint` passes
- [ ] `npm test` passes (all suites)

## Out of scope
- Changes to `server.js`, `room-manager.js`, `auth.js`, `validator.js`, `rate-limiter.js`, `logger.js`.
- Implementing a health check endpoint (that is a separate concern).
- Zero-downtime deploy orchestration (that requires a load balancer and is outside this service's scope).
- Persisting pending messages to disk or a queue during shutdown.

## Hints and references
- `wss.clients` is a `Set<WebSocket>` of all connected clients — iterate it for the drain phase.
- `ws.bufferedAmount` (RFC 6455 §5.2) returns bytes queued. Poll it with `setInterval(100)` during the drain phase.
- WebSocket close code 1001 ("Going Away") is the standard code for server-initiated shutdown (RFC 6455 §7.4.1).
- The notification message `{ type: "server_shutting_down" }` is not part of the current protocol — you'll need to add it to the `messageSchema` in `validator.js`... **except** issue constraints say not to modify `validator.js`. Solution: send the notification as a raw JSON string bypassing validation (use `ws.send(JSON.stringify(...))` directly, not through the validated message path).
- Consider: what if `wss.close()` is called while a drain is in progress? The existing test expects `wss.close` to be called. Ensure both the drain AND the `wss.close()` happen.
