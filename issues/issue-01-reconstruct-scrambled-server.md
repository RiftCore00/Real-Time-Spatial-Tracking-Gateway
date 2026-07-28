## Title
Reconstruct the fatally corrupted `server.js` message pipeline — duplicate exports, scrambled control flow, and undefined variable references

## Difficulty
10/10 — Expert. Estimated effort: 3–5 days for a senior engineer.

## Context
`src/server.js` is the central nervous system of the entire gateway. It is currently in a catastrophic broken state: it contains **two** `export function createServer()` declarations (lines 9 and 135), scrambled function bodies where variables are referenced before declaration, and references to at least three completely undefined identifiers (`rateLimiter`, `ipConnectionCount`, `req` inside `handleMessage`). Because of the duplicate export, **every test suite that imports `createServer` fails at parse time** with `SyntaxError: Identifier 'createServer' has already been declared`. This means 8 of 18 test suites (`server.test.js`, `binary-frames.test.js`, `conn-rate-limiter.test.js`, `max-connections.test.js`, `index.test.js`, `integration.test.js`, `message-size-limits.test.js`, `validator.test.js`) cannot even load, let alone run.

This is not a refactor. This is a reconstruction of a broken module using the surviving test expectations and the other working modules as behavioral contracts.

## Problem statement
Reconstruct `src/server.js` so that it exports exactly one `createServer` function which:

1. Binds a `WebSocketServer` on the configured port with `maxPayload`.
2. On each connection: extracts the IP from `req.socket.remoteAddress`, checks the per-IP connection rate limiter (`createConnRateLimiter`), parses the auth token from the query string, and calls `verifyConnection(token)`. If auth fails, closes with code 4001. If connection rate limit is exceeded, closes with code 4029.
3. Resolves the effective `clientId` from the auth result (`authResult.clientId ?? clientId` where `clientId` is the uuid generated per connection).
4. Sets up per-message rate limiting using `createRateLimiter` (imported from `./rate-limiter.js`), checked on every incoming message before validation.
5. Validates each incoming message via `validateMessage` from `./validator.js`. On validation failure, sends `{ type: "error", payload: { message: "<error>" } }` back to the client.
6. Routes validated messages through the room manager: `join_room`, `leave_room`, and `location_update` (fan-out to all rooms the client belongs to, excluding the sender).
7. On disconnect: calls `rooms.disconnect(actualClientId)` and decrements the per-IP connection count.
8. Sets up heartbeat ping/pong via `setupHeartbeat` that terminates zombie connections (`ws.isAlive === false`).
9. Returns `{ wss, rooms }` (not `ipConnectionCount`).

The scrambled code currently interleaves fragments of what was clearly a `handleConnection` function and a `handleMessage` function in the wrong order, with the `handleMessage` body referencing `req` which was only available in the connection handler scope.

## Current behavior
`src/server.js` has:
- **Line 9**: First `export function createServer({ port, heartbeatMs, maxPayloadBytes, connRateLimit } = {})` — creates WSS, rooms, and connRateLimiter, defines `safeSend` and a scrambled `handleMessage`, then falls through to undefined variable references.
- **Line 26**: `function handleMessage(ws, clientId, rooms, raw)` — but the body references `req` (line 29), `ip` (line 30), `connRateLimiter` (line 31), `url` (line 37), `token` (line 46), `authResult` (line 47), `rateLimiter` (line 84, never defined anywhere), and `actualClientId` (line 92, assigned after first use on line 84).
- **Line 135**: Second `export function createServer({ port, heartbeatMs, maxPayloadBytes } = {})` — a cleaner but incomplete implementation that wires up `wss.on("connection", ...)` and `setupHeartbeat` but doesn't handle auth, rate limiting, or message routing.
- **Line 151**: Returns `{ wss, rooms, ipConnectionCount }` where `ipConnectionCount` is never declared.

Additionally, `createRateLimiter` from `./rate-limiter.js` is never imported. The variable `rateLimiter` referenced on line 84 is undefined. The variable `ipConnectionCount` referenced on line 100 and returned on line 151 is never declared.

## Required behavior
- Exactly one `export function createServer()` that accepts `{ port, heartbeatMs, maxPayloadBytes, connRateLimit }`.
- Imports `createRateLimiter` from `./rate-limiter.js`.
- Implements the full connection lifecycle described in the problem statement.
- All 8 currently-failing test suites pass.
- No references to undefined variables.
- `safeSend` is defined and used for all outbound messages.

## Constraints
- Do not change the public API of `createServer` beyond adding the optional `connRateLimit` parameter (already tested).
- Do not change any file other than `src/server.js`.
- Do not add new npm dependencies.
- Do not modify any test file.
- The `handleMessage` function must be properly scoped — it should receive `ws`, `clientId`, `rooms`, `raw`, and the rate limiter instance (or a closure over it), but NOT `req`.
- Auth check (`verifyConnection`) must use the token extracted from `req.url` query params, not from `raw`.

## Acceptance criteria
- [ ] `src/server.js` exports exactly one `createServer` function
- [ ] No `SyntaxError` when importing `src/server.js`
- [ ] `npm run lint` passes with zero errors
- [ ] `npm test` passes: all 18 test suites green (166+ tests)
- [ ] `server.test.js`: connection with valid token accepted, missing token rejected with 4001, invalid JSON returns error frame, join_room returns room_joined, leave_room returns room_left, location_update broadcasts to room members, sender excluded from own broadcast, disconnect cleans up room membership
- [ ] `binary-frames.test.js`: binary Buffer frames accepted for join_room and location_update, invalid JSON in binary frame returns error
- [ ] `conn-rate-limiter.test.js`: connections exceeding per-IP rate limit rejected with 4029
- [ ] `max-connections.test.js`: connections exceeding per-IP max rejected with 4029

## Out of scope
- Changes to `auth.js`, `validator.js`, `room-manager.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, or any test file.
- Adding new message types or protocol features.
- Database integration.
- TLS or HTTP server setup.

## Hints and references
- The test file `tests/server.test.js` defines the exact behavioral contract: `connect()` helper builds `ws://localhost:${port}/?token=${token}`, `nextMessages()` collects N messages, `closeAll()` tears down sockets. Study these helpers to understand the expected protocol.
- The variable `clientId` should be generated with `uuid.v4()` (already imported but unused in the second `createServer`).
- The `rateLimiter` (per-message) should be a module-level `createRateLimiter()` instance, not per-connection, since the rate limit is per-client-id and the same client ID could theoretically reconnect.
- The `ipConnectionCount` tracking in the close handler (lines 98-108 of current file) is for a max-connections-per-IP feature. The test `max-connections.test.js` expects a `maxConnectionsPerIp` option. You must implement this as an in-process Map tracking active connection count per IP, incremented on connection and decremented on close.
