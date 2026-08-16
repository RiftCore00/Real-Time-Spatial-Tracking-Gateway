## Title
Implement adaptive message compression with per-client capability negotiation, dictionary-based delta encoding for location streams, and zero-copy buffer management

## Difficulty
10/10 — Expert. Estimated effort: 4–6 days for a senior engineer.

## Context
The `README.md` mentions "Pluggable serialization — JSON by default; MessagePack or Protocol Buffers can be substituted for bandwidth-constrained links" (line 68). However, no compression or serialization abstraction exists. A fleet of 10,000 vehicles sending location updates every second at ~200 bytes JSON each generates 2 MB/s outbound — 170 GB/day. Over cellular networks with metered data plans, this is prohibitively expensive. Delta encoding (transmitting only changes from last known position) can reduce payload by 60–80% for moving assets.

## Problem statement
Design and implement an adaptive compression layer that:

1. **Capability negotiation**: On connect, client sends `compression: ["gzip", "deflate", "zstd", "delta-json"]` in query params. Server responds with selected algorithm in `room_joined` or first message.

2. **Delta encoding for location streams**: For `location_update` messages, encode as `{ seq, baseSeq, latDelta, lonDelta, altDelta?, speedDelta?, headingDelta? }` where deltas are varint-encoded (LEB128) differences from the message at `baseSeq` (client's highest ACKed seq from issue 16). Reconstruct absolute position on client by applying deltas sequentially.

3. **Dictionary compression**: Build a shared string dictionary for repeated field names (`latitude`, `longitude`, `roomId`, etc.) and common values (room IDs, client IDs). Transmit dictionary indices instead of strings.

4. **Per-client algorithm selection**: Server maintains `Map<clientId, CompressionContext>` with `{ algorithm, dictionary, lastAbsolutePosition, lastSeq }`. Slow clients get aggressive compression; fast clients get minimal overhead.

5. **Zero-copy buffer management**: Use `Buffer` pools and `Uint8Array` views to avoid allocations in hot path. Compression runs on `setImmediate` batches (issue 4 backpressure).

6. **Fallback and transparency**: If client doesn't support compression, server sends plain JSON. Compression is opt-in per message type — `join_room`/`leave_room` stay JSON (infrequent), `location_update` compressed.

## Current behavior
- All messages JSON.stringify() → ws.send() — no compression, no delta encoding.
- No capability negotiation.
- `room-manager.js` broadcasts raw JSON strings.

## Required behavior
- New module `src/compression.js` exporting `CompressionManager` class.
- `CompressionManager` constructor: `{ algorithms: ["gzip", "deflate", "zstd", "delta-json"], dictionarySize: 4096, deltaEnabled: true }`.
- `compressionManager.negotiate(clientId, clientAlgorithms)` returns selected algorithm.
- `compressionManager.compress(clientId, message)` returns `Buffer` (compressed) or string (uncompressed).
- `compressionManager.decompress(clientId, buffer)` returns parsed object (for inbound if client compresses).
- Delta encoding: `encodeDelta(base: LocationUpdate, current: LocationUpdate)` → `{ baseSeq, latDelta: number, lonDelta: number, ... }` using 0.000001° precision (11 cm) × 1e6 = integer microdegrees.
- Dictionary: LRU map of string → index, shared across all clients per room. Transmit as `{"d": ["latitude", "longitude", ...], "m": [[idx, val], ...]}`.
- Metrics: `compression_ratio{algorithm="delta-json"}`, `compression_latency_ms`.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`.
- Do not modify existing test files. New test files required.
- Add `zstd` (or `zstd-codec`) to `package.json` — only new dependency allowed.
- Delta decoding must be exact — no floating point drift. Use integer microdegrees.
- Compression must not add >1ms latency per message p99.
- Dictionary must be bounded (max 4096 entries) and evicted LRU.
- Works with issue 13 protocol versioning — compression is a per-version serializer option.

## Acceptance criteria
- [ ] Client negotiates `delta-json`, sends location update → server decodes correctly
- [ ] Delta encoding reduces `location_update` size by ≥ 60% for moving vehicles
- [ ] Dictionary compression reduces repeated room IDs/client IDs by ≥ 80%
- [ ] gzip/deflate/zstd fallback works for clients not supporting delta
- [ ] Zero-copy: no `Buffer.concat` in hot path, uses pre-allocated pools
- [ ] Metrics show compression ratio and latency
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/compression.test.js` with unit tests for each algorithm, delta encoding round-trip, dictionary encoding

## Out of scope
- Client-side decompression implementation (protocol spec only).
- Compression for inbound client messages (optional, server can accept compressed).
- Dynamic dictionary synchronization across gateway instances (issue 11) — local dictionary per instance.
- Custom binary protocol — delta-json is JSON-compatible for debugging.

## Hints and references
- LEB128 varint encoding for deltas:
  ```js
  function encodeVarint(n) { const buf = []; while (n >= 0x80) { buf.push((n & 0x7f) | 0x80); n >>= 7; } buf.push(n); return Buffer.from(buf); }
  function decodeVarint(buf, offset) { let n = 0, shift = 0, b; do { b = buf[offset++]; n |= (b & 0x7f) << shift; shift += 7; } while (b & 0x80); return { value: n, offset }; }
  ```
- Microdegree precision: `Math.round(lat * 1e6)` — fits in 32-bit signed int (±90° × 1e6 = ±90M < 2^31).
- Delta base: use client's `highestAckedSeq` (issue 16) so client can reconstruct from known state.
- `zstd` in Node: `const zstd = require('zstd-codec'); zstd.compress(buf, 3)`.
- Integration: in `server.js` message handler, after validation, `const compressed = compressionManager.compress(clientId, msg); ws.send(compressed);`.