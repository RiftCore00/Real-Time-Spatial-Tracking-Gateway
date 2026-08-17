## Title
Implement protocol versioning with schema registry, forward/backward compatibility, and zero-downtime schema migration for WebSocket message contracts

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The current protocol uses a single Zod schema (`messageSchema` in `src/validator.js` lines 27–44) with no version field. All clients and the server must share the exact same schema. In a production fleet-tracking deployment with 50,000+ mobile devices, app updates roll out over weeks — old app versions (protocol v1) and new versions (protocol v2) must coexist. The server must accept and correctly process messages from all supported protocol versions, and must encode outbound messages in the version each client understands.

The `README.md` describes "Pluggable serialization — JSON by default; MessagePack or Protocol Buffers can be substituted" (line 68) but there is no serialization abstraction, no version negotiation, and no schema evolution strategy. Adding a required field (e.g., `heading` to `location_update`) breaks old clients. Removing a field breaks new clients that expect it.

## Problem statement
Design and implement a protocol versioning and schema evolution system that:

1. **Protocol version negotiation**: On WebSocket handshake, client sends `protocol_version` in query string (e.g., `?token=...&protocol_version=2`). Server responds with `server_protocol_version` in the first message (or close frame if unsupported). Supported versions: `minProtocolVersion` to `maxProtocolVersion` (configurable).

2. **Schema registry**: Define schemas as versioned objects, not a single discriminated union. Each version has its own Zod schema for inbound messages and a serializer for outbound messages. Schemas are registered at startup: `registry.register(v1Schema, v1Serializer)`, `registry.register(v2Schema, v2Serializer)`.

3. **Forward compatibility (server reads old clients)**: When a v1 client sends a `location_update` without the new `heading` field, the server accepts it, fills in a default (e.g., `heading: null`), and processes it as a v2 message internally.

4. **Backward compatibility (server writes for old clients)**: When broadcasting to a v1 client, the server serializes the internal v2 message using the v1 serializer, which omits `heading` and any other v2-only fields.

5. **Internal canonical representation**: The server operates on a single "latest" internal schema (vN). Inbound messages are up-converted (v1→vN, v2→vN, ...). Outbound messages are down-converted (vN→v1, vN→v2, ...) per client's negotiated version.

6. **Schema migration functions**: For each version pair (v1→v2, v2→v3, ...), provide pure functions `upConvert_v1_to_v2(msg)`, `downConvert_v2_to_v1(msg)`. These are composed for multi-version jumps.

7. **Deprecation and sunset**: Track usage per protocol version. Log warning when a version hasn't been seen in 30 days. Configurable `deprecateAfterDays` and `sunsetAfterDays` — sunset versions are rejected with close code 4002 "Protocol version deprecated".

8. **Serialization abstraction**: Extract serialization (JSON, MessagePack, Protobuf) into a `Serializer` interface. The protocol version selects both the schema and the serializer. This enables the "Pluggable serialization" claim in the README.

## Current behavior
- `src/validator.js`: single `messageSchema` discriminated union, no version field.
- `src/server.js`: no protocol version handling. All messages validated against the single schema.
- `src/room-manager.js`: broadcasts raw JSON strings — no per-client serialization.
- No schema registry, no migration functions, no version tracking.

## Required behavior
- New module `src/protocol-registry.js` exporting `ProtocolRegistry` class.
- `ProtocolRegistry` constructor: `{ minVersion: 1, maxVersion: 3, defaultVersion: 3, deprecateAfterDays: 30, sunsetAfterDays: 90 }`.
- `registry.register(version, { inboundSchema: ZodSchema, outboundSerializer: Serializer, upConvert?: (msg) => msg, downConvert?: (msg) => msg })`.
- `registry.getInboundSchema(version)` returns Zod schema for that version.
- `registry.getOutboundSerializer(version)` returns Serializer for that version.
- `registry.upConvert(version, msg)` composes up-conversions from `version` to `maxVersion`.
- `registry.downConvert(version, msg)` composes down-conversions from `maxVersion` to `version`.
- `Serializer` interface: `{ serialize(obj): string|Buffer, deserialize(data): obj, contentType: string }`.
- Built-in `JsonSerializer`, `MessagePackSerializer` (using `msgpackr`), `ProtobufSerializer` (using `@protobufjs/aspromise` + compiled protos).
- `src/validator.js` refactored to use `ProtocolRegistry` — `validateMessage(raw, clientProtocolVersion)` returns `{ ok, data, version }` where `data` is in canonical (maxVersion) form.
- `src/room-manager.js` `broadcast()` accepts optional `serializerMap: Map<clientId, Serializer>` — serializes per-client. Or: `broadcast()` returns canonical messages, and `server.js` serializes per-client before `ws.send()`.
- Connection handshake in `server.js`: read `protocol_version` from query, validate against registry, store on `ws._protocolVersion`.
- Metrics: `protocol_version_usage{version="1"} 1234` counter incremented on each message.

## Constraints
- Do not modify `auth.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`.
- Do not modify existing test files. New test files required.
- Add `msgpackr` and `@protobufjs/aspromise` to `package.json` — only new dependencies allowed.
- Zod schemas for each version must be defined in a single file (`src/protocol-schemas.js`) for auditability.
- The canonical internal schema (maxVersion) must be the source of truth — all business logic (room manager, geofence engine, storage) operates on canonical messages.
- Up/down conversion functions must be pure, deterministic, and side-effect-free.
- Adding a new protocol version must not require changes to `server.js`, `room-manager.js`, or `geofence-engine.js` — only register the new schema/converters.
- Latency overhead: version negotiation + up/down conversion must add <0.5ms per message p99.
- Memory: registry holds all versions simultaneously — <10MB for 10 versions.

## Acceptance criteria
- [ ] `ProtocolRegistry` registers v1, v2, v3 schemas with up/down converters
- [ ] v1 client (no `heading` field) connects with `protocol_version=1`, sends `location_update` → server accepts, up-converts to v3 with `heading: null`
- [ ] v3 client connects with `protocol_version=3`, receives broadcast → server down-converts to v3 (no change), serializes with v3 serializer
- [ ] v1 client receives broadcast from v3 publisher → server down-converts v3→v1 (strips `heading`), serializes with v1 serializer
- [ ] v2 client (has `heading` but not `accuracy`) connects → up-converts v2→v3 (adds `accuracy: null`), down-converts v3→v2 (strips `accuracy`)
- [ ] Sunset version (v0) rejected with close code 4002 and message "Protocol version deprecated"
- [ ] MessagePack serializer: `serialize({a:1})` → `Buffer`, `deserialize(buf)` → `{a:1}`
- [ ] Protobuf serializer: requires pre-compiled `.proto` files — provide example `location.proto` and generated JS
- [ ] Metrics counter `protocol_version_usage` increments per message per version
- [ ] `npm run lint` passes
- [ ] All existing tests pass (they use default version — ensure backward compatibility)
- [ ] New test file: `tests/protocol-versioning.test.js` with all above scenarios + fuzz testing of up/down conversion round-trips

## Out of scope
- Automatic schema generation from TypeScript types or Protobuf definitions.
- Schema validation for outbound messages (trusted path).
- Client-side SDK implementation.
- Protocol version negotiation via WebSocket subprotocols (Sec-WebSocket-Protocol) — query string is sufficient.
- Encryption/framing changes — only payload serialization changes.

## Hints and references
- Schema composition pattern: each version's upConvert only knows how to go to the NEXT version. `upConvert_v1_to_v3 = msg => upConvert_v2_to_v3(upConvert_v1_to_v2(msg))`. Store converters as `Map<fromVersion, Map<toVersion, fn>>` or just adjacent pairs and compose at runtime.
- Zod schema for v1 (current):
  ```js
  const v1LocationPayload = z.object({
    latitude: z.number().min(-90).max(90),
    longitude: z.number().min(-180).max(180),
    altitude: z.number().optional(),
    accuracy: z.number().min(0).optional(),
    speed: z.number().min(0).optional(),
    timestamp: z.string().datetime().optional(),
  });
  ```
  v2 adds `heading: z.number().min(0).max(360).optional()`. v3 adds `satellites: z.number().int().min(0).optional()`.
- Serializer interface:
  ```js
  class JsonSerializer {
    contentType = "application/json";
    serialize(obj) { return JSON.stringify(obj); }
    deserialize(data) { return JSON.parse(data.toString()); }
  }
  ```
- For Protobuf: define `location.proto` with `message LocationUpdate { double lat = 1; double lon = 2; ... }`, compile with `pbjs -t static-module -w es6 -o location_pb.js location.proto`, then use generated code in `ProtobufSerializer`.
- Version tracking: `ws._protocolVersion` set in `server.js` connection handler. `registry.trackUsage(version)` called on each message.
- Deprecation check: periodic timer (daily) iterates `registry.usageCounts`, logs warning if `lastSeen < now - deprecateAfterDays`.