# Real-Time Spatial Tracking Gateway

A backend WebSocket service built with **Node.js** that securely ingests, processes, and broadcasts live geolocation coordinates for mobile or web-based tracking platforms.

---

## Table of Contents

- [Overview](#overview)
- [Core Features](#core-features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [API / WebSocket Protocol](#api--websocket-protocol)
- [Testing](#testing)
- [Project Waves & Issue Tracking](#project-waves--issue-tracking)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The **Real-Time Spatial Tracking Gateway** is the central data-plane component of a real-time location system. It accepts persistent WebSocket connections from thousands of concurrent mobile or web clients, validates incoming GPS/GNSS payloads, and intelligently routes location updates to the appropriate subscribers using a room-based broadcasting model.

This service is designed for low-latency, high-throughput telemetry scenarios such as fleet tracking, asset monitoring, geofencing enforcement, and live mapping.

---

## Core Features

- **WebSocket Connection Lifecycle** – Full session management from handshake through heartbeat to graceful teardown, with automatic cleanup of orphaned connections.
- **Room-Based Broadcasting** – Clients join logical rooms (e.g., fleet ID, region, user group); location updates published to a room are fanned out to all members in real time.
- **Payload Validation** – Incoming coordinate data is validated against strict schemas before any processing or storage; malformed payloads are dropped immediately with a descriptive error response.
- **Secure Ingestion** – Supports token-based authentication at connection time; all communication runs over `wss://` in production.
- **Graceful Cleanup** – Disconnection events trigger full teardown of associated subscriptions, in-memory state, and room membership, ensuring a leak-free runtime.
- **Observability** – Structured logging for connection events, message throughput, validation failures, and errors, enabling operational dashboards.
- **Extensible Storage Adapter** – Plug in your preferred database (PostgreSQL, MongoDB, InfluxDB, etc.) for persisting historical tracks.

---

## Architecture

```
┌──────────────┐     wss://      ┌─────────────────────────────────────┐
│  Mobile/Web  │ ──────────────▶ │       Real-Time Tracking Gateway    │
│   Clients    │ ◀────────────── │                                     │
└──────────────┘                 │  ┌───────────┐   ┌──────────────┐  │
                                 │  │  Auth     │   │  Validator   │  │
                                 │  │  Middlewar │   │  (Zod/Joi)   │  │
                                 │  └─────┬─────┘   └──────┬───────┘  │
                                 │        │                 │          │
                                 │  ┌─────▼─────────────────▼───────┐  │
                                 │  │        Room Manager          │  │
                                 │  │  (Map<RoomId, Set<Client>>)  │  │
                                 │  └─────────────┬───────────────┘  │
                                 │                │                  │
                                 │  ┌─────────────▼───────────────┐  │
                                 │  │     Storage Adapter         │  │
                                 │  │  (Postgres / Mongo / etc.)  │  │
                                 │  └─────────────────────────────┘  │
                                 └─────────────────────────────────────┘
```

### Key Design Decisions

- **Single-process, async I/O** – Node.js event loop handles thousands of concurrent connections without thread-per-connection overhead.
- **Room map in memory** – Subscriptions live in a `Map<RoomId, Set<WebSocket>>` for O(1) broadcast dispatch; no external pub/sub dependency required for single-instance deployments.
- **Pluggable serialization** – JSON by default; MessagePack or Protocol Buffers can be substituted for bandwidth-constrained links.
- **Backpressure awareness** – The broadcaster respects `ws` backpressure signals to avoid overwhelming slow consumers.

---

## Getting Started

### Prerequisites

- **Node.js** >= 18.x (LTS recommended)
- **npm** >= 9.x or **yarn** >= 1.22
- A running **PostgreSQL** instance (or other supported database) if persistence is enabled

### Installation

```bash
git clone https://github.com/your-org/real-time-spatial-tracking-gateway.git
cd real-time-spatial-tracking-gateway
npm install
```

### Configuration

Copy the environment template and adjust values:

```bash
cp .env.example .env
```

| Variable                   | Default   | Description                                            |
|----------------------------|-----------|--------------------------------------------------------|
| `PORT`                     | `8080`    | WebSocket server listen port                           |
| `WS_HEARTBEAT_MS`          | `30000`   | Interval for connection pings (ms)                     |
| `MAX_PAYLOAD_BYTES`        | `1024`    | Maximum incoming message size (bytes)                  |
| `MAX_ROOM_SIZE`            | `500`     | Maximum number of clients in a room                    |
| `DATABASE_URL`             | —         | Connection string for storage adapter                  |
| `AUTH_SECRET`              | —         | Secret for token verification                          |
| `MAX_TIMESTAMP_SKEW_MS`    | `30000`   | Maximum allowed clock skew on location timestamps (ms) |
| `LOG_LEVEL`                | `info`    | Minimum log severity (`debug` \| `info` \| `warn` \| `error`) |
| `MAX_MESSAGES_PER_SECOND`  | `100`     | Per-client message rate limit (messages per second)    |
| `CONN_RATE_LIMIT`          | `30`      | Max new connections per IP address per minute          |
| `SESSION_ENCRYPTION_KEY`   | —         | 32-byte base64 key (or `{"v1":"…"}` key map) that enables session resumption |
| `SESSION_TTL_MS`           | `3600000` | Sliding TTL of a stored session (ms)                   |
| `INSTANCE_ID`              | uuid      | Identity published in the `GW_AFFINITY` cookie          |

#### Tuning rate limits for high-traffic deployments

For high-traffic scenarios such as dense fleet tracking or large-scale live events, tune the two rate-limit variables to match your expected load. Raise `MAX_MESSAGES_PER_SECOND` (e.g. `500`) if individual clients publish location updates at sub-second intervals. Raise `CONN_RATE_LIMIT` (e.g. `120`) when a large number of devices reconnect simultaneously — common after a server restart or network partition. Lower these values in consumer-facing deployments to reduce the blast radius of misbehaving or malicious clients. Both variables take effect at startup; restart the server after changing them.

### Running

```bash
# Development (with auto-reload)
npm run dev

# Production
npm start
```

The server will listen on the configured port and accept WebSocket connections at `/`.

---

## API / WebSocket Protocol

### Connection

```
wss://<host>:<port>/?token=<jwt>
```

Clients must provide a valid JWT as a query parameter. Connections without a valid token are rejected with a `4001` close code.

### Session Resumption

Set `SESSION_ENCRYPTION_KEY` to enable it; without the key the gateway behaves exactly as before.

The gateway seals each client's session state (rooms, sequence numbers, rate-limit window) with AES-256-GCM. The blob is the `session_id`. On reconnect the client presents it as a URL-encoded query parameter, or as the JWT `sid` claim:

```
wss://<host>:<port>/?token=<jwt>&session_id=<url-encoded blob>
```

The gateway restores the rooms and replies with `session_resumed`, carrying each room's saved `highestAckedSeq` / `highestReceivedSeq` plus the room's live `currentSeqPerRoom`. The client then sends the usual `reconnect` message for any room that shows a gap. A blob that fails to decrypt, has expired, or belongs to another identity is ignored and the connection continues as a new session.

Clients that cannot store a blob get the `GW_AFFINITY=<instanceId>` cookie on the handshake response: when they land back on the same instance, the session is restored from that instance's local cache.

A fresh blob also arrives with `server_shutting_down` on graceful shutdown, and with close code `4100` (in the close reason, or a preceding `migrate` frame) after `POST /admin/v1/clients/{clientId}/migrate`. `GET /metrics` reports the `session_resumption_total` counters.

### HTTP Health Check

```
GET /health
```

Returns `200 OK` with JSON `{"status":"OK"}` when the gateway is active. This endpoint is unauthenticated and intended for container health checks and orchestrator liveness probes.

### Message Format

All messages are JSON-encoded. Every message **must** contain a `type` field:

```json
{
  "type": "location_update",
  "payload": {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "altitude": 10.5,
    "accuracy": 3.0,
    "speed": 0.5,
    "timestamp": "2026-06-10T12:00:00Z"
  }
}
```

### Client-to-Server Messages

| Type               | Description                               |
|--------------------|-------------------------------------------|
| `join_room`        | Subscribe to a room (e.g., `fleet-alpha`) |
| `leave_room`       | Unsubscribe from a room                   |
| `location_update`  | Publish current coordinates               |

### Server-to-Client Messages

| Type               | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `location_update`  | Broadcast from another room member                                          |
| `room_joined`      | Confirmation of room join                                                    |
| `room_left`        | Confirmation of room leave                                                   |
| `error`            | Validation error or server error. Payload includes `message` and `code`.    |

#### Error codes

The `error` frame payload includes a machine-readable `code` field that clients should use to handle failures programmatically.

- `INVALID_JSON` — malformed JSON input.
- `VALIDATION_ERROR` — message schema or payload validation failed.
- `RATE_LIMITED` — request rejected due to connection or rate limits.
- `ROOM_FULL` — room join request rejected because the room is full.
- `AUTH_FAILED` — authentication failed.

---

## Testing

```bash
# Run all unit tests
npm test

# Run with coverage
npm run test:coverage
```

Tests are written with **Vitest** (or Jest — check `package.json`). Key test areas:

- **Validation** – Malformed, missing, and out-of-range coordinate payloads are rejected.
- **Room lifecycle** – Joining, leaving, and broadcasting respect membership boundaries.
- **Cleanup on disconnect** – Client teardown releases room subscriptions and in-memory references.

---

## Project Waves & Issue Tracking

This project is organized into waves that incrementally build toward a production-ready gateway. Each wave corresponds to a vertical slice of functionality.

### 🔵 Wave 1 — Core Connection Lifecycle (High · 200 pts)

Architect the WebSocket connection lifecycle to efficiently manage room-based broadcasting for multiple active geolocation streams.

- Define the `RoomManager` service with thread-safe join/leave/disconnect semantics.
- Implement heartbeat ping/pong to detect zombie connections.
- Ensure O(1) broadcast to all members of a room.
- Deliverable: all members of a room receive location updates from any publisher within that room.

### 🟡 Wave 2 — Payload Validation (Medium · 150 pts)

Implement strict payload validation to immediately drop malformed coordinate data before it reaches the database.

- Define a validation schema (latitude [-90, 90], longitude [-180, 180], valid ISO 8601 timestamp, etc.).
- Reject messages that violate the schema with a descriptive `error` frame.
- Log every validation failure with the reason and client identifier.
- Deliverable: no invalid coordinate is ever persisted or broadcast.

### 🟢 Wave 3 — Client Disconnection Cleanup (Trivial · 100 pts)

Write unit tests ensuring that client disconnection events properly clean up server memory and log the exit.

- Assert that a client's room subscriptions are removed on disconnect.
- Assert that the `RoomManager` no longer holds a reference to the disconnected client.
- Assert that a structured log entry is emitted with the client ID and disconnect reason.

---

## Contributing

1. Fork the repository.
2. Create a feature branch from `main`.
3. Commit changes with clear, descriptive messages.
4. Run the full test suite before opening a pull request.
5. Open a PR and link any related issues.

All contributions must follow the existing code style and include tests where applicable.

---

## License

[MIT](LICENSE)
