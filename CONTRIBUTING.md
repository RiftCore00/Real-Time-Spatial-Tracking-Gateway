# Contributing to Real-Time Spatial Tracking Gateway

Thank you for your interest in contributing! This document outlines the development workflow, project structure, and guidelines.

## Project Structure

```
├── src/
│   ├── index.js         # Entry point, signal handling
│   ├── server.js        # WebSocket server, message routing
│   ├── room-manager.js  # Room-based subscription management
│   ├── validator.js     # Zod-based message validation
│   ├── auth.js          # JWT connection authentication
│   └── logger.js        # Structured JSON logging
├── tests/
│   ├── room-manager.test.js
│   ├── validator.test.js
│   └── cleanup.test.js
├── .github/workflows/   # CI pipeline
└── README.md
```

## Development Setup

```bash
git clone https://github.com/RiftCore00/Real-Time-Spatial-Tracking-Gateway.git
cd Real-Time-Spatial-Tracking-Gateway
npm install
cp .env.example .env
npm test
```

## Coding Standards

### JavaScript Style

- Run `npm run lint` before committing
- Use ES module syntax (`import`/`export`)
- Follow the existing code style (2-space indent, trailing semicolons)

### Testing

Every new feature must include tests. Tests use **Vitest**. Key test areas:

- **Validation**: Malformed, missing, and out-of-range payloads
- **Room lifecycle**: Join, leave, broadcast membership boundaries
- **Cleanup**: Disconnect teardown, memory leak prevention

Run tests with:

```bash
npm test          # Run once
npm run test:watch  # Watch mode
npm run test:coverage  # With coverage report
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes with clear, descriptive commit messages.
3. Ensure tests pass (`npm test`) and lint is clean (`npm run lint`).
4. Open a pull request linking any related issues.
5. Ensure CI passes across all Node.js versions (18, 20, 22).

## Protocol

See [README.md](README.md#api--websocket-protocol) for the WebSocket message protocol.

---

## Adding a New Message Type

This section walks through adding a new WebSocket message type end-to-end. We'll use a fictional **ping** / **pong** exchange as the worked example.

### 1. Define the Zod schema (`src/validator.js`)

Add a payload schema for your new message, then add a variant to the `messageSchema` discriminated union and a size limit entry.

```js
// 1a. Payload schema (if your message carries data)
const pingSchema = z.object({
  seq: z.number().int().nonnegative(),
});

// 1b. Add to the discriminated union
const messageSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("location_update"), payload: locationPayloadSchema }),
  z.object({ type: z.literal("join_room"), ...joinRoomSchema.shape }),
  z.object({ type: z.literal("leave_room"), ...leaveRoomSchema.shape }),
  z.object({ type: z.literal("ping"), ...pingSchema.shape }),   // <-- new
]);

// 1c. Add a per-type size limit (bytes, optional but recommended)
const MESSAGE_SIZE_LIMITS = {
  location_update: 512,
  join_room: 256,
  leave_room: 256,
  ping: 128,            // <-- new
};
```

**Schema conventions:**
- Use `z.literal("your_type")` for the `type` discriminator value.
- If the message carries a named payload object, use `payload: yourPayloadSchema`.
- If the message uses flat keys (like `roomId`), spread the shape with `...schema.shape`.

### 2. Add the handler (`src/server.js`)

Add a `case` to the `switch (msg.type)` block inside `handleMessage`.

```js
switch (msg.type) {
  // ... existing cases ...

  case "ping": {
    safeSend(ws, { type: "pong", payload: { seq: msg.seq } }, clientId);
    break;
  }
}
```

**Response shapes:**
| Direction | Shape |
|-----------|-------|
| Server → Client (confirmation) | `{ type: "event_name", payload: { ... } }` |
| Server → Client (broadcast) | `{ type: "event_name", payload: { clientId, ...data } }` |
| Server → Client (error) | `{ type: "error", payload: { message: "..." } }` |

### 3. Add a validator unit test (`tests/validator.test.js`)

Test schema acceptance, rejection of bad payloads, and rejection of unknown types.

```js
describe("ping", () => {
  it("accepts a valid ping", () => {
    const result = validateMessage({ type: "ping", seq: 1 });
    expect(result.ok).toBe(true);
    expect(result.data.seq).toBe(1);
  });

  it("rejects ping with negative seq", () => {
    const result = validateMessage({ type: "ping", seq: -1 });
    expect(result.ok).toBe(false);
  });

  it("rejects ping with missing seq", () => {
    const result = validateMessage({ type: "ping" });
    expect(result.ok).toBe(false);
  });
});
```

### 4. Add a server integration test (`tests/server.test.js`)

Use the test helpers to exercise the full wire flow.

```js
it("responds with pong on ping", async () => {
  const ws = await connect(port, makeToken("ping-client"));
  const pending = nextMessages(ws, 1);
  ws.send(JSON.stringify({ type: "ping", seq: 42 }));
  const [msg] = await pending;
  expect(msg.type).toBe("pong");
  expect(msg.payload.seq).toBe(42);
  await closeAll(ws);
});
```

### 5. Run the tests

```bash
npm test          # All tests pass
npm run lint      # Lint is clean
```

---

## Test Helper Reference

The following helpers are defined at the top of `tests/server.test.js` and used in every server integration test.

| Helper | Signature | Description |
|--------|-----------|-------------|
| `connect` | `(port: number, token?: string) => Promise<WebSocket>` | Opens a WebSocket connection to `ws://localhost:{port}`. If a JWT `token` is provided, appends it as a `?token=` query parameter. Resolves once the connection is open. |
| `nextMessages` | `(ws: WebSocket, n?: number) => Promise<object[]>` | Collects the next `n` messages from the socket. Each message is JSON-parsed. Resolves with an array of parsed objects. Default `n` is 1. |
| `waitClose` | `(ws: WebSocket) => Promise<[code, reason]>` | Resolves when the socket closes, yielding the close code and reason. |
| `closeAll` | `(...sockets: WebSocket[]) => Promise<void>` | Closes every open socket passed as arguments and waits for each to emit its `close` event. |

**Usage pattern for a typical test:**

```js
it("does something", async () => {
  const ws = await connect(port, makeToken("client-id"));
  const pending = nextMessages(ws, 1);
  ws.send(JSON.stringify({ type: "some_type", ... }));
  const [msg] = await pending;
  expect(msg.type).toBe("expected_response");
  // ...
  await closeAll(ws);
});
```

---

## Worked Example: Ping / Pong End-to-End

Here is the complete set of changes required for a `ping` → `pong` message type.

**`src/validator.js`** — add schema, union variant, and size limit:

```js
const pingSchema = z.object({ seq: z.number().int().nonnegative() });

const messageSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("location_update"), payload: locationPayloadSchema }),
  z.object({ type: z.literal("join_room"), ...joinRoomSchema.shape }),
  z.object({ type: z.literal("leave_room"), ...leaveRoomSchema.shape }),
  z.object({ type: z.literal("ping"), ...pingSchema.shape }),
]);

const MESSAGE_SIZE_LIMITS = {
  location_update: 512,
  join_room: 256,
  leave_room: 256,
  ping: 128,
};
```

**`src/server.js`** — add handler case:

```js
case "ping": {
  safeSend(ws, { type: "pong", payload: { seq: msg.seq } }, clientId);
  break;
}
```

**`tests/validator.test.js`** — add tests:

```js
describe("ping", () => {
  it("accepts a valid ping", () => {
    const result = validateMessage({ type: "ping", seq: 1 });
    expect(result.ok).toBe(true);
  });

  it("rejects ping with negative seq", () => {
    const result = validateMessage({ type: "ping", seq: -1 });
    expect(result.ok).toBe(false);
  });
});
```

**`tests/server.test.js`** — add integration test:

```js
it("responds with pong on ping", async () => {
  const ws = await connect(port, makeToken("pinger"));
  const pending = nextMessages(ws, 1);
  ws.send(JSON.stringify({ type: "ping", seq: 7 }));
  const [msg] = await pending;
  expect(msg.type).toBe("pong");
  expect(msg.payload.seq).toBe(7);
  await closeAll(ws);
});
```
