import http from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { URL } from "node:url";
import { v4 as uuid } from "uuid";
import jwt from "jsonwebtoken";
import { RoomManager } from "./room-manager.js";
import { SessionManager } from "./session-manager.js";
import { PredictiveEngine } from "./predictor.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createRateLimiter } from "./rate-limiter.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";
import { VALIDATION_ERROR } from "./errors.js";
import { createStorageAdapter } from "./storage.js";
import {
  baseEventType,
  createEventSourcing,
  AcknowledgeCommand,
  JoinRoomCommand,
  LeaveRoomCommand,
  NegativeAcknowledgeCommand,
  PublishLocationCommand,
  ResumeSessionCommand,
} from "./event-sourcing.js";

const AFFINITY_COOKIE = "GW_AFFINITY";
const AFFINITY_MAX_AGE_S = 3600;
const DEFAULT_SESSION_TTL_MS = 3600000;
const RATE_WINDOW_MS = 1000;
const MAX_LOCAL_SESSIONS = 10000;
const MAX_CLOSE_REASON_BYTES = 123;
const MIGRATE_CLOSE_CODE = 4100;
const MIGRATE_PATH = /^\/admin\/v1\/clients\/([^/]+)\/migrate$/;
const PROTOCOL_VERSION = 1;
const LAG_SAMPLE_MS = 1000;

/**
 * Reads one cookie out of a raw `Cookie` header.
 *
 * @param {unknown} header - Value of the `Cookie` request header.
 * @param {string} name - Cookie name to look for.
 * @returns {string|null} The value, or null when absent.
 */
function readCookie(header, name) {
  if (typeof header !== "string") return null;
  for (const pair of header.split(";")) {
    const eq = pair.indexOf("=");
    if (eq === -1) continue;
    if (pair.slice(0, eq).trim() === name) return pair.slice(eq + 1).trim();
  }
  return null;
}

/**
 * Reads the `sid` claim of an already-verified token. `auth.js` verified the
 * signature, so decoding without re-verification is safe here.
 *
 * @param {string|null|undefined} token
 * @returns {string|null} The session blob, or null when the claim is absent.
 */
function sessionIdFromToken(token) {
  if (typeof token !== "string" || token.length === 0) return null;
  let payload;
  try {
    payload = jwt.decode(token);
  } catch {
    return null;
  }
  const sid = payload && typeof payload === "object" ? payload.sid : null;
  return typeof sid === "string" && sid.length > 0 ? sid : null;
}

/**
 * Creates the co-located HTTP server (health checks, Prometheus metrics,
 * admin migration) and the WebSocket gateway on the same port.
 *
 * Session resumption is opt-in: it activates when `sessionManager` is injected
 * or when an encryption key is available, and stays completely inert otherwise.
 *
 * Predictive location modeling (issue #248): every accepted `location_update`
 * feeds a per-client Kalman filter; geofence pre-alerts and anomaly events are
 * broadcast to the client's rooms as new message types.
 *
 * @param {object} [options]
 * @param {number} [options.port] - TCP port to listen on. Defaults to 8080.
 * @param {number} [options.heartbeatMs] - Ping interval used to detect zombies. Defaults to 30000.
 * @param {number} [options.maxPayloadBytes] - Max WebSocket frame size. Defaults to 1024.
 * @param {number} [options.connRateLimit] - New connections allowed per IP per minute.
 * @param {number} [options.maxConnectionsPerIp] - Concurrent connections allowed per IP.
 * @param {number} [options.maxMessagesPerSecond] - Per-client message rate limit.
 * @param {number} [options.maxRoomSize] - Max members per room.
 * @param {number} [options.maxRoomsPerClient] - Rooms one client may join.
 * @param {number} [options.maxMembersPerRoom] - Alias kept for API compatibility.
 * @param {number} [options.maxRooms] - Total rooms.
 * @param {number} [options.ringBufferSize] - Replay entries kept per room.
 * @param {number} [options.deduplicationWindowMs] - TTL of a `location_update` dedup key.
 * @param {number} [options.maxBufferBytes] - Byte ceiling for one room's replay buffer.
 * @param {number} [options.maxDedupEntries] - Max dedup keys retained.
 * @param {number} [options.ackWindowSize] - Replay window for `reconnect`.
 * @param {SessionManager} [options.sessionManager] - Pre-built manager; takes precedence over `sessionEncryptionKey`.
 * @param {string|object} [options.sessionEncryptionKey] - Key (or key map) used to build a manager.
 *   Falls back to `SESSION_ENCRYPTION_KEY`.
 * @param {number} [options.sessionTtlMs] - Session TTL. Falls back to `SESSION_TTL_MS`, then 1 hour.
 * @param {string} [options.instanceId] - Value published in the `GW_AFFINITY` cookie.
 *   Falls back to `INSTANCE_ID`, then a uuid.
 * @param {object} [options.redis] - node-redis v4 style client handed to a self-built manager.
 * @returns {{ wss: WebSocketServer, server: http.Server, httpServer: http.Server, rooms: RoomManager, ipConnectionCount: Map<string, number>, rateLimiter: object, metrics: object, markShuttingDown: () => void, sessionManager: SessionManager|null, instanceId: string, saveAllSessions: () => Promise<Map<string, string>>, predictor: PredictiveEngine }}
 */
export function createServer({
  port,
  heartbeatMs,
  maxPayloadBytes,
  connRateLimit,
  maxConnectionsPerIp,
  maxRoomSize,
  maxRoomsPerClient,
  maxMembersPerRoom: _maxMembersPerRoom,
  maxRooms,
  ringBufferSize: _ringBufferSize,
  deduplicationWindowMs: _deduplicationWindowMs,
  maxBufferBytes: _maxBufferBytes,
  maxDedupEntries: _maxDedupEntries,
  ackWindowSize: _ackWindowSize,
  maxMessagesPerSecond,
  sessionManager,
  sessionEncryptionKey,
  sessionTtlMs,
  instanceId,
  redis,
  storageAdapter,
  eventSourcing,
} = {}) {
  let isShuttingDown = false;
  let isReady = true;

  const metrics = {
    messages: { location_update: 0, join_room: 0, leave_room: 0, ack: 0, nack: 0 },
    rateLimitRejections: { connection: 0 },
    authFailures: 0,
    eventLoopLagMs: 0,
  };

  const sessionKey = sessionEncryptionKey ?? process.env.SESSION_ENCRYPTION_KEY ?? null;
  const sessionTtl = sessionTtlMs ?? (Number(process.env.SESSION_TTL_MS) || DEFAULT_SESSION_TTL_MS);
  const ownsSessions = sessionManager == null && sessionKey != null;
  /** @type {import("./session-manager.js").SessionManager|null} */
  const sessions = ownsSessions
    ? new SessionManager({
        redis: redis ?? null,
        encryptionKey: sessionKey,
        ttlMs: sessionTtl,
        logger,
      })
    : (sessionManager ?? null);
  const resolvedInstanceId = instanceId ?? process.env.INSTANCE_ID ?? uuid();

  /** @type {Map<string, object>} clientId → live connection context */
  const liveClients = new Map();
  /** @type {Map<string, object>} clientId → last known state, for the sticky-cookie path */
  const localSessions = new Map();

  const ownsStorage = storageAdapter == null;
  const storage = storageAdapter ?? createStorageAdapter();

  // Event-sourcing stack (CQRS write side + projections). Defaults to an
  // in-memory event store so no database is required; inject `eventSourcing`
  // (or a full `createEventSourcing({ eventStore })` result) to persist.
  const es = eventSourcing ?? createEventSourcing();
  const { commands } = es;

  /**
   * Delivery reaction to committed LocationUpdated events: broadcasts the
   * wire frame to room members (excluding sender) and persists via the
   * legacy storage adapter for backwards compatibility.
   *
   * @param {string} clientId
   * @param {object} payload
   */
  function deliverLocationUpdate(clientId, payload) {
    const roomIds = rooms.getClientRooms(clientId);
    for (const roomId of roomIds) {
      rooms.broadcast(roomId, {
        type: "location_update",
        payload: { clientId, ...payload },
      }, clientId);

      storage.saveLocation(clientId, roomId, payload).catch((err) => {
        logger.error("Failed to persist location", { clientId, roomId, error: err.message });
      });
    }
  }

  commands.subscribe((events) => {
    for (const event of events) {
      if (baseEventType(event.eventType) === "location_update") {
        deliverLocationUpdate(event.aggregateId, event.payload);
      }
    }
  });

  const effectiveMaxRoomSize = maxRoomSize ?? (Number(process.env.MAX_ROOM_SIZE) || undefined);

  const httpServer = http.createServer((req, res) => {
    let url;
    try {
      url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Bad Request" }));
      return;
    }
    const { pathname } = url;

    const migrate = req.method === "POST" ? MIGRATE_PATH.exec(pathname) : null;
    if (migrate) {
      migrateClient(decodeURIComponent(migrate[1]))
        .then((blob) => {
          if (!blob) {
            res.writeHead(404, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: "Not Found" }));
            return;
          }
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true }));
        })
        .catch((err) => {
          logger.error("Client migration failed", { error: err.message });
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Internal Server Error" }));
        });
      return;
    }

    if (req.method !== "GET") {
      res.writeHead(405, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Method Not Allowed" }));
      return;
    }

    if (pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "OK" }));
      return;
    }

    if (pathname === "/healthz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "shutting down" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
      return;
    }

    if (pathname === "/readyz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "not ready", reason: "server is shutting down" }));
        return;
      }
      if (!isReady) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "not ready", reason: "initializing" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        status: "ready",
        connections: wss.clients.size,
        rooms: rooms.roomCount,
      }));
      return;
    }

    if (url.pathname === "/metrics") {
      const mem = process.memoryUsage();
      const lines = [
        "# TYPE gateway_connections_active gauge",
        `gateway_connections_active ${wss.clients.size}`,
        "# TYPE gateway_rooms_active gauge",
        `gateway_rooms_active ${rooms.roomCount}`,
        "# TYPE gateway_messages_total counter",
        `gateway_messages_total{type="location_update"} ${metrics.messages.location_update}`,
        `gateway_messages_total{type="join_room"} ${metrics.messages.join_room}`,
        `gateway_messages_total{type="leave_room"} ${metrics.messages.leave_room}`,
        `gateway_messages_total{type="ack"} ${metrics.messages.ack}`,
        `gateway_messages_total{type="nack"} ${metrics.messages.nack}`,
        "# TYPE gateway_rate_limit_rejections_total counter",
        `gateway_rate_limit_rejections_total{kind="connection"} ${metrics.rateLimitRejections.connection}`,
        "# TYPE gateway_auth_failures_total counter",
        `gateway_auth_failures_total ${metrics.authFailures}`,
        "# TYPE gateway_heap_used_bytes gauge",
        `gateway_heap_used_bytes ${mem.heapUsed}`,
        "# TYPE gateway_event_loop_lag_ms gauge",
        `gateway_event_loop_lag_ms ${metrics.eventLoopLagMs}`,
      ];
      if (es) {
        const snapshotCount =
          es.vehicles.snapshot_count + es.roomsRepo.snapshot_count + es.fleets.snapshot_count;
        lines.push(
          "# TYPE event_store_append_duration_ms gauge",
          `event_store_append_duration_ms ${Number(es.eventStore.event_store_append_duration_ms ?? 0).toFixed(3)}`,
          "# TYPE event_store_appends_total counter",
          `event_store_appends_total ${es.eventStore.event_store_appends_total ?? 0}`,
          "# TYPE projection_lag_events gauge",
          `projection_lag_events ${es.projections.projection_lag_events ?? 0}`,
          "# TYPE snapshot_count counter",
          `snapshot_count ${snapshotCount}`,
          "# TYPE commands_total counter",
          `commands_total ${es.commands.commands_total ?? 0}`,
          "# TYPE commands_deduplicated_total counter",
          `commands_deduplicated_total ${es.commands.commands_deduplicated_total ?? 0}`
        );
      }
      if (sessions) {
        const sm = sessions.metrics;
        lines.push(
          "# TYPE session_resumption_total counter",
          `session_resumption_total{result="success"} ${sm.session_resumption_total.success}`,
          `session_resumption_total{result="decrypt_failed"} ${sm.session_resumption_total.decrypt_failed}`,
          `session_resumption_total{result="expired"} ${sm.session_resumption_total.expired}`,
          `session_resumption_total{result="mismatch"} ${sm.session_resumption_total.mismatch}`,
          `session_resumption_total{result="new_session"} ${sm.session_resumption_total.new_session}`,
          "# TYPE session_state_size_bytes gauge",
          `session_state_size_bytes ${sm.session_state_size_bytes}`
        );
      }
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(lines.join("\n"));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not Found" }));
  });

  const wss = new WebSocketServer({
    server: httpServer,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  httpServer.listen(port ?? 8080);

  const rooms = new RoomManager({
    maxRoomSize: effectiveMaxRoomSize,
    maxRoomsPerClient,
    maxMembersPerRoom: _maxMembersPerRoom,
    maxRooms,
    ringBufferSize: _ringBufferSize,
    deduplicationWindowMs: _deduplicationWindowMs,
    maxBufferBytes: _maxBufferBytes,
    maxDedupEntries: _maxDedupEntries,
    ackWindowSize: _ackWindowSize,
  });
  const rateLimiter = createRateLimiter(maxMessagesPerSecond);
  const connRateLimiter = createConnRateLimiter(connRateLimit);
  const ipConnectionCount = new Map();
  const MAX_CONNS_PER_IP = maxConnectionsPerIp ?? (Number(process.env.MAX_CONNECTIONS_PER_IP) || 10);
  const MAX_MESSAGES_PER_SECOND =
    maxMessagesPerSecond ?? (Number(process.env.MAX_MESSAGES_PER_SECOND) || 100);

  // Issue #248: predictive engine (Kalman CV model per client). The geofence
  // engine is not part of the gateway yet, so only trajectory/anomaly/ETA
  // features are active here.
  //
  // Predictive events are delivered out-of-band: they must not consume room
  // sequence numbers or enter the replay buffer, otherwise reconnecting
  // clients would observe phantom sequence gaps in the telemetry stream.
  function broadcastPredictiveEvent(roomId, message, excludeClientId) {
    for (const client of wss.clients) {
      const id = client._clientId;
      if (!id || id === excludeClientId) continue;
      if (!rooms.getClientRooms(id)?.has(roomId)) continue;
      safeSend(client, message);
    }
  }
  /** Room-manager facade handed to the predictor; routes events out-of-band. */
  const predictorRooms = {
    getClientRooms: (id) => rooms.getClientRooms(id),
    broadcast: (roomId, message, excludeClientId) =>
      broadcastPredictiveEvent(roomId, message, excludeClientId),
  };
  const predictor = new PredictiveEngine({ roomManager: predictorRooms });

  const HEARTBEAT_MS = heartbeatMs ?? 30000;
  // A client is a zombie once it stays silent for two ping intervals. The floor
  // keeps event-loop jitter from reaping healthy clients under tiny intervals.
  const PONG_TIMEOUT_MS = Math.max(HEARTBEAT_MS * 2, 1000);

  function heartbeat() {
    this._lastPongAt = Date.now();
  }

  /** Serialises and sends a frame, skipping sockets that are already going away. */
  function safeSend(ws, message) {
    if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) return;
    try {
      ws.send(typeof message === "string" ? message : JSON.stringify(message));
    } catch (err) {
      logger.warn("Failed to send frame", { error: err.message });
    }
  }

  function sendError(ws, message, code) {
    safeSend(ws, { type: "error", payload: { message, code } });
  }

  /**
   * Per-connection state that session capture reads from.
   *
   * @param {string} clientId
   * @param {import("http").IncomingMessage} req
   * @returns {object}
   */
  function createContext(clientId, req) {
    return {
      clientId,
      ip: req.socket.remoteAddress,
      userAgent: req.headers["user-agent"] ?? null,
      connectedAt: Date.now(),
      lastActivityAt: Date.now(),
      /** @type {Map<string, number>} roomId → highest seq the client ACKed via `reconnect` */
      ackedSeq: new Map(),
      /** @type {Map<string, string[]>} roomId → opaque geofence set carried across resumptions */
      geofence: new Map(),
      /** @type {number[]} timestamps of accepted messages, mirroring the rate-limiter window */
      messageWindow: [],
      /** @type {object|null} snapshot taken before teardown wipes room membership */
      frozenState: null,
    };
  }

  /** Drops timestamps outside the rate-limit window and caps the retained count. */
  function pruneWindow(timestamps, now = Date.now()) {
    const cutoff = now - RATE_WINDOW_MS;
    const live = timestamps.filter((ts) => typeof ts === "number" && ts > cutoff);
    return live.length > MAX_MESSAGES_PER_SECOND ? live.slice(-MAX_MESSAGES_PER_SECOND) : live;
  }

  /**
   * Builds the session state for a client from live room and rate-limit state.
   *
   * @param {object} ctx
   * @returns {import("./session-manager.js").SessionState}
   */
  function captureState(ctx) {
    if (ctx.frozenState) return ctx.frozenState;
    const roomIds = [...rooms.getClientRooms(ctx.clientId)];
    return {
      clientId: ctx.clientId,
      protocolVersion: PROTOCOL_VERSION,
      authIdentity: { sub: ctx.clientId },
      rooms: roomIds.map((roomId) => ({
        roomId,
        highestAckedSeq: ctx.ackedSeq.get(roomId) ?? 0,
        highestReceivedSeq: rooms.getRoomSeq(roomId),
        geofenceInsideSet: ctx.geofence.get(roomId) ?? [],
      })),
      rateLimitState: {
        messageWindow: pruneWindow(ctx.messageWindow),
        connectionWindow: [],
      },
      metadata: {
        ip: ctx.ip,
        userAgent: ctx.userAgent,
        connectedAt: ctx.connectedAt,
        lastActivityAt: ctx.lastActivityAt,
      },
    };
  }

  /** Keeps the newest state for the sticky-cookie path, bounded by insertion order. */
  function rememberLocal(clientId, state) {
    localSessions.delete(clientId);
    localSessions.set(clientId, state);
    while (localSessions.size > MAX_LOCAL_SESSIONS) {
      const oldest = localSessions.keys().next().value;
      localSessions.delete(oldest);
    }
  }

  /** Reads the local cache, dropping entries older than the session TTL. */
  function readLocal(clientId) {
    const state = localSessions.get(clientId);
    if (!state) return null;
    const lastActivityAt = state.metadata?.lastActivityAt ?? 0;
    if (Date.now() - lastActivityAt > sessionTtl) {
      localSessions.delete(clientId);
      return null;
    }
    return state;
  }

  /**
   * Marks activity, refreshes the local cache and schedules an encrypted save.
   *
   * @param {object|null} ctx
   */
  function touchSession(ctx) {
    if (!sessions || !ctx) return;
    ctx.lastActivityAt = Date.now();
    rememberLocal(ctx.clientId, captureState(ctx));
    sessions.debouncedSave(ctx.clientId, () => captureState(ctx)).catch((err) => {
      logger.error("Failed to schedule session save", { clientId: ctx.clientId, error: err.message });
    });
  }

  /**
   * Re-applies a decrypted session to this instance and confirms it to the client.
   *
   * @param {import("ws").WebSocket} ws
   * @param {object} ctx
   * @param {import("./session-manager.js").SessionState} state
   */
  function restoreSession(ws, ctx, state) {
    const restored = [];
    const skipped = [];

    for (const entry of Array.isArray(state.rooms) ? state.rooms : []) {
      if (!entry || typeof entry.roomId !== "string") continue;
      const joinResult = rooms.join(ctx.clientId, entry.roomId, ws);
      if (joinResult?.type === "error" || joinResult?.ok === false) {
        skipped.push(entry.roomId);
        continue;
      }
      const highestAckedSeq = Number(entry.highestAckedSeq) || 0;
      ctx.ackedSeq.set(entry.roomId, highestAckedSeq);
      ctx.geofence.set(
        entry.roomId,
        Array.isArray(entry.geofenceInsideSet) ? entry.geofenceInsideSet : []
      );
      restored.push({
        roomId: entry.roomId,
        highestAckedSeq,
        highestReceivedSeq: Number(entry.highestReceivedSeq) || 0,
      });
    }

    // Conservative choice: the limiter exposes no window import, so every saved
    // in-window timestamp is re-consumed to deny a burst after migration.
    ctx.messageWindow = pruneWindow(state.rateLimitState?.messageWindow ?? []);
    for (let i = 0; i < ctx.messageWindow.length; i++) rateLimiter.check(ctx.clientId);

    ctx.connectedAt = Number(state.metadata?.connectedAt) || ctx.connectedAt;

    if (skipped.length > 0) {
      logger.warn("Session rooms skipped on resume", { clientId: ctx.clientId, rooms: skipped });
    }

    const currentSeqPerRoom = {};
    for (const room of restored) currentSeqPerRoom[room.roomId] = rooms.getRoomSeq(room.roomId);

    safeSend(ws, { type: "session_resumed", payload: { rooms: restored, currentSeqPerRoom } });
    logger.info("Session resumed", { clientId: ctx.clientId, rooms: restored.length });
    commands.dispatch(new ResumeSessionCommand({
      commandId: uuid(),
      clientId: ctx.clientId,
      rooms: restored,
    })).catch((err) => {
      logger.error("Failed to record SessionResumed event", {
        clientId: ctx.clientId,
        error: err.message,
      });
    });
    touchSession(ctx);
  }

  /**
   * Runs the resumption handshake: explicit `session_id` first, then the
   * sticky-cookie fallback, otherwise a fresh session.
   *
   * @param {import("ws").WebSocket} ws
   * @param {import("http").IncomingMessage} req
   * @param {URL} url
   * @param {object} ctx
   * @param {string|null} token
   * @returns {Promise<boolean>} True when a session was restored.
   */
  async function resumeSession(ws, req, url, ctx, token) {
    const sessionId = url.searchParams.get("session_id") ?? sessionIdFromToken(token);

    if (sessionId) {
      // `load()` counts decrypt_failed / expired; success is recorded here,
      // after the identity check, so a mismatch is not also a success.
      const state = await sessions.load(sessionId, { countSuccess: false });
      if (!state) {
        logger.info("Session not resumable", { clientId: ctx.clientId });
        return false;
      }
      if (state.clientId !== ctx.clientId) {
        sessions?.recordResumption("mismatch");
        logger.warn("Session identity mismatch", {
          clientId: ctx.clientId,
          sessionClientId: state.clientId,
        });
        return false;
      }
      sessions?.recordResumption("success");
      restoreSession(ws, ctx, state);
      return true;
    }

    const affinity = readCookie(req.headers.cookie, AFFINITY_COOKIE);
    if (affinity === resolvedInstanceId) {
      const cached = readLocal(ctx.clientId);
      if (cached) {
        sessions?.recordResumption("success");
        restoreSession(ws, ctx, cached);
        return true;
      }
    }

    sessions?.recordResumption("new_session");
    return false;
  }

  /**
   * Persists a client's state now and hands the blob over as it disconnects.
   *
   * @param {string} clientId
   * @returns {Promise<string|null>} The blob, or null when the client is unknown.
   */
  async function migrateClient(clientId) {
    const ctx = sessions ? liveClients.get(clientId) : null;
    if (!ctx) return null;

    await sessions.flush(clientId);
    const state = captureState(ctx);
    const blob = await sessions.save(clientId, state);
    rememberLocal(clientId, state);

    if (Buffer.byteLength(blob, "utf8") <= MAX_CLOSE_REASON_BYTES) {
      ctx.ws.close(MIGRATE_CLOSE_CODE, blob);
    } else {
      safeSend(ctx.ws, { type: "migrate", payload: { session_id: blob } });
      ctx.ws.close(MIGRATE_CLOSE_CODE, "migrated");
    }
    logger.info("Client migrated", { clientId });
    return blob;
  }

  /**
   * Flushes pending saves and persists every live client, for graceful shutdown.
   *
   * @returns {Promise<Map<string, string>>} clientId → fresh session blob.
   */
  async function saveAllSessions() {
    /** @type {Map<string, string>} */
    const blobs = new Map();
    if (!sessions) return blobs;

    await sessions.flushAll();
    for (const ctx of liveClients.values()) {
      const state = captureState(ctx);
      rememberLocal(ctx.clientId, state);
      try {
        blobs.set(ctx.clientId, await sessions.save(ctx.clientId, state));
      } catch (err) {
        logger.error("Failed to save session", { clientId: ctx.clientId, error: err.message });
      }
    }
    return blobs;
  }

  if (sessions) {
    wss.on("headers", (headers) => {
      headers.push(
        `Set-Cookie: ${AFFINITY_COOKIE}=${resolvedInstanceId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${AFFINITY_MAX_AGE_S}`
      );
    });
  }

  wss.on("connection", async (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;
    ws._lastPongAt = Date.now();

    const ip = req.socket.remoteAddress;

    // Per-IP connection rate limit (new connections per minute)
    if (!connRateLimiter.check(ip)) {
      logger.warn("Connection rate limit exceeded", { ip });
      metrics.rateLimitRejections.connection++;
      ws.close(4029, "Connection rate limit exceeded");
      return;
    }

    const currentCount = ipConnectionCount.get(ip) ?? 0;
    if (currentCount >= MAX_CONNS_PER_IP) {
      logger.warn("Max connections per IP exceeded", { ip });
      metrics.rateLimitRejections.connection++;
      ws.close(4029, "Too many connections from this IP");
      return;
    }
    ipConnectionCount.set(ip, currentCount + 1);
    ws._trackedIp = ip;

    let url;
    try {
      url = new URL(req.url, "http://localhost");
    } catch {
      logger.warn("Invalid request URL", { clientId, url: req.url });
      ws.close(4000, "Invalid request URL");
      return;
    }

    /** Filled once authentication resolves; used by the close/error handlers. */
    let identity = null;
    /** Per-connection session context; null when resumption is disabled. */
    let ctx = null;

    /**
     * Teardown shared by every exit path. Registered synchronously so it runs
     * even when the socket dies during async auth or resumption.
     */
    ws.on("close", (code, reason) => {
      const id = identity?.clientId ?? null;
      if (ctx && id && sessions) {
        // Snapshot before teardown so a later save cannot persist empty rooms.
        ctx.frozenState = captureState(ctx);
        rememberLocal(id, ctx.frozenState);
        if (liveClients.get(id) === ctx) liveClients.delete(id);
        sessions.flush(id).catch((err) => {
          logger.error("Failed to flush session on close", { clientId: id, error: err.message });
        });
      }
      if (id) {
        rooms.disconnect(id);
        rateLimiter.remove(id);
        predictor.removeClient(id);
      }
      const trackedIp = ws._trackedIp;
      if (trackedIp) {
        const count = ipConnectionCount.get(trackedIp) ?? 1;
        if (count <= 1) {
          ipConnectionCount.delete(trackedIp);
        } else {
          ipConnectionCount.set(trackedIp, count - 1);
        }
        connRateLimiter.cleanup(trackedIp);
      }
      logger.info("Client disconnected", {
        clientId: id ?? clientId,
        code,
        reason: reason?.toString() ?? "unknown",
      });
    });

    ws.on("error", (err) => {
      logger.error("WebSocket error", { clientId: identity?.clientId ?? clientId, error: err.message });
    });

    // Frames that arrive before auth/resumption completes are queued so no
    // telemetry is lost; they are drained in order afterwards.
    const preReadyQueue = [];
    let dispatchMessage = null;
    let ready = false;

    ws.on("message", (raw) => {
      if (ready && dispatchMessage) dispatchMessage(raw);
      else preReadyQueue.push(raw);
    });

    const token = url.searchParams.get("token");
    let authResult;
    try {
      authResult = await verifyConnection(token);
    } catch {
      ws.close(4001, "Authentication failed");
      return;
    }

    if (!authResult.ok) {
      logger.warn("Authentication failed", { clientId, reason: authResult.error });
      metrics.authFailures++;
      ws.close(4001, authResult.error);
      return;
    }

    const identity = { clientId: authResult.clientId ?? clientId };
    ws._clientId = identity.clientId;
    logger.info("Client connected", { clientId: identity.clientId, ip });

    ws.on("pong", heartbeat);

    const ctx = sessions ? createContext(identity.clientId, req) : null;
    if (ctx) {
      ctx.ws = ws;
      liveClients.set(identity.clientId, ctx);
    }
    /** @type {Promise<unknown>|null} Frames wait for the handshake so replayed state lands first. */
    let pendingResume = null;
    if (ctx) {
      pendingResume = resumeSession(ws, req, url, ctx, token).catch((err) => {
        logger.error("Session resumption failed", { clientId: ctx.clientId, error: err.message });
      });
    }

    ws.on("message", async (raw) => {
      if (pendingResume) {
        await pendingResume;
        pendingResume = null;
      }

      if (!rateLimiter.check(identity.clientId)) {
        logger.warn("Message rate limit exceeded", { clientId: identity.clientId });
        safeSend(ws, { type: "error", payload: { message: "Rate limit exceeded" } });
        return;
      }
      if (ctx) ctx.messageWindow = pruneWindow([...ctx.messageWindow, Date.now()]);

      const validation = validateMessage(raw.toString());

      if (!validation.ok) {
        logger.warn("Validation failed", { clientId: identity.clientId, error: validation.error });
        sendError(ws, validation.error, validation.code ?? VALIDATION_ERROR);
        return;
      }

      const msg = validation.data;

      switch (msg.type) {
        case "join_room": {
          metrics.messages.join_room++;
          const joinResult = rooms.join(identity.clientId, msg.roomId, ws);
          if (joinResult && joinResult.type === "error") {
            logger.warn("Join rejected", { clientId: identity.clientId, roomId: msg.roomId, code: joinResult.payload.code });
            safeSend(ws, joinResult);
            break;
          }
          if (joinResult && joinResult.ok === false) {
            logger.warn("Room is full", { clientId: identity.clientId, roomId: msg.roomId });
            safeSend(ws, { type: "error", payload: { message: "Room is full", code: "ROOM_FULL" } });
            break;
          }
          logger.info("Client joined room", { clientId: identity.clientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_joined", payload: { roomId: msg.roomId } });
          // Record membership in the event store (write side). The read-model
          // join above already succeeded; persistence failures are logged and
          // never break the realtime path.
          commands.dispatch(new JoinRoomCommand({
            commandId: uuid(),
            clientId: identity.clientId,
            roomId: msg.roomId,
          })).catch((err) => {
            logger.error("Failed to record RoomJoined event", {
              clientId: identity.clientId,
              roomId: msg.roomId,
              error: err.message,
            });
          });
          touchSession(ctx);
          break;
        }
        case "leave_room": {
          metrics.messages.leave_room++;
          rooms.leave(identity.clientId, msg.roomId);
          if (ctx) {
            ctx.ackedSeq.delete(msg.roomId);
            ctx.geofence.delete(msg.roomId);
          }
          logger.info("Client left room", { clientId: identity.clientId, roomId: msg.roomId });
          safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } });
          commands.dispatch(new LeaveRoomCommand({
            commandId: uuid(),
            clientId: identity.clientId,
            roomId: msg.roomId,
          })).catch((err) => {
            logger.error("Failed to record RoomLeft event", {
              clientId: identity.clientId,
              roomId: msg.roomId,
              error: err.message,
            });
          });
          touchSession(ctx);
          break;
        }
        case "reconnect": {
          const clientRooms = rooms.getClientRooms(identity.clientId);
          if (!clientRooms.has(msg.roomId)) {
            safeSend(ws, { type: "error", payload: { message: "Must join room before reconnecting" } });
            break;
          }
          if (ctx) ctx.ackedSeq.set(msg.roomId, msg.lastSeq);
          const replayResult = rooms.handleReconnect(msg.roomId, msg.lastSeq, msg.highestAckedSeq);
          safeSend(ws, replayResult);
          touchSession(ctx);
          break;
        }
        case "ack": {
          metrics.messages.ack++;
          rooms.ack(identity.clientId, msg.roomId, msg.seq);
          commands.dispatch(new AcknowledgeCommand({
            commandId: uuid(),
            clientId: identity.clientId,
            roomId: msg.roomId,
            seq: msg.seq,
          })).catch((err) => {
            logger.error("Failed to record MessageAcknowledged event", {
              clientId: identity.clientId,
              error: err.message,
            });
          });
          break;
        }
        case "nack": {
          metrics.messages.nack++;
          logger.warn("NACK received", { clientId: identity.clientId, roomId: msg.roomId, seq: msg.seq, reason: msg.reason });
          rooms.nack(identity.clientId, msg.roomId, msg.seq);
          commands.dispatch(new NegativeAcknowledgeCommand({
            commandId: uuid(),
            clientId: identity.clientId,
            roomId: msg.roomId,
            seq: msg.seq,
            reason: msg.reason,
          })).catch((err) => {
            logger.error("Failed to record MessageNacked event", {
              clientId: identity.clientId,
              error: err.message,
            });
          });
          break;
        }
        case "token_refresh": {
          try {
            const decoded = jwt.decode(msg.token, { complete: true });
            if (!decoded) {
              safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
              break;
            }
            verifyConnection(msg.token).then((refreshResult) => {
              if (!refreshResult.ok) {
                safeSend(ws, { type: "error", payload: { message: refreshResult.error } });
                return;
              }
              const oldClientId = identity.clientId;
              const newClientId = refreshResult.clientId;
              authResult = refreshResult;
              identity.clientId = newClientId;
              ws._clientId = newClientId;
              rooms.updateClientId(oldClientId, newClientId);
              rateLimiter.remove(oldClientId);
              if (ctx && oldClientId !== newClientId) {
                if (liveClients.get(oldClientId) === ctx) liveClients.delete(oldClientId);
                ctx.clientId = newClientId;
                liveClients.set(newClientId, ctx);
              }
              safeSend(ws, { type: "token_refresh_ok", payload: { clientId: newClientId } });
              touchSession(ctx);
            }).catch(() => {
              safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
            });
          } catch {
            safeSend(ws, { type: "error", payload: { message: "Invalid refresh token" } });
          }
          break;
        }
        case "location_update": {
          metrics.messages.location_update++;
          // Write side: the location is committed to the event store as an
          // immutable LocationUpdated event (causationId = commandId). The
          // broadcast + legacy persistence happen as a reaction to the commit
          // (see commands.subscribe above). If the store rejects the write we
          // degrade gracefully: log and deliver directly so realtime never
          // depends on storage availability.
          try {
            const produced = await commands.dispatch(new PublishLocationCommand({
              commandId: typeof msg.commandId === "string" && msg.commandId.length > 0
                ? msg.commandId
                : uuid(),
              clientId: identity.clientId,
              payload: msg.payload,
            }));
            if (produced.length === 0) {
              // Duplicate commandId — already committed and delivered once.
              logger.debug?.("Duplicate location command ignored", { clientId: identity.clientId });
            }
          } catch (err) {
            logger.error("Failed to record LocationUpdated event", {
              clientId: identity.clientId,
              error: err.message,
            });
            deliverLocationUpdate(identity.clientId, msg.payload);
          }
          touchSession(ctx);
          break;
        }
      }
    });

    ws.on("close", (code, reason) => {
      if (ctx) {
        // Snapshot before teardown so a pending save cannot persist empty rooms.
        ctx.frozenState = captureState(ctx);
        rememberLocal(ctx.clientId, ctx.frozenState);
        if (liveClients.get(ctx.clientId) === ctx) liveClients.delete(ctx.clientId);
        sessions.flush(ctx.clientId).catch((err) => {
          logger.error("Failed to flush session on close", { clientId: ctx.clientId, error: err.message });
        });
      }

      rooms.disconnect(identity.clientId);
      rateLimiter.remove(identity.clientId);
      const trackedIp = ws._trackedIp;
      if (trackedIp) {
        const count = ipConnectionCount.get(trackedIp) ?? 1;
        if (count <= 1) {
          ipConnectionCount.delete(trackedIp);
        } else {
          ipConnectionCount.set(trackedIp, count - 1);
        }
        connRateLimiter.cleanup(trackedIp);
      }
      logger.info("Client disconnected", {
        clientId: identity.clientId,
        code,
        reason: reason?.toString() ?? "unknown",
      });
    });

    ws.on("error", (err) => {
      logger.error("WebSocket error", { clientId: identity.clientId, error: err.message });
    });
  });

  const heartbeatInterval = setInterval(() => {
    const now = Date.now();
    wss.clients.forEach((ws) => {
      const silentMs = now - (ws._lastPongAt ?? now);
      if (ws.isAlive === false || silentMs > PONG_TIMEOUT_MS) {
        logger.warn("Terminating zombie connection", {
          clientId: ws._clientId ?? ws._trackedIp ?? "unknown",
        });
        return ws.terminate();
      }
      ws.ping();
    });
  }, HEARTBEAT_MS);

  // Sampled event-loop lag: a zero-delay timer that fires late measures how
  // far behind the loop is running.
  function measureLag() {
    const start = Date.now();
    setTimeout(() => {
      metrics.eventLoopLagMs = Date.now() - start;
    }, 0);
  }
  const lagInterval = setInterval(measureLag, LAG_SAMPLE_MS);
  measureLag();

  function markShuttingDown() {
    isShuttingDown = true;
    isReady = false;
  }

  wss.on("close", () => {
    clearInterval(heartbeatInterval);
    clearInterval(lagInterval);
    if (ownsSessions) sessions.close();
    if (ownsStorage) storage.close().catch((err) => {
      logger.error("Failed to close storage adapter", { error: err.message });
    });
    if (eventSourcing == null) {
      // Only close stacks this server created.
      es.eventStore.close().catch((err) => {
        logger.error("Failed to close event store", { error: err.message });
      });
    }
    httpServer.close();
  });

  return {
    wss,
    httpServer,
    rooms,
    ipConnectionCount,
    rateLimiter,
    metrics,
    markShuttingDown,
    sessionManager: sessions,
    instanceId: resolvedInstanceId,
    saveAllSessions,
    storage,
    eventSourcing: es,
  };
}
