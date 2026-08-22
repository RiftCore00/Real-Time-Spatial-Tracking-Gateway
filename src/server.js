import http from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { v4 as uuid } from "uuid";
import jwt from "jsonwebtoken";
import { RoomManager } from "./room-manager.js";
import { SessionManager } from "./session-manager.js";
import { validateMessage } from "./validator.js";
import { verifyConnection } from "./auth.js";
import { logger } from "./logger.js";
import { createRateLimiter } from "./rate-limiter.js";
import { createConnRateLimiter } from "./conn-rate-limiter.js";
import { VALIDATION_ERROR } from "./errors.js";

/**
 * Creates the co-located HTTP server (health checks, Prometheus metrics,
 * admin migration) and the WebSocket gateway on the same port.
 *
 * Session resumption is opt-in: it activates when `sessionManager` is injected
 * or when an encryption key is available, and stays completely inert otherwise.
 *
 * @param {object} [options]
 * @param {number} [options.port] - TCP port to listen on. Defaults to 8080.
 * @param {number} [options.heartbeatMs] - Ping interval used to detect zombies. Defaults to 30000.
 * @param {number} [options.maxPayloadBytes] - Max WebSocket frame size. Defaults to 1024.
 * @param {number} [options.connRateLimit] - New connections allowed per IP per minute.
 * @param {number} [options.maxConnectionsPerIp] - Concurrent connections allowed per IP.
 * @param {number} [options.maxMessagesPerSecond] - Per-client message rate limit.
 * @param {number} [options.ringBufferSize] - Replay entries kept per room.
 * @param {number} [options.deduplicationWindowMs] - TTL of a `location_update` dedup key.
 * @param {number} [options.maxBufferBytes] - Byte ceiling for one room's replay buffer.
 * @param {number} [options.maxDedupEntries] - Max dedup keys retained.
 * @param {SessionManager} [options.sessionManager] - Pre-built manager; takes precedence over `sessionEncryptionKey`.
 * @param {string|object} [options.sessionEncryptionKey] - Key (or key map) used to build a manager.
 *   Falls back to `SESSION_ENCRYPTION_KEY`.
 * @param {number} [options.sessionTtlMs] - Session TTL. Falls back to `SESSION_TTL_MS`, then 1 hour.
 * @param {string} [options.instanceId] - Value published in the `GW_AFFINITY` cookie.
 *   Falls back to `INSTANCE_ID`, then a uuid.
 * @param {object} [options.redis] - node-redis v4 style client handed to a self-built manager.
 * @returns {{ wss: WebSocketServer, server: http.Server, httpServer: http.Server, rooms: RoomManager, ipConnectionCount: Map<string, number>, rateLimiter: object, metrics: object, markShuttingDown: () => void, sessionManager: SessionManager|null, instanceId: string, saveAllSessions: () => Promise<Map<string, string>> }}
 */
export function createServer({
  port,
  heartbeatMs,
  maxPayloadBytes,
  connRateLimit,
  maxConnectionsPerIp,
  maxRoomSize,
  maxRoomsPerClient,
  maxMembersPerRoom,
  maxRooms,
  ringBufferSize: _ringBufferSize,
  deduplicationWindowMs: _deduplicationWindowMs,
  maxBufferBytes: _maxBufferBytes,
  maxDedupEntries: _maxDedupEntries,
  ackWindowSize: _ackWindowSize,
  maxMessagesPerSecond,
} = {}) {
  let isShuttingDown = false;

  const metrics = {
    messages: { location_update: 0, join_room: 0, leave_room: 0, ack: 0, nack: 0 },
    rateLimitRejections: { connection: 0 },
    authFailures: 0,
    sessionResumption: { success: 0, decrypt_failed: 0, expired: 0, mismatch: 0, new_session: 0 },
    eventLoopLagMs: 0,
  };

  const sessionManager = new SessionManager({
    encryptionKey: process.env.SESSION_ENCRYPTION_KEY || undefined,
  });

  const effectiveMaxRoomSize = maxRoomSize ?? (Number(process.env.MAX_ROOM_SIZE) || undefined);

  const httpServer = http.createServer((req, res) => {
    if (req.method !== "GET") {
      res.writeHead(405, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Method Not Allowed" }));
      return;
    }

    const migrate = req.method === "POST" ? MIGRATE_PATH.exec(url.pathname) : null;
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

    if (url.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "OK" }));
      return;
    }

    if (url.pathname === "/healthz") {
      if (isShuttingDown) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "shutting down" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
      return;
    }

    if (url.pathname === "/readyz") {
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
    } else if (pathname === "/metrics") {
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
        "# TYPE session_resumption_total counter",
        `session_resumption_total{result="success"} ${metrics.sessionResumption.success}`,
        `session_resumption_total{result="decrypt_failed"} ${metrics.sessionResumption.decrypt_failed}`,
        `session_resumption_total{result="expired"} ${metrics.sessionResumption.expired}`,
        `session_resumption_total{result="mismatch"} ${metrics.sessionResumption.mismatch}`,
        `session_resumption_total{result="new_session"} ${metrics.sessionResumption.new_session}`,
        "# TYPE gateway_heap_used_bytes gauge",
        `gateway_heap_used_bytes ${mem.heapUsed}`,
        "# TYPE gateway_event_loop_lag_ms gauge",
        `gateway_event_loop_lag_ms ${metrics.eventLoopLagMs}`,
      ];
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(renderMetrics());
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not Found" }));
  });

  const wss = new WebSocketServer({
    server,
    maxPayload: maxPayloadBytes ?? 1024,
  });

  const rooms = new RoomManager({
    maxRoomSize: effectiveMaxRoomSize,
    maxRoomsPerClient,
    maxMembersPerRoom,
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
        sessions.recordResumption("mismatch");
        logger.warn("Session identity mismatch", {
          clientId: ctx.clientId,
          sessionClientId: state.clientId,
        });
        return false;
      }
      sessions.recordResumption("success");
      restoreSession(ws, ctx, state);
      return true;
    }

    const affinity = readCookie(req.headers.cookie, AFFINITY_COOKIE);
    if (affinity === resolvedInstanceId) {
      const cached = readLocal(ctx.clientId);
      if (cached) {
        sessions.recordResumption("success");
        restoreSession(ws, ctx, cached);
        return true;
      }
    }

    sessions.recordResumption("new_session");
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

  function safeSend(ws, data) {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(typeof data === "string" ? data : JSON.stringify(data));
      }
    } catch {
      // ignore send errors
    }
  }

  wss.on("connection", async (ws, req) => {
    const clientId = uuid();
    ws.isAlive = true;
    ws._lastPongAt = Date.now();

    const ip = req.socket.remoteAddress;

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

    const token = url.searchParams.get("token");
    let authResult;
    verifyConnection(token).then((result) => {
      authResult = result;

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

      const sessionId = url.searchParams.get("session_id");
      if (sessionId) {
        sessionManager.load(sessionId).then((savedSession) => {
          if (!savedSession) {
            metrics.sessionResumption.new_session++;
            return;
          }
          const savedIdentity = savedSession.authIdentity?.sub ?? savedSession.authIdentity?.clientId;
          if (savedIdentity !== identity.clientId) {
            metrics.sessionResumption.mismatch++;
            return;
          }
          if (savedSession.rooms && savedSession.rooms.length > 0) {
            for (const roomState of savedSession.rooms) {
              rooms.join(identity.clientId, roomState.roomId, ws);
            }
            const roomIds = savedSession.rooms.map((r) => r.roomId);
            metrics.sessionResumption.success++;
            safeSend(ws, {
              type: "session_resumed",
              payload: {
                sessionId,
                rooms: roomIds,
                currentSeqPerRoom: savedSession.rooms.map((r) => ({
                  roomId: r.roomId,
                  highestAckedSeq: r.highestAckedSeq,
                  highestReceivedSeq: r.highestReceivedSeq,
                })),
              },
            });
          }
        }).catch(() => {});
      }
      if (ctx) ctx.messageWindow = pruneWindow([...ctx.messageWindow, Date.now()]);

      ws.on("message", (raw) => {
        if (!rateLimiter.check(identity.clientId)) {
          logger.warn("Message rate limit exceeded", { clientId: identity.clientId });
          safeSend(ws, { type: "error", payload: { message: "Rate limit exceeded" } });
          return;
        }

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

            const currentRooms = rooms.getClientRooms(identity.clientId);
            const roomStates = Array.from(currentRooms).map((roomId) => ({
              roomId,
              highestAckedSeq: 0,
              highestReceivedSeq: 0,
              geofenceInsideSet: [],
            }));
            sessionManager.debouncedSave(identity.clientId, {
              clientId: identity.clientId,
              protocolVersion: 3,
              authIdentity: authResult,
              rooms: roomStates,
              rateLimitState: { messageWindow: [], connectionWindow: [] },
              metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
            });
            break;
          }
          case "leave_room": {
            metrics.messages.leave_room++;
            rooms.leave(identity.clientId, msg.roomId);
            logger.info("Client left room", { clientId: identity.clientId, roomId: msg.roomId });
            safeSend(ws, { type: "room_left", payload: { roomId: msg.roomId } });

            const currentRooms = rooms.getClientRooms(identity.clientId);
            const roomStates = Array.from(currentRooms).map((roomId) => ({
              roomId,
              highestAckedSeq: 0,
              highestReceivedSeq: 0,
              geofenceInsideSet: [],
            }));
            sessionManager.debouncedSave(identity.clientId, {
              clientId: identity.clientId,
              protocolVersion: 3,
              authIdentity: authResult,
              rooms: roomStates,
              rateLimitState: { messageWindow: [], connectionWindow: [] },
              metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
            });
            break;
          }
          case "reconnect": {
            const clientRooms = rooms.getClientRooms(identity.clientId);
            if (!clientRooms.has(msg.roomId)) {
              safeSend(ws, { type: "error", payload: { message: "Must join room before reconnecting" } });
              break;
            }
            const replayResult = rooms.handleReconnect(msg.roomId, msg.lastSeq, msg.highestAckedSeq);
            safeSend(ws, replayResult);
            break;
          }
          case "ack": {
            metrics.messages.ack++;
            rooms.ack(identity.clientId, msg.roomId, msg.seq);
            break;
          }
          case "nack": {
            metrics.messages.nack++;
            logger.warn("NACK received", { clientId: identity.clientId, roomId: msg.roomId, seq: msg.seq, reason: msg.reason });
            rooms.nack(identity.clientId, msg.roomId, msg.seq);
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
                safeSend(ws, { type: "token_refresh_ok", payload: { clientId: newClientId } });
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
            const roomIds = rooms.getClientRooms(identity.clientId);
            for (const roomId of roomIds) {
              rooms.broadcast(roomId, {
                type: "location_update",
                payload: { clientId: identity.clientId, ...msg.payload },
              }, identity.clientId);
            }
            break;
          }
        }
      });

      ws.on("close", (code, reason) => {
        const currentRooms = rooms.getClientRooms(identity.clientId);
        const roomStates = Array.from(currentRooms).map((roomId) => ({
          roomId,
          highestAckedSeq: 0,
          highestReceivedSeq: 0,
          geofenceInsideSet: [],
        }));
        sessionManager.save(identity.clientId, {
          clientId: identity.clientId,
          protocolVersion: 3,
          authIdentity: authResult,
          rooms: roomStates,
          rateLimitState: { messageWindow: [], connectionWindow: [] },
          metadata: { ip, userAgent: req.headers?.["user-agent"] ?? "", connectedAt: Date.now(), lastActivityAt: Date.now() },
        }).catch(() => {});

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
    }).catch(() => {
      ws.close(4001, "Authentication failed");
    });

    if (ctx) {
      pendingResume = resumeSession(ws, req, url, ctx, token).catch((err) => {
        logger.error("Session resumption failed", { clientId: ctx.clientId, error: err.message });
      });
    }
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
    httpServer.close();
  });

  function markShuttingDown() {
    isShuttingDown = true;
  }

  return { wss, httpServer, rooms, sessionManager, ipConnectionCount, rateLimiter, markShuttingDown };
}
