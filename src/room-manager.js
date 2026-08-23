import { WebSocket } from "ws";
import { v7 as uuidv7 } from "uuid";

/**
 * @typedef {Object} BackpressureOptions
 * @property {boolean} [enabled=false] - Enable backpressure-aware broadcasting
 * @property {number} [highWaterMark=1048576] - Bytes threshold to flag client as slow (default 1MB)
 * @property {number} [slowConsumerTimeout=30000] - Ms before terminating slow consumer (default 30s)
 * @property {number} [batchSize=100] - Number of sends per event loop tick (default 100)
 */

/**
 * Manages room membership and message broadcasting for connected WebSocket clients.
 *
 * Rooms are keyed by an arbitrary string ID. Each room holds a map of
 * `clientId → WebSocket` so broadcasts are O(members). A reverse index
 * (`_clientRooms`) enables O(1) lookup of all rooms a client belongs to,
 * which is used during disconnection cleanup.
 *
 * Supports exactly-once delivery semantics via client acknowledgments,
 * server-side messageId tracking, and flow control with ACK windows.
 */
export class RoomManager {
  constructor({
    maxRoomSize = Infinity,
    maxRoomsPerClient = Infinity,
    maxMembersPerRoom = Infinity,
    maxRooms = Infinity,
    ringBufferSize = 1000,
    maxBufferBytes = 10 * 1024 * 1024,
    deduplicationWindowMs = 5000,
    maxDedupEntries = 10000,
    enabled = false,
    highWaterMark = 1048576,
    slowConsumerTimeout = 30000,
    batchSize = 100,
    circuitBreaker = { enabled: false, memoryThresholdBytes: 500 * 1024 * 1024, recoveryThresholdBytes: 300 * 1024 * 1024 },
    ackWindowSize = Infinity,
  } = {}) {
    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();
    this._maxRoomSize = maxRoomSize;
    this._maxRoomsPerClient = maxRoomsPerClient;
    this._maxMembersPerRoom = maxMembersPerRoom;
    this._maxRooms = maxRooms;
    /** @type {Map<string, number>} */
    this._roomSeq = new Map();
    /** @type {Map<string, Array<{seq: number, messageId: string, payload: any, timestamp: number}>>} */
    this._roomBuffers = new Map();
    /** @type {Map<string, number>} */
    this._roomBufferBytes = new Map();
    /** @type {Map<string, number>} */
    this._dedupCache = new Map();
    this._maxDedupEntries = maxDedupEntries;
    this._deduplicationWindowMs = deduplicationWindowMs;
    this._ringBufferSize = ringBufferSize;
    this._maxBufferBytes = maxBufferBytes;
    this._backpressureOptions = { enabled, highWaterMark, slowConsumerTimeout, batchSize };
    /** @type {Map<string, {ws: import("ws").WebSocket, slowSince: number|null, coalescedMessage: any, _timeoutId: any}>} */
    this._clientState = new Map();
    this._totalMembers = 0;
    this._circuitBreakerState = "CLOSED";
    this._circuitBreakerConfig = circuitBreaker;
    this._ackWindowSize = ackWindowSize;
    /** @type {Map<string, Map<string, {highestAckedSeq: number, unackedCount: number, paused: boolean}>>} */
    this._ackState = new Map();
    this._useNewJoinFormat = (maxRoomsPerClient !== Infinity) || (maxMembersPerRoom !== Infinity) || (maxRooms !== Infinity) || circuitBreaker.enabled;
  }

  /** @private */
  _ensureRoom(roomId) {
    if (!this._rooms.has(roomId)) {
      this._rooms.set(roomId, new Map());
    }
    return this._rooms.get(roomId);
  }

  /** @private */
  _ensureClientRooms(clientId) {
    if (!this._clientRooms.has(clientId)) {
      this._clientRooms.set(clientId, new Set());
    }
    return this._clientRooms.get(clientId);
  }

  /** @private */
  _ensureClientState(clientId, ws) {
    if (!this._clientState.has(clientId)) {
      this._clientState.set(clientId, {
        ws,
        slowSince: null,
        coalescedMessage: null,
      });
    }
    return this._clientState.get(clientId);
  }

  /** @private */
  _cleanupRoom(roomId) {
    const room = this._rooms.get(roomId);
    if (room && room.size === 0) {
      this._rooms.delete(roomId);
    }
  }

  /** @private */
  _cleanupClient(clientId) {
    const rooms = this._clientRooms.get(clientId);
    if (rooms && rooms.size === 0) {
      this._clientRooms.delete(clientId);
    }
  }

  /** @private */
  _cleanupClientState(clientId) {
    const state = this._clientState.get(clientId);
    if (state) {
      if (state._timeoutId) {
        clearTimeout(state._timeoutId);
      }
      this._clientState.delete(clientId);
    }
  }

  /** @private */
  _cleanupAckState(clientId) {
    this._ackState.delete(clientId);
  }

  /** @private */
  _checkCircuitBreaker() {
    const config = this._circuitBreakerConfig;
    if (!config.enabled) return;

    const { heapUsed } = process.memoryUsage();

    if (this._circuitBreakerState === "CLOSED" && heapUsed >= config.memoryThresholdBytes) {
      this._circuitBreakerState = "OPEN";
    } else if (this._circuitBreakerState === "OPEN" && heapUsed <= config.recoveryThresholdBytes) {
      this._circuitBreakerState = "CLOSED";
    }
  }

  /**
   * Returns or initializes the ACK tracking state for a client in a room.
   * @private
   */
  _getAckState(clientId, roomId) {
    let clientAck = this._ackState.get(clientId);
    if (!clientAck) {
      clientAck = new Map();
      this._ackState.set(clientId, clientAck);
    }
    let ackState = clientAck.get(roomId);
    if (!ackState) {
      ackState = { highestAckedSeq: 0, unackedCount: 0, paused: false };
      clientAck.set(roomId, ackState);
    }
    return ackState;
  }

  /** @private */
  _isDuplicate(roomId, message, excludeClientId) {
    let parsed = message;
    if (typeof message === "string") {
      try {
        parsed = JSON.parse(message);
      } catch {
        return false;
      }
    }
    if (parsed && typeof parsed === "object" && parsed.type === "location_update") {
      const clientId = parsed.payload?.clientId || excludeClientId;
      const timestamp = parsed.payload?.timestamp;
      if (clientId && timestamp) {
        const key = `${roomId}:${clientId}:${timestamp}`;
        const now = Date.now();
        if (this._dedupCache.has(key)) {
          const recordedTime = this._dedupCache.get(key);
          if (now - recordedTime <= this._deduplicationWindowMs) {
            return true;
          }
        }
        this._dedupCache.set(key, now);
        if (this._dedupCache.size > this._maxDedupEntries) {
          const oldestKey = this._dedupCache.keys().next().value;
          if (oldestKey !== undefined) {
            this._dedupCache.delete(oldestKey);
          }
        }
      }
    }
    return false;
  }

  /**
   * Subscribes a client to a room, enforcing the configured DoS limits.
   *
   * Re-joining a room the client already occupies only replaces the stored
   * socket, so it is never rejected by a limit.
   *
   * @param {string} clientId - Unique identifier for the client.
   * @param {string} roomId - Identifier of the room to join.
   * @param {import("ws").WebSocket} ws - Socket to register for broadcasts.
   * @returns {undefined | { ok: boolean, reason?: string } | { type: "error", payload: { code: string, message: string } }}
   *   `{ ok }` when `maxRoomSize` is configured, an error frame when a limit or the
   *   circuit breaker rejects the join, otherwise `undefined`.
   */
  join(clientId, roomId, ws) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (ws == null) throw new TypeError("ws is required");

    if (this._circuitBreakerConfig.enabled) {
      this._checkCircuitBreaker();
      if (this._circuitBreakerState === "OPEN") {
        return {
          type: "error",
          payload: {
            code: "CIRCUIT_BREAKER_OPEN",
            message: "Circuit breaker is open due to high resource pressure",
          },
        };
      }
    }

    if (this._maxRoomsPerClient !== Infinity) {
      const clientRooms = this._clientRooms.get(clientId);
      const isRejoin = clientRooms && clientRooms.has(roomId);
      if (!isRejoin) {
        const currentCount = clientRooms ? clientRooms.size : 0;
        if (currentCount >= this._maxRoomsPerClient) {
          return {
            type: "error",
            payload: {
              code: "ROOM_LIMIT_EXCEEDED",
              message: `Client room limit exceeded (${this._maxRoomsPerClient})`,
            },
          };
        }
      }
    }

    if (this._maxMembersPerRoom !== Infinity) {
      const existingRoom = this._rooms.get(roomId);
      const isRejoin = existingRoom && existingRoom.has(clientId);
      if (!isRejoin) {
        const currentSize = existingRoom ? existingRoom.size : 0;
        if (currentSize >= this._maxMembersPerRoom) {
          return {
            type: "error",
            payload: {
              code: "ROOM_FULL",
              message: `Room member limit reached (${this._maxMembersPerRoom})`,
            },
          };
        }
      }
    }

    if (this._maxRooms !== Infinity) {
      if (!this._rooms.has(roomId)) {
        if (this._rooms.size >= this._maxRooms) {
          return {
            type: "error",
            payload: {
              code: "MAX_ROOMS_REACHED",
              message: `Maximum room count ceiling reached (${this._maxRooms})`,
            },
          };
        }
      }
    }

    const room = this._ensureRoom(roomId);
    if (!room.has(clientId) && room.size >= this._maxRoomSize) {
      if (this._useNewJoinFormat) {
        return {
          type: "error",
          payload: {
            code: "ROOM_FULL",
            message: `Room member limit reached (${this._maxRoomSize})`,
          },
        };
      }
      return { ok: false, reason: 'ROOM_FULL' };
    }

    const isNewMembership = !room.has(clientId);
    room.set(clientId, ws);
    this._ensureClientRooms(clientId).add(roomId);

    if (isNewMembership) {
      this._totalMembers++;
    }

    if (this._backpressureOptions.enabled) {
      this._ensureClientState(clientId, ws);
    }

    if (this._useNewJoinFormat) {
      return;
    }
    return { ok: true };
  }

  /**
   * Unsubscribes a client from a room.
   *
   * Empty rooms are automatically deleted. If the client has no remaining
   * room memberships, their reverse-index entry is also removed.
   *
   * @param {string} clientId - Unique identifier for the client.
   * @param {string} roomId - Identifier of the room to leave.
   */
  leave(clientId, roomId) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");

    const room = this._rooms.get(roomId);
    if (room && room.has(clientId)) {
      room.delete(clientId);
      this._totalMembers--;
      this._cleanupRoom(roomId);
    }

    const clientRooms = this._clientRooms.get(clientId);
    if (clientRooms) {
      clientRooms.delete(roomId);
      this._cleanupClient(clientId);
    }

    if (this._backpressureOptions.enabled) {
      const clientRoomsRemaining = this._clientRooms.get(clientId);
      if (!clientRoomsRemaining || clientRoomsRemaining.size === 0) {
        this._cleanupClientState(clientId);
      }
    }

    if (this._ackWindowSize !== Infinity) {
      const clientAck = this._ackState.get(clientId);
      if (clientAck) {
        clientAck.delete(roomId);
        if (clientAck.size === 0) {
          this._ackState.delete(clientId);
        }
      }
    }
  }

  /**
   * Broadcasts a message to every open connection in a room, optionally
   * excluding the sender. Stores message in ring buffer with a sequence number
   * and a UUID v7 messageId for exactly-once delivery tracking.
   *
   * Objects are serialised to JSON; strings are sent as-is.
   * Clients whose `readyState` is not `OPEN` are silently skipped.
   *
   * @param {string} roomId - Identifier of the target room.
   * @param {object|string} message - The payload to send.
   * @param {string|null} [excludeClientId=null] - Client to skip (typically the publisher).
   */
  broadcast(roomId, message, excludeClientId = null) {
    if (roomId == null) throw new TypeError("roomId is required");

    if (this._isDuplicate(roomId, message, excludeClientId)) {
      return;
    }

    const currentSeq = (this._roomSeq.get(roomId) ?? 0) + 1;
    this._roomSeq.set(roomId, currentSeq);

    let payload = message;
    if (typeof message === "string") {
      try {
        payload = JSON.parse(message);
      } catch {
        payload = message;
      }
    }

    const msgId = uuidv7();

    const entry = {
      seq: currentSeq,
      messageId: msgId,
      payload,
      timestamp: Date.now(),
    };

    if (!this._roomBuffers.has(roomId)) {
      this._roomBuffers.set(roomId, []);
      this._roomBufferBytes.set(roomId, 0);
    }

    const buffer = this._roomBuffers.get(roomId);
    const entryBytes = Buffer.byteLength(JSON.stringify(entry), "utf8");
    let currentBytes = (this._roomBufferBytes.get(roomId) ?? 0) + entryBytes;
    buffer.push(entry);

    while (buffer.length > 0 && (buffer.length > this._ringBufferSize || currentBytes > this._maxBufferBytes)) {
      const evicted = buffer.shift();
      const evictedBytes = Buffer.byteLength(JSON.stringify(evicted), "utf8");
      currentBytes -= evictedBytes;
    }
    this._roomBufferBytes.set(roomId, Math.max(0, currentBytes));

    const room = this._rooms.get(roomId);
    if (!room) return;

    const wireData = typeof message === "string" ? message : JSON.stringify(message);

    if (!this._backpressureOptions.enabled) {
      for (const [clientId, ws] of room) {
        if (clientId === excludeClientId) continue;
        if (ws != null && ws.readyState === WebSocket.OPEN) {
          if (this._ackWindowSize !== Infinity) {
            const ackState = this._getAckState(clientId, roomId);
            if (ackState.paused || ackState.unackedCount >= this._ackWindowSize) {
              ackState.paused = true;
              continue;
            }
            ackState.unackedCount++;
          }
          try {
            ws.send(wireData);
          } catch {
            // ignore send errors for individual clients
          }
        }
      }
      return;
    }

    this._broadcastWithBackpressure(roomId, room, message, excludeClientId);
  }

  /** @private */
  _broadcastWithBackpressure(roomId, room, message, excludeClientId) {
    const entries = Array.from(room.entries());
    const batchSize = this._backpressureOptions.batchSize;
    let index = 0;

    const wireData = typeof message === "string" ? message : JSON.stringify(message);

    const processBatch = () => {
      const end = Math.min(index + batchSize, entries.length);
      while (index < end) {
        const [clientId, ws] = entries[index];
        index++;

        if (clientId === excludeClientId) continue;
        if (ws == null || ws.readyState !== WebSocket.OPEN) continue;

        if (this._ackWindowSize !== Infinity) {
          const ackState = this._getAckState(clientId, roomId);
          if (ackState.paused || ackState.unackedCount >= this._ackWindowSize) {
            ackState.paused = true;
            continue;
          }
          ackState.unackedCount++;
        }

        const state = this._clientState.get(clientId);
        const bufferedAmount = ws.bufferedAmount || 0;
        const isSlow = state && state.slowSince !== null;
        const exceedsHighWater = bufferedAmount > this._backpressureOptions.highWaterMark;

        if (exceedsHighWater && state && state.slowSince === null) {
          state.slowSince = Date.now();
          this._startSlowConsumerTimeout(clientId, state);
        }

        if (isSlow || exceedsHighWater) {
          if (typeof message === "object" && message !== null && message.type === "location_update") {
            if (state) {
              state.coalescedMessage = message;
            }
          } else {
            this._sendToClientRaw(clientId, ws, wireData);
          }
        } else {
          this._sendToClientRaw(clientId, ws, wireData);
        }
      }

      if (index < entries.length) {
        setImmediate(processBatch);
      } else {
        this._drainCoalescedMessages(roomId, room);
      }
    };

    setImmediate(processBatch);
  }

  /** @private */
  _drainCoalescedMessages(roomId, room) {
    for (const [clientId, ws] of room) {
      if (ws == null || ws.readyState !== WebSocket.OPEN) continue;

      const state = this._clientState.get(clientId);
      if (state && state.coalescedMessage !== null) {
        const msg = state.coalescedMessage;
        state.coalescedMessage = null;
        this._sendToClient(clientId, ws, msg);
      }
    }
  }

  /** @private */
  _sendToClient(clientId, ws, message) {
    try {
      const data = typeof message === "string" ? message : JSON.stringify(message);
      ws.send(data);
    } catch {
      // ignore send errors for individual clients
    }
  }

  /** @private */
  _sendToClientRaw(clientId, ws, data) {
    try {
      ws.send(data);
    } catch {
      // ignore send errors for individual clients
    }
  }

  /** @private */
  _sendToClientById(clientId, roomId, entry) {
    const room = this._rooms.get(roomId);
    if (!room) return;
    const ws = room.get(clientId);
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    try {
      ws.send(JSON.stringify(entry));
    } catch {
      // ignore send errors
    }
  }

  /** @private */
  _startSlowConsumerTimeout(clientId, state) {
    if (state._timeoutId) {
      clearTimeout(state._timeoutId);
    }

    state._timeoutId = setTimeout(() => {
      if (state.slowSince !== null) {
        const ws = state.ws;
        if (ws && ws.readyState === WebSocket.OPEN) {
          try {
            ws.close(4000, "Slow consumer");
          } catch {
            // ignore close errors
          }
        }
        this._cleanupClientState(clientId);
      }
    }, this._backpressureOptions.slowConsumerTimeout);

    if (state._timeoutId.unref) {
      state._timeoutId.unref();
    }
  }

  /**
   * Returns the current sequence number for a room.
   *
   * @param {string} roomId - Identifier of the room.
   * @returns {number} Current sequence number, or 0 if room has no broadcasts.
   */
  getRoomSeq(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");
    return this._roomSeq.get(roomId) ?? 0;
  }

  /**
   * Returns the array of stored ring buffer entries for a room in order.
   *
   * @param {string} roomId - Identifier of the room.
   * @returns {Array<{ seq: number, messageId: string, payload: any, timestamp: number }>}
   */
  getRingBuffer(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");
    const buffer = this._roomBuffers.get(roomId);
    return buffer ? [...buffer] : [];
  }

  /**
   * Evaluates a reconnection request against stored room sequence and ring buffer.
   * Uses highestAckedSeq when provided to implement exactly-once delivery.
   *
   * @param {string} roomId - Identifier of the target room.
   * @param {number} lastSeq - The sequence number last received by the client.
   * @param {number} [highestAckedSeq] - The highest sequence number the client has acknowledged.
   * @returns {object} Replay payload (type: "replay", "replay_complete", or "replay_gap").
   */
  handleReconnect(roomId, lastSeq, highestAckedSeq) {
    if (roomId == null) throw new TypeError("roomId is required");
    if (lastSeq == null) throw new TypeError("lastSeq is required");

    const currentSeq = this.getRoomSeq(roomId);
    const buffer = this._roomBuffers.get(roomId) ?? [];

    const replayFrom = (highestAckedSeq != null && highestAckedSeq >= 0) ? highestAckedSeq : lastSeq;

    if (replayFrom === currentSeq) {
      return {
        type: "replay_complete",
        roomId,
      };
    }

    if (buffer.length > 0) {
      const oldestSeq = buffer[0].seq;
      if (replayFrom >= oldestSeq - 1 && replayFrom < currentSeq) {
        const missed = buffer.filter((entry) => entry.seq > replayFrom);
        return {
          type: "replay",
          roomId,
          messages: missed,
          currentSeq,
        };
      }
    }

    const oldestSeq = buffer.length > 0 ? buffer[0].seq : currentSeq;
    return {
      type: "replay_gap",
      roomId,
      fromSeq: oldestSeq,
      currentSeq,
    };
  }

  /**
   * Processes an acknowledgment from a client for a specific sequence number.
   * Updates the highest ACKed sequence, decrements unacked count, and resumes
   * delivery if the flow control window has space.
   *
   * @param {string} clientId - Unique identifier for the client.
   * @param {string} roomId - Identifier of the room.
   * @param {number} seq - The sequence number being acknowledged.
   */
  ack(clientId, roomId, seq) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (seq == null) throw new TypeError("seq is required");

    const ackState = this._getAckState(clientId, roomId);
    if (seq > ackState.highestAckedSeq) {
      const delta = seq - ackState.highestAckedSeq;
      ackState.highestAckedSeq = seq;
      ackState.unackedCount = Math.max(0, ackState.unackedCount - delta);
    }
    if (ackState.paused && ackState.unackedCount < this._ackWindowSize) {
      ackState.paused = false;
    }
  }

  /**
   * Processes a negative acknowledgment from a client. Re-sends the specific
   * message from the ring buffer to that client only, bypassing flow control.
   *
   * @param {string} clientId - Unique identifier for the client.
   * @param {string} roomId - Identifier of the room.
   * @param {number} seq - The sequence number of the corrupted message.
   */
  nack(clientId, roomId, seq) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (seq == null) throw new TypeError("seq is required");

    const buffer = this._roomBuffers.get(roomId) ?? [];
    const entry = buffer.find((e) => e.seq === seq);
    if (entry) {
      this._sendToClientById(clientId, roomId, entry);
    }
  }

  /**
   * Moves a client's room memberships from one clientId to another.
   * Used during token refresh when the identity changes.
   *
   * @param {string} oldClientId - The previous client identifier.
   * @param {string} newClientId - The new client identifier.
   */
  updateClientId(oldClientId, newClientId) {
    if (oldClientId == null) throw new TypeError("oldClientId is required");
    if (newClientId == null) throw new TypeError("newClientId is required");
    if (oldClientId === newClientId) return;

    const oldRooms = this._clientRooms.get(oldClientId);
    if (!oldRooms) return;

    const newClientRooms = this._ensureClientRooms(newClientId);

    for (const roomId of oldRooms) {
      const room = this._rooms.get(roomId);
      if (room) {
        const ws = room.get(oldClientId);
        if (ws) {
          room.delete(oldClientId);
          room.set(newClientId, ws);
        }
      }
      newClientRooms.add(roomId);
    }

    this._clientRooms.delete(oldClientId);

    if (this._backpressureOptions.enabled) {
      const oldState = this._clientState.get(oldClientId);
      if (oldState) {
        this._clientState.set(newClientId, oldState);
        this._clientState.delete(oldClientId);
      }
    }
  }

  /**
   * Removes a client from all rooms they belong to and cleans up empty rooms.
   *
   * Should be called when a WebSocket `close` event fires.
   *
   * @param {string} clientId - Unique identifier for the disconnecting client.
   */
  disconnect(clientId) {
    if (clientId == null) throw new TypeError("clientId is required");

    const rooms = this._clientRooms.get(clientId);
    if (rooms) {
      this._totalMembers -= rooms.size;
      for (const roomId of rooms) {
        const room = this._rooms.get(roomId);
        if (room) {
          room.delete(clientId);
          this._cleanupRoom(roomId);
        }
      }
      this._clientRooms.delete(clientId);
    }

    if (this._backpressureOptions.enabled) {
      this._cleanupClientState(clientId);
    }

    this._cleanupAckState(clientId);
  }

  /**
   * Returns the number of clients currently in a room.
   *
   * @param {string} roomId - Identifier of the room to query.
   * @returns {number} Member count, or `0` if the room does not exist.
   */
  getRoomSize(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");
    const room = this._rooms.get(roomId);
    return room ? room.size : 0;
  }

  /**
   * Returns a copy of the set of room IDs the client is currently joined to.
   *
   * Mutating the returned Set has no effect on internal state.
   *
   * @param {string} clientId - Unique identifier for the client.
   * @returns {Set<string>} Room IDs, or an empty Set if the client is not tracked.
   */
  getClientRooms(clientId) {
    if (clientId == null) throw new TypeError("clientId is required");
    const rooms = this._clientRooms.get(clientId);
    return rooms ? new Set(rooms) : new Set();
  }

  /**
   * Returns statistics for a room including member count, send queue depths,
   * and list of slow consumers.
   *
   * @param {string} roomId - Identifier of the room to query.
   * @returns {{ memberCount: number, sendQueueDepths: { [clientId: string]: number }, slowConsumers: string[] }} Room statistics
   */
  getRoomStats(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");

    const room = this._rooms.get(roomId);
    if (!room) {
      return { memberCount: 0, sendQueueDepths: {}, slowConsumers: [] };
    }

    const stats = {
      memberCount: room.size,
      sendQueueDepths: {},
      slowConsumers: [],
    };

    if (!this._backpressureOptions.enabled) {
      return stats;
    }

    for (const [clientId, ws] of room) {
      if (ws != null && ws.readyState === WebSocket.OPEN) {
        stats.sendQueueDepths[clientId] = ws.bufferedAmount || 0;
      } else {
        stats.sendQueueDepths[clientId] = 0;
      }

      const state = this._clientState.get(clientId);
      if (state && state.slowSince !== null) {
        stats.slowConsumers.push(clientId);
      }
    }

    return stats;
  }

  /**
   * Total number of active rooms (rooms with at least one member).
   * @type {number}
   */
  get roomCount() {
    return this._rooms.size;
  }

  /**
   * Total number of tracked clients (clients in at least one room).
   * @type {number}
   */
  get clientCount() {
    return this._clientRooms.size;
  }

  /**
   * RoomManager statistics and metrics.
   * @type {{ roomCount: number, clientCount: number, totalMembers: number, circuitBreakerState: string }}
   */
  get stats() {
    return {
      roomCount: this._rooms.size,
      clientCount: this._clientRooms.size,
      totalMembers: this._totalMembers,
      circuitBreakerState: this._circuitBreakerState,
    };
  }
}
