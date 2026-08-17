import { WebSocket } from "ws";

const DEFAULT_RING_BUFFER_SIZE = 100;
const DEFAULT_MAX_BUFFER_BYTES = 1024 * 1024;
const DEFAULT_DEDUP_WINDOW_MS = 5000;
const DEFAULT_MAX_DEDUP_ENTRIES = 10_000;

/**
 * Manages room membership and message broadcasting for connected WebSocket clients.
 *
 * Rooms are keyed by an arbitrary string ID. Each room holds a map of
 * `clientId → WebSocket` so broadcasts are O(members). A reverse index
 * (`_clientRooms`) enables O(1) lookup of all rooms a client belongs to,
 * which is used during disconnection cleanup.
 *
 * Each room also keeps a monotonic sequence number and a bounded ring buffer of
 * recent broadcasts so reconnecting clients can replay what they missed.
 */
export class RoomManager {
  /**
   * @param {object} [options]
   * @param {number} [options.maxRoomSize] - Legacy per-room member cap. When set,
   *   `join()` reports `{ ok }` results instead of an error frame.
   * @param {number} [options.maxRoomsPerClient] - Max rooms a single client may join.
   * @param {number} [options.maxMembersPerRoom] - Max members allowed in one room.
   * @param {number} [options.maxRooms] - Ceiling on the number of live rooms.
   * @param {{ enabled?: boolean, memoryThresholdBytes?: number, recoveryThresholdBytes?: number }} [options.circuitBreaker]
   *   Heap-pressure breaker: opens above `memoryThresholdBytes`, closes below `recoveryThresholdBytes`.
   * @param {number} [options.ringBufferSize] - Replay entries kept per room.
   * @param {number} [options.maxBufferBytes] - Byte ceiling for one room's replay buffer.
   * @param {number} [options.deduplicationWindowMs] - TTL of a `location_update` dedup key.
   * @param {number} [options.maxDedupEntries] - Max dedup keys retained.
   */
  constructor({
    maxRoomSize,
    maxRoomsPerClient = Infinity,
    maxMembersPerRoom = Infinity,
    maxRooms = Infinity,
    circuitBreaker = {},
    ringBufferSize = DEFAULT_RING_BUFFER_SIZE,
    maxBufferBytes = DEFAULT_MAX_BUFFER_BYTES,
    deduplicationWindowMs = DEFAULT_DEDUP_WINDOW_MS,
    maxDedupEntries = DEFAULT_MAX_DEDUP_ENTRIES,
  } = {}) {
    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();

    this._maxRoomSize = maxRoomSize ?? Infinity;
    this._legacyJoinResult = maxRoomSize != null;
    this._maxRoomsPerClient = maxRoomsPerClient;
    this._maxMembersPerRoom = maxMembersPerRoom;
    this._maxRooms = maxRooms;
    this._totalMembers = 0;

    this._circuitBreakerEnabled = circuitBreaker.enabled === true;
    this._memoryThresholdBytes = circuitBreaker.memoryThresholdBytes ?? Infinity;
    this._recoveryThresholdBytes = circuitBreaker.recoveryThresholdBytes ?? 0;
    this._circuitBreakerState = "CLOSED";

    /** @type {Map<string, number>} roomId → last assigned sequence number */
    this._roomSeq = new Map();
    /** @type {Map<string, Array<{ seq: number, payload: any, timestamp: number }>>} */
    this._roomBuffers = new Map();
    /** @type {Map<string, number>} roomId → approximate buffer size in bytes */
    this._roomBufferBytes = new Map();
    this._ringBufferSize = ringBufferSize;
    this._maxBufferBytes = maxBufferBytes;

    /** @type {Map<string, number>} dedup key → time the message was first seen (ms) */
    this._dedupCache = new Map();
    this._deduplicationWindowMs = deduplicationWindowMs;
    this._maxDedupEntries = maxDedupEntries;
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
  _rejection(code, message) {
    return { type: "error", payload: { code, message } };
  }

  /**
   * @private
   * Re-evaluates heap pressure and returns true while the breaker rejects joins.
   */
  _circuitBreakerOpen() {
    if (!this._circuitBreakerEnabled) return false;

    const { heapUsed } = process.memoryUsage();
    if (this._circuitBreakerState === "OPEN") {
      if (heapUsed < this._recoveryThresholdBytes) {
        this._circuitBreakerState = "CLOSED";
        return false;
      }
      return true;
    }

    if (heapUsed > this._memoryThresholdBytes) {
      this._circuitBreakerState = "OPEN";
      return true;
    }
    return false;
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

    if (this._circuitBreakerOpen()) {
      return this._rejection(
        "CIRCUIT_BREAKER_OPEN",
        "Circuit breaker is open due to high resource pressure",
      );
    }

    const room = this._rooms.get(roomId);
    const isMember = room?.has(clientId) === true;

    if (!isMember) {
      const joinedRooms = this._clientRooms.get(clientId)?.size ?? 0;
      if (joinedRooms >= this._maxRoomsPerClient) {
        return this._rejection(
          "ROOM_LIMIT_EXCEEDED",
          `Client room limit exceeded (${this._maxRoomsPerClient})`,
        );
      }
      if (!room && this._rooms.size >= this._maxRooms) {
        return this._rejection(
          "MAX_ROOMS_REACHED",
          `Maximum room count ceiling reached (${this._maxRooms})`,
        );
      }
      const members = room?.size ?? 0;
      if (members >= this._maxMembersPerRoom) {
        return this._rejection(
          "ROOM_FULL",
          `Room member limit reached (${this._maxMembersPerRoom})`,
        );
      }
      if (members >= this._maxRoomSize) {
        return { ok: false, reason: "ROOM_FULL" };
      }
    }

    this._ensureRoom(roomId).set(clientId, ws);
    this._ensureClientRooms(clientId).add(roomId);
    if (!isMember) this._totalMembers++;

    return this._legacyJoinResult ? { ok: true } : undefined;
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
  }

  /**
   * Broadcasts a message to every open connection in a room, optionally
   * excluding the sender. Stores message in ring buffer with a sequence number.
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

    const entry = {
      seq: currentSeq,
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

    const data = typeof message === "string" ? message : JSON.stringify(message);

    for (const [clientId, ws] of room) {
      if (clientId === excludeClientId) continue;
      if (ws != null && ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(data);
        } catch {
          // ignore send errors for individual clients
        }
      }
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
   * @returns {Array<{ seq: number, payload: any, timestamp: number }>}
   */
  getRingBuffer(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");
    const buffer = this._roomBuffers.get(roomId);
    return buffer ? [...buffer] : [];
  }

  /**
   * Evaluates a reconnection request against stored room sequence and ring buffer.
   *
   * @param {string} roomId - Identifier of the target room.
   * @param {number} lastSeq - The sequence number last received by the client.
   * @returns {object} Replay payload (type: "replay", "replay_complete", or "replay_gap").
   */
  handleReconnect(roomId, lastSeq) {
    if (roomId == null) throw new TypeError("roomId is required");
    if (lastSeq == null) throw new TypeError("lastSeq is required");

    const currentSeq = this.getRoomSeq(roomId);
    const buffer = this._roomBuffers.get(roomId) ?? [];

    if (lastSeq === currentSeq) {
      return {
        type: "replay_complete",
        roomId,
      };
    }

    if (buffer.length > 0) {
      const oldestSeq = buffer[0].seq;
      if (lastSeq >= oldestSeq - 1 && lastSeq < currentSeq) {
        const missed = buffer.filter((entry) => entry.seq > lastSeq);
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
