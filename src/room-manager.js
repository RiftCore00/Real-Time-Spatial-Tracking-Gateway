import { WebSocket } from "ws";

/**
 * Manages room membership and message broadcasting for connected WebSocket clients.
 *
 * Rooms are keyed by an arbitrary string ID. Each room holds a map of
 * `clientId → WebSocket` so broadcasts are O(members). A reverse index
 * (`_clientRooms`) enables O(1) lookup of all rooms a client belongs to,
 * which is used during disconnection cleanup.
 */
export class RoomManager {
  /**
   * @param {object} [options]
   * @param {number} [options.ringBufferSize=1000] - Maximum stored messages per room.
   * @param {number} [options.deduplicationWindowMs=5000] - Deduplication TTL window in milliseconds.
   * @param {number} [options.maxBufferBytes=5242880] - Maximum memory limit for room ring buffer in bytes.
   * @param {number} [options.maxDedupEntries=10000] - Maximum tracked deduplication keys.
   */
  constructor({
    ringBufferSize = 1000,
    deduplicationWindowMs = 5000,
    maxBufferBytes = 5 * 1024 * 1024,
    maxDedupEntries = 10000,
  } = {}) {
    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();

    this._ringBufferSize = ringBufferSize;
    this._deduplicationWindowMs = deduplicationWindowMs;
    this._maxBufferBytes = maxBufferBytes;
    this._maxDedupEntries = maxDedupEntries;

    /** @type {Map<string, number>} */
    this._roomSeq = new Map();
    /** @type {Map<string, Array<{ seq: number, payload: any, timestamp: number }>>} */
    this._roomBuffers = new Map();
    /** @type {Map<string, number>} */
    this._roomBufferBytes = new Map();
    /** @type {Map<string, number>} */
    this._dedupCache = new Map();
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

  join(clientId, roomId, ws) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (ws == null) throw new TypeError("ws is required");

    this._ensureRoom(roomId).set(clientId, ws);
    this._ensureClientRooms(clientId).add(roomId);
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
    if (room) {
      room.delete(clientId);
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
}
