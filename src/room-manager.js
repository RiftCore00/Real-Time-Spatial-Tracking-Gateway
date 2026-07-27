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
   * @param {number} [options.maxRoomsPerClient=50]
   * @param {number} [options.maxMembersPerRoom=10000]
   * @param {number} [options.maxRooms=10000]
   * @param {object} [options.circuitBreaker]
   * @param {boolean} [options.circuitBreaker.enabled=false]
   * @param {number} [options.circuitBreaker.memoryThresholdBytes]
   * @param {number} [options.circuitBreaker.recoveryThresholdBytes]
   */
  constructor(options = {}) {
    const {
      maxRoomsPerClient = 50,
      maxMembersPerRoom = 10000,
      maxRooms = 10000,
      circuitBreaker = {},
    } = options;

    this._maxRoomsPerClient = maxRoomsPerClient;
    this._maxMembersPerRoom = maxMembersPerRoom;
    this._maxRooms = maxRooms;

    const cbMemoryThreshold = circuitBreaker.memoryThresholdBytes ?? 512 * 1024 * 1024;
    const cbRecoveryThreshold = circuitBreaker.recoveryThresholdBytes ?? Math.floor(cbMemoryThreshold * 0.75);

    this._circuitBreaker = {
      enabled: circuitBreaker.enabled ?? (circuitBreaker.memoryThresholdBytes != null || circuitBreaker.recoveryThresholdBytes != null),
      memoryThresholdBytes: cbMemoryThreshold,
      recoveryThresholdBytes: cbRecoveryThreshold,
    };
    this._circuitBreakerState = "CLOSED";

    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();
    /** @type {number} */
    this._totalMembers = 0;
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

  join(clientId, roomId, ws) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (ws == null) throw new TypeError("ws is required");

    if (this._circuitBreaker.enabled) {
      const heapUsed = process.memoryUsage().heapUsed;
      if (this._circuitBreakerState === "CLOSED") {
        if (heapUsed > this._circuitBreaker.memoryThresholdBytes) {
          this._circuitBreakerState = "OPEN";
        }
      } else if (this._circuitBreakerState === "OPEN") {
        if (heapUsed < this._circuitBreaker.recoveryThresholdBytes) {
          this._circuitBreakerState = "CLOSED";
        }
      }

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

    const roomExists = this._rooms.has(roomId);
    if (!roomExists && this._rooms.size >= this._maxRooms) {
      return {
        type: "error",
        payload: {
          code: "MAX_ROOMS_REACHED",
          message: `Maximum room count ceiling reached (${this._maxRooms})`,
        },
      };
    }

    const clientRooms = this._clientRooms.get(clientId);
    const isClientInRoom = clientRooms ? clientRooms.has(roomId) : false;

    if (!isClientInRoom && (clientRooms?.size ?? 0) >= this._maxRoomsPerClient) {
      return {
        type: "error",
        payload: {
          code: "ROOM_LIMIT_EXCEEDED",
          message: `Client room limit exceeded (${this._maxRoomsPerClient})`,
        },
      };
    }

    const room = roomExists ? this._rooms.get(roomId) : null;
    if (!isClientInRoom && (room?.size ?? 0) >= this._maxMembersPerRoom) {
      return {
        type: "error",
        payload: {
          code: "ROOM_FULL",
          message: `Room member limit reached (${this._maxMembersPerRoom})`,
        },
      };
    }

    const targetRoom = this._ensureRoom(roomId);
    if (!targetRoom.has(clientId)) {
      this._totalMembers++;
    }
    targetRoom.set(clientId, ws);
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
   * excluding the sender.
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
