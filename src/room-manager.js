import { WebSocket } from "ws";

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
 * When backpressure options are provided, broadcast() operates in a
 * non-blocking batched mode with per-client slow consumer detection,
 * message coalescing, and automatic eviction.
 */
export class RoomManager {
  /**
   * @param {BackpressureOptions} [backpressure] - Backpressure configuration
   */
  constructor(backpressure = undefined) {
    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();

    /** @type {BackpressureOptions} */
    this._backpressureOptions = backpressure && backpressure.enabled
      ? {
        enabled: true,
        highWaterMark: backpressure.highWaterMark ?? 1048576,
        slowConsumerTimeout: backpressure.slowConsumerTimeout ?? 30000,
        batchSize: backpressure.batchSize ?? 100,
      }
      : { enabled: false };

    /** @type {Map<string, { ws: import("ws").WebSocket, slowSince: number | null, coalescedMessage: object | null }>} */
    this._clientState = new Map();
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
      if (state.slowSince !== null && this._backpressureOptions.slowConsumerTimeout) {
        if (state._timeoutId) {
          clearTimeout(state._timeoutId);
        }
      }
      this._clientState.delete(clientId);
    }
  }

  join(clientId, roomId, ws) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (ws == null) throw new TypeError("ws is required");

    this._ensureRoom(roomId).set(clientId, ws);
    this._ensureClientRooms(clientId).add(roomId);

    if (this._backpressureOptions.enabled) {
      this._ensureClientState(clientId, ws);
    }
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

    if (this._backpressureOptions.enabled) {
      const clientRoomsRemaining = this._clientRooms.get(clientId);
      if (!clientRoomsRemaining || clientRoomsRemaining.size === 0) {
        this._cleanupClientState(clientId);
      }
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

    if (!this._backpressureOptions.enabled) {
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
      return;
    }

    this._broadcastWithBackpressure(roomId, room, message, excludeClientId);
  }

  /** @private */
  _broadcastWithBackpressure(roomId, room, message, excludeClientId) {
    const entries = Array.from(room.entries());
    const batchSize = this._backpressureOptions.batchSize;
    let index = 0;

    const processBatch = () => {
      const end = Math.min(index + batchSize, entries.length);
      while (index < end) {
        const [clientId, ws] = entries[index];
        index++;

        if (clientId === excludeClientId) continue;
        if (ws == null || ws.readyState !== WebSocket.OPEN) continue;

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
            this._sendToClient(clientId, ws, message);
          }
        } else {
          this._sendToClient(clientId, ws, message);
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

    if (this._backpressureOptions.enabled) {
      this._cleanupClientState(clientId);
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
}
