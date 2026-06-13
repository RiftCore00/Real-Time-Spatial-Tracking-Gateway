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
  constructor() {
    /** @type {Map<string, Map<string, import("ws").WebSocket>>} */
    this._rooms = new Map();
    /** @type {Map<string, Set<string>>} */
    this._clientRooms = new Map();
  }

  /**
   * Subscribes a client to a room.
   *
   * If the client is already a member, the stored WebSocket reference is
   * updated to the new one (useful after reconnection).
   *
   * @param {string} clientId - Unique identifier for the client.
   * @param {string} roomId - Identifier of the room to join.
   * @param {import("ws").WebSocket} ws - The client's active WebSocket connection.
   */
  join(clientId, roomId, ws) {
    if (!this._rooms.has(roomId)) {
      this._rooms.set(roomId, new Map());
    }
    this._rooms.get(roomId).set(clientId, ws);

    if (!this._clientRooms.has(clientId)) {
      this._clientRooms.set(clientId, new Set());
    }
    this._clientRooms.get(clientId).add(roomId);
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
    const room = this._rooms.get(roomId);
    if (room) {
      room.delete(clientId);
      if (room.size === 0) {
        this._rooms.delete(roomId);
      }
    }

    const clientRooms = this._clientRooms.get(clientId);
    if (clientRooms) {
      clientRooms.delete(roomId);
      if (clientRooms.size === 0) {
        this._clientRooms.delete(clientId);
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
    const room = this._rooms.get(roomId);
    if (!room) return;

    const data = typeof message === "string" ? message : JSON.stringify(message);

    for (const [clientId, ws] of room) {
      if (clientId === excludeClientId) continue;
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(data);
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
    const rooms = this._clientRooms.get(clientId);
    if (rooms) {
      for (const roomId of rooms) {
        const room = this._rooms.get(roomId);
        if (room) {
          room.delete(clientId);
          if (room.size === 0) {
            this._rooms.delete(roomId);
          }
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
