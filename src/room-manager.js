import { WebSocket } from "ws";

export class RoomManager {
  constructor() {
    this._rooms = new Map();
    this._clientRooms = new Map();
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

    this._ensureRoom(roomId).set(clientId, ws);
    this._ensureClientRooms(clientId).add(roomId);
  }

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

  getRoomSize(roomId) {
    if (roomId == null) throw new TypeError("roomId is required");
    const room = this._rooms.get(roomId);
    return room ? room.size : 0;
  }

  getClientRooms(clientId) {
    if (clientId == null) throw new TypeError("clientId is required");
    const rooms = this._clientRooms.get(clientId);
    return rooms ? new Set(rooms) : new Set();
  }

  get roomCount() {
    return this._rooms.size;
  }

  get clientCount() {
    return this._clientRooms.size;
  }
}
