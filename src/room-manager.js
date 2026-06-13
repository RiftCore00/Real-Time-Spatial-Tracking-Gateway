import { WebSocket } from "ws";

export class RoomManager {
  constructor() {
    this._rooms = new Map();
    this._clientRooms = new Map();
  }

  join(clientId, roomId, ws) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");
    if (ws == null) throw new TypeError("ws is required");

    if (!this._rooms.has(roomId)) {
      this._rooms.set(roomId, new Map());
    }
    this._rooms.get(roomId).set(clientId, ws);

    if (!this._clientRooms.has(clientId)) {
      this._clientRooms.set(clientId, new Set());
    }
    this._clientRooms.get(clientId).add(roomId);
  }

  leave(clientId, roomId) {
    if (clientId == null) throw new TypeError("clientId is required");
    if (roomId == null) throw new TypeError("roomId is required");

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
          if (room.size === 0) {
            this._rooms.delete(roomId);
          }
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
