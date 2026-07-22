import { describe, it, expect, beforeEach } from "vitest";
import { RoomManager } from "../src/room-manager.js";

describe("RoomManager", () => {
  let rooms;
  let fakeWs;

  beforeEach(() => {
    rooms = new RoomManager();
    fakeWs = { readyState: 1, send: () => {} };
  });

  describe("join / leave", () => {
    it("adds a client to a room", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      expect(rooms.getRoomSize("room-alpha")).toBe(1);
    });

    it("adds multiple clients to the same room", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.join("client-2", "room-alpha", fakeWs);
      expect(rooms.getRoomSize("room-alpha")).toBe(2);
    });

    it("returns ok: true when joining under capacity", () => {
      const limitedRooms = new RoomManager({ maxRoomSize: 2 });
      const res = limitedRooms.join("client-1", "room-alpha", fakeWs);
      expect(res).toEqual({ ok: true });
    });

    it("rejects join when room is at capacity", () => {
      const limitedRooms = new RoomManager({ maxRoomSize: 1 });
      limitedRooms.join("client-1", "room-alpha", fakeWs);
      
      const res = limitedRooms.join("client-2", "room-alpha", fakeWs);
      expect(res).toEqual({ ok: false, reason: 'ROOM_FULL' });
      expect(limitedRooms.getRoomSize("room-alpha")).toBe(1);
    });

    it("removes a client from a room and cleans up empty rooms", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.leave("client-1", "room-alpha");
      expect(rooms.getRoomSize("room-alpha")).toBe(0);
      expect(rooms.roomCount).toBe(0);
    });

    it("does not affect other rooms when a client leaves one room", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.join("client-1", "room-beta", fakeWs);
      rooms.leave("client-1", "room-alpha");
      expect(rooms.getRoomSize("room-beta")).toBe(1);
      expect(rooms.getRoomSize("room-alpha")).toBe(0);
    });
  });

  describe("broadcast", () => {
    it("sends a message to all room members", () => {
      const sent = [];
      const ws1 = { readyState: 1, send: (m) => sent.push(m) };
      const ws2 = { readyState: 1, send: (m) => sent.push(m) };

      rooms.join("c1", "room-alpha", ws1);
      rooms.join("c2", "room-alpha", ws2);
      rooms.broadcast("room-alpha", { type: "update", data: "hello" });

      expect(sent).toHaveLength(2);
      expect(sent[0]).toBe(JSON.stringify({ type: "update", data: "hello" }));
    });

    it("excludes the sender when excludeClientId is provided", () => {
      const sent = [];
      const ws1 = { readyState: 1, send: (m) => sent.push(m) };
      const ws2 = { readyState: 1, send: (m) => sent.push(m) };

      rooms.join("c1", "room-alpha", ws1);
      rooms.join("c2", "room-alpha", ws2);
      rooms.broadcast("room-alpha", "msg", "c1");

      expect(sent).toHaveLength(1);
    });

    it("does nothing for a non-existent room", () => {
      expect(() => rooms.broadcast("ghost-room", "data")).not.toThrow();
    });
  });

  describe("disconnect", () => {
    it("removes the client from all rooms", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.join("client-1", "room-beta", fakeWs);
      rooms.disconnect("client-1");

      expect(rooms.getRoomSize("room-alpha")).toBe(0);
      expect(rooms.getRoomSize("room-beta")).toBe(0);
      expect(rooms.getClientRooms("client-1").size).toBe(0);
    });

    it("cleans up empty rooms after disconnect", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.disconnect("client-1");
      expect(rooms.roomCount).toBe(0);
    });

    it("does not affect other clients in the same room", () => {
      rooms.join("client-1", "room-alpha", fakeWs);
      rooms.join("client-2", "room-alpha", fakeWs);
      rooms.disconnect("client-1");

      expect(rooms.getRoomSize("room-alpha")).toBe(1);
    });

    it("is idempotent for unknown client IDs", () => {
      expect(() => rooms.disconnect("no-such-client")).not.toThrow();
    });
  });
});
