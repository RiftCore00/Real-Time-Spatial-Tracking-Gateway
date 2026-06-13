import { describe, it, expect, beforeEach, vi } from "vitest";
import { RoomManager } from "../src/room-manager.js";

describe("RoomManager — extended coverage", () => {
  let rooms;

  beforeEach(() => {
    rooms = new RoomManager();
  });

  const openWs = () => ({ readyState: 1, send: vi.fn() });

  describe("join edge cases", () => {
    it("re-joining the same room updates the ws reference without duplicating entry", () => {
      const ws1 = openWs();
      const ws2 = openWs();
      rooms.join("c1", "room-a", ws1);
      rooms.join("c1", "room-a", ws2); // overwrite with new socket
      expect(rooms.getRoomSize("room-a")).toBe(1);
      // broadcast should reach ws2, not ws1
      rooms.broadcast("room-a", "ping");
      expect(ws2.send).toHaveBeenCalled();
      expect(ws1.send).not.toHaveBeenCalled();
    });

    it("joining multiple rooms increments roomCount correctly", () => {
      const ws = openWs();
      rooms.join("c1", "r1", ws);
      rooms.join("c1", "r2", ws);
      rooms.join("c1", "r3", ws);
      expect(rooms.roomCount).toBe(3);
      expect(rooms.clientCount).toBe(1);
    });
  });

  describe("leave edge cases", () => {
    it("leaving a room the client never joined does not throw", () => {
      expect(() => rooms.leave("ghost", "room-a")).not.toThrow();
    });

    it("leaving keeps client entry if client is still in other rooms", () => {
      const ws = openWs();
      rooms.join("c1", "r1", ws);
      rooms.join("c1", "r2", ws);
      rooms.leave("c1", "r1");
      expect(rooms.clientCount).toBe(1);
      expect(rooms.getClientRooms("c1")).toEqual(new Set(["r2"]));
    });

    it("removes client entry when last room is left", () => {
      const ws = openWs();
      rooms.join("c1", "r1", ws);
      rooms.leave("c1", "r1");
      expect(rooms.clientCount).toBe(0);
    });
  });

  describe("broadcast edge cases", () => {
    it("accepts a pre-serialised string message", () => {
      const ws = openWs();
      rooms.join("c1", "room-a", ws);
      rooms.broadcast("room-a", '{"type":"ping"}');
      expect(ws.send).toHaveBeenCalledWith('{"type":"ping"}');
    });

    it("skips connections with readyState CONNECTING (0)", () => {
      const ws = { readyState: 0, send: vi.fn() };
      rooms.join("c1", "room-a", ws);
      rooms.broadcast("room-a", "hi");
      expect(ws.send).not.toHaveBeenCalled();
    });

    it("skips connections with readyState CLOSING (2)", () => {
      const ws = { readyState: 2, send: vi.fn() };
      rooms.join("c1", "room-a", ws);
      rooms.broadcast("room-a", "hi");
      expect(ws.send).not.toHaveBeenCalled();
    });
  });

  describe("disconnect edge cases", () => {
    it("disconnect removes client from all rooms and decrements roomCount", () => {
      const ws = openWs();
      rooms.join("c1", "r1", ws);
      rooms.join("c1", "r2", ws);
      rooms.disconnect("c1");
      expect(rooms.roomCount).toBe(0);
      expect(rooms.clientCount).toBe(0);
    });

    it("disconnect of one client does not remove shared room when another client remains", () => {
      const ws = openWs();
      rooms.join("c1", "room-a", ws);
      rooms.join("c2", "room-a", openWs());
      rooms.disconnect("c1");
      expect(rooms.roomCount).toBe(1);
      expect(rooms.getRoomSize("room-a")).toBe(1);
    });
  });

  describe("getClientRooms isolation", () => {
    it("returns a copy — mutating result does not affect internal state", () => {
      const ws = openWs();
      rooms.join("c1", "r1", ws);
      const copy = rooms.getClientRooms("c1");
      copy.add("injected");
      expect(rooms.getClientRooms("c1").has("injected")).toBe(false);
    });
  });
});
