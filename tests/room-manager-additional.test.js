import { describe, it, expect, beforeEach, vi } from "vitest";
import { RoomManager } from "../src/room-manager.js";

describe("RoomManager additional scenarios", () => {
  let rooms;
  let fakeWs1;
  let fakeWs2;

  beforeEach(() => {
    rooms = new RoomManager();
    fakeWs1 = { readyState: 1, send: vi.fn() };
    fakeWs2 = { readyState: 1, send: vi.fn() };
  });

  it("tracks room count correctly after multiple joins and leaves", () => {
    expect(rooms.roomCount).toBe(0);

    rooms.join("c1", "room-a", fakeWs1);
    expect(rooms.roomCount).toBe(1);

    rooms.join("c2", "room-b", fakeWs2);
    expect(rooms.roomCount).toBe(2);

    rooms.leave("c1", "room-a");
    expect(rooms.roomCount).toBe(1);

    rooms.leave("c2", "room-b");
    expect(rooms.roomCount).toBe(0);
  });

  it("tracks client count correctly", () => {
    expect(rooms.clientCount).toBe(0);

    rooms.join("c1", "room-a", fakeWs1);
    expect(rooms.clientCount).toBe(1);

    rooms.join("c2", "room-b", fakeWs2);
    expect(rooms.clientCount).toBe(2);

    rooms.disconnect("c1");
    expect(rooms.clientCount).toBe(1);

    rooms.disconnect("c2");
    expect(rooms.clientCount).toBe(0);
  });

  it("client in multiple rooms is counted once", () => {
    rooms.join("c1", "room-a", fakeWs1);
    rooms.join("c1", "room-b", fakeWs1);
    rooms.join("c1", "room-c", fakeWs1);

    expect(rooms.clientCount).toBe(1);
    expect(rooms.roomCount).toBe(3);
  });

  it("broadcast to empty room does nothing", () => {
    const spy = vi.spyOn(fakeWs1, "send");
    rooms.broadcast("empty-room", "hello");
    expect(spy).not.toHaveBeenCalled();
  });

  it("leaving a room the client is not in does not throw", () => {
    rooms.join("c1", "room-a", fakeWs1);
    expect(() => rooms.leave("c1", "room-b")).not.toThrow();
    expect(rooms.getRoomSize("room-a")).toBe(1);
  });

  it("returns empty set for unknown client rooms", () => {
    const result = rooms.getClientRooms("ghost");
    expect(result).toBeInstanceOf(Set);
    expect(result.size).toBe(0);
  });

  it("returns zero size for unknown room", () => {
    expect(rooms.getRoomSize("ghost")).toBe(0);
  });

  it("broadcast skips non-open connections", () => {
    const closedWs = { readyState: 3, send: vi.fn() };
    rooms.join("c1", "room-a", fakeWs1);
    rooms.join("c2", "room-a", closedWs);

    rooms.broadcast("room-a", "test");
    expect(fakeWs1.send).toHaveBeenCalled();
    expect(closedWs.send).not.toHaveBeenCalled();
  });
});
