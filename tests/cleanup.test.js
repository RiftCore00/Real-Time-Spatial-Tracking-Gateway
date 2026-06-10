import { describe, it, expect, vi, beforeEach } from "vitest";
import { RoomManager } from "../src/room-manager.js";

describe("Client disconnection cleanup", () => {
  let rooms;
  let fakeWs;

  beforeEach(() => {
    rooms = new RoomManager();
    fakeWs = { readyState: 1, send: vi.fn() };
  });

  it("removes the client from all room subscriptions on disconnect", () => {
    rooms.join("client-1", "room-alpha", fakeWs);
    rooms.join("client-1", "room-beta", fakeWs);
    rooms.disconnect("client-1");

    expect(rooms.getClientRooms("client-1").size).toBe(0);
    expect(rooms.getRoomSize("room-alpha")).toBe(0);
    expect(rooms.getRoomSize("room-beta")).toBe(0);
  });

  it("releases the in-memory reference to the disconnected client", () => {
    rooms.join("client-1", "room-alpha", fakeWs);
    rooms.disconnect("client-1");

    expect(rooms.clientCount).toBe(0);
  });

  it("does not hold the client reference after disconnect", () => {
    const clientId = "client-1";
    rooms.join(clientId, "room-alpha", fakeWs);
    rooms.disconnect(clientId);

    const roomsAfter = rooms.getClientRooms(clientId);
    expect(roomsAfter).toBeInstanceOf(Set);
    expect(roomsAfter.size).toBe(0);

    expect(rooms.roomCount).toBe(0);
  });

  it("only removes the disconnected client, leaving others intact", () => {
    const ws1 = { readyState: 1, send: vi.fn() };
    const ws2 = { readyState: 1, send: vi.fn() };

    rooms.join("client-1", "room-alpha", ws1);
    rooms.join("client-2", "room-alpha", ws2);

    rooms.disconnect("client-1");

    expect(rooms.getRoomSize("room-alpha")).toBe(1);
    expect(rooms.clientCount).toBe(1);
  });

  it("serial disconnect of all clients empties the manager", () => {
    rooms.join("c1", "room-alpha", fakeWs);
    rooms.join("c2", "room-alpha", fakeWs);
    rooms.join("c3", "room-beta", fakeWs);

    rooms.disconnect("c1");
    rooms.disconnect("c2");
    rooms.disconnect("c3");

    expect(rooms.clientCount).toBe(0);
    expect(rooms.roomCount).toBe(0);
  });

  it("re-joining after disconnect works cleanly", () => {
    rooms.join("client-1", "room-alpha", fakeWs);
    rooms.disconnect("client-1");
    rooms.join("client-1", "room-alpha", fakeWs);

    expect(rooms.getRoomSize("room-alpha")).toBe(1);
    expect(rooms.clientCount).toBe(1);
  });
});
