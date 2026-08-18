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

describe("RoomManager with backpressure enabled", () => {
  let bpRooms;
  let ws1;
  let ws2;

  beforeEach(() => {
    bpRooms = new RoomManager({
      enabled: true,
      highWaterMark: 100,
      slowConsumerTimeout: 5000,
      batchSize: 10,
    });
    ws1 = { readyState: 1, send: vi.fn(), bufferedAmount: 0 };
    ws2 = { readyState: 1, send: vi.fn(), bufferedAmount: 0 };
  });

  const flush = () => new Promise((resolve) => setImmediate(resolve));

  it("broadcasts to all members when backpressure is enabled", async () => {
    bpRooms.join("c1", "room-a", ws1);
    bpRooms.join("c2", "room-a", ws2);
    bpRooms.broadcast("room-a", { type: "ping" });
    await flush();
    expect(ws1.send).toHaveBeenCalledWith(JSON.stringify({ type: "ping" }));
    expect(ws2.send).toHaveBeenCalledWith(JSON.stringify({ type: "ping" }));
  });

  it("excludes sender when backpressure is enabled", async () => {
    bpRooms.join("c1", "room-a", ws1);
    bpRooms.join("c2", "room-a", ws2);
    bpRooms.broadcast("room-a", { type: "ping" }, "c1");
    await flush();
    expect(ws1.send).not.toHaveBeenCalled();
    expect(ws2.send).toHaveBeenCalled();
  });

  it("flags slow consumer and coalesces location_update messages", async () => {
    const slowWs = { readyState: 1, send: vi.fn(), bufferedAmount: 200 };
    bpRooms.join("c1", "room-a", ws1);
    bpRooms.join("c2", "room-a", slowWs);

    bpRooms.broadcast("room-a", {
      type: "location_update",
      payload: { latitude: 1, longitude: 2 },
    });
    await flush();

    expect(ws1.send).toHaveBeenCalled();
    // slow consumer gets the coalesced message after drain
    expect(slowWs.send).toHaveBeenCalledWith(
      JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 2 } })
    );

    const stats = bpRooms.getRoomStats("room-a");
    expect(stats.slowConsumers).toContain("c2");
  });

  it("non-location messages are sent even to slow consumers", async () => {
    const slowWs = { readyState: 1, send: vi.fn(), bufferedAmount: 200 };
    bpRooms.join("c2", "room-a", slowWs);

    bpRooms.broadcast("room-a", { type: "text", payload: "hello" });
    await flush();

    expect(slowWs.send).toHaveBeenCalled();
  });

  it("getRoomStats returns member count and queue depths", () => {
    bpRooms.join("c1", "room-a", ws1);
    bpRooms.join("c2", "room-a", ws2);

    const stats = bpRooms.getRoomStats("room-a");
    expect(stats.memberCount).toBe(2);
    expect(stats.sendQueueDepths.c1).toBe(0);
    expect(stats.sendQueueDepths.c2).toBe(0);
    expect(stats.slowConsumers).toEqual([]);
  });

  it("getRoomStats for non-existent room returns zeros", () => {
    const stats = bpRooms.getRoomStats("ghost");
    expect(stats).toEqual({ memberCount: 0, sendQueueDepths: {}, slowConsumers: [] });
  });

  it("getRoomStats without backpressure enabled returns empty stats", () => {
    const plain = new RoomManager();
    plain.join("c1", "room-a", ws1);
    const stats = plain.getRoomStats("room-a");
    expect(stats.memberCount).toBe(1);
    expect(stats.sendQueueDepths).toEqual({});
    expect(stats.slowConsumers).toEqual([]);
  });

  it("cleans up slow consumer state on disconnect", async () => {
    const slowWs = { readyState: 1, send: vi.fn(), bufferedAmount: 200 };
    bpRooms.join("c2", "room-a", slowWs);

    bpRooms.broadcast("room-a", { type: "location_update", payload: {} });
    await flush();

    const stats = bpRooms.getRoomStats("room-a");
    expect(stats.slowConsumers).toContain("c2");

    bpRooms.disconnect("c2");

    const stats2 = bpRooms.getRoomStats("room-a");
    expect(stats2.slowConsumers).toEqual([]);
    expect(stats2.memberCount).toBe(0);
  });

  it("cleans up slow consumer state on leave when no rooms remain", async () => {
    const slowWs = { readyState: 1, send: vi.fn(), bufferedAmount: 200 };
    bpRooms.join("c2", "room-a", slowWs);

    bpRooms.broadcast("room-a", { type: "location_update", payload: {} });
    await flush();

    bpRooms.leave("c2", "room-a");

    const stats = bpRooms.getRoomStats("room-a");
    expect(stats.slowConsumers).toEqual([]);
  });

  it("broadcast to empty room does not throw with backpressure", async () => {
    expect(() => bpRooms.broadcast("empty-room", "hello")).not.toThrow();
    await flush();
  });

  it("slow consumer timeout closes the connection", async () => {
    const closeFn = vi.fn();
    const slowWs = { readyState: 1, send: vi.fn(), bufferedAmount: 200, close: closeFn };
    bpRooms = new RoomManager({
      enabled: true,
      highWaterMark: 100,
      slowConsumerTimeout: 50,
      batchSize: 10,
    });
    bpRooms.join("c1", "room-a", slowWs);

    bpRooms.broadcast("room-a", { type: "location_update", payload: {} });
    await flush();

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(closeFn).toHaveBeenCalledWith(4000, "Slow consumer");
  });
});
