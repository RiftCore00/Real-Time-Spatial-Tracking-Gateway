import { describe, it, expect, vi, beforeEach } from "vitest";
import { RoomManager } from "../src/room-manager.js";
import { validateMessage } from "../src/validator.js";

describe("full message flow integration", () => {
  let rooms;
  let fakeWs1;
  let fakeWs2;

  beforeEach(() => {
    rooms = new RoomManager();
    fakeWs1 = { readyState: 1, send: vi.fn() };
    fakeWs2 = { readyState: 1, send: vi.fn() };
  });

  it("valid message is accepted by validator and routed through room manager", () => {
    const raw = JSON.stringify({
      type: "location_update",
      payload: { latitude: 40.7128, longitude: -74.006, accuracy: 5 },
    });

    const validation = validateMessage(raw);
    expect(validation.ok).toBe(true);

    rooms.join("client-1", "room-alpha", fakeWs1);
    rooms.join("client-2", "room-alpha", fakeWs2);

    const msg = validation.data;
    rooms.broadcast("room-alpha", {
      type: "location_update",
      payload: { clientId: "client-1", ...msg.payload },
    }, "client-1");

    expect(fakeWs1.send).not.toHaveBeenCalled();
    expect(fakeWs2.send).toHaveBeenCalledTimes(1);

    const sent = JSON.parse(fakeWs2.send.mock.calls[0][0]);
    expect(sent.type).toBe("location_update");
    expect(sent.payload.latitude).toBe(40.7128);
    expect(sent.payload.longitude).toBe(-74.006);
    expect(sent.payload.clientId).toBe("client-1");
  });

  it("invalid message is rejected by validator and not broadcast", () => {
    const raw = JSON.stringify({
      type: "location_update",
      payload: { latitude: 200, longitude: 0 },
    });

    const validation = validateMessage(raw);
    expect(validation.ok).toBe(false);
    expect(validation.error).toBeTruthy();
  });

  it("client joins room, sends location, and leaves", () => {
    rooms.join("client-1", "room-alpha", fakeWs1);
    rooms.join("client-2", "room-alpha", fakeWs2);

    const raw = JSON.stringify({
      type: "location_update",
      payload: { latitude: 34.0522, longitude: -118.2437 },
    });
    const validation = validateMessage(raw);
    expect(validation.ok).toBe(true);

    rooms.broadcast("room-alpha", {
      type: "location_update",
      payload: { clientId: "client-1", ...validation.data.payload },
    }, "client-1");

    expect(fakeWs2.send).toHaveBeenCalledTimes(1);

    rooms.leave("client-1", "room-alpha");
    expect(rooms.getRoomSize("room-alpha")).toBe(1);
  });

  it("handles concurrent rooms independently", () => {
    rooms.join("client-1", "room-alpha", fakeWs1);
    rooms.join("client-2", "room-beta", fakeWs2);

    const validation1 = validateMessage(JSON.stringify({
      type: "location_update",
      payload: { latitude: 40.0, longitude: -74.0 },
    }));
    rooms.broadcast("room-alpha", validation1.data, "client-1");
    expect(fakeWs2.send).not.toHaveBeenCalled();

    const validation2 = validateMessage(JSON.stringify({
      type: "location_update",
      payload: { latitude: 34.0, longitude: -118.0 },
    }));
    rooms.broadcast("room-beta", validation2.data, "client-2");
    expect(fakeWs1.send).not.toHaveBeenCalled();
  });
});
