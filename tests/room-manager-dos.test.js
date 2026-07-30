import { describe, it, expect, vi, afterEach } from "vitest";
import { RoomManager } from "../src/room-manager.js";

describe("RoomManager — DoS protection and limits", () => {
  let rooms;
  const fakeWs = { readyState: 1, send: () => {} };

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Constructor defaults & stats getter", () => {
    it("provides sane default stats initially", () => {
      rooms = new RoomManager();
      expect(rooms.stats).toEqual({
        roomCount: 0,
        clientCount: 0,
        totalMembers: 0,
        circuitBreakerState: "CLOSED",
      });
    });

    it("accurately tracks totalMembers across multiple joins and leaves", () => {
      rooms = new RoomManager();
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c1", "r2", fakeWs);
      rooms.join("c2", "r1", fakeWs);

      expect(rooms.stats).toEqual({
        roomCount: 2,
        clientCount: 2,
        totalMembers: 3,
        circuitBreakerState: "CLOSED",
      });

      rooms.leave("c1", "r1");
      expect(rooms.stats.totalMembers).toBe(2);

      rooms.disconnect("c1");
      expect(rooms.stats.totalMembers).toBe(1);
      expect(rooms.stats.clientCount).toBe(1);

      rooms.disconnect("c2");
      expect(rooms.stats.totalMembers).toBe(0);
      expect(rooms.stats.clientCount).toBe(0);
      expect(rooms.stats.roomCount).toBe(0);
    });
  });

  describe("Per-client room limit (maxRoomsPerClient)", () => {
    it("allows joining up to maxRoomsPerClient rooms", () => {
      rooms = new RoomManager({ maxRoomsPerClient: 3 });
      expect(rooms.join("c1", "r1", fakeWs)).toBeUndefined();
      expect(rooms.join("c1", "r2", fakeWs)).toBeUndefined();
      expect(rooms.join("c1", "r3", fakeWs)).toBeUndefined();
      expect(rooms.getClientRooms("c1").size).toBe(3);
    });

    it("rejects joins when maxRoomsPerClient is exceeded", () => {
      rooms = new RoomManager({ maxRoomsPerClient: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c1", "r2", fakeWs);

      const err = rooms.join("c1", "r3", fakeWs);
      expect(err).toEqual({
        type: "error",
        payload: {
          code: "ROOM_LIMIT_EXCEEDED",
          message: "Client room limit exceeded (2)",
        },
      });
      expect(rooms.getClientRooms("c1").size).toBe(2);
    });

    it("allows re-joining an already joined room even if at maxRoomsPerClient", () => {
      rooms = new RoomManager({ maxRoomsPerClient: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c1", "r2", fakeWs);

      const newWs = { readyState: 1, send: () => {} };
      expect(rooms.join("c1", "r1", newWs)).toBeUndefined();
      expect(rooms.getClientRooms("c1").size).toBe(2);
    });
  });

  describe("Per-room member limit (maxMembersPerRoom)", () => {
    it("allows joining a room up to maxMembersPerRoom", () => {
      rooms = new RoomManager({ maxMembersPerRoom: 2 });
      expect(rooms.join("c1", "r1", fakeWs)).toBeUndefined();
      expect(rooms.join("c2", "r1", fakeWs)).toBeUndefined();
      expect(rooms.getRoomSize("r1")).toBe(2);
    });

    it("rejects new members when maxMembersPerRoom is exceeded", () => {
      rooms = new RoomManager({ maxMembersPerRoom: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c2", "r1", fakeWs);

      const err = rooms.join("c3", "r1", fakeWs);
      expect(err).toEqual({
        type: "error",
        payload: {
          code: "ROOM_FULL",
          message: "Room member limit reached (2)",
        },
      });
      expect(rooms.getRoomSize("r1")).toBe(2);
    });

    it("allows existing member to update socket even if room is full", () => {
      rooms = new RoomManager({ maxMembersPerRoom: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c2", "r1", fakeWs);

      const updatedWs = { readyState: 1, send: () => {} };
      expect(rooms.join("c1", "r1", updatedWs)).toBeUndefined();
      expect(rooms.getRoomSize("r1")).toBe(2);
    });
  });

  describe("Total room count ceiling (maxRooms)", () => {
    it("allows creating new rooms up to maxRooms", () => {
      rooms = new RoomManager({ maxRooms: 2 });
      expect(rooms.join("c1", "r1", fakeWs)).toBeUndefined();
      expect(rooms.join("c2", "r2", fakeWs)).toBeUndefined();
      expect(rooms.roomCount).toBe(2);
    });

    it("rejects first join to a new room when maxRooms is reached", () => {
      rooms = new RoomManager({ maxRooms: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c2", "r2", fakeWs);

      const err = rooms.join("c3", "r3", fakeWs);
      expect(err).toEqual({
        type: "error",
        payload: {
          code: "MAX_ROOMS_REACHED",
          message: "Maximum room count ceiling reached (2)",
        },
      });
      expect(rooms.roomCount).toBe(2);
    });

    it("allows joins to existing rooms when maxRooms is reached", () => {
      rooms = new RoomManager({ maxRooms: 2 });
      rooms.join("c1", "r1", fakeWs);
      rooms.join("c2", "r2", fakeWs);

      expect(rooms.join("c3", "r1", fakeWs)).toBeUndefined();
      expect(rooms.getRoomSize("r1")).toBe(2);
    });
  });

  describe("Resource-pressure circuit breaker", () => {
    it("opens circuit breaker when heapUsed exceeds memoryThresholdBytes", () => {
      const memorySpy = vi.spyOn(process, "memoryUsage");
      memorySpy.mockReturnValue({ heapUsed: 1000, heapTotal: 2000, rss: 3000, external: 0, arrayBuffers: 0 });

      rooms = new RoomManager({
        circuitBreaker: {
          enabled: true,
          memoryThresholdBytes: 500,
          recoveryThresholdBytes: 300,
        },
      });

      const err = rooms.join("c1", "r1", fakeWs);
      expect(err).toEqual({
        type: "error",
        payload: {
          code: "CIRCUIT_BREAKER_OPEN",
          message: "Circuit breaker is open due to high resource pressure",
        },
      });
      expect(rooms.stats.circuitBreakerState).toBe("OPEN");
      expect(rooms.roomCount).toBe(0);
    });

    it("recovers and closes circuit breaker when heapUsed drops below recoveryThresholdBytes", () => {
      const memorySpy = vi.spyOn(process, "memoryUsage");

      rooms = new RoomManager({
        circuitBreaker: {
          enabled: true,
          memoryThresholdBytes: 500,
          recoveryThresholdBytes: 300,
        },
      });

      // Trip breaker
      memorySpy.mockReturnValue({ heapUsed: 600, heapTotal: 1000, rss: 2000, external: 0, arrayBuffers: 0 });
      const err = rooms.join("c1", "r1", fakeWs);
      expect(err?.payload?.code).toBe("CIRCUIT_BREAKER_OPEN");
      expect(rooms.stats.circuitBreakerState).toBe("OPEN");

      // Memory pressure drops to 250 (< 300 recovery threshold)
      memorySpy.mockReturnValue({ heapUsed: 250, heapTotal: 1000, rss: 2000, external: 0, arrayBuffers: 0 });
      const res = rooms.join("c1", "r1", fakeWs);
      expect(res).toBeUndefined();
      expect(rooms.stats.circuitBreakerState).toBe("CLOSED");
      expect(rooms.roomCount).toBe(1);
    });

    it("remains open if heapUsed is between recovery and memory thresholds", () => {
      const memorySpy = vi.spyOn(process, "memoryUsage");

      rooms = new RoomManager({
        circuitBreaker: {
          enabled: true,
          memoryThresholdBytes: 500,
          recoveryThresholdBytes: 300,
        },
      });

      // Trip breaker
      memorySpy.mockReturnValue({ heapUsed: 600, heapTotal: 1000, rss: 2000, external: 0, arrayBuffers: 0 });
      rooms.join("c1", "r1", fakeWs);
      expect(rooms.stats.circuitBreakerState).toBe("OPEN");

      // Memory drops to 400 (between 300 and 500)
      memorySpy.mockReturnValue({ heapUsed: 400, heapTotal: 1000, rss: 2000, external: 0, arrayBuffers: 0 });
      const err = rooms.join("c1", "r1", fakeWs);
      expect(err?.payload?.code).toBe("CIRCUIT_BREAKER_OPEN");
      expect(rooms.stats.circuitBreakerState).toBe("OPEN");
    });
  });
});
