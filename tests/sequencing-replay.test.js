import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import { RoomManager } from "../src/room-manager.js";
import { validateMessage } from "../src/validator.js";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key";

function makeToken(clientId) {
  return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
}

describe("Message Sequencing & Replay Subsystem", () => {
  describe("RoomManager Sequence & Ring Buffer", () => {
    let rooms;
    let fakeWs;

    beforeEach(() => {
      rooms = new RoomManager({ ringBufferSize: 5, deduplicationWindowMs: 5000 });
      fakeWs = { readyState: 1, send: () => {} };
    });

    it("defaults sequence number to 0 for unbroadcasted room", () => {
      expect(rooms.getRoomSeq("room-1")).toBe(0);
      expect(rooms.getRingBuffer("room-1")).toEqual([]);
    });

    it("increments sequence number monotonically per broadcast", () => {
      rooms.join("c1", "room-1", fakeWs);
      rooms.broadcast("room-1", { type: "test", val: 1 });
      expect(rooms.getRoomSeq("room-1")).toBe(1);

      rooms.broadcast("room-1", { type: "test", val: 2 });
      expect(rooms.getRoomSeq("room-1")).toBe(2);

      const buffer = rooms.getRingBuffer("room-1");
      expect(buffer).toHaveLength(2);
      expect(buffer[0].seq).toBe(1);
      expect(buffer[0].payload).toEqual({ type: "test", val: 1 });
      expect(buffer[1].seq).toBe(2);
      expect(buffer[1].payload).toEqual({ type: "test", val: 2 });
    });

    it("evicts oldest messages when ring buffer size limit is exceeded", () => {
      rooms.join("c1", "room-1", fakeWs);
      for (let i = 1; i <= 7; i++) {
        rooms.broadcast("room-1", { type: "test", id: i });
      }

      expect(rooms.getRoomSeq("room-1")).toBe(7);
      const buffer = rooms.getRingBuffer("room-1");
      expect(buffer).toHaveLength(5);
      expect(buffer[0].seq).toBe(3);
      expect(buffer[4].seq).toBe(7);
    });

    it("evicts oldest messages when maxBufferBytes is exceeded", () => {
      const smallByteRooms = new RoomManager({
        ringBufferSize: 100,
        maxBufferBytes: 250, // Small memory capacity
      });

      smallByteRooms.join("c1", "room-1", fakeWs);
      // Large message payload
      for (let i = 1; i <= 5; i++) {
        smallByteRooms.broadcast("room-1", { type: "location_update", data: "x".repeat(50), index: i });
      }

      const buffer = smallByteRooms.getRingBuffer("room-1");
      expect(buffer.length).toBeLessThan(5);
      expect(smallByteRooms.getRoomSeq("room-1")).toBe(5);
    });
  });

  describe("Deduplication", () => {
    let rooms;
    let fakeWs;

    beforeEach(() => {
      rooms = new RoomManager({ deduplicationWindowMs: 1000, maxDedupEntries: 5 });
      fakeWs = { readyState: 1, send: vi.fn() };
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("drops duplicate location updates within the TTL window", () => {
      const ts = "2026-07-23T12:00:00.000Z";
      rooms.join("c1", "room-1", fakeWs);

      const msg = {
        type: "location_update",
        payload: { clientId: "veh-1", latitude: 10, longitude: 20, timestamp: ts },
      };

      rooms.broadcast("room-1", msg);
      expect(rooms.getRoomSeq("room-1")).toBe(1);

      // Sending identical location_update again
      rooms.broadcast("room-1", msg);
      expect(rooms.getRoomSeq("room-1")).toBe(1); // Not incremented, dropped
      expect(rooms.getRingBuffer("room-1")).toHaveLength(1);
    });

    it("allows location update after deduplication window expires", () => {
      const ts = "2026-07-23T12:00:00.000Z";
      rooms.join("c1", "room-1", fakeWs);

      const msg = {
        type: "location_update",
        payload: { clientId: "veh-1", latitude: 10, longitude: 20, timestamp: ts },
      };

      rooms.broadcast("room-1", msg);
      expect(rooms.getRoomSeq("room-1")).toBe(1);

      // Advance time past deduplication window (1000ms)
      vi.advanceTimersByTime(1001);

      rooms.broadcast("room-1", msg);
      expect(rooms.getRoomSeq("room-1")).toBe(2);
    });

    it("caps deduplication state size to maxDedupEntries", () => {
      const cappedRooms = new RoomManager({ maxDedupEntries: 2, deduplicationWindowMs: 5000 });
      cappedRooms.join("c1", "room-1", fakeWs);

      const msg1 = { type: "location_update", payload: { clientId: "c1", latitude: 1, longitude: 1, timestamp: "ts1" } };
      const msg2 = { type: "location_update", payload: { clientId: "c1", latitude: 2, longitude: 2, timestamp: "ts2" } };
      const msg3 = { type: "location_update", payload: { clientId: "c1", latitude: 3, longitude: 3, timestamp: "ts3" } };

      cappedRooms.broadcast("room-1", msg1);
      cappedRooms.broadcast("room-1", msg2);
      cappedRooms.broadcast("room-1", msg3); // Evicts msg1 key from dedup cache due to maxDedupEntries = 2

      // Re-broadcasting msg1 should now be accepted since msg1 key was evicted
      cappedRooms.broadcast("room-1", msg1);
      expect(cappedRooms.getRoomSeq("room-1")).toBe(4);
    });
  });

  describe("Reconnection Handshake Logic", () => {
    let rooms;

    beforeEach(() => {
      rooms = new RoomManager({ ringBufferSize: 5 });
    });

    it("returns replay_complete when lastSeq equals currentSeq", () => {
      const res = rooms.handleReconnect("room-1", 0);
      expect(res).toEqual({ type: "replay_complete", roomId: "room-1" });
    });

    it("returns replay with missed messages when lastSeq is within range", () => {
      const fakeWs = { readyState: 1, send: () => {} };
      rooms.join("c1", "room-1", fakeWs);

      rooms.broadcast("room-1", { type: "update", id: 1 });
      rooms.broadcast("room-1", { type: "update", id: 2 });
      rooms.broadcast("room-1", { type: "update", id: 3 });

      const res = rooms.handleReconnect("room-1", 1);
      expect(res.type).toBe("replay");
      expect(res.roomId).toBe("room-1");
      expect(res.currentSeq).toBe(3);
      expect(res.messages).toHaveLength(2);
      expect(res.messages[0].seq).toBe(2);
      expect(res.messages[1].seq).toBe(3);
    });

    it("returns replay_gap when lastSeq has been evicted", () => {
      const fakeWs = { readyState: 1, send: () => {} };
      rooms.join("c1", "room-1", fakeWs);

      // Broadcast 7 messages into a buffer of size 5 (seqs 3,4,5,6,7 remain)
      for (let i = 1; i <= 7; i++) {
        rooms.broadcast("room-1", { type: "update", id: i });
      }

      // Reconnecting with lastSeq = 1 (seq 2 was evicted)
      const res = rooms.handleReconnect("room-1", 1);
      expect(res).toEqual({
        type: "replay_gap",
        roomId: "room-1",
        fromSeq: 3,
        currentSeq: 7,
      });
    });
  });

  describe("Validator Integration for Reconnect", () => {
    it("accepts valid reconnect messages", () => {
      const result = validateMessage({
        type: "reconnect",
        roomId: "fleet-room",
        lastSeq: 42,
      });
      expect(result.ok).toBe(true);
      expect(result.data).toEqual({
        type: "reconnect",
        roomId: "fleet-room",
        lastSeq: 42,
      });
    });

    it("rejects reconnect messages missing lastSeq or roomId", () => {
      expect(validateMessage({ type: "reconnect", roomId: "fleet-room" }).ok).toBe(false);
      expect(validateMessage({ type: "reconnect", lastSeq: 10 }).ok).toBe(false);
      expect(validateMessage({ type: "reconnect", roomId: "", lastSeq: 10 }).ok).toBe(false);
      expect(validateMessage({ type: "reconnect", roomId: "r", lastSeq: -1 }).ok).toBe(false);
    });
  });

  describe("End-to-End Server Reconnect Handshake", () => {
    let serverObj;
    let port;

    beforeEach(() => {
      process.env.AUTH_SECRET = TEST_SECRET;
      serverObj = createServer({ port: 0, ringBufferSize: 50 });
      port = serverObj.wss.address().port;
    });

    afterEach(async () => {
      if (serverObj && serverObj.wss) {
        for (const client of serverObj.wss.clients) {
          client.terminate();
        }
        await new Promise((resolve) => serverObj.wss.close(resolve));
      }
      delete process.env.AUTH_SECRET;
    });

    function connectClient(clientId) {
      return new Promise((resolve, reject) => {
        const token = makeToken(clientId);
        const ws = new WebSocket(`ws://localhost:${port}?token=${token}`);
        ws.on("open", () => resolve(ws));
        ws.on("error", reject);
      });
    }

    function waitForMessage(ws) {
      return new Promise((resolve) => {
        ws.once("message", (raw) => resolve(JSON.parse(raw.toString())));
      });
    }

    it("rejects reconnect attempt if client has not joined the room", async () => {
      const client = await connectClient("client-unjoined");

      client.send(JSON.stringify({ type: "reconnect", roomId: "unjoined-room", lastSeq: 0 }));
      const response = await waitForMessage(client);

      expect(response).toEqual({
        type: "error",
        payload: { message: "Must join room before reconnecting" },
      });

      client.close();
    });

    it("performs complete replay handshake over WebSocket server", async () => {
      const publisher = await connectClient("publisher-1");
      const subscriber = await connectClient("subscriber-1");

      // Subscriber joins fleet room
      subscriber.send(JSON.stringify({ type: "join_room", roomId: "fleet-room" }));
      await waitForMessage(subscriber);

      // Publisher joins fleet room and sends two location updates
      publisher.send(JSON.stringify({ type: "join_room", roomId: "fleet-room" }));
      await waitForMessage(publisher);

      publisher.send(
        JSON.stringify({
          type: "location_update",
          payload: { latitude: 40.7128, longitude: -74.006 },
        })
      );
      await waitForMessage(subscriber); // Receive first broadcast

      publisher.send(
        JSON.stringify({
          type: "location_update",
          payload: { latitude: 40.713, longitude: -74.007 },
        })
      );
      await waitForMessage(subscriber); // Receive second broadcast

      // Subscriber sends reconnect with lastSeq = 1
      subscriber.send(JSON.stringify({ type: "reconnect", roomId: "fleet-room", lastSeq: 1 }));
      const replayResponse = await waitForMessage(subscriber);

      expect(replayResponse.type).toBe("replay");
      expect(replayResponse.roomId).toBe("fleet-room");
      expect(replayResponse.currentSeq).toBe(2);
      expect(replayResponse.messages).toHaveLength(1);
      expect(replayResponse.messages[0].seq).toBe(2);

      publisher.close();
      subscriber.close();
    });
  });
});
