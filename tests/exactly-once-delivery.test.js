import { describe, it, expect, vi, beforeEach } from "vitest";
import { validate as uuidValidate } from "uuid";
import { RoomManager } from "../src/room-manager.js";
import { validateMessage } from "../src/validator.js";

describe("Exactly-once delivery (issue #226)", () => {
  let rooms;
  let fakeWs1;
  let fakeWs2;
  let fakeWs3;

  beforeEach(() => {
    rooms = new RoomManager({ ackWindowSize: 5 });
    fakeWs1 = { readyState: 1, send: vi.fn() };
    fakeWs2 = { readyState: 1, send: vi.fn() };
    fakeWs3 = { readyState: 1, send: vi.fn() };
  });

  describe("broadcast() assigns UUID v7 messageId", () => {
    it("generates a valid UUID v7 messageId for each broadcast", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { latitude: 1, longitude: 2 } }, "c1");

      const ringBuffer = rooms.getRingBuffer("room-a");
      expect(ringBuffer).toHaveLength(1);
      expect(uuidValidate(ringBuffer[0].messageId)).toBe(true);

      const parts = ringBuffer[0].messageId.split("-");
      expect(parts[0]).toHaveLength(8);
      expect(parts[1]).toHaveLength(4);
      expect(parts[2].startsWith("7")).toBe(true);
    });

    it("each broadcast gets a distinct messageId", () => {
      rooms.join("c1", "room-a", fakeWs1);

      rooms.broadcast("room-a", { type: "location_update", payload: { latitude: 1 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { latitude: 2 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { latitude: 3 } }, "c1");

      const ringBuffer = rooms.getRingBuffer("room-a");
      expect(ringBuffer).toHaveLength(3);
      const ids = new Set(ringBuffer.map((e) => e.messageId));
      expect(ids.size).toBe(3);
    });
  });

  describe("ring buffer stores { seq, messageId, payload, timestamp }", () => {
    it("each entry has all required fields", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.broadcast("room-a", { type: "location_update", payload: { latitude: 1, longitude: 2 } }, "c1");

      const buffer = rooms.getRingBuffer("room-a");
      expect(buffer).toHaveLength(1);

      const entry = buffer[0];
      expect(typeof entry.seq).toBe("number");
      expect(typeof entry.messageId).toBe("string");
      expect(typeof entry.payload).toBe("object");
      expect(typeof entry.timestamp).toBe("number");
      expect(entry.seq).toBe(1);
      expect(entry.payload.type).toBe("location_update");
      expect(entry.payload.payload.latitude).toBe(1);
      expect(entry.timestamp).toBeGreaterThan(0);
    });

    it("sequence numbers increment monotonically", () => {
      rooms.join("c1", "room-a", fakeWs1);

      rooms.broadcast("room-a", { type: "a" }, "c1");
      rooms.broadcast("room-a", { type: "b" }, "c1");
      rooms.broadcast("room-a", { type: "c" }, "c1");

      const buffer = rooms.getRingBuffer("room-a");
      expect(buffer[0].seq).toBe(1);
      expect(buffer[1].seq).toBe(2);
      expect(buffer[2].seq).toBe(3);
    });

    it("different rooms have independent sequences", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c1", "room-b", fakeWs2);

      rooms.broadcast("room-a", { type: "a1" }, "c1");
      rooms.broadcast("room-b", { type: "b1" }, "c1");
      rooms.broadcast("room-a", { type: "a2" }, "c1");
      rooms.broadcast("room-b", { type: "b2" }, "c1");

      expect(rooms.getRingBuffer("room-a").map((e) => e.seq)).toEqual([1, 2]);
      expect(rooms.getRingBuffer("room-b").map((e) => e.seq)).toEqual([1, 2]);
    });

    it("getRoomSeq returns current sequence", () => {
      rooms.join("c1", "room-a", fakeWs1);
      expect(rooms.getRoomSeq("room-a")).toBe(0);

      rooms.broadcast("room-a", { type: "a" }, "c1");
      expect(rooms.getRoomSeq("room-a")).toBe(1);

      rooms.broadcast("room-a", { type: "b" }, "c1");
      expect(rooms.getRoomSeq("room-a")).toBe(2);
    });

    it("ring buffer respects size limit", () => {
      const small = new RoomManager({ ringBufferSize: 3 });
      small.join("c1", "room-a", fakeWs1);

      for (let i = 0; i < 5; i++) {
        small.broadcast("room-a", { type: `msg-${i}` }, "c1");
      }

      const buffer = small.getRingBuffer("room-a");
      expect(buffer).toHaveLength(3);
      expect(buffer[0].seq).toBe(3);
      expect(buffer[1].seq).toBe(4);
      expect(buffer[2].seq).toBe(5);
    });
  });

  describe("client ACK processing", () => {
    it("ack updates highestAckedSeq and decrements unackedCount", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      for (let i = 0; i < 3; i++) {
        rooms.broadcast("room-a", { type: "location_update", payload: { seq: i } }, "c1");
      }

      const ackStateBefore = rooms._ackState.get("c2")?.get("room-a");
      expect(ackStateBefore.unackedCount).toBe(3);

      rooms.ack("c2", "room-a", 2);

      const ackStateAfter = rooms._ackState.get("c2")?.get("room-a");
      expect(ackStateAfter.highestAckedSeq).toBe(2);
      expect(ackStateAfter.unackedCount).toBe(1);
    });

    it("ack for lower seq does not regress highestAckedSeq", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      for (let i = 0; i < 5; i++) {
        rooms.broadcast("room-a", { type: "location_update", payload: { seq: i } }, "c1");
      }

      rooms.ack("c2", "room-a", 4);
      rooms.ack("c2", "room-a", 2);

      const ackState = rooms._ackState.get("c2")?.get("room-a");
      expect(ackState.highestAckedSeq).toBe(4);
      expect(ackState.unackedCount).toBe(1);
    });

    it("ack for all messages results in zero unackedCount", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      for (let i = 0; i < 3; i++) {
        rooms.broadcast("room-a", { type: "location_update", payload: { seq: i } }, "c1");
      }

      rooms.ack("c2", "room-a", 3);

      const ackState = rooms._ackState.get("c2")?.get("room-a");
      expect(ackState.unackedCount).toBe(0);
    });
  });

  describe("flow control (ackWindowSize)", () => {
    it("pauses delivery when unackedCount reaches ackWindowSize", () => {
      const bw = new RoomManager({ ackWindowSize: 2 });
      bw.join("c1", "room-a", fakeWs1);
      bw.join("c2", "room-a", fakeWs2);

      bw.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      bw.broadcast("room-a", { type: "location_update", payload: { v: 2 } }, "c1");

      expect(fakeWs2.send).toHaveBeenCalledTimes(2);

      bw.broadcast("room-a", { type: "location_update", payload: { v: 3 } }, "c1");

      expect(fakeWs2.send).toHaveBeenCalledTimes(2);
      const ackState = bw._ackState.get("c2")?.get("room-a");
      expect(ackState.paused).toBe(true);
    });

    it("resumes delivery after ack frees window space", () => {
      const bw = new RoomManager({ ackWindowSize: 2 });
      bw.join("c1", "room-a", fakeWs1);
      bw.join("c2", "room-a", fakeWs2);

      bw.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      bw.broadcast("room-a", { type: "location_update", payload: { v: 2 } }, "c1");

      expect(fakeWs2.send).toHaveBeenCalledTimes(2);

      bw.broadcast("room-a", { type: "location_update", payload: { v: 3 } }, "c1");
      expect(fakeWs2.send).toHaveBeenCalledTimes(2);

      bw.ack("c2", "room-a", 2);

      bw.broadcast("room-a", { type: "location_update", payload: { v: 4 } }, "c1");
      expect(fakeWs2.send).toHaveBeenCalledTimes(3);
    });

    it("with Infinity ackWindowSize, no pausing occurs", () => {
      const infinite = new RoomManager({ ackWindowSize: Infinity });
      infinite.join("c1", "room-a", fakeWs1);
      infinite.join("c2", "room-a", fakeWs2);

      for (let i = 0; i < 100; i++) {
        infinite.broadcast("room-a", { type: "location_update", payload: { v: i } }, "c1");
      }

      expect(fakeWs2.send).toHaveBeenCalledTimes(100);
      const ackState = infinite._ackState.get("c2")?.get("room-a");
      expect(ackState).toBeUndefined();
    });
  });

  describe("NACK re-sends specific message", () => {
    it("re-sends the message at the given seq to the requesting client", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 2 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 3 } }, "c1");

      fakeWs2.send.mockClear();

      rooms.nack("c2", "room-a", 2);

      expect(fakeWs2.send).toHaveBeenCalledTimes(1);
      const sent = JSON.parse(fakeWs2.send.mock.calls[0][0]);
      expect(sent.seq).toBe(2);
      expect(sent.payload.payload.v).toBe(2);
    });

    it("NACK does not send to other clients", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);
      rooms.join("c3", "room-a", fakeWs3);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      fakeWs2.send.mockClear();
      fakeWs3.send.mockClear();

      rooms.nack("c2", "room-a", 1);

      expect(fakeWs2.send).toHaveBeenCalledTimes(1);
      expect(fakeWs3.send).not.toHaveBeenCalled();
    });

    it("NACK for non-existent seq does nothing", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      fakeWs2.send.mockClear();

      rooms.nack("c2", "room-a", 999);

      expect(fakeWs2.send).not.toHaveBeenCalled();
    });
  });

  describe("reconnect replay (handleReconnect)", () => {
    it("returns replay_complete when client is up to date", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      const result = rooms.handleReconnect("room-a", 1);
      expect(result.type).toBe("replay_complete");
      expect(result.roomId).toBe("room-a");
    });

    it("returns replay with missed messages", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 2 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 3 } }, "c1");

      const result = rooms.handleReconnect("room-a", 1);
      expect(result.type).toBe("replay");
      expect(result.roomId).toBe("room-a");
      expect(result.messages).toHaveLength(2);
      expect(result.messages[0].seq).toBe(2);
      expect(result.messages[1].seq).toBe(3);
      expect(result.currentSeq).toBe(3);
    });

    it("replay entries include messageId and timestamp", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      const result = rooms.handleReconnect("room-a", 0);
      expect(result.type).toBe("replay");
      const entry = result.messages[0];
      expect(typeof entry.messageId).toBe("string");
      expect(uuidValidate(entry.messageId)).toBe(true);
      expect(typeof entry.timestamp).toBe("number");
      expect(entry.seq).toBe(1);
    });

    it("uses highestAckedSeq to replay from correct position", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 2 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 3 } }, "c1");
      rooms.broadcast("room-a", { type: "location_update", payload: { v: 4 } }, "c1");

      const result = rooms.handleReconnect("room-a", 4, 2);
      expect(result.type).toBe("replay");
      expect(result.messages).toHaveLength(2);
      expect(result.messages[0].seq).toBe(3);
      expect(result.messages[1].seq).toBe(4);
    });

    it("returns replay_gap when messages have been evicted from buffer", () => {
      const small = new RoomManager({ ringBufferSize: 2, ackWindowSize: Infinity });
      small.join("c1", "room-a", fakeWs1);

      small.broadcast("room-a", { type: "a" }, "c1");
      small.broadcast("room-a", { type: "b" }, "c1");
      small.broadcast("room-a", { type: "c" }, "c1");

      const result = small.handleReconnect("room-a", 0);
      expect(result.type).toBe("replay_gap");
      expect(result.fromSeq).toBe(2);
      expect(result.currentSeq).toBe(3);
    });

    it("replay for different rooms is independent", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c1", "room-b", fakeWs2);

      rooms.broadcast("room-a", { type: "a1" }, "c1");
      rooms.broadcast("room-b", { type: "b1" }, "c1");
      rooms.broadcast("room-a", { type: "a2" }, "c1");

      const ra = rooms.handleReconnect("room-a", 0);
      const rb = rooms.handleReconnect("room-b", 0);

      expect(ra.messages).toHaveLength(2);
      expect(rb.messages).toHaveLength(1);
    });
  });

  describe("client disconnect cleans up ACK state", () => {
    it("removes ack state for disconnected client", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      expect(rooms._ackState.has("c2")).toBe(true);

      rooms.disconnect("c2");

      expect(rooms._ackState.has("c2")).toBe(false);
    });

    it("does not remove ack state for other clients", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);
      rooms.join("c3", "room-a", fakeWs3);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      rooms.disconnect("c2");

      expect(rooms._ackState.has("c3")).toBe(true);
    });
  });

  describe("server-side deduplication", () => {
    it("deduplicates location_update messages with same clientId+timestamp", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      const msg = {
        type: "location_update",
        payload: { clientId: "c1", latitude: 1, longitude: 2, timestamp: "2025-01-01T00:00:00.000Z" },
      };

      rooms.broadcast("room-a", msg, "c1");
      rooms.broadcast("room-a", msg, "c1");

      expect(fakeWs2.send).toHaveBeenCalledTimes(1);
      expect(rooms.getRingBuffer("room-a")).toHaveLength(1);
    });

    it("does not deduplicate messages with different timestamps", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", {
        type: "location_update",
        payload: { clientId: "c1", latitude: 1, longitude: 2, timestamp: "2025-01-01T00:00:00.000Z" },
      }, "c1");
      rooms.broadcast("room-a", {
        type: "location_update",
        payload: { clientId: "c1", latitude: 1, longitude: 2, timestamp: "2025-01-01T00:00:01.000Z" },
      }, "c1");

      expect(fakeWs2.send).toHaveBeenCalledTimes(2);
      expect(rooms.getRingBuffer("room-a")).toHaveLength(2);
    });
  });

  describe("validator schema for ack/nack", () => {
    it("validates ack message correctly", () => {
      const result = validateMessage(JSON.stringify({ type: "ack", roomId: "room-a", seq: 5 }));
      expect(result.ok).toBe(true);
      expect(result.data.type).toBe("ack");
      expect(result.data.roomId).toBe("room-a");
      expect(result.data.seq).toBe(5);
    });

    it("validates nack message correctly", () => {
      const result = validateMessage(JSON.stringify({ type: "nack", roomId: "room-a", seq: 3, reason: "corrupt" }));
      expect(result.ok).toBe(true);
      expect(result.data.type).toBe("nack");
      expect(result.data.reason).toBe("corrupt");
    });

    it("rejects ack without roomId", () => {
      const result = validateMessage(JSON.stringify({ type: "ack", seq: 5 }));
      expect(result.ok).toBe(false);
    });

    it("rejects ack without seq", () => {
      const result = validateMessage(JSON.stringify({ type: "ack", roomId: "room-a" }));
      expect(result.ok).toBe(false);
    });

    it("validates reconnect with optional highestAckedSeq", () => {
      const result = validateMessage(JSON.stringify({ type: "reconnect", roomId: "room-a", lastSeq: 10, highestAckedSeq: 5 }));
      expect(result.ok).toBe(true);
      expect(result.data.highestAckedSeq).toBe(5);
    });

    it("validates reconnect without highestAckedSeq", () => {
      const result = validateMessage(JSON.stringify({ type: "reconnect", roomId: "room-a", lastSeq: 10 }));
      expect(result.ok).toBe(true);
      expect(result.data.highestAckedSeq).toBeUndefined();
    });
  });

  describe("join() with ackWindowSize", () => {
    it("initializes ack state on first broadcast to a client", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      const ackState = rooms._ackState.get("c2")?.get("room-a");
      expect(ackState).toBeDefined();
      expect(ackState.highestAckedSeq).toBe(0);
      expect(ackState.unackedCount).toBe(1);
      expect(ackState.paused).toBe(false);
    });

    it("broadcast excludes sender from ack tracking", () => {
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c2", "room-a", fakeWs2);

      rooms.broadcast("room-a", { type: "location_update", payload: { v: 1 } }, "c1");

      const senderAck = rooms._ackState.get("c1")?.get("room-a");
      expect(senderAck).toBeUndefined();
    });
  });

  describe("leave() cleans up ACK state", () => {
    it("removes ack state for the left room", () => {
      const ws4 = { readyState: 1, send: vi.fn() };
      rooms.join("c1", "room-a", fakeWs1);
      rooms.join("c1", "room-b", fakeWs2);
      rooms.join("c2", "room-a", fakeWs3);
      rooms.join("c3", "room-b", ws4);

      rooms.broadcast("room-a", { type: "a" }, "c2");
      rooms.broadcast("room-b", { type: "b" }, "c3");

      rooms.leave("c1", "room-a");

      const clientAck = rooms._ackState.get("c1");
      expect(clientAck).toBeDefined();
      expect(clientAck.has("room-a")).toBe(false);
      expect(clientAck.has("room-b")).toBe(true);
    });
  });
});
