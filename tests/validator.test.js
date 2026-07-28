import { describe, it, expect, vi, afterEach } from "vitest";
import { validateMessage } from "../src/validator.js";

describe("validateMessage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("location_update", () => {
    it("accepts a valid location payload", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 40.7128, longitude: -74.006 },
      });
      expect(result.ok).toBe(true);
    });

    it("accepts a location with all optional fields", () => {
      const now = new Date("2026-07-20T12:00:00Z");
      vi.spyOn(Date, "now").mockReturnValue(now.getTime());
      const result = validateMessage({
        type: "location_update",
        payload: {
          latitude: 40.7128,
          longitude: -74.006,
          altitude: 10.5,
          accuracy: 3.0,
          speed: 0.5,
          timestamp: now.toISOString(),
        },
      });
      expect(result.ok).toBe(true);
    });

    it("rejects latitude > 90", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 100, longitude: 0 },
      });
      expect(result.ok).toBe(false);
      expect(result.error).toBeTruthy();
    });

    it("rejects latitude < -90", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: -100, longitude: 0 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects longitude > 180", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 0, longitude: 200 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects longitude < -180", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 0, longitude: -200 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects non-numeric coordinates", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: "forty", longitude: -74 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects negative accuracy", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 0, longitude: 0, accuracy: -1 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects negative speed", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 0, longitude: 0, speed: -5 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects invalid timestamp format", () => {
      const result = validateMessage({
        type: "location_update",
        payload: {
          latitude: 0,
          longitude: 0,
          timestamp: "not-a-real-date",
        },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects missing latitude", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { longitude: 0 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects missing type field", () => {
      const result = validateMessage({
        payload: { latitude: 0, longitude: 0 },
      });
      expect(result.ok).toBe(false);
    });

    it("rejects unknown message type", () => {
      const result = validateMessage({
        type: "unknown_type",
        payload: { latitude: 0, longitude: 0 },
      });
      expect(result.ok).toBe(false);
    });
  });

  describe("join_room / leave_room", () => {
    it("accepts a valid join_room message", () => {
      const result = validateMessage({
        type: "join_room",
        roomId: "fleet-alpha",
      });
      expect(result.ok).toBe(true);
      expect(result.data.roomId).toBe("fleet-alpha");
    });

    it("accepts a valid leave_room message", () => {
      const result = validateMessage({
        type: "leave_room",
        roomId: "fleet-alpha",
      });
      expect(result.ok).toBe(true);
    });

    it("rejects join_room with empty roomId", () => {
      const result = validateMessage({
        type: "join_room",
        roomId: "",
      });
      expect(result.ok).toBe(false);
    });

    it("rejects join_room with missing roomId", () => {
      const result = validateMessage({ type: "join_room" });
      expect(result.ok).toBe(false);
    });
  });

  describe("malformed JSON", () => {
    it("rejects unparseable strings", () => {
      const result = validateMessage("this is not json");
      expect(result.ok).toBe(false);
      expect(result.error).toBe("Invalid JSON");
    });
  });

  describe("timestamp freshness", () => {
    it("accepts a timestamp within the skew window", () => {
      const now = new Date("2026-07-20T12:00:00Z");
      vi.spyOn(Date, "now").mockReturnValue(now.getTime());
      const result = validateMessage({
        type: "location_update",
        payload: {
          latitude: 40.7128,
          longitude: -74.006,
          timestamp: new Date(now.getTime() - 10000).toISOString(),
        },
      });
      expect(result.ok).toBe(true);
    });

    it("rejects a timestamp too old", () => {
      const now = new Date("2026-07-20T12:00:00Z");
      vi.spyOn(Date, "now").mockReturnValue(now.getTime());
      const result = validateMessage({
        type: "location_update",
        payload: {
          latitude: 40.7128,
          longitude: -74.006,
          timestamp: new Date(now.getTime() - 60000).toISOString(),
        },
      });
      expect(result.ok).toBe(false);
      expect(result.error).toBe("Timestamp is too old or too far in the future");
    });

    it("rejects a timestamp too far in the future", () => {
      const now = new Date("2026-07-20T12:00:00Z");
      vi.spyOn(Date, "now").mockReturnValue(now.getTime());
      const result = validateMessage({
        type: "location_update",
        payload: {
          latitude: 40.7128,
          longitude: -74.006,
          timestamp: new Date(now.getTime() + 60000).toISOString(),
        },
      });
      expect(result.ok).toBe(false);
      expect(result.error).toBe("Timestamp is too old or too far in the future");
    });

    it("accepts a location_update with no timestamp", () => {
      const result = validateMessage({
        type: "location_update",
        payload: { latitude: 40.7128, longitude: -74.006 },
      });
      expect(result.ok).toBe(true);
    });

    it("rejects any timestamp when MAX_TIMESTAMP_SKEW_MS is 0", () => {
      const original = process.env.MAX_TIMESTAMP_SKEW_MS;
      process.env.MAX_TIMESTAMP_SKEW_MS = "0";
      const now = new Date("2026-07-20T12:00:00Z");
      vi.spyOn(Date, "now").mockReturnValue(now.getTime());
      try {
        const result = validateMessage({
          type: "location_update",
          payload: {
            latitude: 40.7128,
            longitude: -74.006,
            timestamp: now.toISOString(),
          },
        });
        expect(result.ok).toBe(false);
        expect(result.error).toBe("Timestamp is too old or too far in the future");
      } finally {
        if (original === undefined) {
          delete process.env.MAX_TIMESTAMP_SKEW_MS;
        } else {
          process.env.MAX_TIMESTAMP_SKEW_MS = original;
        }
      }
    });
  });
});
