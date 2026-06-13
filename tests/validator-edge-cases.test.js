import { describe, it, expect } from "vitest";
import { validateMessage } from "../src/validator.js";

describe("validateMessage — edge cases", () => {
  describe("boundary values", () => {
    it("accepts latitude exactly 90", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 90, longitude: 0 } });
      expect(r.ok).toBe(true);
    });

    it("accepts latitude exactly -90", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: -90, longitude: 0 } });
      expect(r.ok).toBe(true);
    });

    it("accepts longitude exactly 180", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 0, longitude: 180 } });
      expect(r.ok).toBe(true);
    });

    it("accepts longitude exactly -180", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 0, longitude: -180 } });
      expect(r.ok).toBe(true);
    });

    it("accepts accuracy exactly 0", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 0, longitude: 0, accuracy: 0 } });
      expect(r.ok).toBe(true);
    });

    it("accepts speed exactly 0", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 0, longitude: 0, speed: 0 } });
      expect(r.ok).toBe(true);
    });
  });

  describe("roomId boundary values", () => {
    it("accepts roomId of exactly 128 characters", () => {
      const r = validateMessage({ type: "join_room", roomId: "a".repeat(128) });
      expect(r.ok).toBe(true);
    });

    it("rejects roomId longer than 128 characters", () => {
      const r = validateMessage({ type: "join_room", roomId: "a".repeat(129) });
      expect(r.ok).toBe(false);
    });
  });

  describe("non-object and null inputs", () => {
    it("rejects null", () => {
      const r = validateMessage(null);
      expect(r.ok).toBe(false);
    });

    it("rejects a number", () => {
      const r = validateMessage(42);
      expect(r.ok).toBe(false);
    });

    it("rejects an array", () => {
      const r = validateMessage([]);
      expect(r.ok).toBe(false);
    });
  });

  describe("JSON string input", () => {
    it("accepts valid JSON string for location_update", () => {
      const r = validateMessage(JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 2 } }));
      expect(r.ok).toBe(true);
    });

    it("returns error for truncated JSON string", () => {
      const r = validateMessage('{"type": "location_update"');
      expect(r.ok).toBe(false);
      expect(r.error).toBe("Invalid JSON");
    });
  });

  describe("error message content", () => {
    it("returns a non-empty error string on schema failure", () => {
      const r = validateMessage({ type: "location_update", payload: { latitude: 200, longitude: 0 } });
      expect(r.ok).toBe(false);
      expect(typeof r.error).toBe("string");
      expect(r.error.length).toBeGreaterThan(0);
    });
  });
});
