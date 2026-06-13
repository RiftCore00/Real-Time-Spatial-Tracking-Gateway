import { describe, it, expect } from "vitest";
import { validateMessage } from "../src/validator.js";

// Produce a location_update JSON string with approximately target bytes
function locationUpdateOfSize(targetBytes) {
  const base = JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 1 } });
  const needed = targetBytes - base.length - 20; // account for extra field structure
  const pad = needed > 0 ? "x".repeat(needed) : "";
  return JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 1, _pad: pad } });
}

// Produce a join_room JSON string with approximately target bytes
function joinRoomOfSize(targetBytes) {
  const overhead = JSON.stringify({ type: "join_room", roomId: "" }).length;
  const roomIdLen = Math.max(1, targetBytes - overhead);
  return JSON.stringify({ type: "join_room", roomId: "r".repeat(roomIdLen) });
}

describe("message size limits per type (issue 28)", () => {
  it("accepts location_update within 512 bytes", () => {
    const raw = JSON.stringify({ type: "location_update", payload: { latitude: 1, longitude: 1 } });
    expect(Buffer.byteLength(raw, "utf8")).toBeLessThanOrEqual(512);
    expect(validateMessage(raw).ok).toBe(true);
  });

  it("rejects location_update exceeding 512 bytes", () => {
    const raw = locationUpdateOfSize(600);
    expect(Buffer.byteLength(raw, "utf8")).toBeGreaterThan(512);
    const result = validateMessage(raw);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/512/);
  });

  it("accepts join_room within 256 bytes", () => {
    const raw = JSON.stringify({ type: "join_room", roomId: "fleet-alpha" });
    expect(Buffer.byteLength(raw, "utf8")).toBeLessThanOrEqual(256);
    expect(validateMessage(raw).ok).toBe(true);
  });

  it("rejects join_room exceeding 256 bytes", () => {
    const raw = joinRoomOfSize(300);
    expect(Buffer.byteLength(raw, "utf8")).toBeGreaterThan(256);
    const result = validateMessage(raw);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/256/);
  });

  it("accepts leave_room within 256 bytes", () => {
    const raw = JSON.stringify({ type: "leave_room", roomId: "fleet-alpha" });
    expect(validateMessage(raw).ok).toBe(true);
  });

  it("rejects leave_room exceeding 256 bytes", () => {
    const raw = JSON.stringify({ type: "leave_room", roomId: "x".repeat(240) });
    expect(Buffer.byteLength(raw, "utf8")).toBeGreaterThan(256);
    const result = validateMessage(raw);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/256/);
  });

  it("does not apply size limits when input is a plain object", () => {
    const obj = { type: "join_room", roomId: "x".repeat(240) };
    // should fail schema (roomId max 128), not size limit
    const result = validateMessage(obj);
    expect(result.ok).toBe(false);
    expect(result.error).not.toMatch(/size limit/);
  });
});
