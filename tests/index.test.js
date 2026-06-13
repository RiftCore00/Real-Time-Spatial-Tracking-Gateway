import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { parseConfig, shutdown } from "../src/index.js";

describe("parseConfig", () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  it("returns defaults when env vars are absent", () => {
    delete process.env.PORT;
    delete process.env.WS_HEARTBEAT_MS;
    delete process.env.MAX_PAYLOAD_BYTES;
    const config = parseConfig();
    expect(config).toEqual({ port: 8080, heartbeatMs: 30000, maxPayloadBytes: 1024 });
  });

  it("reads values from environment variables", () => {
    process.env.PORT = "9090";
    process.env.WS_HEARTBEAT_MS = "15000";
    process.env.MAX_PAYLOAD_BYTES = "2048";
    const config = parseConfig();
    expect(config).toEqual({ port: 9090, heartbeatMs: 15000, maxPayloadBytes: 2048 });
  });

  it("returns integer values", () => {
    const { port, heartbeatMs, maxPayloadBytes } = parseConfig();
    expect(Number.isInteger(port)).toBe(true);
    expect(Number.isInteger(heartbeatMs)).toBe(true);
    expect(Number.isInteger(maxPayloadBytes)).toBe(true);
  });
});

describe("shutdown", () => {
  let mockWss;
  let exitSpy;

  beforeEach(() => {
    mockWss = { close: vi.fn((cb) => cb()) };
    exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("calls wss.close and then process.exit(0)", () => {
    shutdown(mockWss, "SIGTERM");
    expect(mockWss.close).toHaveBeenCalledOnce();
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  it("forces process.exit(1) after 5 seconds if server does not close", () => {
    mockWss.close = vi.fn(); // does NOT call callback
    shutdown(mockWss, "SIGINT");
    vi.advanceTimersByTime(5000);
    expect(exitSpy).toHaveBeenCalledWith(1);
  });
});
