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

  it("sends shutdown notification to all connected clients before closing", () => {
    const sendSpy = vi.fn();
    mockWss.clients = new Set([{ send: sendSpy, readyState: 1, bufferedAmount: 0 }]);
    shutdown(mockWss, "SIGTERM");
    vi.advanceTimersByTime(200);
    expect(sendSpy).toHaveBeenCalledOnce();
    expect(sendSpy).toHaveBeenCalledWith(JSON.stringify({ type: "server_shutting_down", payload: { reconnectIn: 5 } }));
  });

  it("waits for bufferedAmount === 0 before closing each client", async () => {
    let bufferedAmount = 100;
    const client = {
      readyState: 1,
      get bufferedAmount() { return bufferedAmount; },
      send: vi.fn(() => { bufferedAmount = 0; }),
      close: vi.fn(),
    };
    mockWss.clients = new Set([client]);
    shutdown(mockWss, "SIGTERM");
    vi.advanceTimersByTime(4200);
    expect(client.close).toHaveBeenCalledOnce();
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  it("sends close frame with code 1001", () => {
    const client = { readyState: 1, bufferedAmount: 0, send: vi.fn(), close: vi.fn() };
    mockWss.clients = new Set([client]);
    shutdown(mockWss, "SIGTERM");
    vi.advanceTimersByTime(4200);
    expect(client.close).toHaveBeenCalledWith(1001, "Going Away");
  });

  it("total shutdown time does not exceed 5 seconds even with unresponsive clients", () => {
    const client = {
      readyState: 1,
      bufferedAmount: 100,
      send: vi.fn(),
      close: vi.fn(),
    };
    mockWss.clients = new Set([client]);
    mockWss.close = vi.fn();
    shutdown(mockWss, "SIGTERM");
    vi.advanceTimersByTime(5000);
    expect(exitSpy).toHaveBeenCalledWith(1);
  });

  it("emits structured log entries at each phase transition", async () => {
    const { logger } = await import("../src/logger.js");
    const logSpy = vi.spyOn(logger, "info").mockImplementation(() => {});
    const mockWssLog = { close: vi.fn((cb) => cb()), clients: new Set() };
    shutdown(mockWssLog, "SIGTERM");
    vi.advanceTimersByTime(4200);
    expect(logSpy).toHaveBeenCalledWith("shutdown: stopping accept", { signal: "SIGTERM" });
    expect(logSpy).toHaveBeenCalledWith("shutdown: notifying N clients", { clientCount: 0 });
    expect(logSpy).toHaveBeenCalledWith("shutdown: draining N clients", { clientCount: 0 });
    expect(logSpy).toHaveBeenCalledWith("shutdown: closing N clients", { clientCount: 0 });
    logSpy.mockRestore();
  });
});
