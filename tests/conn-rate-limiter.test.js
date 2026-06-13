import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createConnRateLimiter } from "../src/conn-rate-limiter.js";
import { createServer } from "../src/server.js";

describe("createConnRateLimiter", () => {
  it("allows connections up to the limit within a minute", () => {
    const limiter = createConnRateLimiter(3);
    expect(limiter.check("1.1.1.1")).toBe(true);
    expect(limiter.check("1.1.1.1")).toBe(true);
    expect(limiter.check("1.1.1.1")).toBe(true);
  });

  it("blocks connections exceeding the limit", () => {
    const limiter = createConnRateLimiter(2);
    limiter.check("2.2.2.2");
    limiter.check("2.2.2.2");
    expect(limiter.check("2.2.2.2")).toBe(false);
  });

  it("different IPs are independent", () => {
    const limiter = createConnRateLimiter(1);
    expect(limiter.check("3.3.3.3")).toBe(true);
    expect(limiter.check("4.4.4.4")).toBe(true);
    expect(limiter.check("3.3.3.3")).toBe(false);
  });

  it("defaults to CONN_RATE_LIMIT env var", () => {
    process.env.CONN_RATE_LIMIT = "1";
    const limiter = createConnRateLimiter();
    expect(limiter.check("5.5.5.5")).toBe(true);
    expect(limiter.check("5.5.5.5")).toBe(false);
    delete process.env.CONN_RATE_LIMIT;
  });
});

describe("server connection rate limiting (issue 27)", () => {
  let server;

  function makeReq(ip = "10.0.0.1") {
    return { url: "/?token=test", socket: { remoteAddress: ip } };
  }

  function makeWs() {
    return { isAlive: false, close: vi.fn(), send: vi.fn(), on: vi.fn() };
  }

  beforeEach(() => {
    server = createServer({ port: 0, connRateLimit: 2 });
  });

  afterEach(() => new Promise((res) => server.wss.close(res)));

  it("rejects with 4029 when connection rate exceeded", () => {
    const ip = "20.0.0.1";
    const ws1 = makeWs(); const ws2 = makeWs(); const ws3 = makeWs();
    server.wss.emit("connection", ws1, makeReq(ip));
    server.wss.emit("connection", ws2, makeReq(ip));
    server.wss.emit("connection", ws3, makeReq(ip));
    expect(ws3.close).toHaveBeenCalledWith(4029, expect.any(String));
  });

  it("allows connections under the rate limit", () => {
    const ws1 = makeWs(); const ws2 = makeWs();
    server.wss.emit("connection", ws1, makeReq("30.0.0.1"));
    server.wss.emit("connection", ws2, makeReq("30.0.0.2"));
    const rejected = [ws1, ws2].filter(w => w.close.mock.calls.some(c => c[0] === 4029));
    expect(rejected).toHaveLength(0);
  });
});
