import { describe, it, expect, vi, beforeEach } from "vitest";
import { createServer } from "../src/server.js";

function makeReq(ip = "1.2.3.4") {
  return {
    url: "/?token=test",
    socket: { remoteAddress: ip },
  };
}

function makeWs() {
  return {
    isAlive: false,
    _trackedIp: undefined,
    close: vi.fn(),
    send: vi.fn(),
    on: vi.fn(),
    terminate: vi.fn(),
  };
}

describe("max connections per IP (issue 26)", () => {
  let wss;
  let server;

  beforeEach(() => {
    server = createServer({ port: 0, maxConnectionsPerIp: 2 });
    wss = server.wss;
  });

  afterEach(() => new Promise((res) => wss.close(res)));

  it("allows connections up to the limit", () => {
    const ws1 = makeWs();
    const ws2 = makeWs();
    wss.emit("connection", ws1, makeReq("10.0.0.1"));
    wss.emit("connection", ws2, makeReq("10.0.0.1"));
    // Neither should be rejected with 4029
    const closed4029 = [ws1, ws2].filter(
      (w) => w.close.mock.calls.some((c) => c[0] === 4029)
    );
    expect(closed4029).toHaveLength(0);
  });

  it("rejects connections exceeding the limit with code 4029", () => {
    const ws1 = makeWs();
    const ws2 = makeWs();
    const ws3 = makeWs();
    wss.emit("connection", ws1, makeReq("10.0.0.2"));
    wss.emit("connection", ws2, makeReq("10.0.0.2"));
    wss.emit("connection", ws3, makeReq("10.0.0.2"));
    expect(ws3.close).toHaveBeenCalledWith(4029, expect.any(String));
  });

  it("different IPs have independent limits", () => {
    const ws1 = makeWs();
    const ws2 = makeWs();
    wss.emit("connection", ws1, makeReq("10.0.0.3"));
    wss.emit("connection", ws2, makeReq("10.0.0.4"));
    const closed4029 = [ws1, ws2].filter(
      (w) => w.close.mock.calls.some((c) => c[0] === 4029)
    );
    expect(closed4029).toHaveLength(0);
  });

  it("defaults to MAX_CONNECTIONS_PER_IP env var", () => {
    process.env.MAX_CONNECTIONS_PER_IP = "1";
    const s = createServer({ port: 0 });
    const ws1 = makeWs();
    const ws2 = makeWs();
    s.wss.emit("connection", ws1, makeReq("10.0.0.5"));
    s.wss.emit("connection", ws2, makeReq("10.0.0.5"));
    expect(ws2.close).toHaveBeenCalledWith(4029, expect.any(String));
    delete process.env.MAX_CONNECTIONS_PER_IP;
    s.wss.close();
  });
});
