import { describe, it, expect, vi, afterEach } from "vitest";
import { createRateLimiter } from "../src/rate-limiter.js";

describe("createRateLimiter", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("allows messages within the limit", () => {
    const rl = createRateLimiter(5);
    for (let i = 0; i < 5; i++) {
      expect(rl.check("c1")).toBe(true);
    }
  });

  it("blocks the message that exceeds the limit", () => {
    const rl = createRateLimiter(3);
    rl.check("c1");
    rl.check("c1");
    rl.check("c1");
    expect(rl.check("c1")).toBe(false);
  });

  it("tracks clients independently", () => {
    const rl = createRateLimiter(2);
    rl.check("a");
    rl.check("a");
    expect(rl.check("a")).toBe(false);
    expect(rl.check("b")).toBe(true); // b unaffected
  });

  it("allows messages again after the window expires", () => {
    const rl = createRateLimiter(2);
    const now = Date.now();
    // Simulate old timestamps by stubbing Date.now
    vi.spyOn(Date, "now").mockReturnValueOnce(now - 1001).mockReturnValueOnce(now - 1001);
    rl.check("c1");
    rl.check("c1");
    // Now back to real time — window has expired
    vi.spyOn(Date, "now").mockReturnValue(now);
    expect(rl.check("c1")).toBe(true);
  });

  it("remove clears state for a client", () => {
    const rl = createRateLimiter(1);
    rl.check("c1"); // hits limit
    expect(rl.check("c1")).toBe(false);
    rl.remove("c1");
    expect(rl.check("c1")).toBe(true); // fresh after remove
  });

  it("defaults to 100 messages per second when no argument given", () => {
    delete process.env.MAX_MESSAGES_PER_SECOND;
    const rl = createRateLimiter();
    for (let i = 0; i < 100; i++) {
      expect(rl.check("c1")).toBe(true);
    }
    expect(rl.check("c1")).toBe(false);
  });

  it("uses MAX_MESSAGES_PER_SECOND env var as default", () => {
    process.env.MAX_MESSAGES_PER_SECOND = "2";
    const rl = createRateLimiter();
    rl.check("c1");
    rl.check("c1");
    expect(rl.check("c1")).toBe(false);
    delete process.env.MAX_MESSAGES_PER_SECOND;
  });
});
