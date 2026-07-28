import { describe, it, expect } from "vitest";

describe("auth", () => {
  it("rejects connection when AUTH_SECRET is not configured", async () => {
    process.env.AUTH_SECRET = "";
    const { verifyConnection } = await import("../src/auth.js");
    const result = verifyConnection(null);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/AUTH_SECRET|misconfiguration/i);
  });

  it("rejects a missing token when secret is set", async () => {
    process.env.AUTH_SECRET = "test-secret";
    const { verifyConnection } = await import("../src/auth.js?2");
    const result = verifyConnection(null);
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
  });

  it("rejects an invalid token when secret is set", async () => {
    process.env.AUTH_SECRET = "test-secret";
    const { verifyConnection } = await import("../src/auth.js?3");
    const result = verifyConnection("bad-token");
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
  });
});
