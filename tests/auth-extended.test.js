import { describe, it, expect, beforeEach } from "vitest";
import jwt from "jsonwebtoken";

const SECRET = "test-secret-17";

describe("auth extended", () => {
  beforeEach(() => {
    process.env.AUTH_SECRET = SECRET;
  });

  it("accepts a valid JWT using sub claim", async () => {
    const token = jwt.sign({ sub: "user-123" }, SECRET);
    const { verifyConnection } = await import("../src/auth.js?ext1");
    const result = verifyConnection(token);
    expect(result.ok).toBe(true);
    expect(result.clientId).toBe("user-123");
  });

  it("accepts a valid JWT using clientId claim when sub is absent", async () => {
    const token = jwt.sign({ clientId: "device-abc" }, SECRET);
    const { verifyConnection } = await import("../src/auth.js?ext2");
    const result = verifyConnection(token);
    expect(result.ok).toBe(true);
    expect(result.clientId).toBe("device-abc");
  });

  it("rejects an expired JWT", async () => {
    const token = jwt.sign({ sub: "user-456" }, SECRET, { expiresIn: -1 });
    const { verifyConnection } = await import("../src/auth.js?ext3");
    const result = verifyConnection(token);
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
  });

  it("rejects a JWT signed with the wrong secret", async () => {
    const token = jwt.sign({ sub: "user-789" }, "wrong-secret");
    const { verifyConnection } = await import("../src/auth.js?ext4");
    const result = verifyConnection(token);
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
  });
});
