import { describe, it, expect, beforeEach, afterEach } from "vitest";
import jwt from "jsonwebtoken";
import http from "node:http";
import crypto from "node:crypto";
import { verifyConnection } from "../src/auth.js";

const SECRET = "test-secret-hardening";

describe("JWT Hardening", () => {
  beforeEach(() => {
    process.env.AUTH_SECRET = SECRET;
    process.env.AUTH_ISSUER = "test-issuer";
    process.env.AUTH_AUDIENCE = "test-audience";
    process.env.AUTH_CLOCK_SKEW_MS = "30000";
    // Ensure we don't have leftover JWKS URI
    delete process.env.AUTH_JWKS_URI;
  });

  afterEach(() => {
    delete process.env.AUTH_SECRET;
    delete process.env.AUTH_ISSUER;
    delete process.env.AUTH_AUDIENCE;
    delete process.env.AUTH_CLOCK_SKEW_MS;
    delete process.env.AUTH_JWKS_URI;
  });

  it("rejects token with invalid iss claim", async () => {
    const token = jwt.sign({ sub: "user-1", iss: "wrong-issuer" }, SECRET);
    const result = await verifyConnection(token);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/jwt issuer invalid/i);
  });

  it("rejects token with invalid aud claim", async () => {
    const token = jwt.sign({ sub: "user-1", iss: "test-issuer", aud: "wrong-audience" }, SECRET);
    const result = await verifyConnection(token);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/jwt audience invalid/i);
  });

  it("rejects token with nbf 31 seconds in the future", async () => {
    const now = Math.floor(Date.now() / 1000);
    const token = jwt.sign({ sub: "user-1", iss: "test-issuer", aud: "test-audience", nbf: now + 31 }, SECRET);
    const result = await verifyConnection(token);
    expect(result.ok).toBe(false);
    expect(result.error).toBe("Token not yet valid");
  });

  it("accepts token with exp 29 seconds in the past due to skew", async () => {
    const now = Math.floor(Date.now() / 1000);
    const token = jwt.sign({ sub: "user-1", iss: "test-issuer", aud: "test-audience", exp: now - 29 }, SECRET);
    const result = await verifyConnection(token);
    expect(result.ok).toBe(true);
  });

  it("rejects token with alg none", async () => {
    const payload = { sub: "user-1", iss: "test-issuer", aud: "test-audience" };
    const header = { alg: "none", typ: "JWT" };
    const badToken = Buffer.from(JSON.stringify(header)).toString("base64url") + "." + 
                     Buffer.from(JSON.stringify(payload)).toString("base64url") + ".";
    
    const result = await verifyConnection(badToken);
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/unsupported algorithm|invalid algorithm/i);
  });

  describe("JWKS Support", () => {
    let jwksServer;
    let jwksPort;
    const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
      modulusLength: 2048,
    });
    const jwk = publicKey.export({ format: "jwk" });
    jwk.kid = "test-kid";
    jwk.alg = "RS256";
    jwk.use = "sig";

    beforeEach(async () => {
      jwksServer = http.createServer((req, res) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ keys: [jwk] }));
      });
      await new Promise(resolve => jwksServer.listen(0, "127.0.0.1", resolve));
      jwksPort = jwksServer.address().port;
      process.env.AUTH_JWKS_URI = `http://127.0.0.1:${jwksPort}/jwks`;
      delete process.env.AUTH_SECRET;
    });

    afterEach(async () => {
      await new Promise(resolve => jwksServer.close(resolve));
    });

    it("verifies token using JWKS", async () => {
      const token = jwt.sign({ sub: "user-jwks", iss: "test-issuer", aud: "test-audience" }, privateKey, {
        algorithm: "RS256",
        keyid: "test-kid",
      });
      const result = await verifyConnection(token);
      expect(result.ok).toBe(true);
      expect(result.clientId).toBe("user-jwks");
    });
    
    it("rejects token with unknown kid", async () => {
       const token = jwt.sign({ sub: "user-jwks", iss: "test-issuer", aud: "test-audience" }, privateKey, {
        algorithm: "RS256",
        keyid: "unknown-kid",
      });
      const result = await verifyConnection(token);
      expect(result.ok).toBe(false);
      expect(result.error).toMatch(/unknown kid/i);
    });
  });
});
