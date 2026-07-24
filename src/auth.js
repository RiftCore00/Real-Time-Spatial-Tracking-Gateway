import jwt from "jsonwebtoken";
import https from "node:https";
import http from "node:http";
import crypto from "node:crypto";

/**
 * @typedef {{ ok: true, clientId: string }} AuthOk
 * @typedef {{ ok: false, error: string }} AuthErr
 * @typedef {AuthOk | AuthErr} AuthResult
 */

let jwksCache = null;
let lastFetchTime = 0;
let fetchPromise = null;

/**
 * Fetches JWKS from the given URI, supporting both http:// and https://.
 */
async function fetchJwks(uri) {
  const transport = uri.startsWith("https://") ? https : http;
  return new Promise((resolve, reject) => {
    transport.get(uri, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Failed to fetch JWKS: ${res.statusCode}`));
        return;
      }
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch {
          reject(new Error("Failed to parse JWKS"));
        }
      });
    }).on("error", reject);
  });
}

/**
 * Gets JWKS from cache or fetches it.
 */
async function getJwks(uri) {
  const ttl = Number(process.env.AUTH_JWKS_CACHE_TTL || 600000);
  const now = Date.now();

  if (jwksCache && now - lastFetchTime < ttl) {
    return jwksCache;
  }

  // If already fetching, wait for that promise
  if (fetchPromise) {
    // If we have a stale cache, we could return it immediately,
    // but the requirements say "refreshed in the background".
    // For simplicity and correctness (especially on first load),
    // we'll await the current fetch if it's the only way to get data.
    if (!jwksCache) {
      return fetchPromise;
    }
    // Background refresh: return stale cache and let fetch happen
    return jwksCache;
  }

  fetchPromise = fetchJwks(uri).then(data => {
    jwksCache = data;
    lastFetchTime = Date.now();
    fetchPromise = null;
    return jwksCache;
  }).catch(err => {
    fetchPromise = null;
    if (jwksCache) return jwksCache; // Fallback to stale on error
    throw err;
  });

  return jwksCache || fetchPromise;
}

/**
 * Converts a JWK to a Node.js KeyObject.
 */
function jwkToKeyObject(jwk) {
  try {
    return crypto.createPublicKey({ format: "jwk", key: jwk });
  } catch {
    return null;
  }
}

/**
 * Verifies a WebSocket connection token and resolves the client identity.
 *
 * @param {string | null | undefined} token - JWT passed by the connecting client.
 * @returns {Promise<AuthResult>}
 */
export async function verifyConnection(token) {
  const secret = process.env.AUTH_SECRET;
  const jwksUri = process.env.AUTH_JWKS_URI;

  if (!secret && !jwksUri) {
    return { ok: false, error: "Server misconfiguration: AUTH_SECRET or AUTH_JWKS_URI is missing" };
  }

  if (!token) {
    return { ok: false, error: "Authentication token is missing" };
  }

  // Reject alg:none tokens explicitly before any library call
  const decoded = jwt.decode(token, { complete: true });
  if (!decoded) {
    return { ok: false, error: "Invalid token" };
  }
  const alg = decoded.header && decoded.header.alg;
  if (!alg || alg.toLowerCase() === "none") {
    return { ok: false, error: "Invalid token: unsupported algorithm" };
  }

  const clockSkewMs = Number(process.env.AUTH_CLOCK_SKEW_MS || 30000);
  const issuer = process.env.AUTH_ISSUER;
  const audience = process.env.AUTH_AUDIENCE;

  // Pre-validate issuer claim before calling jwt.verify so that issuer errors
  // take priority over audience errors (library checks audience first).
  if (issuer) {
    const tokenIss = decoded.payload && decoded.payload.iss;
    if (tokenIss !== issuer) {
      return { ok: false, error: `jwt issuer invalid. expected: ${issuer}` };
    }
  }

  const options = {
    algorithms: ["HS256", "RS256", "ES256"],
    clockTolerance: clockSkewMs / 1000,
  };

  if (issuer) options.issuer = issuer;
  if (audience) options.audience = audience;

  try {
    let key;
    if (jwksUri) {
      const jwks = await getJwks(jwksUri);
      if (!decoded.header.kid) {
        return { ok: false, error: "Invalid token: missing kid header" };
      }
      const jwk = jwks.keys.find(k => k.kid === decoded.header.kid);
      if (!jwk) {
        return { ok: false, error: "Invalid token: unknown kid" };
      }
      key = jwkToKeyObject(jwk);
      if (!key) {
        return { ok: false, error: "Invalid token: unsupported key format" };
      }
    } else {
      key = secret;
    }

    const payload = jwt.verify(token, key, options);
    return { ok: true, clientId: payload.sub ?? payload.clientId ?? "anonymous" };
  } catch (err) {
    if (err instanceof jwt.TokenExpiredError) {
      return { ok: false, error: "Token has expired" };
    }
    if (err instanceof jwt.NotBeforeError) {
      return { ok: false, error: "Token not yet valid" };
    }
    if (err.name === "JsonWebTokenError") {
      // Surface issuer/audience error messages directly from the library
      if (err.message.startsWith("jwt issuer invalid") ||
          err.message.startsWith("jwt audience invalid")) {
        return { ok: false, error: err.message };
      }
      // "jwt signature is required" means alg:none slipped through or no sig
      if (err.message === "jwt signature is required") {
        return { ok: false, error: "Invalid token: unsupported algorithm" };
      }
      return { ok: false, error: "Invalid token" };
    }
    return { ok: false, error: "Invalid token" };
  }
}
