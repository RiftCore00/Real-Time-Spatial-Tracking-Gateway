## Title
Harden JWT authentication with claim validation, JWKS endpoint support, clock-skew tolerance, and token-refresh protocol

## Difficulty
10/10 — Expert. Estimated effort: 4–5 days for a senior engineer.

## Context
`src/auth.js` (lines 27–47) performs JWT verification but only checks signature and expiry. It does not validate the `iss` (issuer), `aud` (audience), or `nbf` (not-before) claims beyond the library defaults. In a microservices environment, this means a JWT minted by an unrelated service (e.g., an internal SSO that shares the same symmetric secret) would be accepted by the gateway. The `notBefore` check is delegated to the library, but there is no clock-skew tolerance — clients whose clocks are even 1 second ahead of the server receive `Token has expired` on tokens that are nominally valid.

Additionally, there is no token-refresh mechanism. When a JWT expires, the client is disconnected with close code 4001 and must obtain a new token out-of-band. For mobile fleet-tracking clients that maintain persistent connections through cell handoffs, this means connection drops every `expiresIn` interval (commonly 1 hour), causing the gaps that issue 6 aims to fix. A proper refresh protocol allows the client to send a new token on the existing connection without disconnecting.

Finally, the auth module has no support for asymmetric key verification (RSA/ES256 via JWKS endpoint). The current implementation only supports HMAC symmetric secrets. Production deployments behind an API gateway or identity provider (Auth0, Keycloak, AWS Cognito) require JWKS-based key rotation.

## Problem statement
Extend `src/auth.js` to support:

1. **Claim validation**: Validate `iss` (must match `AUTH_ISSUER` env var if set), `aud` (must match `AUTH_AUDIENCE` env var if set), and `nbf` (reject tokens not yet valid with a configurable clock-skew tolerance of ±30 seconds).
2. **JWKS endpoint support**: When `AUTH_JWKS_URI` is set, fetch the JWKS document (with caching and automatic refresh) and verify tokens using the matching `kid` header against the public key. Support both RSA (RS256) and EC (ES256) algorithms.
3. **Clock-skew tolerance**: Allow ±30 seconds of clock drift between client and server for `exp` and `nbf` checks.
4. **Token refresh protocol**: Add a new message type `token_refresh` that allows a connected client to send a new JWT on the existing connection. The server verifies the new token, updates the client's identity, and responds with `{ type: "token_refresh_ok" }` or `{ type: "error", payload: { message: "..." } }`.
5. **Algorithm restriction**: Reject tokens using the `none` algorithm and restrict to explicitly allowed algorithms (`HS256`, `RS256`, `ES256`).

## Current behavior
`src/auth.js` lines 27–47:
```js
export function verifyConnection(token) {
  const secret = process.env.AUTH_SECRET;
  if (!secret) {
    return { ok: false, error: "Server misconfiguration: AUTH_SECRET is missing" };
  }
  try {
    const payload = jwt.verify(token, secret);
    return { ok: true, clientId: payload.sub ?? payload.clientId ?? "anonymous" };
  } catch (err) {
    // ... error handling
  }
}
```
- No `iss` or `aud` claim validation.
- No JWKS support — only symmetric HMAC.
- No clock-skew tolerance — `jwt.verify` uses default options.
- No token refresh — client must disconnect and reconnect with a new token.
- No algorithm restriction — accepts whatever algorithm the JWT header specifies (algorithm confusion attack vector).

## Required behavior
- `verifyConnection(token)` validates `iss`, `aud`, `nbf` with clock-skew tolerance when env vars are set.
- When `AUTH_JWKS_URI` is set, verification uses the public key from the JWKS document.
- `none` algorithm is explicitly rejected.
- Token refresh message type is accepted by the validator and processed by the server.
- All existing `auth.test.js` and `auth-extended.test.js` tests pass.
- New tests for: claim validation, JWKS verification, clock skew, token refresh, algorithm restriction.

## Constraints
- Do not add new npm dependencies beyond the existing `jsonwebtoken`. For JWKS, implement the HTTP fetch and JWK-to-PEM conversion using Node.js built-in `crypto.createPublicKey` (available since Node 12) — do NOT add `jose` or `node-jose`.
- Do not break the existing `AUTH_SECRET` HMAC flow — it must remain the default when `AUTH_JWKS_URI` is not set.
- JWKS document must be cached with a configurable TTL (default 10 minutes) and refreshed in the background.
- Clock-skew tolerance must be ±30 seconds, configurable via `AUTH_CLOCK_SKEW_MS` env var.
- Do not modify `server.js` beyond adding the `token_refresh` message handling (or note that server.js is broken and this depends on issue 1).

## Acceptance criteria
- [ ] `auth.test.js` passes (after accounting for the stale test — see below)
- [ ] `auth-extended.test.js` passes
- [ ] Token with invalid `iss` claim is rejected when `AUTH_ISSUER` is set
- [ ] Token with invalid `aud` claim is rejected when `AUTH_AUDIENCE` is set
- [ ] Token with `nbf` 31 seconds in the future is rejected
- [ ] Token with `exp` 29 seconds in the future (but past nominal expiry) is accepted with ±30s skew
- [ ] Token with `alg: "none"` is rejected
- [ ] JWKS verification works when `AUTH_JWKS_URI` is set (mock the HTTP endpoint in tests)
- [ ] Token refresh: connected client sends `token_refresh` with new JWT, receives `token_refresh_ok`
- [ ] Token refresh: invalid new token returns error frame, connection remains open
- [ ] `npm run lint` passes

## Out of scope
- Changes to `room-manager.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`.
- Full OAuth2/OIDC integration (that is an identity provider concern).
- Token introspection endpoint (RFC 7662).
- Refresh token rotation (this is about refreshing the WS session token, not OAuth refresh tokens).

## Hints and references
- The `jsonwebtoken` library's `jwt.verify` accepts an `algorithms` option: `jwt.verify(token, secret, { algorithms: ["HS256", "RS256", "ES256"] })`. Always pass this explicitly to prevent algorithm confusion attacks.
- For JWKS, the standard endpoint returns `{ keys: [{ kid, kty, n, e, ... }] }`. Use `crypto.createPublicKey({ format: "jwk", key: jwk })` (Node 16+) to convert JWK to a Node.js KeyObject.
- Clock-skew tolerance: subtract `clockTolerance` from `currentTimestamp` before comparing with `exp`, and add it before comparing with `nbf`. The `jsonwebtoken` library does not have a built-in clock-skew option — you must adjust the `clockTolerance` parameter manually or use the `clockTimestamp` option.
- The stale `auth.test.js` test (line 4–9) expects `verifyConnection(null)` to return `{ ok: true }` when `AUTH_SECRET` is empty string. But `auth.js` was hardened to fail-closed. This test needs to be updated to expect `{ ok: false }`. However, the issue constraint says not to modify test files — so ensure the test passes by making `AUTH_SECRET=""` behave as expected (empty string is falsy in JS, so the existing `if (!secret)` check already rejects it). The test will need updating — note this dependency.
