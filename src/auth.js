import jwt from "jsonwebtoken";

/**
 * @typedef {{ ok: true, clientId: string }} AuthOk
 * @typedef {{ ok: false, error: string }} AuthErr
 * @typedef {AuthOk | AuthErr} AuthResult
 */

/**
 * Verifies a WebSocket connection token and resolves the client identity.
 *
 * When `AUTH_SECRET` is not set, all connections are accepted as anonymous.
 * When `AUTH_SECRET` is set, the token must be a valid, non-expired JWT signed
 * with that secret. The client ID is resolved from the `sub` claim first,
 * then `clientId`, falling back to `"anonymous"`.
 *
 * @param {string | null | undefined} token - JWT passed by the connecting client.
 * @returns {AuthResult} On success `ok` is `true` and `clientId` identifies the
 *   client. On failure `ok` is `false` and `error` describes the reason.
 *
 * @example
 * const result = verifyConnection(req.query.token);
 * if (!result.ok) {
 *   socket.close(4001, result.error);
 * }
 */
export function verifyConnection(token) {
  const secret = process.env.AUTH_SECRET;
  if (!secret) {
    return { ok: true, clientId: "anonymous" };
  }

  try {
    const payload = jwt.verify(token, secret);
    return { ok: true, clientId: payload.sub ?? payload.clientId ?? "anonymous" };
  } catch (err) {
    if (err instanceof jwt.TokenExpiredError) {
      return { ok: false, error: "Token has expired" };
    }
    if (err instanceof jwt.NotBeforeError) {
      return { ok: false, error: "Token not yet valid" };
    }
    // JsonWebTokenError and any other jwt error
    return { ok: false, error: "Invalid token" };
  }
}
