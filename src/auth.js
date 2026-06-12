import jwt from "jsonwebtoken";

export function verifyConnection(token) {
  const secret = process.env.AUTH_SECRET;
  if (!secret) {
    // Security fix: fail closed when auth secret is missing to prevent bypass
    return { ok: false, error: "Server misconfiguration: AUTH_SECRET is missing" };
  }

  try {
    const payload = jwt.verify(token, secret);
    return { ok: true, clientId: payload.sub ?? payload.clientId ?? "anonymous" };
  } catch {
    return { ok: false, error: "Invalid or expired token" };
  }
}
