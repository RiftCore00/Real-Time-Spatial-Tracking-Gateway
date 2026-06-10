import jwt from "jsonwebtoken";

export function verifyConnection(token) {
  const secret = process.env.AUTH_SECRET;
  if (!secret) {
    return { ok: true, clientId: "anonymous" };
  }

  try {
    const payload = jwt.verify(token, secret);
    return { ok: true, clientId: payload.sub ?? payload.clientId ?? "anonymous" };
  } catch {
    return { ok: false, error: "Invalid or expired token" };
  }
}
