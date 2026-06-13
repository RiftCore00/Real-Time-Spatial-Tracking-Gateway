import { z } from "zod";

/**
 * @typedef {{ ok: true, data: import('zod').infer<typeof messageSchema> }} ValidOk
 * @typedef {{ ok: false, error: string }} ValidErr
 * @typedef {ValidOk | ValidErr} ValidationResult
 */

const locationPayloadSchema = z.object({
  latitude: z.number().min(-90).max(90),
  longitude: z.number().min(-180).max(180),
  altitude: z.number().optional(),
  accuracy: z.number().min(0).optional(),
  speed: z.number().min(0).optional(),
  timestamp: z.string().datetime().optional(),
});

const joinRoomSchema = z.object({
  roomId: z.string().min(1).max(128),
});

const leaveRoomSchema = z.object({
  roomId: z.string().min(1).max(128),
});

const messageSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("location_update"),
    payload: locationPayloadSchema,
  }),
  z.object({
    type: z.literal("join_room"),
    ...joinRoomSchema.shape,
  }),
  z.object({
    type: z.literal("leave_room"),
    ...leaveRoomSchema.shape,
  }),
]);

/**
 * Formats a Zod issue into a human-readable string including the field path.
 *
 * @param {import('zod').ZodIssue} issue
 * @returns {string}
 */
function formatIssue(issue) {
  const path = issue.path.length > 0 ? `${issue.path.join(".")}: ` : "";
  return `${path}${issue.message}`;
}

/**
 * Validates a raw WebSocket message against the known message schema.
 *
 * Accepts either a JSON string or a pre-parsed object. If a string is provided
 * it is parsed first; a parse failure returns `{ ok: false, error: "Invalid JSON" }`.
 * Schema violations return a human-readable concatenation of all Zod issue messages.
 *
 * @param {string | unknown} raw - Raw message from the WebSocket, either a JSON
 *   string or an already-parsed value.
 * @returns {ValidationResult} Result object. On success `ok` is `true` and `data`
 *   holds the validated, typed message. On failure `ok` is `false` and `error`
 *   contains a description of what went wrong.
 *
 * @example
 * const result = validateMessage('{"type":"join_room","roomId":"fleet-alpha"}');
 * if (result.ok) {
 *   console.log(result.data.roomId); // "fleet-alpha"
 * } else {
 *   console.error(result.error);
 * }
 */
export function validateMessage(raw) {
  let parsed;
  try {
    parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    return { ok: false, error: "Invalid JSON" };
  }

  const result = messageSchema.safeParse(parsed);
  if (!result.success) {
    return { ok: false, error: result.error.issues.map(formatIssue).join("; ") };
  }

  return { ok: true, data: result.data };
}
