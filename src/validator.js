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

const joinRoomSchema = z.object({ roomId: z.string().min(1).max(128) });
const leaveRoomSchema = z.object({ roomId: z.string().min(1).max(128) });

const messageSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("location_update"), payload: locationPayloadSchema }),
  z.object({ type: z.literal("join_room"), ...joinRoomSchema.shape }),
  z.object({ type: z.literal("leave_room"), ...leaveRoomSchema.shape }),
]);

const MESSAGE_SIZE_LIMITS = {
  location_update: 512,
  join_room: 256,
  leave_room: 256,
};

export function validateMessage(raw) {
  const isString = typeof raw === "string";
  let parsed;
  try {
    parsed = isString ? JSON.parse(raw) : raw;
  } catch {
    return { ok: false, error: "Invalid JSON" };
  }
}

/**
 * Build an error string from a list of Zod issues.
 *
 * @param {import('zod').ZodIssue[]} issues
 * @returns {string}
 */
export function buildError(issues) {
  return issues.map(i => i.message).join("; ");
}

/**
 * Validates a raw WebSocket message against the known message schema.
 *
 * @param {string | unknown} raw
 * @returns {ValidationResult}
 *
 * @example
 * const result = validateMessage('{"type":"join_room","roomId":"fleet-alpha"}');
 * if (result.ok) console.log(result.data.roomId);
 */
export function validateMessage(raw) {
  const parsed = parseJSON(raw);
  if (!parsed.ok) return parsed;

  if (isString) {
    const type = parsed?.type;
    const sizeLimit = MESSAGE_SIZE_LIMITS[type];
    if (sizeLimit !== undefined) {
      const byteSize = Buffer.byteLength(raw, "utf8");
      if (byteSize > sizeLimit) {
        return { ok: false, error: `Message exceeds size limit of ${sizeLimit} bytes for type '${type}'` };
      }
    }
  }

  const result = messageSchema.safeParse(parsed);
  if (!result.success) {
    return { ok: false, error: result.error.issues.map(formatIssue).join("; ") };
  }

  return { ok: true, data: result.data };
}
