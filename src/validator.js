import { z } from "zod";

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

export function validateMessage(raw) {
  let parsed;
  try {
    parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    return { ok: false, error: "Invalid JSON" };
  }

  const result = messageSchema.safeParse(parsed);
  if (!result.success) {
    return { ok: false, error: result.error.issues.map(i => i.message).join("; ") };
  }

  return { ok: true, data: result.data };
}
