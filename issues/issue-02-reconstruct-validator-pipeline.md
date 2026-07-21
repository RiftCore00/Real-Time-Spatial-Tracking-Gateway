## Title
Reconstruct the corrupted `validator.js` — resolve duplicate declarations and undefined helper references to restore the dual-input validation pipeline

## Difficulty
9/10 — Expert. Estimated effort: 2–3 days for a senior engineer.

## Context
`src/validator.js` has two competing `export function validateMessage()` declarations (lines 33–41 and 63–84). Because ES modules do not allow duplicate exports of the same name, this module fails at parse time with `SyntaxError: Identifier 'validateMessage' has already been declared`. This breaks every test suite that imports the validator: `validator.test.js`, `integration.test.js`, `message-size-limits.test.js`, and transitively `server.test.js` (which imports `validateMessage` indirectly through `server.js`).

The first definition (lines 33–41) is incomplete — it parses JSON but never invokes the Zod schema, never returns a result for valid input, and doesn't enforce per-type byte size limits. The second definition (lines 63–84) is more complete but references three functions that do not exist anywhere in the file: `parseJSON` (line 64), `isString` (line 67), and `formatIssue` (line 80). A standalone `buildError` helper exists (lines 49–51) but is never called — `formatIssue` is used instead.

## Problem statement
Reconstruct `src/validator.js` into a single, correct validation pipeline that:

1. Accepts either a raw string (from WebSocket `Buffer.toString()`) or a pre-parsed object (from tests that call `validateMessage` with an object directly).
2. Parses JSON from strings; passes objects through.
3. Enforces per-type byte size limits **only** when the input is a string (binary frame → string). The limits are defined in `MESSAGE_SIZE_LIMITS`: `location_update` ≤ 512 bytes, `join_room` ≤ 256 bytes, `leave_room` ≤ 256 bytes.
4. Validates the parsed object against `messageSchema` (the Zod discriminated union).
5. Returns `{ ok: true, data }` on success or `{ ok: false, error }` on failure.
6. Exports `validateMessage` exactly once and exports the `buildError` helper.

## Current behavior
- **Line 33**: First `validateMessage(raw)` — calls `JSON.parse` on strings, but the function body ends at line 41 without returning a validation result or calling the Zod schema.
- **Line 63**: Second `validateMessage(raw)` — calls `parseJSON(raw)` (undefined), checks `isString` (undefined), uses `formatIssue` (undefined) instead of the existing `buildError` helper.
- **Line 49**: `buildError(issues)` is defined but never referenced.
- **Lines 1–25**: The Zod schemas (`locationPayloadSchema`, `joinRoomSchema`, `leaveRoomSchema`, `messageSchema`) and `MESSAGE_SIZE_LIMITS` are correct and should be preserved.

## Required behavior
- `validateMessage` exported once, accepting `string | unknown`.
- Returns `{ ok: true, data }` where `data` matches the Zod discriminated union.
- Returns `{ ok: false, error: string }` for: invalid JSON, oversized messages (string input only), schema validation failures.
- `buildError` exported for external use.
- All Zod schemas and size limits preserved exactly as-is.

## Constraints
- Do not change any file other than `src/validator.js`.
- Do not add new npm dependencies.
- Do not modify any test file.
- The byte size check must use `Buffer.byteLength(raw, "utf8")` against the original raw string, not the parsed object.
- The `MESSAGE_SIZE_LIMITS` object must remain exactly as defined (512/256/256).
- Schema validation errors should be human-readable, joined with `"; "`.

## Acceptance criteria
- [ ] `src/validator.js` exports exactly one `validateMessage` function
- [ ] No `SyntaxError` when importing `src/validator.js`
- [ ] `npm run lint` passes
- [ ] `validator.test.js` passes: all location_update, join_room, leave_room, and malformed JSON tests green
- [ ] `message-size-limits.test.js` passes: all per-type size limit tests green
- [ ] `integration.test.js` passes: valid messages route through validator → room manager, invalid messages are rejected
- [ ] `validateMessage("not json")` returns `{ ok: false, error: "Invalid JSON" }`
- [ ] `validateMessage({ type: "location_update", payload: { latitude: 40, longitude: -74 } })` returns `{ ok: true, data: ... }`

## Out of scope
- Changes to `server.js`, `auth.js`, `room-manager.js`, or any test file.
- Adding new message types to the schema.
- Changing the Zod schema definitions or size limits.

## Hints and references
- The `isString` check in the second `validateMessage` was checking `typeof raw === "string"` before the JSON parse, to decide whether to apply byte size limits. After parsing, you no longer have the original string to measure — so the size check must happen **before** parsing, using the raw input directly.
- The `parseJSON` helper should attempt `JSON.parse` for strings and pass objects through, returning `{ ok: true, data }` or `{ ok: false, error: "Invalid JSON" }`.
- The `formatIssue` that was referenced was likely meant to be a single-issue formatter; `buildError` already does this for arrays. Use `buildError(result.error.issues)` for the Zod failure path.
