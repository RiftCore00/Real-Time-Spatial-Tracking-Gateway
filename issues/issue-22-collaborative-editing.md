## Title
Implement real-time collaborative map editing with operational transformation, conflict-free replicated data types (CRDTs), and multi-user geofence authoring

## Difficulty
10/10 — Expert. Estimated effort: 7–10 days for a senior engineer.

## Context
The `README.md` targets "fleet tracking, asset monitoring, geofencing enforcement, and live mapping" (line 25). In multi-user operations centers, dispatchers collaboratively create and edit geofences (depot boundaries, customer sites, hazard zones) on a shared map. Current geofence engine (issue 12) has no collaborative editing — concurrent edits by multiple users cause lost updates or corruption. Operational Transformation (OT) or CRDTs are required for real-time, conflict-free co-authoring of polygon geometries.

## Problem statement
Implement a collaborative geofence authoring system that:

1. **CRDT-based polygon geometry**: Represent polygons as CRDTs (RGA sequence CRDT for vertex arrays, or LWW-Element-Set for whole polygons). Each vertex `{ lat, lon, id }` has a unique ID (UUID v7). Insert/delete vertices concurrently without conflicts — merges automatically.

2. **Operational transformation for metadata**: Geofence metadata (`name`, `type`, `severity`, `attributes`) uses OT (e.g., `Automerge` or custom JSON OT). Concurrent edits to same field → last-writer-wins with vector clock; concurrent edits to different fields → merge.

3. **Real-time presence and cursors**: Connected editors see each other's cursors (vertex being dragged, polygon being drawn) in real-time. Presence state: `{ userId, userName, color, cursor: { lat, lon }, selection: [vertexIds] }` broadcast via `presence:{geofenceId}` room.

4. **Version history and time-travel**: Every edit creates an immutable version (event sourced, issue 21). Users can browse history, diff versions, restore previous version. Versions stored as CRDT operation log.

5. **Lock-free concurrent editing**: No explicit locks. Conflict resolution is automatic via CRDT/OT semantics. UI shows "User X edited this vertex" annotations.

6. **Permission model**: Geofence has `ownerId`, `editors: Set<userId>`, `viewers: Set<userId>`. Only editors can modify geometry; viewers see presence but cannot edit. Admin can transfer ownership.

7. **Integration with geofence engine (issue 12)**: When a collaborative session ends (all editors leave), the CRDT state is serialized to canonical GeoJSON and persisted via storage adapter. Active geofence evaluation uses the latest committed version.

8. **Offline-first support**: Editors can make changes offline (Service Worker caches CRDT operations). On reconnect, operations sync and merge.

## Current behavior
- Issue 12: `GeofenceEngine` has `upsertFence`/`deleteFence` — single-writer, no collaboration.
- No CRDT, OT, presence, or version history.
- No multi-user editing protocol.

## Required behavior
- New module `src/collaborative-editor.js` exporting `CollaborativeGeofenceEditor` class.
- `CollaborativeGeofenceEditor` constructor: `{ eventStore, geofenceEngine, presenceManager, config }`.
- `editor.startSession(geofenceId, userId, userName)` — joins presence room, loads CRDT state, returns `{ state, version, presence }`.
- `editor.applyOperation(geofenceId, userId, operation)` — operation is `{ type: "insertVertex" | "deleteVertex" | "moveVertex" | "updateMetadata", ... }`. Returns transformed operation for broadcast.
- `editor.getState(geofenceId)` — returns current CRDT state as GeoJSON.
- `editor.getHistory(geofenceId, fromVersion, toVersion)` — returns operations for diff.
- `editor.restoreVersion(geofenceId, version)` — creates new version from historical state.
- `PresenceManager` — manages `presence:{geofenceId}` room, broadcasts cursor/selection updates (throttled to 50ms).
- CRDT implementation: use `yjs` (if adding dep) or custom RGA for vertices + LWW for metadata. **Constraint: no new deps** — implement minimal RGA (~200 lines).
- Permission checks on every operation.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`, `topology-manager.js`, `event-sourcing.js`.
- Do not modify existing test files. New test files required.
- **No new npm dependencies** — implement RGA CRDT from scratch.
- CRDT must converge: same operations in different orders → same final state.
- Vertex IDs must be globally unique (UUID v7) for RGA identity.
- Presence updates throttled to 50ms, batched via `setImmediate`.
- Offline sync: operations stored locally with vector clock, sent on reconnect, merged via CRDT.
- Integration: `geofenceEngine` reads committed version from storage; collaborative editor writes draft versions to separate `geofence_drafts` table.

## Acceptance criteria
- [ ] Two editors simultaneously insert vertex at different positions → both vertices appear in final polygon
- [ ] Two editors move same vertex → last move wins (LWW on vertex position), no corruption
- [ ] Editor A deletes vertex, Editor B moves same vertex → vertex deleted (delete wins in RGA)
- [ ] Presence: Editor A sees Editor B's cursor moving in real-time (<100ms latency)
- [ ] Version history: each edit creates version, `getHistory` returns diffs
- [ ] Restore version: `restoreVersion` creates new version from old state
- [ ] Permissions: viewer cannot apply geometry operations, gets error
- [ ] Offline: editor makes changes offline, reconnects → operations merge correctly
- [ ] Committed version (all editors left) → geofence engine uses it for evaluation
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/collaborative-editor.test.js` with concurrent edit scenarios, presence, version history, offline sync

## Out of scope
- Full rich-text CRDT (only polygon vertices + metadata).
- Conflict resolution UI — automatic only.
- Access control lists beyond owner/editors/viewers.
- Real-time rendering — presence protocol only.

## Hints and references
- RGA (Replicated Growable Array) CRDT for vertex sequence:
  - Each vertex has unique `id` (UUID v7) and `originLeft` (ID of vertex to its left at insertion time).
  - Insert: find position by `originLeft`, insert new vertex with new ID.
  - Delete: mark vertex as deleted (tombstone), keep ID for ordering.
  - Merge: sort by `(originLeft, id)` total order — converges.
- LWW (Last-Writer-Wins) for metadata: each field has `{ value, timestamp, userId }`. Merge: max timestamp wins.
- Vector clock for offline sync: `{ userId: counter }`. Increment on each operation. Merge: max per user.
- Presence protocol (over WebSocket):
  ```js
  // Client → Server
  { type: "presence_update", geofenceId, cursor: { lat, lon }, selection: [vertexId1, ...] }
  // Server → All editors in room
  { type: "presence", geofenceId, userId, userName, color, cursor, selection }
  ```
- Color assignment: hash `userId` to HSL hue for consistent colors.
- Integration with issue 21 event store: collaborative operations are events with `eventType: "geofence.vertex_inserted.v1"`, etc.
- GeoJSON serialization from CRDT state: filter non-deleted vertices, sort by RGA order, output `Polygon` coordinates.