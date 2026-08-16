## Title
Implement a geofencing engine with polygon storage, real-time point-in-polygon evaluation, entry/exit event generation, and spatial indexing for O(log N) fence lookup

## Difficulty
10/10 — Expert. Estimated effort: 6–8 days for a senior engineer.

## Context
The `README.md` explicitly lists "geofencing enforcement" as a core use case (line 25): "This service is designed for low-latency, high-throughput telemetry scenarios such as fleet tracking, asset monitoring, **geofencing enforcement**, and live mapping." The architecture diagram shows a "Room Manager" but no geofencing component. The `validator.js` accepts `latitude`/`longitude` but there is zero polygon storage, no spatial query engine, and no event generation when a tracked asset crosses a boundary.

In production fleet tracking, geofences define: depot boundaries (entry = "vehicle returned"), customer sites (entry = "arrived", exit = "departed"), restricted zones (entry = "violation"), corridor routes (exit = "off-route alert"). A single fleet may have 10,000+ geofences (polygons with 10–500 vertices each). Evaluating every location update against every fence is O(M×N) and infeasible at 10K updates/sec × 10K fences.

## Problem statement
Design and implement a geofencing subsystem that:

1. **Polygon storage and management**: CRUD API for geofences (create, read, update, delete) with fields: `fenceId`, `name`, `geometry` (GeoJSON Polygon/MultiPolygon), `metadata` (arbitrary JSON for customer-defined attributes like `type: "depot"`, `severity: "critical"`), `roomId` (optional — fences can be global or room-scoped).

2. **Spatial indexing for O(log N) lookup**: Build an in-memory R-tree (or STR-tree) over all fence bounding boxes. On each `location_update`, query the R-tree with the point's bounding box (point = degenerate box) to get candidate fences, then run precise point-in-polygon only on candidates. Target: <1ms per location update for 10K fences.

3. **Real-time entry/exit detection**: For each client, track which fences they are currently inside (maintain a `Set<fenceId>` per client). On location update, compute new inside-set via spatial index + point-in-polygon. Emit `geofence_entry` and `geofence_exit` events for symmetric difference. Events are broadcast to the client's room(s) with payload: `{ fenceId, fenceName, eventType: "entry"|"exit", clientId, location: {lat, lon}, timestamp, metadata }`.

4. **Event deduplication and hysteresis**: GPS noise causes flickering at boundaries. Implement a configurable hysteresis distance (default 10m): a client must move `hysteresis` meters inside/outside the fence boundary before the state flips. Also implement a minimum dwell time (default 5s): state change only commits if the new state persists for `dwellTime`.

5. **Integration with room broadcast**: Geofence events are injected into the room broadcast pipeline as a new message type `geofence_event`. They follow the same sequencing, deduplication, and replay semantics as `location_update` (issue 6).

6. **Persistence**: Fence definitions stored in the storage adapter (issue 10) — new methods `upsertFence`, `deleteFence`, `listFences(roomId?)`, `getFence(fenceId)`. PostgresAdapter uses PostGIS `geometry` type with GiST index; MemoryAdapter uses in-memory R-tree.

## Current behavior
- No geofence concept exists anywhere in the codebase.
- `validator.js` only validates point coordinates, not polygons.
- `room-manager.js` only broadcasts `location_update` — no derived events.
- Storage adapter interface (`src/storage/adapter.js`) has no fence methods.
- No spatial indexing library in `package.json`.

## Required behavior
- New module `src/geofence-engine.js` exporting `GeofenceEngine` class.
- Constructor: `{ storageAdapter, roomManager, hysteresisMeters: 10, dwellTimeMs: 5000, rtreeMaxEntries: 9 }`.
- `async upsertFence(fence: Geofence): Promise<void>` — validates GeoJSON, updates R-tree, persists via storage adapter.
- `async deleteFence(fenceId: string): Promise<void>` — removes from R-tree and storage.
- `async listFences(roomId?: string): Promise<Geofence[]>` — filters by roomId if provided.
- `processLocationUpdate(clientId, roomId, location: { lat, lon, timestamp }): GeofenceEvent[]` — called from `server.js` after validation, before broadcast. Returns array of entry/exit events to broadcast.
- Uses `rbush` (R-tree) for spatial index. Fence bounding boxes inserted with `fenceId` as metadata.
- Point-in-polygon: implement winding number algorithm (handles complex polygons, holes) or ray casting. Must correctly handle MultiPolygon (any polygon true = inside).
- State tracking: `Map<clientId, Map<roomId, Set<fenceId>>>` for current inside-set. Cleaned up on client disconnect.
- Hysteresis: buffer the fence geometry by `±hysteresisMeters` (positive for entry, negative for exit) using a simplified approach — compute point-to-polygon distance, only flip state if distance > hysteresis.
- Dwell time: pending state changes stored with timestamp; only committed on subsequent update if `now - pendingTimestamp >= dwellTimeMs`.

## Constraints
- Do not modify `validator.js`, `auth.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`.
- Do not modify `server.js` beyond adding a call to `geofenceEngine.processLocationUpdate()` in the `location_update` case (or note dependency on issue 1).
- Do not modify existing test files. New test files required.
- Add `rbush` and `@turf/helpers` (for GeoJSON validation) to `package.json` — only new dependencies allowed.
- GeoJSON validation: reject invalid polygons (self-intersections, unclosed rings, wrong winding order for holes). Use `@turf/boolean-point-in-polygon` for point-in-polygon or implement your own (the algorithm is ~30 lines).
- R-tree must be rebuilt incrementally on fence upsert/delete — not full rebuild.
- Memory usage: ≤ 200MB for 100K fences with avg 50 vertices each.
- Latency: `processLocationUpdate` must complete in <2ms p99 for 10K fences (measured in isolation).
- Fence definitions must be loaded from storage on engine initialization.

## Acceptance criteria
- [ ] `npm install` adds `rbush` and `@turf/helpers` without breaking existing tests
- [ ] `GeofenceEngine` loads fences from storage on init (both MemoryAdapter and PostgresAdapter)
- [ ] `upsertFence` with valid GeoJSON Polygon persists and updates R-tree
- [ ] `upsertFence` with invalid GeoJSON (self-intersecting, unclosed) rejects with descriptive error
- [ ] `processLocationUpdate` for a point inside a fence emits `geofence_entry` event
- [ ] Subsequent update for same point inside same fence emits NO event (deduplication)
- [ ] Update for point outside fence emits `geofence_exit` event
- [ ] Hysteresis: point 5m inside boundary (hysteresis=10m) does NOT trigger entry
- [ ] Dwell time: point enters fence, next update 3s later still inside — no event; update at 6s — entry event emitted
- [ ] MultiPolygon: point in any constituent polygon = inside
- [ ] Room-scoped fences: fence with `roomId: "fleet-A"` only evaluated for clients in "fleet-A"
- [ ] Global fences (no roomId): evaluated for all clients
- [ ] Events broadcast via `roomManager.broadcast()` with type `geofence_event` and correct payload
- [ ] `npm run lint` passes
- [ ] New test file: `tests/geofence-engine.test.js` with all above scenarios + performance benchmark

## Out of scope
- Fence geometry editing UI or API endpoints (engine only).
- Historical fence event queries (storage adapter handles `queryRoom` — fence events are just messages).
- Time-based fences (active only during certain hours) — metadata can encode this, engine evaluates on each update.
- 3D fences (altitude) — 2D only for this issue.
- Distributed geofence evaluation (issue 11 handles cross-instance — engine runs locally on each instance).

## Hints and references
- R-tree: `rbush` is the standard JS implementation. Insert: `tree.insert({ minX: lon, minY: lat, maxX: lon, maxY: lat, fenceId })`. Query: `tree.search({ minX: lon, minY: lat, maxX: lon, maxY: lat })` returns candidate fenceIds.
- Point-in-polygon (ray casting):
  ```js
  function pointInPolygon(lon, lat, polygon) {
    let inside = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const xi = polygon[i][0], yi = polygon[i][1];
      const xj = polygon[j][0], yj = polygon[j][1];
      if (((yi > lat) !== (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
    }
    return inside;
  }
  ```
  For MultiPolygon: iterate polygons, return true if any contains point. For Polygon with holes: exterior ring CCW, holes CW (GeoJSON spec) — ray casting handles this correctly.
- Hysteresis without full buffer: compute point-to-segment distance for each edge, take minimum. If `minDistance > hysteresis`, state change allowed. This is O(vertices) but only run on candidate fences from R-tree (typically 1–5).
- PostGIS schema: `CREATE EXTENSION IF NOT EXISTS postgis; ALTER TABLE geofences ADD COLUMN geometry geometry(Polygon, 4326); CREATE INDEX geofences_geom_idx ON geofences USING GIST (geometry);`
- For `@turf/helpers`: `polygon([[[lon, lat], ...]])` creates a Feature. `booleanPointInPolygon` from `@turf/boolean-point-in-polygon` is battle-tested.
- Integration point: in `server.js` `location_update` case, after validation and before `rooms.broadcast()`, call `geofenceEngine.processLocationUpdate(actualClientId, roomId, { lat, lon, timestamp })` and broadcast any returned events.