## Title
Implement real-time topology awareness with dynamic proximity rooms, automatic room assignment by geohash, and hierarchical region aggregation

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The current room model requires clients to explicitly `join_room` with a known `roomId` (e.g., `fleet-alpha`). In large-scale deployments (100K+ assets across a continent), manually managing room memberships is infeasible. Assets move across regions, enter/exit depots, follow corridors — room membership should be **automatic** based on real-time position. The `README.md` describes "Room-Based Broadcasting — Clients join logical rooms (e.g., fleet ID, region, user group)" (line 32) but provides no mechanism for dynamic, position-based room assignment.

## Problem statement
Design and implement a topology-aware room system that:

1. **Geohash-based dynamic rooms**: Divide the world into geohash cells (configurable precision, default 6 → ~1.2km × 0.6km). Each cell is an implicit room `geo:{geohash}`. When a client publishes a location, the server automatically adds them to the cell's room and removes them from the previous cell's room.

2. **Hierarchical region aggregation**: Parent rooms `geo:{geohash_prefix}*` aggregate child cells. A dispatcher subscribing to `geo:dr5r*` (precision 4, ~20km) receives all updates from child cells `dr5ru`, `dr5rv`, etc. Implementation: on `broadcast` to `geo:dr5r*`, fan out to all matching child rooms.

3. **Proximity rooms**: For a given client, automatically create ephemeral rooms `prox:{clientId}:{radius}m` containing all other clients within `radius` meters. Updated every N seconds (configurable). Used for collision avoidance, platooning, "vehicles near me" views.

4. **Polygon-based rooms (geofence-linked)**: When a geofence (issue 12) is created with `roomId`, clients inside that fence are automatically members of that room. Entry/exit events trigger room join/leave.

5. **Room membership as derived state**: Explicit `join_room`/`leave_room` still work for logical rooms (fleet IDs, user groups). Dynamic rooms are **additional** memberships managed by the server. Client's `getClientRooms()` returns both explicit and dynamic rooms.

6. **Efficient spatial indexing for room assignment**: Use the same R-tree (issue 12) or a geohash trie to determine cell membership in O(log N). Batch updates: on each location update, compute new cell, if changed → `leave(oldCell)`, `join(newCell)`.

7. **Configuration**: `TOPOLOGY_GEOHASH_PRECISION: 6`, `TOPOLOGY_PROXIMITY_RADIUS_M: 500`, `TOPOLOGY_PROXIMITY_UPDATE_MS: 5000`, `TOPOLOGY_ENABLE_DYNAMIC: true`.

## Current behavior
- `room-manager.js`: only explicit `join_room`/`leave_room`. No automatic assignment.
- `server.js`: `location_update` broadcasts to explicit rooms only.
- No geohash, proximity, or hierarchical rooms.

## Required behavior
- New module `src/topology-manager.js` exporting `TopologyManager` class.
- `TopologyManager` constructor: `{ roomManager, geofenceEngine, config }`.
- `topologyManager.processLocationUpdate(clientId, location)` — computes geohash cell, updates dynamic memberships, updates proximity room.
- `topologyManager.getDynamicRooms(clientId)` — returns Set of dynamic room IDs for a client.
- `topologyManager.getProximityMembers(clientId)` — returns client IDs in proximity room.
- `topologyManager.subscribeToRegion(clientId, geohashPrefix)` — subscribes client to hierarchical region room.
- Geohash encoding/decoding: use `ngeohash` algorithm (no dependency — implement 20 lines).
- Hierarchical room broadcast: `roomManager.broadcast("geo:dr5r*", msg)` → internally fans out to all `geo:dr5ru*`, `geo:dr5rv*`, etc. rooms.
- Proximity room: maintained as `Map<clientId, Set<clientId>>` updated on timer. Broadcast to `prox:{clientId}:500` sends to all members.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`.
- Do not modify existing test files. New test files required.
- No new npm dependencies — implement geohash encoding in ~30 lines.
- Dynamic room joins/leaves must not trigger `join_room`/`leave_room` protocol messages to client (they're server-managed).
- Client's `getClientRooms()` includes dynamic rooms — but client doesn't need to know which are dynamic vs explicit.
- Proximity room updates must be batched and debounced (5s default) to avoid churn.
- Memory: proximity rooms for 100K clients × avg 10 neighbors = 1M entries max.

## Acceptance criteria
- [ ] Client at lat/lon → automatically member of `geo:dr5reg` (precision 6)
- [ ] Client moves to adjacent cell → leaves old `geo:...`, joins new `geo:...` within 100ms
- [ ] Dispatcher subscribes to `geo:dr5r*` → receives updates from all child cells
- [ ] Proximity room `prox:clientA:500` contains clientB within 500m
- [ ] Proximity room updates every 5s, no churn for stationary clients
- [ ] Geofence with `roomId: "depot-1"` → clients inside automatically in room "depot-1"
- [ ] `getClientRooms(clientId)` returns explicit + dynamic rooms
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/topology-manager.test.js` with geohash, hierarchical, proximity, geofence-room scenarios

## Out of scope
- Client-side geohash computation (server-only).
- Dynamic room persistence across restarts (recomputed on reconnect).
- Multi-level hierarchy beyond 2 levels (cell + prefix) — sufficient for this issue.
- Proximity room for non-location message types.

## Hints and references
- Geohash encoding (base32, 5 bits per char):
  ```js
  const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';
  function encodeGeohash(lat, lon, precision = 6) {
    let latMin = -90, latMax = 90, lonMin = -180, lonMax = 180;
    let hash = '', bits = 0, bit = 0, evenBit = true;
    while (hash.length < precision) {
      if (evenBit) {
        const mid = (lonMin + lonMax) / 2;
        if (lon >= mid) { bit = (bit << 1) | 1; lonMin = mid; }
        else { bit = (bit << 1) | 0; lonMax = mid; }
      } else {
        const mid = (latMin + latMax) / 2;
        if (lat >= mid) { bit = (bit << 1) | 1; latMin = mid; }
        else { bit = (bit << 1) | 0; latMax = mid; }
      }
      evenBit = !evenBit;
      if (++bits === 5) { hash += BASE32[bit]; bits = 0; bit = 0; }
    }
    return hash;
  }
  ```
- Hierarchical broadcast: `roomManager` stores `Map<roomId, Set<roomId>>` of parent → children. On `broadcast(parentPrefix + "*")`, expand to children.
- Proximity: use R-tree from issue 12 (`rbush`) with `minX/maxX = lon ± radius/111km`, `minY/maxY = lat ± radius/111km` for candidate filter, then exact haversine.
- Integration: in `server.js` `location_update` case, after geofence processing, call `topologyManager.processLocationUpdate(actualClientId, { lat, lon })`.