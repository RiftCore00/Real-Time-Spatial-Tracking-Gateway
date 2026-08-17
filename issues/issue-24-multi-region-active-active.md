## Title
Implement multi-region active-active deployment with conflict-free geo-replication, cross-region failover, and eventual consistency for global fleet operations

## Difficulty
10/10 — Expert. Estimated effort: 8–12 days for a senior engineer.

## Context
Issue 11 implements distributed room state within a single region (Redis pub/sub). Global enterprises (shipping, aviation, logistics) need **multi-region active-active** deployment: gateways in us-east-1, eu-west-1, ap-southeast-1 all accepting connections from local assets. A vessel crossing the Atlantic must seamlessly handoff from us-east to eu-west gateway without message loss, duplicate processing, or split-brain room state. The `README.md` architecture shows a single region — no multi-region strategy exists.

## Problem statement
Design and implement a multi-region active-active architecture that:

1. **Conflict-free geo-replication**: Room membership, sequence numbers, session state, and geofence definitions replicated across regions using **CRDTs** (not last-writer-wins). 
   - Room membership: `OR-Set` (Observed-Remove Set) for client IDs — add/remove wins, concurrent add+remove → add wins (safe for presence).
   - Sequence numbers: `RGA` sequence CRDT for message ordering — each region assigns local sequence, merged via causal ordering.
   - Session state: `LWW-Map` with vector clocks for client session data.
   - Geofence definitions: `LWW-Element-Set` per fence — last update wins (admin edits are rare, serialized).

2. **Cross-region message routing**: When client in us-east publishes to room `fleet-global`, message is:
   - Assigned global sequence via merged RGA.
   - Replicated to eu-west and ap-southeast via dedicated replication links (not public Internet — use AWS Global Accelerator, Cloudflare Tunnel, or dedicated fiber).
   - Delivered to local subscribers in each region with same global sequence.

3. **Region affinity and failover**:
   - Client DNS resolves to nearest healthy region (GeoDNS / Route 53 latency-based).
   - On region failure (detected via health checks + replication lag), clients reconnect to next region with `session_id` (issue 18).
   - New region has full session state via CRDT replication — zero-downtime failover.

4. **Split-brain prevention**: 
   - Quorum-based writes for critical metadata (geofence definitions, rate limit config): require 2/3 regions ack.
   - Room membership and messages are eventually consistent — no quorum needed (CRDTs handle conflicts).
   - `admin-server` (issue 14) operations that mutate global state (rate limits, room limits) go through quorum.

5. **Replication protocol**: 
   - Each region runs a `ReplicationAgent` connecting to peers via mTLS (issue 15).
   - Protocol: `operation: { type, payload, vectorClock, regionId, timestamp }`.
   - Anti-entropy: periodic Merkle tree sync to detect/correct divergence.
   - Lag metric: `replication_lag_ms{from="us-east", to="eu-west"}` — alert if > 5s.

6. **Global sequence numbering**: 
   - Hybrid logical clocks (HLC) for causal ordering across regions.
   - Each message gets `{ hlcTimestamp, regionId, localSeq }` — totally ordered.
   - Replay (issue 6) uses global sequence — client can resume from any region.

7. **Data residency compliance**: 
   - Configurable `DATA_RESIDENCY: { "eu": ["eu-west-1"], "us": ["us-east-1", "us-west-2"] }`.
   - Client data (location history, PII) only replicated to allowed regions.
   - Room membership for EU-only fleets never leaves EU regions.

## Current behavior
- Issue 11: single-region Redis pub/sub only.
- No cross-region replication, no CRDTs, no HLC, no failover.
- `docker-compose.yml`: single Postgres, single Redis, single gateway.

## Required behavior
- New module `src/multi-region.js` exporting `MultiRegionCoordinator` class.
- `MultiRegionCoordinator` constructor: `{ regionId, peerRegions, replicationTransport, crdtRegistry, config }`.
- `coordinator.joinRoom(clientId, roomId)` → adds to local OR-Set, replicates `AddOp` to peers.
- `coordinator.leaveRoom(clientId, roomId)` → adds `RemoveOp` to OR-Set.
- `coordinator.broadcast(roomId, message)` → assigns HLC timestamp, appends to RGA sequence, replicates.
- `coordinator.getRoomMembers(roomId)` → merges local OR-Set with peer state (read-repair).
- `coordinator.syncGeofence(fence)` → LWW-Element-Set with quorum write.
- `coordinator.handlePeerOperation(op)` — applies remote operation to local CRDTs.
- `coordinator.antiEntropy()` — periodic Merkle sync with peers.
- CRDT implementations: `ORSet`, `RGASequence`, `LWWMap`, `LWWElementSet` — pure JS, no deps.
- HLC implementation: `hlc.now()`, `hlc.receive(peerHLC)`, `hlc.compare(a, b)`.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`, `topology-manager.js`, `event-sourcing.js`, `collaborative-editor.js`, `predictor.js`.
- Do not modify existing test files. New test files required.
- No new npm dependencies — implement CRDTs and HLC from scratch.
- CRDTs must satisfy convergence: same operations delivered in any order → same state.
- HLC must provide causal ordering: if A happens-before B, then HLC(A) < HLC(B).
- Replication transport: abstract interface — can be WebSocket, gRPC, or dedicated link.
- Data residency enforced at replication layer — filter operations by region policy.
- Metrics: `replication_lag_ms`, `replication_conflicts_total`, `crdt_merge_duration_ms`.

## Acceptance criteria
- [ ] OR-Set: region A adds client, region B removes same client concurrently → client present (add wins)
- [ ] RGA Sequence: region A inserts msg at seq 5, region B inserts at seq 5 concurrently → both appear, deterministic order
- [ ] HLC: event in us-east at T, replicated to eu-west at T+50ms → eu-west HLC > us-east HLC
- [ ] Client in us-east publishes → message delivered in eu-west with same global sequence within 200ms
- [ ] Region us-east fails → clients reconnect to eu-west with session_id → full state restored
- [ ] Geofence update in eu-west → quorum write (2/3) → replicated to us-east, ap-southeast
- [ ] EU-only fleet data never replicated to us-east (data residency)
- [ ] Anti-entropy detects divergence, repairs via Merkle sync
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/multi-region.test.js` with CRDT convergence, HLC ordering, failover, data residency

## Out of scope
- DNS / load balancer configuration (GeoDNS, Route 53) — infrastructure concern.
- Replication transport implementation — interface only.
- Manual region failover CLI — automatic via health checks.
- Cross-region billing / cost allocation.

## Hints and references
- OR-Set (Observed-Remove Set):
  ```js
  // Each element has unique tag (UUID). Add: { element, tag }. Remove: { element, tag }.
  // Element present if ∃ add-tag not removed. Concurrent add+remove → add wins (tag not in remove set).
  class ORSet {
    constructor() { this.adds = new Map(); this.removes = new Set(); }
    add(elem, tag) { this.adds.set(elem, tag); }
    remove(elem, tag) { this.removes.add(tag); }
    has(elem) { const tag = this.adds.get(elem); return tag && !this.removes.has(tag); }
    merge(other) { /* union adds, union removes */ }
  }
  ```
- RGA Sequence (for message ordering):
  ```js
  // Each message has unique ID (HLC + region). Insert after 'originLeft' ID.
  // Total order: sort by (originLeft, id). Converges.
  ```
- Hybrid Logical Clocks:
  ```js
  class HLC {
    constructor() { this.l = 0; this.c = 0; }
    now() { const wall = Date.now(); this.l = Math.max(this.l, wall); return { l: this.l, c: this.c++ }; }
    receive(hlc) { this.l = Math.max(this.l, hlc.l); this.c = (this.l === hlc.l) ? Math.max(this.c, hlc.c) + 1 : 1; }
    static compare(a, b) { if (a.l !== b.l) return a.l - b.l; return a.c - b.c; }
  }
  ```
- Merkle tree for anti-entropy: each region computes Merkle root of CRDT state per room. Exchange roots, request missing leaves.
- Integration: `DistributedRoomManager` (issue 11) delegates to `MultiRegionCoordinator` for cross-region ops. Local ops still use Redis pub/sub.