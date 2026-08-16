## Title
Implement end-to-end formal verification of core safety properties using TLA+ model checking, property-based testing, and runtime assertion monitoring

## Difficulty
10/10 — Expert. Estimated effort: 8–12 days for a senior engineer.

## Context
The gateway handles safety-critical operations: geofence enforcement (preventing unauthorized entry to hazardous zones), collision avoidance (proximity alerts), and regulatory compliance (ELD mandates). A single correctness bug — message reordering, duplicate processing, state machine violation — can cause a vessel to enter a restricted zone undetected, a collision warning to be suppressed, or an audit trail to be corrupted. The current test suite (166+ tests) covers functional behavior but **cannot** prove absence of race conditions, liveness violations, or invariant violations under all possible interleavings.

## Problem statement
Establish a formal verification framework that:

1. **TLA+ specification of core protocols**: Write a TLA+ spec (`spec/gateway.tla`) modeling:
   - Room membership state machine (join, leave, disconnect, crash).
   - Message sequencing and replay (issue 6).
   - Exactly-once delivery with ACKs (issue 16).
   - Geofence entry/exit state transitions (issue 12).
   - Session resumption (issue 18).
   - Distributed CRDT convergence (issue 24).
   - Model check with TLC for: **Safety** (no duplicate deliveries, no lost messages, geofence state consistency), **Liveness** (every message eventually delivered or nacked, every client eventually reconnects), **Invariants** (sequence numbers monotonic, room membership matches broadcast targets).

2. **Property-based testing with fast-check**: For each TypeScript module, define generators and properties:
   - `RoomManager`: `join/leave/disconnect` preserves `∀c,r: c∈r.members ⇔ r∈c.rooms`.
   - `RateLimiter`: `check` never allows > limit in window; `cleanup` removes only expired.
   - `Predictor`: Kalman filter covariance `P` remains positive semi-definite.
   - `CRDTs`: `merge(a, merge(b, c)) = merge(merge(a, b), c)` (associativity), `merge(a, b) = merge(b, a)` (commutativity), `merge(a, a) = a` (idempotence).
   - Run 10,000+ iterations per property in CI.

3. **Runtime assertion monitoring**: Instrument production code with `assert()` checks for critical invariants (enabled via `NODE_ENV=production`):
   - `roomManager.broadcast`: `assert(message.seq === roomSeq + 1)`.
   - `sessionManager.load`: `assert(decrypted.clientId === authenticatedClientId)`.
   - `geofenceEngine.processLocationUpdate`: `assert(newInsideSet ⊆ allFences ∪ oldInsideSet)` (no spontaneous entries).
   - `predictor.update`: `assert(P.isPositiveSemiDefinite())`.
   - Violations log structured error + increment metric `assertion_violation_total{component, invariant}` — do NOT crash (fail-open for availability).

4. **Contract testing for cross-module boundaries**: 
   - `server.js` ↔ `room-manager.js`: contract test that every `broadcast` call matches expected signature and side effects.
   - `geofence-engine` ↔ `storage`: contract that `upsertFence` persists all vertices.
   - Use `pact` or custom contract runner.

5. **Mutation testing**: Run `stryker` or custom mutator on core modules (`room-manager`, `validator`, `rate-limiter`, `geofence-engine`, `predictor`, `crdt`). Target: ≥ 90% mutation score. Mutants that survive must be documented as false positives or added as regression tests.

6. **Chaos engineering integration**: 
   - `ChaosMonkey` class injects faults in test environment: network partition (drop messages), clock skew (±5s), process crash (kill -9), Redis failover.
   - Verify safety properties hold under chaos: no duplicate geofence alerts, no message loss beyond acknowledged window.

7. **CI/CD integration**: 
   - `npm run verify:tla` — runs TLC model checker (must pass).
   - `npm run verify:property` — runs fast-check (must pass).
   - `npm run verify:mutation` — runs stryker (must meet threshold).
   - `npm run verify:chaos` — runs chaos tests (must pass).
   - All verification steps required for merge.

## Current behavior
- `tests/*.test.js`: example-based unit/integration tests only.
- No TLA+ spec, no property-based tests, no runtime assertions, no mutation testing, no chaos engineering.
- No formal verification in CI.

## Required behavior
- `spec/gateway.tla` — TLA+ specification of core protocols.
- `spec/README.md` — how to run TLC, interpret results.
- `tests/property/*.test.js` — fast-check properties for each module.
- `src/assertions.js` — runtime assertion helpers, enabled via `ASSERTIONS_ENABLED=true`.
- `tests/contract/*.test.js` — contract tests for module boundaries.
- `tests/chaos/*.test.js` — chaos monkey tests.
- `package.json` scripts: `verify:tla`, `verify:property`, `verify:mutation`, `verify:chaos`, `verify:all`.
- GitHub Actions workflow `.github/workflows/verify.yml` running all verification steps.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`, `topology-manager.js`, `event-sourcing.js`, `collaborative-editor.js`, `predictor.js`, `multi-region.js`.
- Do not modify existing test files (except adding assertion calls where appropriate).
- Add `fast-check`, `stryker` (or `mutation-testing`), `tla2tools` (TLC) to devDependencies.
- TLA+ spec must be maintainable — use modules, constants for configuration.
- Property tests must complete in < 60s total in CI.
- Runtime assertions must add < 1% overhead when enabled.
- Mutation testing runs on core modules only (not full codebase).

## Acceptance criteria
- [ ] TLA+ spec models room membership, sequencing, replay, exactly-once, geofence, session, CRDT
- [ ] TLC finds 0 safety/liveness violations for config: 3 clients, 2 rooms, 5 messages
- [ ] Property tests: 10+ properties per core module, all pass 10,000 iterations
- [ ] Runtime assertions: critical invariants instrumented, violations logged not crashed
- [ ] Contract tests: all module boundaries covered
- [ ] Mutation score ≥ 90% for `room-manager`, `validator`, `rate-limiter`, `geofence-engine`, `predictor`, `crdt`
- [ ] Chaos tests: safety properties hold under network partition, clock skew, crash
- [ ] `npm run verify:all` passes in CI
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New verification artifacts committed: `spec/`, `tests/property/`, `tests/contract/`, `tests/chaos/`

## Out of scope
- Full system TLA+ spec (only core safety-critical protocols).
- Theorem proving (TLAPS) — model checking only.
- Production chaos engineering (test environment only).
- Formal verification of cryptographic primitives (TLS, JWT, AES-GCM) — assume correct.

## Hints and references
- TLA+ room membership spec sketch:
  ```tla
  VARIABLES members, clientRooms, seqNum
  Join(c, r) == 
    /\ c \notin members[r]
    /\ members' = [members EXCEPT ![r] = @ \cup {c}]
    /\ clientRooms' = [clientRooms EXCEPT ![c] = @ \cup {r}]
  Leave(c, r) == 
    /\ c \in members[r]
    /\ members' = [members EXCEPT ![r] = @ \ {c}]
    /\ clientRooms' = [clientRooms EXCEPT ![c] = @ \ {r}]
  Invariant == \A c, r: c \in members[r] <=> r \in clientRooms[c]
  ```
- fast-check property for RoomManager:
  ```js
  test.prop([fc.array(fc.nat()), fc.array(fc.nat())], (joins, leaves) => {
    const rm = new RoomManager();
    joins.forEach(([c, r]) => rm.join(c, r, ws));
    leaves.forEach(([c, r]) => rm.leave(c, r));
    // Check invariant
    for (const [c, rooms] of rm._clientRooms) {
      for (const r of rooms) {
        expect(rm._rooms.get(r).has(c)).toBe(true);
      }
    }
  });
  ```
- Runtime assertion helper:
  ```js
  export function assert(condition, message, meta = {}) {
    if (!condition) {
      logger.error('ASSERTION_VIOLATION', { message, ...meta });
      metrics.increment('assertion_violation_total', { component: meta.component });
      if (process.env.ASSERTIONS_THROW === 'true') throw new Error(message);
    }
  }
  ```
- Mutation testing: `npx stryker run --mutate "src/room-manager.js,src/validator.js,..."`
- Chaos monkey: randomly drop `ws.send` calls, delay `setTimeout`, kill process mid-test.