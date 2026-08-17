## Title
Implement event sourcing with CQRS for location state, immutable event log, and materialized view projections for audit and replay

## Difficulty
10/10 — Expert. Estimated effort: 6–8 days for a senior engineer.

## Context
The current architecture treats location updates as ephemeral messages — broadcast, optionally persisted (issue 10), but no immutable event log. For regulatory compliance (ELD mandates, GDPR audit trails, insurance disputes), every state change must be recorded as an immutable, append-only event. The `README.md` mentions "geofencing enforcement" and "historical tracks" but the storage adapter (issue 10) only stores current state (latest location per room), not the full event history with causality.

## Problem statement
Implement an Event Sourcing + CQRS layer that:

1. **Immutable event log**: Every `location_update`, `geofence_entry`, `geofence_exit`, `room_join`, `room_leave`, `session_resumed`, `ack`, `nack` is persisted as an event `{ eventId, eventType, aggregateId, aggregateType, payload, metadata, timestamp, causationId, correlationId }` in an append-only store (PostgreSQL table with `eventId` UUID v7, partitioned by day).

2. **Aggregate roots**: 
   - `VehicleAggregate(clientId)` — current location, room memberships, geofence inside-set, sequence numbers.
   - `RoomAggregate(roomId)` — member list, message rate, sequence counter.
   - `FleetAggregate(fleetId)` — aggregated statistics, alert counts.

3. **Command side (write model)**: `CommandHandler` processes commands (`PublishLocation`, `JoinRoom`, `Acknowledge`, `UpdateGeofence`) → validates business rules → emits events → persists to event store. Commands are idempotent (deduplication via `commandId`).

4. **Query side (read models / projections)**: Materialized views rebuilt from event log:
   - `vehicle_current_state` — latest location, rooms, geofences per client (for fast `getLatest`).
   - `room_membership` — current members per room (for broadcast).
   - `geofence_violations` — time-series of entry/exit events (for compliance reports).
   - `message_sequence` — per-room sequence numbers (for replay).
   - Projections updated asynchronously via event handlers (eventual consistency < 100ms).

5. **Event replay and reconstruction**: `VehicleAggregate` can be reconstructed from event log for any point in time (temporal queries). `replayEvents(aggregateId, fromSequence, toSequence)` returns events for debugging/audit.

6. **Snapshotting**: Every 1000 events per aggregate, write a snapshot `{ aggregateId, sequence, state, timestamp }` to avoid full replay. Reconstruction loads latest snapshot + subsequent events.

7. **Integration with existing systems**:
   - `server.js` sends commands instead of direct broadcasts.
   - `room-manager.js` becomes a projection (read model).
   - `geofence-engine.js` emits events, doesn't directly broadcast.
   - Storage adapter (issue 10) gets new `EventStore` interface.

## Current behavior
- `server.js`: direct `rooms.broadcast()` — no event log.
- `storage/postgres.js`: `location_events` table stores locations, not events.
- No aggregate roots, no projections, no snapshotting.
- No `causationId`/`correlationId` tracking.

## Required behavior
- New module `src/event-sourcing.js` exporting `EventStore`, `CommandHandler`, `ProjectionManager`, `AggregateBase`.
- `EventStore` interface: `append(events)`, `getEvents(aggregateId, fromSeq)`, `getEventsByCorrelation(correlationId)`, `getSnapshot(aggregateId)`, `saveSnapshot(snapshot)`.
- `PostgresEventStore` implements with partitioned `events` table and `snapshots` table.
- `CommandHandler` registers command handlers: `PublishLocationCommand`, `JoinRoomCommand`, `AcknowledgeCommand`, etc.
- Each command handler returns `Event[]` — persisted atomically (single transaction).
- `ProjectionManager` runs projection handlers: `VehicleStateProjection`, `RoomMembershipProjection`, `GeofenceViolationProjection`, `SequenceProjection`.
- Projections are idempotent (track last processed `eventId`).
- `AggregateBase` provides `apply(event)`, `loadFromHistory(events)`, `getUncommittedEvents()`.
- Metrics: `event_store_append_duration_ms`, `projection_lag_events`, `snapshot_count`.

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`, `topology-manager.js`.
- Do not modify existing test files. New test files required.
- Add no new npm dependencies — use existing `pg`, `uuid`.
- Event store must be the **source of truth** — all state derived from events.
- Projections must handle out-of-order events (use `eventId` timestamp ordering).
- Snapshotting must not block command processing — async background task.
- Event schema must be versioned (issue 13) — `eventType` includes version: `location_update.v2`.
- Backwards compatibility: existing `location_events` table coexists; new event store is additional.

## Acceptance criteria
- [ ] `PublishLocationCommand` → persists `LocationUpdated` event with `causationId` = commandId
- [ ] `VehicleAggregate` reconstructed from events matches live state
- [ ] `RoomMembershipProjection` updated within 100ms of `RoomJoined` event
- [ ] Snapshot created every 1000 events, reconstruction loads snapshot + delta
- [ ] `getEventsByCorrelation(correlationId)` returns full causal chain
- [ ] Temporal query: `VehicleAggregate.at(timestamp)` returns state at that time
- [ ] Idempotent commands: duplicate `commandId` → no duplicate events
- [ ] `npm run lint` passes
- [ ] All existing tests pass (projections mirror current behavior)
- [ ] New test file: `tests/event-sourcing.test.js` with command handling, projection, snapshotting, temporal queries

## Out of scope
- Full CQRS framework — minimal implementation for this domain.
- Event store replication / multi-master — single writer assumed.
- Complex saga/orchestration — simple command → events only.
- Event schema migration tooling — versioned event types handled in code.

## Hints and references
- Event store schema:
  ```sql
  CREATE TABLE events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    sequence BIGINT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    causation_id UUID,
    correlation_id UUID
  ) PARTITION BY RANGE (timestamp);
  CREATE INDEX events_aggregate_seq_idx ON events (aggregate_id, sequence);
  CREATE INDEX events_correlation_idx ON events (correlation_id);
  ```
- Aggregate base:
  ```js
  class AggregateBase {
    constructor(id) { this.id = id; this.sequence = 0; this.uncommitted = []; }
    apply(event) { this.sequence = event.sequence; this.mutate(event); }
    mutate(event) { throw new Error('Not implemented'); }
    loadFromHistory(events) { events.forEach(e => this.apply(e)); }
    getUncommittedEvents() { return this.uncommitted; }
    markCommitted() { this.uncommitted = []; }
    record(event) { this.uncommitted.push(event); this.apply(event); }
  }
  ```
- Command handler pattern:
  ```js
  async function handlePublishLocation(cmd) {
    const aggregate = await vehicleRepository.get(cmd.clientId);
    aggregate.record(new LocationUpdatedEvent({ ... }));
    await eventStore.append(aggregate.getUncommittedEvents());
    aggregate.markCommitted();
  }
  ```
- Projection handler:
  ```js
  async function vehicleStateProjection(event) {
    if (event.eventType === 'LocationUpdated') {
      await pool.query('INSERT INTO vehicle_current_state ... ON CONFLICT DO UPDATE ...');
    }
  }
  ```