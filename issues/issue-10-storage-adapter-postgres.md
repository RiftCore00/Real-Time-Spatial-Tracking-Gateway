## Title
Implement the pluggable Storage Adapter interface with PostgreSQL backend, write batching, spatial indexing, and backpressure-aware integration into the broadcast pipeline

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The `README.md` architecture diagram (lines 43–62) explicitly shows a **Storage Adapter** layer between the Room Manager and "Postgres / Mongo / etc." The `Core Features` section (line 37) lists "Extensible Storage Adapter — Plug in your preferred database (PostgreSQL, MongoDB, InfluxDB, etc.) for persisting historical tracks." The `docker-compose.yml` provisions a PostgreSQL 16 instance with database `spatial_tracking` and user `tracker`. The `.env.example` includes `DATABASE_URL=`.

**None of this exists.** There is zero database code anywhere in the codebase. No schema, no queries, no connection pooling, no adapter interface. The `docker-compose.yml` Postgres service sits unused. The `DATABASE_URL` env var is never read. The promised storage layer is entirely unimplemented.

For a fleet-tracking system, this gap means: location updates are broadcast in real-time but never persisted. When a client reconnects (issue 6), there is no historical data to replay from. When a dispatcher wants to view a vehicle's route history, there is nothing to query. Geofence violation events have no audit trail. The system is a pure relay with amnesia — useful for live viewing, but useless for any after-the-fact analysis.

## Problem statement
Implement a pluggable Storage Adapter with:

1. **Adapter interface** (`StorageAdapter`): Define an abstract interface with methods:
   - `writeBatch(events: LocationEvent[]): Promise<void>` — persist a batch of location events.
   - `queryRoom(roomId: string, options: { from?: Date, to?: Date, limit?: number }): Promise<LocationEvent[]>` — retrieve historical locations for a room.
   - `querySpatial(bounds: { minLat, maxLat, minLon, maxLon }, options?: { limit?: number }): Promise<LocationEvent[]>` — spatial range query.
   - `getLatest(roomId: string): Promise<LocationEvent | null>` — get the most recent location for a room.
   - `close(): Promise<void>` — clean shutdown.

2. **PostgreSQL backend** (`PostgresAdapter`): Implement the adapter using `pg` (node-postgres) with:
   - Connection pooling (configurable `poolSize`, default 10).
   - Schema migration: auto-create the `location_events` table with columns `(id, client_id, room_id, latitude, longitude, altitude, accuracy, speed, timestamp, created_at)` and appropriate indexes.
   - Spatial index: use PostGIS `geography` type or a GiST index on `(latitude, longitude)` for efficient range queries.
   - Write batching: accumulate events in a buffer (configurable `batchSize`, default 100, `flushIntervalMs`, default 1000) and flush via `INSERT ... SELECT unnest(...)` for amortized round-trip cost.

3. **In-memory backend** (`MemoryAdapter`): A trivial in-memory implementation for testing, implementing the same interface with `Array` storage and linear scan for spatial queries.

4. **Backpressure integration**: The adapter's `writeBatch` must not block the event loop. If the database is slow (connection pool exhausted, network latency), the write buffer must grow to a configurable `maxBufferSize` and then start dropping oldest events (with a counter metric). The adapter must never cause the WebSocket broadcast pipeline to stall.

5. **Configuration**: `STORAGE_ADAPTER` env var selects the backend (`"postgres"` | `"memory"` | `"none"`). `DATABASE_URL` configures the Postgres connection. `STORAGE_BATCH_SIZE`, `STORAGE_FLUSH_INTERVAL_MS`, `STORAGE_MAX_BUFFER_SIZE` tune the batching.

## Current behavior
- `docker-compose.yml` lines 15–24: Postgres service exists but is unused.
- `.env.example` line 4: `DATABASE_URL=` is empty.
- No `package.json` dependency for `pg` or any database driver.
- No file in `src/` references `DATABASE_URL` or imports any database module.
- The `RoomManager.broadcast()` sends location updates to WebSocket clients but never persists them.

## Required behavior
- `StorageAdapter` interface defined and exported from a new `src/storage/adapter.js`.
- `PostgresAdapter` implemented in `src/storage/postgres.js`.
- `MemoryAdapter` implemented in `src/storage/memory.js`.
- Factory function `createStorageAdapter(config)` in `src/storage/index.js` selects the backend based on config.
- Write batching accumulates events and flushes periodically or when `batchSize` is reached.
- Spatial query uses database-level indexing (not in-memory linear scan for Postgres).
- All writes are async and non-blocking — `writeBatch` returns a Promise that resolves when the batch is persisted, but the caller is not blocked from continuing broadcasts.
- `npm test` passes with the `MemoryAdapter` (used in tests).
- `npm run lint` passes.

## Constraints
- Do not add `pg` to `package.json` for the initial implementation — use dynamic `import("pg")` only when `STORAGE_ADAPTER=postgres`. This keeps the dev/test dependency light.
- Do not change `server.js`, `room-manager.js`, `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, or any existing test file.
- New test files for the storage adapters are allowed.
- The `MemoryAdapter` must implement the exact same interface as `PostgresAdapter` — tests must be runnable against both.
- The write buffer must be bounded: if `maxBufferSize` events are queued and the database is unreachable, the oldest events are dropped and a `storage_dropped_events_total` counter is incremented.
- The Postgres schema must use `TIMESTAMPTZ` for temporal columns and `DOUBLE PRECISION` for coordinates.

## Acceptance criteria
- [ ] `src/storage/adapter.js` exports the `StorageAdapter` interface (as a documented JSDoc typedef or abstract class)
- [ ] `src/storage/postgres.js` exports `PostgresAdapter` that implements all 5 interface methods
- [ ] `src/storage/memory.js` exports `MemoryAdapter` that implements all 5 interface methods
- [ ] `src/storage/index.js` exports `createStorageAdapter(config)` factory
- [ ] `PostgresAdapter` auto-creates the `location_events` table with correct schema on first use
- [ ] `PostgresAdapter.writeBatch` batches inserts (uses `unnest` or similar bulk insert, not individual INSERT statements)
- [ ] `MemoryAdapter.writeBatch` stores events in an array, `queryRoom` returns filtered results, `querySpatial` does linear scan
- [ ] `getLatest(roomId)` returns the most recent event by timestamp
- [ ] Write buffer is bounded: after `maxBufferSize` queued events, oldest are dropped
- [ ] `storage_dropped_events_total` counter increments when events are dropped
- [ ] All existing tests pass unchanged
- [ ] New test file: `tests/storage-adapter.test.js` tests both adapters against the same interface contract
- [ ] New test file: `tests/storage-postgres.test.js` integration tests (skipped when `DATABASE_URL` is not set)

## Out of scope
- Changes to existing source files (`server.js`, `room-manager.js`, etc.) — the storage integration into the broadcast pipeline is a separate issue.
- MongoDB, InfluxDB, or other backend implementations (only Postgres and in-memory).
- Schema migration tooling (the adapter creates the table if it doesn't exist — no Flyway/Alembic).
- Real-time CDC (Change Data Capture) from the database.
- Geo-fencing event detection (that's a separate service concern).

## Hints and references
- The `pg` library's `Pool` class handles connection pooling: `new Pool({ connectionString: process.env.DATABASE_URL, max: 10 })`.
- For batched inserts in Postgres, use `INSERT INTO location_events (client_id, room_id, ...) SELECT * FROM unnest($1::text[], $2::text[], ...)` where the arrays are built from the batch. This is a single round-trip for up to thousands of rows.
- For spatial indexing without PostGIS, a composite index on `(latitude, longitude)` with a bounding-box query `(lat BETWEEN minLat AND maxLat AND lon BETWEEN minLon AND maxLon)` is sufficient for moderate scale. With PostGIS, use `ST_MakePoint(lon, lat)::geography` and `ST_Intersects`.
- The `MemoryAdapter` spatial query is simply `events.filter(e => e.latitude >= minLat && e.latitude <= maxLat && e.longitude >= minLon && e.longitude <= maxLon)`. This is O(n) but acceptable for testing.
- For the write buffer: use a `Array` as a FIFO queue. `push()` to enqueue, `splice(0, batchSize)` to dequeue a batch. When `length > maxBufferSize`, `splice(0, length - maxBufferSize)` to drop oldest.
- Dynamic import for `pg`: `const { Pool } = await import("pg")` inside the `PostgresAdapter` constructor. This allows the module to be loaded without `pg` installed when using `memory` or `none` adapters.
