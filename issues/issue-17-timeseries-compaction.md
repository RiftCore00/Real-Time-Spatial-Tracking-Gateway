## Title
Implement time-series data compaction with automated downsampling, retention policies, and continuous aggregates for cost-effective long-term historical storage

## Difficulty
10/10 — Expert. Estimated effort: 5–7 days for a senior engineer.

## Context
The storage adapter (issue 10) persists every location update as a raw row in `location_events`. A fleet of 10,000 vehicles sending 1 update/second generates 864M rows/day (74B rows/90 days). At ~200 bytes/row, that's 170 GB/day raw — completely unsustainable for PostgreSQL without compaction.

The `README.md` mentions "persisting historical tracks" (line 37) but has no retention, downsampling, or compaction strategy. Querying raw data for a 30-day route history returns millions of points — unusable for visualization (browser can't render 1M points) and slow for analysis.

Production fleet tracking requires:
- **Raw retention**: 7 days (for replay, exactly-once, geofence audit).
- **Downsampled retention**: 1 minute resolution for 90 days (for route visualization).
- **Hourly aggregates**: 1 year (for analytics: distance, speed, idle time, geofence dwell).
- **Continuous aggregates**: Materialized views that auto-refresh, so dashboards query pre-computed data.

## Problem statement
Design and implement a time-series compaction subsystem integrated with the storage adapter that:

1. **Tiered retention policies**: Configurable per room (or globally):
   - `rawRetentionDays: 7` — keep every point.
   - `downsample1mRetentionDays: 90` — keep 1-minute downsampled (median/mean position per minute).
   - `downsample1hRetentionDays: 365` — keep 1-hour aggregates (count, min/max/avg speed, distance, bounding box).
   - `aggregate1dRetentionDays: 2555` (7 years) — daily rollups for compliance.

2. **Automated downsampling jobs**: Background workers (pg_cron or Node.js timers) that:
   - **1-minute downsample**: For each room, each minute bucket, compute: `median(lat)`, `median(lon)`, `avg(altitude)`, `avg(accuracy)`, `max(speed)`, `count(*) as pointCount`, `min(timestamp) as bucketStart`, `max(timestamp) as bucketEnd`. Store in `location_events_1m` (hypertable or partitioned table).
   - **1-hour aggregate**: From 1m table, compute: `sum(pointCount)`, `sum(distance)` (haversine between consecutive median points), `avg(speed)`, `max(speed)`, `min(speed)`, `st_makeline` of median points for route geometry.
   - **Daily rollup**: From 1h table, compute daily totals.

3. **Continuous aggregates (TimescaleDB native)**: If Postgres has TimescaleDB extension, use `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) REFRESH ...` for automatic maintenance. If not, implement equivalent with pg_cron + manual refresh.

4. **Transparent query routing**: `queryRoom(roomId, { from, to, limit, resolution })` where `resolution` can be `"raw" | "1m" | "1h" | "1d" | "auto"`. `"auto"` selects the finest resolution that returns ≤ `limit` points (default 1000). This allows dashboards to request "last 30 days" and get 1-hour aggregates automatically.

5. **Data lifecycle management**:
   - Raw data deletion: `DELETE FROM location_events WHERE timestamp < now() - rawRetentionDays` (partitioned by day — `DROP PARTITION` is instant).
   - Downsampled data deletion: same pattern for each tier.
   - Compaction must not block writes — use `pg_sleep` between batches, or `DELETE ... WHERE ctid IN (SELECT ctid FROM ... LIMIT 10000)` loops.

6. **Storage adapter interface extension**: New methods:
   - `compact(rawRetentionDays, downsample1mRetentionDays, ...): Promise<CompactionResult>`
   - `queryRoom(roomId, options: { from, to, limit, resolution }): Promise<LocationEvent[]>`
   - `getCompactionStatus(): Promise<{ lastRun, nextRun, tiers: [{ name, rows, sizeBytes, oldest, newest }] }>`

7. **Metrics and observability**:
   - `storage_compaction_duration_seconds{phase="raw_delete|downsample_1m|aggregate_1h"}`
   - `storage_tier_rows{tier="raw|1m|1h|1d"}`
   - `storage_tier_size_bytes{tier="raw|1m|1h|1d"}`
   - `storage_compaction_lag_minutes{tier="1m|1h|1d"}` — how far behind the continuous aggregate is.

## Current behavior
- `src/storage/postgres.js`: single `location_events` table, no partitioning, no downsampling, no retention.
- `src/storage/adapter.js`: interface has `queryRoom`, `querySpatial`, `getLatest`, `writeBatch` — no resolution parameter, no compaction.
- No background workers, no pg_cron, no TimescaleDB usage.
- `docker-compose.yml`: plain Postgres 16, no TimescaleDB.

## Required behavior
- `src/storage/postgres.js` extended with:
  - Table partitioning: `location_events` partitioned by `DAY(timestamp)` (native PG16 declarative partitioning).
  - Downsample tables: `location_events_1m`, `location_events_1h`, `location_events_1d` — also partitioned.
  - Compaction job: `async runCompaction()` called every 5 minutes (configurable). Uses `LIMIT` batches to avoid long locks.
  - Continuous aggregate setup: if `TIMESCALEDB_ENABLED=true`, create materialized views with continuous refresh. Else, manual refresh in compaction job.
  - `queryRoom` with `resolution` parameter: routes to appropriate table based on time range and resolution.
- `src/storage/memory.js`: in-memory implementation of downsampling (for tests) — simple array reduction.
- `src/storage/adapter.js`: extended interface with new methods.
- Configuration: `STORAGE_RAW_RETENTION_DAYS`, `STORAGE_1M_RETENTION_DAYS`, `STORAGE_1H_RETENTION_DAYS`, `STORAGE_1D_RETENTION_DAYS`, `STORAGE_COMPACTION_INTERVAL_MS`, `STORAGE_COMPACTION_BATCH_SIZE`.
- Migration script: `src/storage/migration-compaction.sql` for existing deployments.

## Constraints
- Do not modify `server.js`, `room-manager.js`, `validator.js`, `auth.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`.
- Do not modify existing test files. New test files required.
- Add `pg-cron` as a Postgres extension (not npm dependency) — document in `docker-compose.yml` to use `timescale/timescaledb:latest-pg16` image.
- If TimescaleDB not used, implement manual continuous aggregates with triggers or pg_cron — no new npm deps.
- Compaction must be idempotent and resumable — if killed mid-run, next run continues.
- Raw data deletion must use partition `DROP` (instant) not `DELETE` (slow). Requires declarative partitioning.
- Downsampling queries must be efficient: use `LATERAL` joins or window functions to compute median without sorting entire partition.
- Memory usage during compaction: ≤ 100MB (stream results, don't load entire partition).
- The `queryRoom` resolution routing must be backwards compatible — default `resolution: "auto"` preserves current behavior for existing callers.

## Acceptance criteria
- [ ] `location_events` table is partitioned by day (verify with `\d+ location_events`)
- [ ] `runCompaction()` deletes raw partitions older than `rawRetentionDays` via `DROP PARTITION`
- [ ] `runCompaction()` populates `location_events_1m` with correct median positions per minute per room
- [ ] `runCompaction()` populates `location_events_1h` with correct aggregates (distance, speed, count)
- [ ] `queryRoom(roomId, { from: -30d, to: now, resolution: "auto" })` returns ~720 points (1h resolution) not 2.5M raw points
- [ ] `queryRoom(roomId, { from: -1h, to: now, resolution: "raw" })` returns raw points
- [ ] `queryRoom(roomId, { from: -30d, to: now, resolution: "1m" })` returns 1-minute downsampled
- [ ] Continuous aggregate (if TimescaleDB) refreshes automatically within 5 minutes of new data
- [ ] Metrics: `storage_tier_rows` shows correct counts for each tier
- [ ] Compaction job completes in <60s for 100M raw rows (partition drop is instant; downsample is the cost)
- [ ] `npm run lint` passes
- [ ] All existing tests pass (MemoryAdapter implements same interface)
- [ ] New test file: `tests/storage-compaction.test.js` with integration tests (requires TimescaleDB or pg_cron)

## Out of scope
- TimescaleDB licensing/commercial features — use Apache 2.0 features only (continuous aggregates are free).
- Cross-room spatial aggregates (heatmaps) — single-room only for this issue.
- Real-time materialized view refresh (continuous aggregates have ~1min lag — acceptable).
- Data export / backup — separate concern.
- Compression (PostgreSQL `pglz` or `zstd` on partitions) — optional optimization.

## Hints and references
- PG16 declarative partitioning:
  ```sql
  CREATE TABLE location_events (
    ...,
    timestamp TIMESTAMPTZ NOT NULL
  ) PARTITION BY RANGE (timestamp);
  -- Create partitions for each day:
  CREATE TABLE location_events_2026_01_15 PARTITION OF location_events
    FOR VALUES FROM ('2026-01-15') TO ('2026-01-16');
  -- Drop old partition:
  ALTER TABLE location_events DETACH PARTITION location_events_2026_01_01;
  DROP TABLE location_events_2026_01_01;
  ```
- Median in Postgres: `percentile_cont(0.5) WITHIN GROUP (ORDER BY latitude)` — but this is slow on large partitions. Better: `approximate_percentile` from `postgresql-hll` or use `percentile_disc` with `GROUP BY minute_bucket`.
- 1-minute bucket: `date_trunc('minute', timestamp) as bucket`.
- Haversine distance in SQL:
  ```sql
  6371000 * 2 * asin(sqrt(
    sin((lat2 - lat1)/2)^2 + cos(lat1)*cos(lat2)*sin((lon2 - lon1)/2)^2
  ))
  ```
- For continuous aggregates (TimescaleDB):
  ```sql
  CREATE MATERIALIZED VIEW location_events_1m
  WITH (timescaledb.continuous) AS
  SELECT room_id, date_trunc('minute', timestamp) as bucket,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY latitude) as lat,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY longitude) as lon,
         count(*) as point_count
  FROM location_events
  GROUP BY room_id, bucket;
  REFRESH MATERIALIZED VIEW location_events_1m; -- or continuous auto-refresh
  ```
- For manual refresh without TimescaleDB: pg_cron job every 5 min runs `REFRESH MATERIALIZED VIEW CONCURRENTLY location_events_1m;`.
- `queryRoom` resolution routing logic:
  ```js
  const range = to - from;
  if (resolution === "auto") {
    if (range < 3600000) resolution = "raw";           // < 1 hour
    else if (range < 86400000) resolution = "1m";      // < 1 day
    else if (range < 604800000) resolution = "1h";     // < 1 week
    else resolution = "1d";
  }
  const table = `location_events_${resolution === "raw" ? "" : resolution}`;
  ```