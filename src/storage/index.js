/**
 * @fileoverview Factory for creating storage adapter instances.
 *
 * Reads configuration from environment variables (or an explicit config object)
 * and returns the appropriate StorageAdapter implementation.
 *
 * Supported adapters (STORAGE_ADAPTER env var):
 *   "postgres" — PostgresAdapter backed by a PostgreSQL database.
 *   "memory"   — MemoryAdapter for tests and local development (default).
 *   "none"     — A no-op adapter that silently discards all writes.
 *
 * Environment variables:
 *   STORAGE_ADAPTER                    "postgres" | "memory" | "none"  (default: "memory")
 *   DATABASE_URL                       Postgres connection string       (required for "postgres")
 *   STORAGE_POOL_SIZE                  Max Postgres pool connections   (default: 10)
 *   STORAGE_BATCH_SIZE                 Number of events per bulk insert (default: 100)
 *   STORAGE_FLUSH_INTERVAL_MS          Ms between periodic flushes    (default: 1000)
 *   STORAGE_MAX_BUFFER_SIZE            Hard cap on the write buffer    (default: 10000)
 *   STORAGE_RAW_RETENTION_DAYS         Days to keep raw events         (default: 7)
 *   STORAGE_1M_RETENTION_DAYS          Days to keep 1m downsample      (default: 90)
 *   STORAGE_1H_RETENTION_DAYS          Days to keep 1h aggregates      (default: 365)
 *   STORAGE_1D_RETENTION_DAYS          Days to keep 1d rollups         (default: 2555)
 *   STORAGE_COMPACTION_INTERVAL_MS     Compaction interval in ms       (default: 300000)
 *   STORAGE_COMPACTION_BATCH_SIZE      Batch size for deletion loops   (default: 10000)
 *   TIMESCALEDB_ENABLED                Enable TimescaleDB features     (default: false)
 */

import { MemoryAdapter } from "./memory.js";
import { PostgresAdapter } from "./postgres.js";
import { assertStorageAdapter } from "./adapter.js";

/**
 * A no-op adapter that satisfies the StorageAdapter interface but discards
 * every write.  Useful when STORAGE_ADAPTER=none to run the server without
 * any storage dependency.
 *
 * @implements {import("./adapter.js").StorageAdapter}
 */
class NoneAdapter {
  async writeBatch(_events) {}
  async queryRoom(_roomId, _options) { return []; }
  async querySpatial(_bounds, _options) { return []; }
  async getLatest(_roomId) { return null; }
  async close() {}
  async compact(_overrides) {
    return { rawDeleted: 0, downsample1m: 0, aggregate1h: 0, aggregate1d: 0, durationMs: 0 };
  }
  async getCompactionStatus() {
    return {
      lastRun: null,
      nextRun: null,
      tiers: [
        { name: "raw", rows: 0, sizeBytes: 0, oldest: null, newest: null },
        { name: "1m", rows: 0, sizeBytes: 0, oldest: null, newest: null },
        { name: "1h", rows: 0, sizeBytes: 0, oldest: null, newest: null },
        { name: "1d", rows: 0, sizeBytes: 0, oldest: null, newest: null },
      ],
    };
  }
}

/**
 * Creates and returns a StorageAdapter based on the supplied configuration
 * (or environment variables when no config object is provided).
 *
 * @param {object} [config={}]
 * @param {string}  [config.adapter]                     - Override STORAGE_ADAPTER env var.
 * @param {string}  [config.connectionString]            - Override DATABASE_URL env var.
 * @param {number}  [config.poolSize]                    - Override STORAGE_POOL_SIZE env var.
 * @param {number}  [config.batchSize]                   - Override STORAGE_BATCH_SIZE env var.
 * @param {number}  [config.flushIntervalMs]             - Override STORAGE_FLUSH_INTERVAL_MS env var.
 * @param {number}  [config.maxBufferSize]               - Override STORAGE_MAX_BUFFER_SIZE env var.
 * @param {number}  [config.rawRetentionDays]            - Override STORAGE_RAW_RETENTION_DAYS env var.
 * @param {number}  [config.downsample1mRetentionDays]   - Override STORAGE_1M_RETENTION_DAYS env var.
 * @param {number}  [config.downsample1hRetentionDays]   - Override STORAGE_1H_RETENTION_DAYS env var.
 * @param {number}  [config.aggregate1dRetentionDays]    - Override STORAGE_1D_RETENTION_DAYS env var.
 * @param {number}  [config.compactionIntervalMs]        - Override STORAGE_COMPACTION_INTERVAL_MS env var.
 * @param {number}  [config.compactionBatchSize]         - Override STORAGE_COMPACTION_BATCH_SIZE env var.
 * @param {boolean} [config.timescaleDbEnabled]          - Override TIMESCALEDB_ENABLED env var.
 * @returns {import("./adapter.js").StorageAdapter}
 * @throws {Error} When an unrecognised adapter name is supplied.
 */
export function createStorageAdapter(config = {}) {
  const adapterName = (
    config.adapter ??
    process.env.STORAGE_ADAPTER ??
    "memory"
  ).toLowerCase();

  let instance;

  switch (adapterName) {
    case "postgres": {
      const connectionString =
        config.connectionString ?? process.env.DATABASE_URL;

      if (!connectionString) {
        throw new Error(
          "STORAGE_ADAPTER=postgres requires DATABASE_URL (or config.connectionString) to be set"
        );
      }

      instance = new PostgresAdapter({
        connectionString,
        poolSize:
          config.poolSize ??
          parseInt(process.env.STORAGE_POOL_SIZE ?? "10", 10),
        batchSize:
          config.batchSize ??
          parseInt(process.env.STORAGE_BATCH_SIZE ?? "100", 10),
        flushIntervalMs:
          config.flushIntervalMs ??
          parseInt(process.env.STORAGE_FLUSH_INTERVAL_MS ?? "1000", 10),
        maxBufferSize:
          config.maxBufferSize ??
          parseInt(process.env.STORAGE_MAX_BUFFER_SIZE ?? "10000", 10),
        rawRetentionDays:
          config.rawRetentionDays ??
          parseInt(process.env.STORAGE_RAW_RETENTION_DAYS ?? "7", 10),
        downsample1mRetentionDays:
          config.downsample1mRetentionDays ??
          parseInt(process.env.STORAGE_1M_RETENTION_DAYS ?? "90", 10),
        downsample1hRetentionDays:
          config.downsample1hRetentionDays ??
          parseInt(process.env.STORAGE_1H_RETENTION_DAYS ?? "365", 10),
        aggregate1dRetentionDays:
          config.aggregate1dRetentionDays ??
          parseInt(process.env.STORAGE_1D_RETENTION_DAYS ?? "2555", 10),
        compactionIntervalMs:
          config.compactionIntervalMs ??
          parseInt(process.env.STORAGE_COMPACTION_INTERVAL_MS ?? "300000", 10),
        compactionBatchSize:
          config.compactionBatchSize ??
          parseInt(process.env.STORAGE_COMPACTION_BATCH_SIZE ?? "10000", 10),
        timescaleDbEnabled:
          config.timescaleDbEnabled ??
          (process.env.TIMESCALEDB_ENABLED === "true"),
      });
      break;
    }

    case "memory": {
      instance = new MemoryAdapter();
      break;
    }

    case "none": {
      instance = new NoneAdapter();
      break;
    }

    default:
      throw new Error(
        `Unknown storage adapter: "${adapterName}". Valid values are "postgres", "memory", "none".`
      );
  }

  // Validate at runtime that the instance fulfils the interface.
  assertStorageAdapter(instance);

  return instance;
}

export { MemoryAdapter } from "./memory.js";
export { PostgresAdapter } from "./postgres.js";
export { assertStorageAdapter } from "./adapter.js";
