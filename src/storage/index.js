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
 *   STORAGE_ADAPTER          "postgres" | "memory" | "none"  (default: "memory")
 *   DATABASE_URL             Postgres connection string       (required for "postgres")
 *   STORAGE_BATCH_SIZE       Number of events per bulk insert (default: 100)
 *   STORAGE_FLUSH_INTERVAL_MS Ms between periodic flushes    (default: 1000)
 *   STORAGE_MAX_BUFFER_SIZE  Hard cap on the write buffer    (default: 10000)
 *   STORAGE_POOL_SIZE        Max Postgres pool connections   (default: 10)
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
}

/**
 * Creates and returns a StorageAdapter based on the supplied configuration
 * (or environment variables when no config object is provided).
 *
 * @param {object} [config={}]
 * @param {string}  [config.adapter]           - Override STORAGE_ADAPTER env var.
 * @param {string}  [config.connectionString]  - Override DATABASE_URL env var.
 * @param {number}  [config.poolSize]          - Override STORAGE_POOL_SIZE env var.
 * @param {number}  [config.batchSize]         - Override STORAGE_BATCH_SIZE env var.
 * @param {number}  [config.flushIntervalMs]   - Override STORAGE_FLUSH_INTERVAL_MS env var.
 * @param {number}  [config.maxBufferSize]     - Override STORAGE_MAX_BUFFER_SIZE env var.
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
