/**
 * @fileoverview StorageAdapter interface definition and base class.
 *
 * All storage backends must implement this interface. The interface is defined
 * both as a JSDoc typedef and as an abstract base class with default
 * method stubs throwing 'Not implemented'.
 */

/**
 * A single location event persisted by the storage adapter.
 *
 * @typedef {object} LocationEvent
 * @property {string}  clientId   - Unique identifier of the client that sent the update.
 * @property {string}  roomId     - Room the client was publishing into.
 * @property {number}  latitude   - Latitude in decimal degrees [-90, 90].
 * @property {number}  longitude  - Longitude in decimal degrees [-180, 180].
 * @property {number}  [altitude] - Altitude in metres above sea level (optional).
 * @property {number}  [accuracy] - Horizontal accuracy radius in metres (optional).
 * @property {number}  [speed]    - Ground speed in m/s (optional).
 * @property {string}  timestamp  - ISO 8601 string representing when the fix was taken.
 * @property {string}  [id]       - Storage-assigned unique row identifier (set on read).
 * @property {string}  [createdAt]- ISO 8601 string representing when the row was inserted (set on read).
 */

/**
 * Spatial bounding-box used for range queries.
 *
 * @typedef {object} SpatialBounds
 * @property {number} minLat - Southern boundary (inclusive).
 * @property {number} maxLat - Northern boundary (inclusive).
 * @property {number} minLon - Western boundary (inclusive).
 * @property {number} maxLon - Eastern boundary (inclusive).
 */

/**
 * Options for time-range / limit queries.
 *
 * @typedef {object} QueryOptions
 * @property {Date}       [from]       - Start of the time range (inclusive; defaults to 24 hours ago).
 * @property {Date}       [to]         - End of the time range (inclusive; defaults to Date.now()).
 * @property {number}     [limit=1000] - Maximum number of events to return (defaults to 1000).
 * @property {Resolution} [resolution="auto"] - Data resolution: "raw" | "1m" | "1h" | "1d" | "auto" (defaults to "auto").
 *                                              "auto" selects the finest resolution tier that returns ≤ limit points.
 */

/**
 * Resolution tier identifier:
 *  - "raw": Exact point-by-point raw events.
 *  - "1m": 1-minute downsampled median coordinates.
 *  - "1h": 1-hour aggregate statistics and distance.
 *  - "1d": 1-day summary rollups.
 *  - "auto": Dynamically selects the finest resolution tier that returns ≤ limit points.
 *
 * @typedef {"raw"|"1m"|"1h"|"1d"|"auto"} Resolution
 */

/**
 * Result of a compaction run.
 *
 * @typedef {object} CompactionResult
 * @property {number} rawDeleted     - Number of raw rows deleted (or partitions dropped).
 * @property {number} downsample1m   - Number of 1-minute downsample rows written.
 * @property {number} aggregate1h    - Number of 1-hour aggregate rows written.
 * @property {number} aggregate1d    - Number of 1-day aggregate rows written.
 * @property {number} durationMs     - Total compaction duration in milliseconds.
 * @property {string} [error]        - Error message if compaction partially failed.
 */

/**
 * Status of a single retention tier.
 *
 * @typedef {object} TierStatus
 * @property {string}      name      - Tier name: "raw", "1m", "1h", "1d".
 * @property {number}      rows      - Approximate row count.
 * @property {number}      sizeBytes - Approximate storage size in bytes.
 * @property {string|null} oldest    - ISO 8601 timestamp of the oldest row, or null.
 * @property {string|null} newest    - ISO 8601 timestamp of the newest row, or null.
 */

/**
 * Compaction subsystem status.
 *
 * @typedef {object} CompactionStatus
 * @property {string|null}  lastRun  - ISO 8601 timestamp of the last completed compaction run.
 * @property {string|null}  nextRun  - ISO 8601 timestamp of the next scheduled run.
 * @property {TierStatus[]} tiers    - Per-tier statistics.
 */

/**
 * The StorageAdapter interface.
 *
 * Every concrete adapter (PostgresAdapter, MemoryAdapter, …) must implement
 * all methods below. Methods are async — callers must await them or
 * handle the returned Promise.
 *
 * @typedef {object} StorageAdapterInterface
 *
 * @property {function(LocationEvent[]): Promise<void>} writeBatch
 *   Persist a batch of location events. Implementations are free to buffer
 *   internally; this call returns once the events have been accepted into the
 *   write pipeline (not necessarily flushed to disk).
 *
 * @property {function(string, QueryOptions=): Promise<LocationEvent[]>} queryRoom
 *   Retrieve historical location events for a given room, optionally filtered
 *   by time range, capped to a maximum result count, and resolved to a specific
 *   data tier via the resolution parameter.
 *
 * @property {function(SpatialBounds, {limit?: number}=): Promise<LocationEvent[]>} querySpatial
 *   Return events whose coordinates fall within the supplied bounding box.
 *   For PostgresAdapter this uses a database-level index; for MemoryAdapter
 *   it performs an in-memory linear scan.
 *
 * @property {function(string): Promise<LocationEvent|null>} getLatest
 *   Return the single most-recent event (by `timestamp`) for a room, or
 *   `null` if no events exist for that room.
 *
 * @property {function(): Promise<void>} close
 *   Release all resources held by the adapter (connections, timers, …).
 *   Must be idempotent — calling it multiple times must not throw.
 *
 * @property {function(number=, number=, number=, number=): Promise<CompactionResult>} compact
 *   Run the compaction pipeline: drop expired raw partitions, compute
 *   1-minute downsamples, compute 1-hour aggregates, compute 1-day rollups.
 *   Accepts optional retention overrides. Returns a summary of work done.
 *
 * @property {function(): Promise<CompactionStatus>} getCompactionStatus
 *   Return current compaction status including last/next run times and
 *   per-tier statistics (row count, size, time range).
 */

/**
 * Base StorageAdapter class.
 * Concrete storage adapters should extend this class or implement its interface.
 */
export class StorageAdapter {
  /**
   * Persist a batch of location events.
   *
   * @param {LocationEvent[]} _events - Location events to persist.
   * @returns {Promise<void>}
   */
  async writeBatch(_events) {
    throw new Error("Not implemented");
  }

  /**
   * Retrieve historical location events for a given room, optionally filtered
   * by time range, capped to a maximum result count, and resolved to a specific
   * data tier via the resolution parameter.
   *
   * @param {string} _roomId - Unique identifier of the room to query.
   * @param {QueryOptions} [_options={}] - Query filtering and resolution options.
   * @param {Date} [_options.from=new Date(Date.now() - 86400000)] - Start of the time range (defaults to 24 hours ago).
   * @param {Date} [_options.to=new Date()] - End of the time range (defaults to Date.now()).
   * @param {number} [_options.limit=1000] - Maximum number of events to return (defaults to 1000).
   * @param {Resolution} [_options.resolution="auto"] - Data resolution: "raw" | "1m" | "1h" | "1d" | "auto" (defaults to "auto").
   *                                                    "auto" selects the finest resolution tier that returns ≤ limit points.
   * @returns {Promise<LocationEvent[]>}
   */
  async queryRoom(_roomId, _options = {}) {
    throw new Error("Not implemented");
  }

  /**
   * Return events whose coordinates fall within the supplied bounding box.
   *
   * @param {SpatialBounds} _bounds - Bounding box boundaries.
   * @param {{limit?: number}} [_options={}] - Query options.
   * @returns {Promise<LocationEvent[]>}
   */
  async querySpatial(_bounds, _options = {}) {
    throw new Error("Not implemented");
  }

  /**
   * Return the single most-recent event (by `timestamp`) for a room, or
   * `null` if no events exist for that room.
   *
   * @param {string} _roomId - Unique identifier of the room.
   * @returns {Promise<LocationEvent|null>}
   */
  async getLatest(_roomId) {
    throw new Error("Not implemented");
  }

  /**
   * Release all resources held by the adapter (connections, timers, …).
   * Must be idempotent — calling it multiple times must not throw.
   *
   * @returns {Promise<void>}
   */
  async close() {
    throw new Error("Not implemented");
  }

  /**
   * Run the compaction pipeline: drop expired raw partitions, compute
   * 1-minute downsamples, compute 1-hour aggregates, compute 1-day rollups.
   *
   * @param {number} [_rawRetentionDays] - Days to retain raw events.
   * @param {number} [_downsample1mRetentionDays] - Days to retain 1-minute downsampled events.
   * @param {number} [_downsample1hRetentionDays] - Days to retain 1-hour aggregate events.
   * @param {number} [_aggregate1dRetentionDays] - Days to retain 1-day aggregate rollups.
   * @returns {Promise<CompactionResult>} Resolves to a CompactionResult object.
   */
  async compact(
    _rawRetentionDays,
    _downsample1mRetentionDays,
    _downsample1hRetentionDays,
    _aggregate1dRetentionDays
  ) {
    throw new Error("Not implemented");
  }

  /**
   * Return current compaction status including last/next run times and
   * per-tier statistics (row count, size, time range).
   *
   * @returns {Promise<CompactionStatus>} Resolves to an object: { lastRun, nextRun, tiers: [{ name, rows, sizeBytes, oldest, newest }] }.
   */
  async getCompactionStatus() {
    throw new Error("Not implemented");
  }
}

/**
 * Verifies that an object implements the StorageAdapter interface at runtime.
 * Throws a TypeError listing every missing method if the check fails.
 *
 * @param {unknown} adapter - The object to validate.
 * @returns {void}
 * @throws {TypeError} When one or more required methods are absent.
 */
export function assertStorageAdapter(adapter) {
  const required = [
    "writeBatch",
    "queryRoom",
    "querySpatial",
    "getLatest",
    "close",
    "compact",
    "getCompactionStatus",
  ];
  const missing = required.filter((m) => typeof adapter[m] !== "function");
  if (missing.length > 0) {
    throw new TypeError(
      `StorageAdapter is missing required method(s): ${missing.join(", ")}`
    );
  }
}
