/**
 * @fileoverview StorageAdapter interface definition.
 *
 * All storage backends must implement this interface. The interface is defined
 * as a JSDoc typedef so it can be used for type checking without introducing
 * a runtime class dependency.
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
 * @property {Date}   [from]  - Start of the time range (inclusive).
 * @property {Date}   [to]    - End of the time range (inclusive).
 * @property {number} [limit] - Maximum number of events to return.
 */

/**
 * The StorageAdapter interface.
 *
 * Every concrete adapter (PostgresAdapter, MemoryAdapter, …) must implement
 * all five methods below.  Methods are async — callers must await them or
 * handle the returned Promise.
 *
 * @typedef {object} StorageAdapter
 *
 * @property {function(LocationEvent[]): Promise<void>} writeBatch
 *   Persist a batch of location events.  Implementations are free to buffer
 *   internally; this call returns once the events have been accepted into the
 *   write pipeline (not necessarily flushed to disk).
 *
 * @property {function(string, QueryOptions=): Promise<LocationEvent[]>} queryRoom
 *   Retrieve historical location events for a given room, optionally filtered
 *   by time range and capped to a maximum result count.
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
 */

/**
 * Verifies that an object implements the StorageAdapter interface at runtime.
 * Throws a TypeError listing every missing method if the check fails.
 *
 * @param {unknown} adapter - The object to validate.
 * @returns {void}
 * @throws {TypeError} When one or more required methods are absent.
 */
export function assertStorageAdapter(adapter) {
  const required = ["writeBatch", "queryRoom", "querySpatial", "getLatest", "close"];
  const missing = required.filter((m) => typeof adapter[m] !== "function");
  if (missing.length > 0) {
    throw new TypeError(
      `StorageAdapter is missing required method(s): ${missing.join(", ")}`
    );
  }
}
