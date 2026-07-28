/**
 * @fileoverview In-memory StorageAdapter implementation.
 *
 * Designed for unit testing and local development.  All data lives in a
 * plain JavaScript array and is lost when the process exits.  Every method
 * of the StorageAdapter interface is implemented so test suites can run the
 * same contract tests against MemoryAdapter and PostgresAdapter.
 */

import { v4 as uuid } from "uuid";

/**
 * @implements {import("./adapter.js").StorageAdapter}
 */
export class MemoryAdapter {
  constructor() {
    /** @type {import("./adapter.js").LocationEvent[]} */
    this._events = [];

    /** Whether close() has been called. */
    this._closed = false;
  }

  /**
   * Stores every event in the internal array, assigning synthetic `id` and
   * `createdAt` fields to mirror what the Postgres adapter returns on reads.
   *
   * @param {import("./adapter.js").LocationEvent[]} events
   * @returns {Promise<void>}
   */
  async writeBatch(events) {
    if (this._closed) throw new Error("MemoryAdapter is closed");
    if (!Array.isArray(events) || events.length === 0) return;

    const now = new Date().toISOString();
    for (const event of events) {
      this._events.push({
        ...event,
        id: uuid(),
        createdAt: now,
      });
    }
  }

  /**
   * Returns events for a specific room, optionally filtered by time range,
   * ordered by `timestamp` ascending and capped to `limit`.
   *
   * @param {string} roomId
   * @param {import("./adapter.js").QueryOptions} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async queryRoom(roomId, options = {}) {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const { from, to, limit } = options;

    let results = this._events.filter((e) => {
      if (e.roomId !== roomId) return false;
      const ts = new Date(e.timestamp).getTime();
      if (from instanceof Date && ts < from.getTime()) return false;
      if (to instanceof Date && ts > to.getTime()) return false;
      return true;
    });

    // Sort ascending by event timestamp
    results.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

    if (typeof limit === "number" && limit > 0) {
      results = results.slice(0, limit);
    }

    return results;
  }

  /**
   * Returns events whose coordinates fall within the bounding box.
   * O(n) linear scan — acceptable for testing; not for production scale.
   *
   * @param {import("./adapter.js").SpatialBounds} bounds
   * @param {{ limit?: number }} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async querySpatial(bounds, options = {}) {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const { minLat, maxLat, minLon, maxLon } = bounds;
    const { limit } = options;

    let results = this._events.filter(
      (e) =>
        e.latitude >= minLat &&
        e.latitude <= maxLat &&
        e.longitude >= minLon &&
        e.longitude <= maxLon
    );

    if (typeof limit === "number" && limit > 0) {
      results = results.slice(0, limit);
    }

    return results;
  }

  /**
   * Returns the most-recent event (by `timestamp`) for a room, or `null`.
   *
   * @param {string} roomId
   * @returns {Promise<import("./adapter.js").LocationEvent|null>}
   */
  async getLatest(roomId) {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const roomEvents = this._events.filter((e) => e.roomId === roomId);
    if (roomEvents.length === 0) return null;

    return roomEvents.reduce((latest, e) =>
      new Date(e.timestamp).getTime() > new Date(latest.timestamp).getTime() ? e : latest
    );
  }

  /**
   * Clears internal state.  Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    this._closed = true;
    this._events = [];
  }

  /**
   * Convenience helper for tests: reset internal state without marking the
   * adapter as closed.
   *
   * @returns {void}
   */
  clear() {
    this._events = [];
  }
}
