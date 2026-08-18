/**
 * @fileoverview In-memory StorageAdapter implementation.
 *
 * Designed for unit testing and local development.  All data lives in a
 * plain JavaScript array and is lost when the process exits.  Every method
 * of the StorageAdapter interface is implemented so test suites can run the
 * same contract tests against MemoryAdapter and PostgresAdapter.
 *
 * Includes in-memory implementations of compaction, downsample, and
 * resolution-aware queryRoom for testing without PostgreSQL.
 */

import { v4 as uuid } from "uuid";

/**
 * Compute median of a numeric array.
 * @param {number[]} arr
 * @returns {number}
 */
function median(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/**
 * Compute haversine distance between two lat/lon points in metres.
 * @param {number} lat1
 * @param {number} lon1
 * @param {number} lat2
 * @param {number} lon2
 * @returns {number}
 */
function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(a));
}

/**
 * Truncate a Date to the start of its minute.
 * @param {Date|string} d
 * @returns {number} Unix timestamp (ms) at start of minute.
 */
function truncateToMinute(d) {
  const t = typeof d === "string" ? new Date(d).getTime() : d.getTime();
  return Math.floor(t / 60000) * 60000;
}

/**
 * Truncate a Date to the start of its hour.
 * @param {Date|string} d
 * @returns {number} Unix timestamp (ms) at start of hour.
 */
function truncateToHour(d) {
  const t = typeof d === "string" ? new Date(d).getTime() : d.getTime();
  return Math.floor(t / 3600000) * 3600000;
}

/**
 * Truncate a Date to the start of its day (UTC).
 * @param {Date|string} d
 * @returns {number} Unix timestamp (ms) at start of day.
 */
function truncateToDay(d) {
  const t = typeof d === "string" ? new Date(d).getTime() : d.getTime();
  return Math.floor(t / 86400000) * 86400000;
}

/**
 * @implements {import("./adapter.js").StorageAdapter}
 */
export class MemoryAdapter {
  constructor() {
    /** @type {import("./adapter.js").LocationEvent[]} */
    this._events = [];

    /** @type {object[]} In-memory 1-minute downsample rows. */
    this._downsample1m = [];

    /** @type {object[]} In-memory 1-hour aggregate rows. */
    this._aggregate1h = [];

    /** @type {object[]} In-memory 1-day aggregate rows. */
    this._aggregate1d = [];

    /** Whether close() has been called. */
    this._closed = false;

    /** Retention configuration. */
    this._retention = {
      rawRetentionDays: 7,
      downsample1mRetentionDays: 90,
      downsample1hRetentionDays: 365,
      aggregate1dRetentionDays: 2555,
    };

    /** Last compaction run timestamp. */
    this._lastCompactionRun = null;

    /** Next scheduled compaction run timestamp. */
    this._nextCompactionRun = null;
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
   * Supports resolution parameter for tiered data access.
   *
   * @param {string} roomId
   * @param {import("./adapter.js").QueryOptions} [options={}]
   * @returns {Promise<import("./adapter.js").LocationEvent[]>}
   */
  async queryRoom(roomId, options = {}) {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const { from, to, limit, resolution = "auto" } = options;

    let resolved = resolution;
    if (resolution === "auto") {
      resolved = this._autoResolve(from, to, limit);
    }

    if (resolved === "raw") {
      return this._queryRoomRaw(roomId, from, to, limit);
    }
    if (resolved === "1m") {
      return this._queryRoomDownsample(roomId, from, to, limit, "1m");
    }
    if (resolved === "1h") {
      return this._queryRoomAggregate(roomId, from, to, limit, "1h");
    }
    if (resolved === "1d") {
      return this._queryRoomAggregate(roomId, from, to, limit, "1d");
    }

    return this._queryRoomRaw(roomId, from, to, limit);
  }

  /**
   * Auto-select resolution based on time range and desired point count.
   * @private
   * @param {Date} [from]
   * @param {Date} [to]
   * @param {number} [limit]
   * @returns {string}
   */
  _autoResolve(from, to, limit) {
    // When no time range is specified or only one bound is given,
    // default to raw for backwards compatibility
    if (!from || !to) return "raw";

    const maxPoints = limit || 1000;
    const fromMs = from.getTime();
    const toMs = to.getTime();
    const rangeMs = toMs - fromMs;

    // Estimate points at each resolution
    if (rangeMs <= 0) return "raw";

    const rawPoints = Math.floor(rangeMs / 1000); // 1 point/sec estimate
    if (rawPoints <= maxPoints) return "raw";

    const m1Points = Math.floor(rangeMs / 60000);
    if (m1Points <= maxPoints) return "1m";

    const h1Points = Math.floor(rangeMs / 3600000);
    if (h1Points <= maxPoints) return "1h";

    return "1d";
  }

  /**
   * Query raw events for a room.
   * @private
   */
  _queryRoomRaw(roomId, from, to, limit) {
    let results = this._events.filter((e) => {
      if (e.roomId !== roomId) return false;
      const ts = new Date(e.timestamp).getTime();
      if (from instanceof Date && ts < from.getTime()) return false;
      if (to instanceof Date && ts > to.getTime()) return false;
      return true;
    });

    results.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

    if (typeof limit === "number" && limit > 0) {
      results = results.slice(0, limit);
    }

    return results;
  }

  /**
   * Query 1-minute downsampled data for a room.
   * @private
   */
  _queryRoomDownsample(roomId, from, to, limit, _tier) {
    let rows = this._downsample1m.filter((r) => {
      if (r.roomId !== roomId) return false;
      if (from instanceof Date && r.bucketEnd < from.getTime()) return false;
      if (to instanceof Date && r.bucketStart > to.getTime()) return false;
      return true;
    });

    rows.sort((a, b) => a.bucketStart - b.bucketStart);

    if (typeof limit === "number" && limit > 0) {
      rows = rows.slice(0, limit);
    }

    return rows.map((r) => ({
      clientId: "downsample",
      roomId: r.roomId,
      latitude: r.latitude,
      longitude: r.longitude,
      altitude: r.altitude,
      accuracy: null,
      speed: r.maxSpeed,
      timestamp: new Date(r.bucketStart).toISOString(),
      id: uuid(),
      createdAt: new Date().toISOString(),
    }));
  }

  /**
   * Query hourly or daily aggregate data for a room.
   * @private
   */
  _queryRoomAggregate(roomId, from, to, limit, tier) {
    const source = tier === "1h" ? this._aggregate1h : this._aggregate1d;
    let rows = source.filter((r) => {
      if (r.roomId !== roomId) return false;
      if (from instanceof Date && r.bucketEnd < from.getTime()) return false;
      if (to instanceof Date && r.bucketStart > to.getTime()) return false;
      return true;
    });

    rows.sort((a, b) => a.bucketStart - b.bucketStart);

    if (typeof limit === "number" && limit > 0) {
      rows = rows.slice(0, limit);
    }

    return rows.map((r) => ({
      clientId: `aggregate-${tier}`,
      roomId: r.roomId,
      latitude: r.latitude,
      longitude: r.longitude,
      altitude: r.altitude,
      accuracy: null,
      speed: r.avgSpeed,
      timestamp: new Date(r.bucketStart).toISOString(),
      id: uuid(),
      createdAt: new Date().toISOString(),
    }));
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
   * Run in-memory compaction: delete expired raw events, compute
   * 1-minute downsamples, 1-hour aggregates, and 1-day rollups.
   *
   * @param {object} [overrides={}]
   * @param {number} [overrides.rawRetentionDays]
   * @param {number} [overrides.downsample1mRetentionDays]
   * @param {number} [overrides.downsample1hRetentionDays]
   * @param {number} [overrides.aggregate1dRetentionDays]
   * @returns {Promise<import("./adapter.js").CompactionResult>}
   */
  async compact(overrides = {}) {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const start = Date.now();
    const retention = { ...this._retention, ...overrides };
    let rawDeleted = 0;

    // Phase 1: Delete expired raw events
    const rawCutoff = Date.now() - retention.rawRetentionDays * 86400000;
    const beforeCount = this._events.length;
    this._events = this._events.filter((e) => new Date(e.timestamp).getTime() >= rawCutoff);
    rawDeleted = beforeCount - this._events.length;

    // Phase 2: Compute 1-minute downsamples
    const downsample1m = this._computeDownsample1m();
    this._downsample1m = downsample1m;

    // Phase 3: Compute 1-hour aggregates from 1m data
    const aggregate1h = this._computeAggregate1h();
    this._aggregate1h = aggregate1h;

    // Phase 4: Compute 1-day aggregates from 1h data
    const aggregate1d = this._computeAggregate1d();
    this._aggregate1d = aggregate1d;

    // Phase 5: Trim downsample/aggregate tiers to retention
    const m1Cutoff = Date.now() - retention.downsample1mRetentionDays * 86400000;
    const h1Cutoff = Date.now() - retention.downsample1hRetentionDays * 86400000;
    const d1Cutoff = Date.now() - retention.aggregate1dRetentionDays * 86400000;
    this._downsample1m = this._downsample1m.filter((r) => r.bucketStart >= m1Cutoff);
    this._aggregate1h = this._aggregate1h.filter((r) => r.bucketStart >= h1Cutoff);
    this._aggregate1d = this._aggregate1d.filter((r) => r.bucketStart >= d1Cutoff);

    this._lastCompactionRun = new Date().toISOString();

    return {
      rawDeleted,
      downsample1m: this._downsample1m.length,
      aggregate1h: this._aggregate1h.length,
      aggregate1d: this._aggregate1d.length,
      durationMs: Date.now() - start,
    };
  }

  /**
   * Compute 1-minute downsample buckets from raw events.
   * @private
   * @returns {object[]}
   */
  _computeDownsample1m() {
    const buckets = new Map();
    for (const e of this._events) {
      const key = `${e.roomId}|${truncateToMinute(e.timestamp)}`;
      if (!buckets.has(key)) {
        buckets.set(key, {
          roomId: e.roomId,
          bucketStart: truncateToMinute(e.timestamp),
          lats: [],
          lons: [],
          alts: [],
          speeds: [],
        });
      }
      const b = buckets.get(key);
      b.lats.push(e.latitude);
      b.lons.push(e.longitude);
      if (e.altitude != null) b.alts.push(e.altitude);
      if (e.speed != null) b.speeds.push(e.speed);
    }

    return Array.from(buckets.values()).map((b) => ({
      roomId: b.roomId,
      bucketStart: b.bucketStart,
      bucketEnd: b.bucketStart + 59999,
      latitude: median(b.lats),
      longitude: median(b.lons),
      altitude: b.alts.length > 0 ? median(b.alts) : null,
      maxSpeed: b.speeds.length > 0 ? Math.max(...b.speeds) : null,
      pointCount: b.lats.length,
    }));
  }

  /**
   * Compute 1-hour aggregates from 1-minute downsample data.
   * @private
   * @returns {object[]}
   */
  _computeAggregate1h() {
    if (this._downsample1m.length === 0) return [];

    const buckets = new Map();
    for (const r of this._downsample1m) {
      const key = `${r.roomId}|${truncateToHour(new Date(r.bucketStart))}`;
      if (!buckets.has(key)) {
        buckets.set(key, {
          roomId: r.roomId,
          bucketStart: truncateToHour(new Date(r.bucketStart)),
          points: [],
          totalPointCount: 0,
        });
      }
      const b = buckets.get(key);
      b.points.push({ lat: r.latitude, lon: r.longitude });
      b.totalPointCount += r.pointCount;
    }

    return Array.from(buckets.values()).map((b) => {
      let totalDistance = 0;
      for (let i = 1; i < b.points.length; i++) {
        totalDistance += haversine(
          b.points[i - 1].lat, b.points[i - 1].lon,
          b.points[i].lat, b.points[i].lon
        );
      }
      const avgLat = b.points.reduce((s, p) => s + p.lat, 0) / b.points.length;
      const avgLon = b.points.reduce((s, p) => s + p.lon, 0) / b.points.length;

      return {
        roomId: b.roomId,
        bucketStart: b.bucketStart,
        bucketEnd: b.bucketStart + 3599999,
        latitude: avgLat,
        longitude: avgLon,
        altitude: null,
        avgSpeed: null,
        maxSpeed: null,
        minSpeed: null,
        totalDistance,
        pointCount: b.totalPointCount,
      };
    });
  }

  /**
   * Compute 1-day aggregates from 1-hour aggregate data.
   * @private
   * @returns {object[]}
   */
  _computeAggregate1d() {
    if (this._aggregate1h.length === 0) return [];

    const buckets = new Map();
    for (const r of this._aggregate1h) {
      const key = `${r.roomId}|${truncateToDay(new Date(r.bucketStart))}`;
      if (!buckets.has(key)) {
        buckets.set(key, {
          roomId: r.roomId,
          bucketStart: truncateToDay(new Date(r.bucketStart)),
          points: [],
          totalPointCount: 0,
          totalDistance: 0,
        });
      }
      const b = buckets.get(key);
      b.points.push({ lat: r.latitude, lon: r.longitude });
      b.totalPointCount += r.pointCount;
      b.totalDistance += r.totalDistance;
    }

    return Array.from(buckets.values()).map((b) => {
      const avgLat = b.points.reduce((s, p) => s + p.lat, 0) / b.points.length;
      const avgLon = b.points.reduce((s, p) => s + p.lon, 0) / b.points.length;

      return {
        roomId: b.roomId,
        bucketStart: b.bucketStart,
        bucketEnd: b.bucketStart + 86399999,
        latitude: avgLat,
        longitude: avgLon,
        altitude: null,
        avgSpeed: null,
        maxSpeed: null,
        minSpeed: null,
        totalDistance: b.totalDistance,
        pointCount: b.totalPointCount,
      };
    });
  }

  /**
   * Return compaction status with per-tier statistics.
   *
   * @returns {Promise<import("./adapter.js").CompactionStatus>}
   */
  async getCompactionStatus() {
    if (this._closed) throw new Error("MemoryAdapter is closed");

    const rawOldest = this._events.length > 0
      ? new Date(Math.min(...this._events.map((e) => new Date(e.timestamp).getTime()))).toISOString()
      : null;
    const rawNewest = this._events.length > 0
      ? new Date(Math.max(...this._events.map((e) => new Date(e.timestamp).getTime()))).toISOString()
      : null;

    const m1Oldest = this._downsample1m.length > 0
      ? new Date(Math.min(...this._downsample1m.map((r) => r.bucketStart))).toISOString()
      : null;
    const m1Newest = this._downsample1m.length > 0
      ? new Date(Math.max(...this._downsample1m.map((r) => r.bucketStart))).toISOString()
      : null;

    const h1Oldest = this._aggregate1h.length > 0
      ? new Date(Math.min(...this._aggregate1h.map((r) => r.bucketStart))).toISOString()
      : null;
    const h1Newest = this._aggregate1h.length > 0
      ? new Date(Math.max(...this._aggregate1h.map((r) => r.bucketStart))).toISOString()
      : null;

    const d1Oldest = this._aggregate1d.length > 0
      ? new Date(Math.min(...this._aggregate1d.map((r) => r.bucketStart))).toISOString()
      : null;
    const d1Newest = this._aggregate1d.length > 0
      ? new Date(Math.max(...this._aggregate1d.map((r) => r.bucketStart))).toISOString()
      : null;

    return {
      lastRun: this._lastCompactionRun,
      nextRun: this._nextCompactionRun
        ? new Date(this._nextCompactionRun).toISOString()
        : null,
      tiers: [
        {
          name: "raw",
          rows: this._events.length,
          sizeBytes: JSON.stringify(this._events).length,
          oldest: rawOldest,
          newest: rawNewest,
        },
        {
          name: "1m",
          rows: this._downsample1m.length,
          sizeBytes: JSON.stringify(this._downsample1m).length,
          oldest: m1Oldest,
          newest: m1Newest,
        },
        {
          name: "1h",
          rows: this._aggregate1h.length,
          sizeBytes: JSON.stringify(this._aggregate1h).length,
          oldest: h1Oldest,
          newest: h1Newest,
        },
        {
          name: "1d",
          rows: this._aggregate1d.length,
          sizeBytes: JSON.stringify(this._aggregate1d).length,
          oldest: d1Oldest,
          newest: d1Newest,
        },
      ],
    };
  }

  /**
   * Clears internal state.  Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    this._closed = true;
    this._events = [];
    this._downsample1m = [];
    this._aggregate1h = [];
    this._aggregate1d = [];
  }

  /**
   * Convenience helper for tests: reset internal state without marking the
   * adapter as closed.
   *
   * @returns {void}
   */
  clear() {
    this._events = [];
    this._downsample1m = [];
    this._aggregate1h = [];
    this._aggregate1d = [];
  }
}
