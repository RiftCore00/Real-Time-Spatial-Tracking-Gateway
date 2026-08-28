/**
 * @fileoverview Event Sourcing + CQRS layer for the spatial tracking gateway.
 *
 * The event store is the source of truth: every state change (location updates,
 * room joins/leaves, geofence crossings, acknowledgements, session resumptions)
 * is recorded as an immutable, append-only event. Aggregates are rebuilt from
 * the log (with periodic snapshots), commands drive writes through
 * `CommandHandler`, and materialised read models are maintained by
 * `ProjectionManager`.
 *
 * Exports:
 *  - {@link EventStore}            interface contract (+ `assertEventStore`)
 *  - {@link InMemoryEventStore}    dependency-free store used by default
 *  - {@link PostgresEventStore}    partitioned-table store (lazy `pg` import)
 *  - {@link AggregateBase}         base class with apply/record/replay support
 *  - {@link VehicleAggregate}      per-client write model
 *  - {@link RoomAggregate}         per-room write model
 *  - {@link FleetAggregate}        fleet-level statistics model
 *  - {@link AggregateRepository}   load/save + background snapshotting
 *  - {@link CommandHandler}        idempotent command dispatch (write side)
 *  - Command envelopes             PublishLocationCommand, JoinRoomCommand, …
 *  - {@link ProjectionManager}     asynchronous read-model maintenance
 *  - Projections                   VehicleStateProjection, RoomMembershipProjection,
 *                                  GeofenceViolationProjection, SequenceProjection
 *  - Projection sinks              MemoryProjectionSink / SqlProjectionSink
 *  - {@link replayEvents}          range query helper for audit/debug
 *  - {@link createEventSourcing}   wiring helper used by server.js
 *
 * Event schema (versioned via the eventType suffix, e.g. `location_update.v2`):
 * `{ eventId, eventType, aggregateId, aggregateType, sequence, payload,
 *    metadata, timestamp, causationId, correlationId }`
 *
 * `causationId` points at the immediate cause (usually the commandId);
 * `correlationId` groups the whole causal chain of an operation.
 */

import { v4 as uuidv4, v7 as uuidv7 } from "uuid";
import { logger } from "./logger.js";

// ─────────────────────────── event types ─────────────────────────────────────

/**
 * Versioned event types. The numeric suffix is the schema version; consumers
 * must match on the base name (see {@link baseEventType}) so new versions do
 * not break projections.
 *
 * @type {Readonly<{
 *   LocationUpdated: string,
 *   RoomJoined: string,
 *   RoomLeft: string,
 *   GeofenceEntered: string,
 *   GeofenceExited: string,
 *   GeofenceUpdated: string,
 *   SessionResumed: string,
 *   MessageAcknowledged: string,
 *   MessageNacked: string,
 * }>}
 */
export const EventTypes = Object.freeze({
  LocationUpdated: "location_update.v2",
  RoomJoined: "room_joined.v1",
  RoomLeft: "room_left.v1",
  GeofenceEntered: "geofence_entered.v1",
  GeofenceExited: "geofence_exited.v1",
  GeofenceUpdated: "geofence_updated.v1",
  SessionResumed: "session_resumed.v1",
  MessageAcknowledged: "message_ack.v1",
  MessageNacked: "message_nack.v1",
});

/**
 * Strips the `.vN` version suffix from an event type.
 *
 * @param {string} eventType - Versioned event type, e.g. `"location_update.v2"`.
 * @returns {string} Base event type, e.g. `"location_update"`.
 */
export function baseEventType(eventType) {
  if (typeof eventType !== "string") return "";
  const dot = eventType.lastIndexOf(".v");
  if (dot === -1) return eventType;
  const suffix = eventType.slice(dot + 2);
  return /^\d+$/.test(suffix) ? eventType.slice(0, dot) : eventType;
}

/**
 * Command type identifiers understood by {@link CommandHandler}.
 */
export const CommandTypes = Object.freeze({
  PublishLocation: "publish_location",
  JoinRoom: "join_room",
  LeaveRoom: "leave_room",
  Acknowledge: "acknowledge",
  NegativeAcknowledge: "negative_acknowledge",
  UpdateGeofence: "update_geofence",
  ResumeSession: "resume_session",
});

/**
 * Aggregate type identifiers.
 */
export const AggregateTypes = Object.freeze({
  Vehicle: "vehicle",
  Room: "room",
  Fleet: "fleet",
});

// ─────────────────────────── event envelope ──────────────────────────────────

/**
 * @typedef {object} DomainEvent
 * @property {string} eventId       - UUID v7 (time-ordered).
 * @property {string} eventType     - Versioned type, e.g. `"location_update.v2"`.
 * @property {string} aggregateId   - Aggregate the event belongs to.
 * @property {string} aggregateType - `"vehicle" | "room" | "fleet"`.
 * @property {number} [sequence]    - Per-aggregate sequence (assigned on record).
 * @property {object} payload       - Event payload.
 * @property {object} [metadata]    - Free-form tracing metadata.
 * @property {string} timestamp     - ISO 8601 timestamp.
 * @property {string} [causationId] - Immediate cause (commandId, eventId…).
 * @property {string} [correlationId] - Causal chain identifier.
 */

/**
 * Builds a fully-formed domain event envelope.
 *
 * @param {object} spec
 * @param {string} spec.eventType - Versioned event type.
 * @param {string} spec.aggregateId
 * @param {string} [spec.aggregateType]
 * @param {object} [spec.payload={}]
 * @param {object} [spec.metadata={}]
 * @param {string} [spec.causationId]
 * @param {string} [spec.correlationId]
 * @param {string} [spec.eventId]   - Defaults to a fresh UUID v7.
 * @param {string|Date} [spec.timestamp] - Defaults to now.
 * @returns {DomainEvent}
 */
export function createEvent({
  eventType,
  aggregateId,
  aggregateType = AggregateTypes.Vehicle,
  payload = {},
  metadata = {},
  causationId,
  correlationId,
  eventId,
  timestamp,
}) {
  if (!eventType) throw new TypeError("eventType is required");
  if (!aggregateId) throw new TypeError("aggregateId is required");
  return {
    eventId: eventId ?? uuidv7(),
    eventType,
    aggregateId,
    aggregateType,
    payload: payload ?? {},
    metadata: metadata ?? {},
    timestamp: normalizeTimestamp(timestamp),
    causationId: causationId ?? null,
    correlationId: correlationId ?? null,
  };
}

/** @private */
function normalizeTimestamp(value) {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number") return new Date(value).toISOString();
  if (typeof value === "string" && value.length > 0) {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }
  return new Date().toISOString();
}

/**
 * Typed event envelopes so handlers can `aggregate.record(new LocationUpdatedEvent(…))`.
 */
export class LocationUpdatedEvent {
  /** @param {string} aggregateId @param {object} payload @param {object} [opts] */
  constructor(aggregateId, payload, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.LocationUpdated,
      aggregateId,
      aggregateType: AggregateTypes.Vehicle,
      payload,
      ...opts,
    });
  }
}

export class RoomJoinedEvent {
  constructor(clientId, roomId, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.RoomJoined,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { roomId },
      ...opts,
    });
  }
}

export class RoomLeftEvent {
  constructor(clientId, roomId, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.RoomLeft,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { roomId },
      ...opts,
    });
  }
}

export class GeofenceUpdatedEvent {
  constructor(clientId, roomId, fences, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.GeofenceUpdated,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { roomId, fences },
      ...opts,
    });
  }
}

export class GeofenceEnteredEvent {
  constructor(clientId, fence, location, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.GeofenceEntered,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { fenceId: fence.fenceId, roomId: fence.roomId ?? null, location },
      ...opts,
    });
  }
}

export class GeofenceExitedEvent {
  constructor(clientId, fence, location, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.GeofenceExited,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { fenceId: fence.fenceId, roomId: fence.roomId ?? null, location },
      ...opts,
    });
  }
}

export class SessionResumedEvent {
  constructor(clientId, rooms, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.SessionResumed,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { rooms },
      ...opts,
    });
  }
}

export class MessageAcknowledgedEvent {
  constructor(clientId, roomId, seq, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.MessageAcknowledged,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { roomId, seq },
      ...opts,
    });
  }
}

export class MessageNackedEvent {
  constructor(clientId, roomId, seq, reason, opts = {}) {
    this.event = createEvent({
      eventType: EventTypes.MessageNacked,
      aggregateId: clientId,
      aggregateType: AggregateTypes.Vehicle,
      payload: { roomId, seq, reason },
      ...opts,
    });
  }
}

// ─────────────────────────── EventStore interface ────────────────────────────

/**
 * Append-only event store interface. Every implementation must provide:
 *
 * - `append(events)`                – persist atomically; rejects duplicate eventIds.
 * - `getEvents(aggregateId, opts)`  – ordered by sequence (`fromSequence`/`toSequence`).
 * - `getEventsByCorrelation(id)`    – full causal chain, time-ordered.
 * - `getEventsAfter(eventId, limit)`– global feed cursor for projection polling.
 * - `getSnapshot(aggregateId)`      – latest snapshot or null.
 * - `saveSnapshot(snapshot)`        – upsert one snapshot per aggregate.
 * - `close()`                       – release resources; idempotent.
 *
 * @typedef {object} EventStore
 */

/**
 * Runtime conformance check mirroring `assertStorageAdapter` in storage/adapter.js.
 *
 * @param {unknown} store
 * @returns {void}
 * @throws {TypeError} When required methods are missing.
 */
export function assertEventStore(store) {
  const required = [
    "append", "getEvents", "getEventsByCorrelation",
    "getSnapshot", "saveSnapshot", "close",
  ];
  const missing = required.filter((m) => typeof store?.[m] !== "function");
  if (missing.length > 0) {
    throw new TypeError(`EventStore is missing required method(s): ${missing.join(", ")}`);
  }
}

/** Validates a batch destined for append(). @private */
function assertAppendable(events) {
  if (!Array.isArray(events) || events.length === 0) {
    throw new TypeError("append(events) requires a non-empty array");
  }
  for (const e of events) {
    if (!e || typeof e.eventId !== "string" || typeof e.eventType !== "string" ||
        typeof e.aggregateId !== "string") {
      throw new TypeError("Event is missing required fields (eventId, eventType, aggregateId)");
    }
    if (typeof e.sequence !== "number" || !Number.isFinite(e.sequence)) {
      throw new TypeError(`Event ${e.eventId} has no finite sequence number`);
    }
  }
  const aggregateIds = new Set(events.map((event) => event.aggregateId));
  if (aggregateIds.size !== 1) {
    throw new TypeError("append(events) requires events for one aggregate");
  }
}

/**
 * Compares two UUID v7 strings lexically (v7 embeds a millisecond timestamp,
 * so lexical order equals chronological order). Falls back to timestamp for
 * non-v7 ids.
 * @private
 */
function compareByTime(a, b) {
  const ta = Date.parse(a.timestamp) || 0;
  const tb = Date.parse(b.timestamp) || 0;
  if (ta !== tb) return ta - tb;
  return String(a.eventId) < String(b.eventId) ? -1 : 1;
}

// ─────────────────────────── In-memory store ─────────────────────────────────

/**
 * Dependency-free append-only event store. Used as the default so the gateway
 * keeps working without PostgreSQL, and as the test double in CI.
 *
 * @implements {EventStore}
 */
export class InMemoryEventStore {
  constructor() {
    /** @type {Map<string, DomainEvent[]>} aggregateId → events ordered by sequence */
    this._byAggregate = new Map();
    /** @type {DomainEvent[]} global insertion log */
    this._all = [];
    /** @type {Set<string>} dedup guard */
    this._ids = new Set();
    /** @type {Map<string, object>} aggregateId → snapshot */
    this._snapshots = new Map();

    /** Last append duration in milliseconds. */
    this.event_store_append_duration_ms = 0;
    /** Total successful appends. */
    this.event_store_appends_total = 0;
  }

  /**
   * Persists events atomically (all-or-nothing validation then commit).
   *
   * @param {DomainEvent[]} events
   * @param {{ expectedSequence?: number }} [opts] - Optimistic concurrency check
   *   against the aggregate's current highest sequence.
   * @returns {Promise<DomainEvent[]>}
   */
  async append(events, opts = {}) {
    const start = performanceNow();
    assertAppendable(events);

    for (const e of events) {
      if (this._ids.has(e.eventId)) {
        throw new Error(`Duplicate eventId ${e.eventId}`);
      }
    }

    const existing = this._byAggregate.get(events[0].aggregateId);
    const currentSeq = existing && existing.length > 0
      ? existing[existing.length - 1].sequence
      : 0;
    if (typeof opts.expectedSequence === "number" && currentSeq !== opts.expectedSequence) {
      throw new Error(
        `Concurrency conflict on aggregate ${events[0].aggregateId}: expected sequence ${opts.expectedSequence}, actual ${currentSeq}`
      );
    }

    try {
      for (const e of events) {
        this._ids.add(e.eventId);
        let list = this._byAggregate.get(e.aggregateId);
        if (!list) {
          list = [];
          this._byAggregate.set(e.aggregateId, list);
        }
        list.push(e);
        this._all.push(e);
      }
    } catch (err) {
      // Roll back partial inserts so append stays atomic.
      const ids = new Set(events.map((e) => e.eventId));
      this._all = this._all.filter((e) => !ids.has(e.eventId));
      for (const [id, list] of this._byAggregate) {
        this._byAggregate.set(id, list.filter((e) => !ids.has(e.eventId)));
      }
      for (const e of events) this._ids.delete(e.eventId);
      throw err;
    }

    this.event_store_append_duration_ms = performanceNow() - start;
    this.event_store_appends_total++;
    return events;
  }

  /**
   * @param {string} aggregateId
   * @param {{ fromSequence?: number, toSequence?: number }} [opts]
   * @returns {Promise<DomainEvent[]>}
   */
  async getEvents(aggregateId, opts = {}) {
    const from = opts.fromSequence ?? 1;
    const to = opts.toSequence ?? Number.POSITIVE_INFINITY;
    const list = this._byAggregate.get(aggregateId) ?? [];
    return list.filter((e) => e.sequence >= from && e.sequence <= to);
  }

  /**
   * Returns every event sharing a correlation id, oldest first.
   *
   * @param {string} correlationId
   * @returns {Promise<DomainEvent[]>}
   */
  async getEventsByCorrelation(correlationId) {
    if (!correlationId) return [];
    return this._all
      .filter((e) => e.correlationId === correlationId)
      .sort(compareByTime);
  }

  /**
   * Global feed after a cursor eventId (UUID v7 lexical ordering), used by
   * polling projections.
   *
   * @param {string|null} afterEventId
   * @param {number} [limit=1000]
   * @returns {Promise<DomainEvent[]>}
   */
  async getEventsAfter(afterEventId, limit = 1000) {
    const sorted = [...this._all].sort(compareByTime);
    if (!afterEventId) return sorted.slice(0, limit);
    const idx = sorted.findIndex((e) => e.eventId === afterEventId);
    if (idx === -1) return sorted.slice(0, limit);
    return sorted.slice(idx + 1, idx + 1 + limit);
  }

  /** @param {string} aggregateId @returns {Promise<object|null>} */
  async getSnapshot(aggregateId) {
    return this._snapshots.get(aggregateId) ?? null;
  }

  /**
   * Upserts a snapshot: `{ aggregateId, aggregateType, sequence, state, timestamp }`.
   *
   * @param {object} snapshot
   * @returns {Promise<void>}
   */
  async saveSnapshot(snapshot) {
    if (!snapshot || typeof snapshot.aggregateId !== "string") {
      throw new TypeError("snapshot.aggregateId is required");
    }
    this._snapshots.set(snapshot.aggregateId, { ...snapshot });
  }

  /** @returns {Promise<void>} */
  async close() {
    this._byAggregate.clear();
    this._all.length = 0;
    this._ids.clear();
    this._snapshots.clear();
  }
}

/** Monotonic-ish high-resolution timer. @private */
function performanceNow() {
  return typeof performance !== "undefined" && performance.now
    ? performance.now()
    : Date.now();
}

// ─────────────────────────── Postgres store ──────────────────────────────────

const CREATE_EVENTS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS events (
  event_id       UUID        NOT NULL,
  event_type     VARCHAR(100) NOT NULL,
  aggregate_id   VARCHAR(255) NOT NULL,
  aggregate_type VARCHAR(50)  NOT NULL,
  sequence       BIGINT       NOT NULL,
  payload        JSONB        NOT NULL,
  metadata       JSONB,
  timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  causation_id   UUID,
  correlation_id UUID
) PARTITION BY RANGE (timestamp);
`;

const CREATE_EVENTS_INDEXES_SQL = `
CREATE INDEX IF NOT EXISTS events_aggregate_seq_idx ON events (aggregate_id, sequence);
CREATE INDEX IF NOT EXISTS events_correlation_idx   ON events (correlation_id);
CREATE INDEX IF NOT EXISTS events_event_id_idx      ON events (event_id);
`;

const CREATE_SNAPSHOTS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS snapshots (
  aggregate_id   VARCHAR(255) PRIMARY KEY,
  aggregate_type VARCHAR(50)  NOT NULL,
  sequence       BIGINT       NOT NULL,
  state          JSONB        NOT NULL,
  timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
`;

const CREATE_EVENTS_PARTITION_FN_SQL = `
CREATE OR REPLACE FUNCTION create_events_daily_partition(partition_date DATE)
RETURNS VOID AS $func$
DECLARE
  partition_name TEXT;
BEGIN
  partition_name := 'events_' || to_char(partition_date, 'YYYY_MM_DD');
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
      partition_name, partition_date, partition_date + INTERVAL '1 day'
    );
  END IF;
END;
$func$ LANGUAGE plpgsql;
`;

const APPEND_BATCH_SQL = `
INSERT INTO events
  (event_id, event_type, aggregate_id, aggregate_type, sequence,
   payload, metadata, timestamp, causation_id, correlation_id)
SELECT * FROM unnest(
  $1::uuid[], $2::varchar[], $3::varchar[], $4::varchar[], $5::bigint[],
  $6::jsonb[], $7::jsonb[], $8::timestamptz[], $9::uuid[], $10::uuid[]
)
`;

/**
 * PostgreSQL-backed event store. The `events` table is range-partitioned by
 * day; daily partitions are pre-created and lazily ensured before inserts.
 *
 * Uses a lazy dynamic import of `pg` (same convention as
 * `storage/postgres.js`) so the package stays optional.
 *
 * @implements {EventStore}
 */
export class PostgresEventStore {
  /**
   * @param {object} [config={}]
   * @param {string} [config.connectionString] - Defaults to DATABASE_URL.
   * @param {number} [config.poolSize=10]
   * @param {number} [config.partitionDaysAhead=7]
   */
  constructor(config = {}) {
    this._connectionString = config.connectionString ?? process.env.DATABASE_URL;
    this._poolSize = config.poolSize ?? 10;
    this._partitionDaysAhead = config.partitionDaysAhead ?? 7;

    /** @type {import("pg").Pool|null} */
    this._pool = null;
    this._initPromise = null;
    this._closed = false;

    this.event_store_append_duration_ms = 0;
    this.event_store_appends_total = 0;
  }

  /** Lazily connects and provisions schema. Idempotent. @returns {Promise<void>} */
  async _init() {
    if (this._initPromise) return this._initPromise;
    this._initPromise = this._doInit().catch((err) => {
      this._initPromise = null;
      throw err;
    });
    return this._initPromise;
  }

  async _doInit() {
    const postgres = (await import("postgres")).default;
    const sql = postgres(this._connectionString, {
      max: this._poolSize,
      transform: { undefined: null },
    });
    this._pool = {
      query: async (text, values = []) => ({ rows: await sql.unsafe(text, values) }),
      connect: async () => ({
        query: async (queryText, queryValues = []) => ({ rows: await sql.unsafe(queryText, queryValues) }),
        release: () => {},
      }),
      transaction: (fn) => sql.begin(async (tx) => fn({
        query: async (queryText, queryValues = []) => ({ rows: await tx.unsafe(queryText, queryValues) }),
      })),
      end: () => sql.end(),
    };

    const client = await this._pool.connect();
    try {
      await client.query(CREATE_EVENTS_TABLE_SQL);
      await client.query(CREATE_EVENTS_INDEXES_SQL);
      await client.query(CREATE_SNAPSHOTS_TABLE_SQL);
      await client.query(CREATE_EVENTS_PARTITION_FN_SQL);
      await client.query(`
        DO $$
        DECLARE d DATE;
        BEGIN
          FOR i IN -1..($1::int) LOOP
            d := CURRENT_DATE + (i || ' days')::INTERVAL;
            PERFORM create_events_daily_partition(d);
          END LOOP;
        END $$;
      `, [this._partitionDaysAhead]);
    } finally {
      client.release();
    }
  }

  /** Best-effort partition creation for an insert timestamp. @private */
  async _ensurePartition(timestampIso) {
    try {
      await this._pool.query(
        `SELECT create_events_daily_partition(date_trunc('day', $1::timestamptz)::DATE)`,
        [timestampIso]
      );
    } catch (err) {
      logger.warn("Failed to ensure events partition", { error: err.message });
    }
  }

  /**
   * Appends events inside a single transaction.
   *
   * @param {DomainEvent[]} events
   * @param {{ expectedSequence?: number }} [opts]
   * @returns {Promise<DomainEvent[]>}
   */
  async append(events, opts = {}) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    const start = performanceNow();
    assertAppendable(events);
    await this._init();

    await this._ensurePartition(events[0].timestamp);
    await this._pool.transaction(async (client) => {
        if (typeof opts.expectedSequence === "number") {
          const cur = await client.query(
            `SELECT COALESCE(MAX(sequence), 0) AS seq FROM events WHERE aggregate_id = $1`,
            [events[0].aggregateId]
          );
          const actual = Number(cur.rows[0].seq);
          if (actual !== opts.expectedSequence) {
            throw new Error(
              `Concurrency conflict on aggregate ${events[0].aggregateId}: expected ${opts.expectedSequence}, actual ${actual}`
            );
          }
        }

        await client.query(APPEND_BATCH_SQL, [
          events.map((e) => e.eventId),
          events.map((e) => e.eventType.slice(0, 100)),
          events.map((e) => e.aggregateId),
          events.map((e) => e.aggregateType ?? AggregateTypes.Vehicle),
          events.map((e) => Math.trunc(e.sequence)),
          events.map((e) => JSON.stringify(e.payload ?? {})),
          events.map((e) => JSON.stringify(e.metadata ?? {})),
          events.map((e) => e.timestamp),
          events.map((e) => e.causationId ?? null),
          events.map((e) => e.correlationId ?? null),
        ]);
    });

    this.event_store_append_duration_ms = performanceNow() - start;
    this.event_store_appends_total++;
    return events;
  }

  /** @see InMemoryEventStore#getEvents */
  async getEvents(aggregateId, opts = {}) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    await this._init();
    const from = opts.fromSequence ?? 1;
    const to = opts.toSequence ?? Number.MAX_SAFE_INTEGER;
    const result = await this._pool.query(
      `SELECT event_id, event_type, aggregate_id, aggregate_type, sequence,
              payload, metadata, timestamp, causation_id, correlation_id
       FROM events
       WHERE aggregate_id = $1 AND sequence >= $2 AND sequence <= $3
       ORDER BY sequence ASC`,
      [aggregateId, from, to]
    );
    return result.rows.map(hydrateEventRow);
  }

  /** @see InMemoryEventStore#getEventsByCorrelation */
  async getEventsByCorrelation(correlationId) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    if (!correlationId) return [];
    await this._init();
    const result = await this._pool.query(
      `SELECT event_id, event_type, aggregate_id, aggregate_type, sequence,
              payload, metadata, timestamp, causation_id, correlation_id
       FROM events
       WHERE correlation_id = $1
       ORDER BY timestamp ASC, event_id ASC`,
      [correlationId]
    );
    return result.rows.map(hydrateEventRow);
  }

  /** @see InMemoryEventStore#getEventsAfter */
  async getEventsAfter(afterEventId, limit = 1000) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    await this._init();
    const result = afterEventId
      ? await this._pool.query(
          `SELECT event_id, event_type, aggregate_id, aggregate_type, sequence,
                  payload, metadata, timestamp, causation_id, correlation_id
           FROM events WHERE event_id > $1
           ORDER BY event_id ASC LIMIT $2`,
          [afterEventId, limit]
        )
      : await this._pool.query(
          `SELECT event_id, event_type, aggregate_id, aggregate_type, sequence,
                  payload, metadata, timestamp, causation_id, correlation_id
           FROM events ORDER BY event_id ASC LIMIT $1`,
          [limit]
        );
    return result.rows.map(hydrateEventRow);
  }

  /** @see InMemoryEventStore#getSnapshot */
  async getSnapshot(aggregateId) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    await this._init();
    const result = await this._pool.query(
      `SELECT aggregate_id, aggregate_type, sequence, state, timestamp
       FROM snapshots WHERE aggregate_id = $1`,
      [aggregateId]
    );
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      aggregateId: r.aggregate_id,
      aggregateType: r.aggregate_type,
      sequence: Number(r.sequence),
      state: r.state,
      timestamp: new Date(r.timestamp).toISOString(),
    };
  }

  /** @see InMemoryEventStore#saveSnapshot */
  async saveSnapshot(snapshot) {
    if (this._closed) throw new Error("PostgresEventStore is closed");
    if (!snapshot || typeof snapshot.aggregateId !== "string") {
      throw new TypeError("snapshot.aggregateId is required");
    }
    await this._init();
    await this._pool.query(
      `INSERT INTO snapshots (aggregate_id, aggregate_type, sequence, state, timestamp)
       VALUES ($1, $2, $3, $4::jsonb, $5)
       ON CONFLICT (aggregate_id) DO UPDATE SET
         aggregate_type = EXCLUDED.aggregate_type,
         sequence = EXCLUDED.sequence,
         state = EXCLUDED.state,
         timestamp = EXCLUDED.timestamp`,
      [
        snapshot.aggregateId,
        snapshot.aggregateType ?? AggregateTypes.Vehicle,
        Math.trunc(snapshot.sequence ?? 0),
        JSON.stringify(snapshot.state ?? {}),
        snapshot.timestamp ?? new Date().toISOString(),
      ]
    );
  }

  /** Flushes the pool. Idempotent. @returns {Promise<void>} */
  async close() {
    if (this._closed) return;
    this._closed = true;
    if (this._pool) {
      try { await this._pool.end(); } catch { /* noop */ }
      this._pool = null;
    }
  }
}

/** Maps a DB row back into a domain event envelope. @private */
function hydrateEventRow(r) {
  return {
    eventId: r.event_id,
    eventType: r.event_type,
    aggregateId: r.aggregate_id,
    aggregateType: r.aggregate_type,
    sequence: Number(r.sequence),
    payload: typeof r.payload === "string" ? JSON.parse(r.payload) : (r.payload ?? {}),
    metadata: typeof r.metadata === "string" ? JSON.parse(r.metadata) : (r.metadata ?? {}),
    timestamp: new Date(r.timestamp).toISOString(),
    causationId: r.causation_id ?? null,
    correlationId: r.correlation_id ?? null,
  };
}

// ─────────────────────────── aggregates ──────────────────────────────────────

/**
 * Base class for aggregate roots. Tracks committed history (for temporal
 * queries), uncommitted events, and per-aggregate sequence numbers.
 */
export class AggregateBase {
  /** @param {string} id @param {string} [aggregateType] */
  constructor(id, aggregateType = AggregateTypes.Vehicle) {
    this.id = id;
    this.aggregateType = aggregateType;
    /** Highest applied sequence. */
    this.sequence = 0;
    this._nextSeq = 0;
    /** @type {DomainEvent[]} */
    this._uncommitted = [];
    /** @type {DomainEvent[]} full applied history (for `.at(timestamp)` queries) */
    this._history = [];
  }

  /**
   * Applies a committed (or freshly recorded) event to state.
   * @param {DomainEvent} event
   */
  apply(event) {
    if (typeof event.sequence === "number") {
      this.sequence = Math.max(this.sequence, event.sequence);
      this._nextSeq = Math.max(this._nextSeq, event.sequence);
    }
    this._history.push(event);
    this.mutate(event);
  }

  /**
   * State transition hook. Subclasses MUST override.
   * @param {DomainEvent} _event
   */
  mutate(_event) {
    throw new Error("Not implemented: AggregateBase.mutate()");
  }

  /**
   * Rebuilds state from committed events.
   * @param {DomainEvent[]} events
   * @returns {this}
   */
  loadFromHistory(events) {
    for (const e of events) this.apply(e);
    return this;
  }

  /**
   * Records a new domain event: assigns the next sequence number, applies it
   * locally and stages it for persistence.
   *
   * Accepts either a prepared event envelope or one of the typed event
   * wrappers (`new LocationUpdatedEvent(...)` etc.).
   *
   * @param {DomainEvent|{event: DomainEvent}} eventOrWrapper
   * @returns {DomainEvent} the staged event
   */
  record(eventOrWrapper) {
    const event = eventOrWrapper && eventOrWrapper.event ? eventOrWrapper.event : eventOrWrapper;
    if (!event || typeof event.eventType !== "string") {
      throw new TypeError("record() requires a domain event");
    }
    if (typeof event.sequence !== "number") {
      event.sequence = ++this._nextSeq;
    }
    this.sequence = Math.max(this.sequence, event.sequence);
    this._uncommitted.push(event);
    this.apply(event);
    return event;
  }

  /** @returns {DomainEvent[]} events awaiting persistence */
  getUncommittedEvents() {
    return this._uncommitted;
  }

  /** Clears the uncommitted buffer after a successful append. */
  markCommitted() {
    this._uncommitted = [];
  }

  /**
   * Serializable snapshot payload for this aggregate's current state.
   * Subclasses should extend (calling super) to add their fields.
   * @returns {object}
   */
  takeSnapshot() {
    return {
      aggregateId: this.id,
      aggregateType: this.aggregateType,
      sequence: this.sequence,
      state: this.serializeState(),
      timestamp: new Date().toISOString(),
    };
  }

  /** @returns {object} plain serializable state */
  serializeState() {
    return { sequence: this.sequence };
  }

  /**
   * Restores state from a snapshot without replaying history.
   * @param {object} snapshot
   * @returns {void}
   */
  restoreFromSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return;
    this.sequence = Number(snapshot.sequence) || 0;
    this._nextSeq = this.sequence;
    const state = snapshot.state ?? {};
    this.deserializeState(state);
  }

  /**
   * Applies snapshot state. Subclasses MUST override to restore their fields.
   * @param {object} _state
   */
  deserializeState(_state) {
    // default: nothing beyond sequence
  }

  /**
   * Temporal query: returns the aggregate state as it existed at `timestamp`,
   * folded from retained history. Events recorded after the target instant are
   * ignored.
   *
   * @param {string|Date} timestamp
   * @returns {this} a detached clone positioned at that point in time
   */
  at(timestamp) {
    const cutoff = timestamp instanceof Date ? timestamp.getTime() : Date.parse(timestamp);
    const CloneCtor = /** @type {any} */ (this.constructor);
    const clone = new CloneCtor(this.id);
    const upto = this._history.filter((e) => {
      const t = Date.parse(e.timestamp);
      return Number.isFinite(t) ? t <= cutoff : true;
    });
    clone.loadFromHistory(upto);
    return clone;
  }
}

/**
 * Vehicle (per-client) aggregate: current location, room memberships,
 * geofence inside-set and definitions, acked sequence numbers.
 */
export class VehicleAggregate extends AggregateBase {
  /** @param {string} clientId */
  constructor(clientId) {
    super(clientId, AggregateTypes.Vehicle);
    /** @type {object|null} last known fix */
    this.location = null;
    /** @type {Set<string>} */
    this.rooms = new Set();
    /** @type {Map<string, number>} roomId → highest acked seq */
    this.ackedSeq = new Map();
    /** @type {Set<string>} currently-inside geofence ids */
    this.insideGeofences = new Set();
    /** @type {Map<string, Array<{fenceId:string,lat:number,lon:number,radiusM:number}>>} roomId → fences */
    this.geofences = new Map();
    /** @type {Map<string, number>} roomId → last location sequence seen */
    this.roomSeqs = new Map();
    this.locationUpdateCount = 0;
    this.geofenceEntryCount = 0;
    this.geofenceExitCount = 0;
  }

  /** @private */
  mutate(event) {
    switch (baseEventType(event.eventType)) {
      case "location_update":
        this.location = { ...event.payload, eventTimestamp: event.timestamp };
        this.locationUpdateCount++;
        break;
      case "room_joined":
        this.rooms.add(event.payload.roomId);
        if (!this.roomSeqs.has(event.payload.roomId)) this.roomSeqs.set(event.payload.roomId, 0);
        break;
      case "room_left":
        this.rooms.delete(event.payload.roomId);
        this.ackedSeq.delete(event.payload.roomId);
        break;
      case "geofence_updated":
        if (Array.isArray(event.payload.fences) && event.payload.fences.length > 0) {
          this.geofences.set(event.payload.roomId, event.payload.fences);
        } else {
          this.geofences.delete(event.payload.roomId);
        }
        break;
      case "geofence_entered":
        this.insideGeofences.add(event.payload.fenceId);
        this.geofenceEntryCount++;
        break;
      case "geofence_exited":
        this.insideGeofences.delete(event.payload.fenceId);
        this.geofenceExitCount++;
        break;
      case "session_resumed":
        if (Array.isArray(event.payload.rooms)) {
          for (const room of event.payload.rooms) {
            if (room && room.roomId != null) {
              this.rooms.add(room.roomId);
              if (typeof room.highestAckedSeq === "number") {
                this.ackedSeq.set(room.roomId, room.highestAckedSeq);
              }
            }
          }
        }
        break;
      case "message_ack":
        this.ackedSeq.set(event.payload.roomId, event.payload.seq);
        break;
      case "message_nack":
        // Nacks are audit-only; they do not change acked state.
        break;
      default:
        break;
    }
  }

  /** @returns {object} */
  serializeState() {
    return {
      location: this.location,
      rooms: [...this.rooms],
      ackedSeq: [...this.ackedSeq.entries()],
      insideGeofences: [...this.insideGeofences],
      geofences: [...this.geofences.entries()].map(([roomId, fences]) => [roomId, fences]),
      roomSeqs: [...this.roomSeqs.entries()],
      locationUpdateCount: this.locationUpdateCount,
      geofenceEntryCount: this.geofenceEntryCount,
      geofenceExitCount: this.geofenceExitCount,
      sequence: this.sequence,
    };
  }

  /** @param {object} s */
  deserializeState(s) {
    this.location = s.location ?? null;
    this.rooms = new Set(Array.isArray(s.rooms) ? s.rooms : []);
    this.ackedSeq = new Map(Array.isArray(s.ackedSeq) ? s.ackedSeq : []);
    this.insideGeofences = new Set(Array.isArray(s.insideGeofences) ? s.insideGeofences : []);
    this.geofences = new Map(
      (Array.isArray(s.geofences) ? s.geofences : []).map(([roomId, fences]) => [roomId, fences])
    );
    this.roomSeqs = new Map(Array.isArray(s.roomSeqs) ? s.roomSeqs : []);
    this.locationUpdateCount = s.locationUpdateCount ?? 0;
    this.geofenceEntryCount = s.geofenceEntryCount ?? 0;
    this.geofenceExitCount = s.geofenceExitCount ?? 0;
  }

  /**
   * Point-in-time state view for audits.
   * @param {string|Date} timestamp
   * @returns {VehicleAggregate}
   */
  at(timestamp) {
    return /** @type {VehicleAggregate} */ (super.at(timestamp));
  }
}

/**
 * Room aggregate: member list and a monotonically increasing message counter.
 */
export class RoomAggregate extends AggregateBase {
  /** @param {string} roomId */
  constructor(roomId) {
    super(roomId, AggregateTypes.Room);
    /** @type {Set<string>} */
    this.members = new Set();
    this.messageCount = 0;
    /** @type {number} exponentially-smoothed messages/sec */
    this.messageRatePerSec = 0;
  }

  /** @private */
  mutate(event) {
    switch (baseEventType(event.eventType)) {
      case "room_joined":
        this.members.add(event.payload.clientId ?? event.aggregateId);
        break;
      case "room_left":
        this.members.delete(event.payload.clientId ?? event.aggregateId);
        break;
      case "location_update":
        this.messageCount++;
        break;
      case "message_ack":
      case "message_nack":
        // tracked on vehicle aggregates; counted here for rate stats
        break;
      default:
        break;
    }
  }

  /** @returns {object} */
  serializeState() {
    return {
      members: [...this.members],
      messageCount: this.messageCount,
      messageRatePerSec: this.messageRatePerSec,
      sequence: this.sequence,
    };
  }

  /** @param {object} s */
  deserializeState(s) {
    this.members = new Set(Array.isArray(s.members) ? s.members : []);
    this.messageCount = s.messageCount ?? 0;
    this.messageRatePerSec = s.messageRatePerSec ?? 0;
  }
}

/**
 * Fleet-level statistics aggregate keyed by an arbitrary fleet id.
 */
export class FleetAggregate extends AggregateBase {
  /** @param {string} fleetId */
  constructor(fleetId) {
    super(fleetId, AggregateTypes.Fleet);
    /** @type {Map<string, object>} clientId → latest location */
    this.vehicles = new Map();
    this.alertCounts = { geofence_entered: 0, geofence_exited: 0, nack: 0 };
  }

  /** @private */
  mutate(event) {
    switch (baseEventType(event.eventType)) {
      case "location_update":
        this.vehicles.set(event.payload.clientId ?? event.aggregateId, {
          ...event.payload,
          updatedAt: event.timestamp,
        });
        break;
      case "geofence_entered":
        this.alertCounts.geofence_entered++;
        break;
      case "geofence_exited":
        this.alertCounts.geofence_exited++;
        break;
      case "message_nack":
        this.alertCounts.nack++;
        break;
      default:
        break;
    }
  }

  /** @returns {object} */
  serializeState() {
    return {
      vehicles: [...this.vehicles.entries()],
      alertCounts: { ...this.alertCounts },
      sequence: this.sequence,
    };
  }

  /** @param {object} s */
  deserializeState(s) {
    this.vehicles = new Map(Array.isArray(s.vehicles) ? s.vehicles : []);
    this.alertCounts = { geofence_entered: 0, geofence_exited: 0, nack: 0, ...(s.alertCounts ?? {}) };
  }
}

// ─────────────────────────── repository ──────────────────────────────────────

const DEFAULT_SNAPSHOT_EVERY = 1000;

/**
 * Loads and saves aggregates against an {@link EventStore}, transparently
 * using snapshots once an aggregate crosses `snapshotEvery` events.
 *
 * Snapshotting never blocks command processing: when due, it is written in a
 * background task scheduled with `setTimeout(0)`.
 */
export class AggregateRepository {
  /**
   * @param {object} opts
   * @param {EventStore} opts.eventStore
   * @param {new (id: string) => AggregateBase} opts.AggregateClass
   * @param {number} [opts.snapshotEvery=1000]
   * @param {boolean} [opts.snapshotsEnabled=true]
   */
  constructor({ eventStore, AggregateClass, snapshotEvery = DEFAULT_SNAPSHOT_EVERY, snapshotsEnabled = true }) {
    this.eventStore = eventStore;
    this.AggregateClass = AggregateClass;
    this.snapshotEvery = Math.max(1, snapshotEvery);
    this.snapshotsEnabled = snapshotsEnabled;

    /** Count of snapshots written by this repository. */
    this.snapshot_count = 0;
    /** @type {Set<string>} aggregates with an in-flight background snapshot */
    this._pendingSnapshots = new Set();
  }

  /**
   * Loads an aggregate: latest snapshot (if any) plus subsequent events.
   *
   * @param {string} id
   * @returns {Promise<AggregateBase>}
   */
  async get(id) {
    const aggregate = new this.AggregateClass(id);
    const snapshot = this.snapshotsEnabled
      ? await this.eventStore.getSnapshot(id)
      : null;

    if (snapshot && typeof snapshot.sequence === "number") {
      aggregate.restoreFromSnapshot(snapshot);
      const delta = await this.eventStore.getEvents(id, {
        fromSequence: snapshot.sequence + 1,
      });
      aggregate.loadFromHistory(delta);
    } else {
      const history = await this.eventStore.getEvents(id);
      aggregate.loadFromHistory(history);
    }
    return aggregate;
  }

  /**
   * Persists uncommitted events atomically, marks them committed and, when
   * the snapshot interval is hit, schedules a background snapshot write.
   *
   * @param {AggregateBase} aggregate
   * @returns {Promise<DomainEvent[]>} persisted events (may be empty)
   */
  async save(aggregate) {
    const events = aggregate.getUncommittedEvents();
    if (!Array.isArray(events) || events.length === 0) return [];

    const expectedSequence = aggregate.sequence - events.length;
    await this.eventStore.append(events, { expectedSequence });
    aggregate.markCommitted();

    if (
      this.snapshotsEnabled &&
      aggregate.sequence > 0 &&
      aggregate.sequence % this.snapshotEvery === 0 &&
      !this._pendingSnapshots.has(aggregate.id)
    ) {
      this._scheduleSnapshot(aggregate);
    }
    return events;
  }

  /**
   * Schedules a non-blocking background snapshot write. The snapshot is
   * captured synchronously so it reflects the aggregate state as of this
   * commit even if the caller keeps mutating the same instance while the
   * write is in flight.
   * @private
   */
  _scheduleSnapshot(aggregate) {
    this._pendingSnapshots.add(aggregate.id);
    const snapshot = aggregate.takeSnapshot();
    setTimeout(() => {
      Promise.resolve(this.eventStore.saveSnapshot(snapshot))
        .then(() => {
          this.snapshot_count++;
        })
        .catch((err) => {
          logger.error("Background snapshot write failed", {
            aggregateId: aggregate.id,
            error: err.message,
          });
        })
        .finally(() => {
          this._pendingSnapshots.delete(aggregate.id);
        });
    }, 0);
    // Intentionally not awaited: snapshotting must not block command processing.
  }
}

/**
 * Range query over an aggregate's event stream — the primitive behind
 * debugging, audits and manual reconstruction.
 *
 * @param {EventStore} eventStore
 * @param {string} aggregateId
 * @param {number} [fromSequence=1]
 * @param {number} [toSequence=Infinity]
 * @returns {Promise<DomainEvent[]>}
 */
export async function replayEvents(eventStore, aggregateId, fromSequence = 1, toSequence = Number.POSITIVE_INFINITY) {
  return eventStore.getEvents(aggregateId, {
    fromSequence,
    toSequence: toSequence === Number.POSITIVE_INFINITY ? Number.MAX_SAFE_INTEGER : toSequence,
  });
}

// ─────────────────────────── commands ────────────────────────────────────────

/**
 * @typedef {object} Command
 * @property {string} commandId - Unique id; duplicates are short-circuited.
 * @property {string} type
 * @property {object} [metadata]
 * @property {string} [correlationId]
 */

/** @private */
function requireCommand(command, expectedType) {
  if (!command || typeof command.commandId !== "string" || command.commandId.length === 0) {
    throw new TypeError("command.commandId is required");
  }
  if (expectedType && command.type !== expectedType) {
    throw new TypeError(`command.type must be "${expectedType}", got "${command.type}"`);
  }
}

export class PublishLocationCommand {
  /**
   * @param {object} c
   * @param {string} c.commandId
   * @param {string} c.clientId
   * @param {object} c.payload - Validated location payload.
   * @param {string} [c.correlationId]
   * @param {object} [c.metadata]
   */
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.PublishLocation });
    this.commandId = c.commandId;
    this.type = CommandTypes.PublishLocation;
    this.clientId = c.clientId;
    this.payload = c.payload;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class JoinRoomCommand {
  /** @param {object} c @param {string} c.clientId @param {string} c.roomId */
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.JoinRoom });
    this.commandId = c.commandId;
    this.type = CommandTypes.JoinRoom;
    this.clientId = c.clientId;
    this.roomId = c.roomId;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class LeaveRoomCommand {
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.LeaveRoom });
    this.commandId = c.commandId;
    this.type = CommandTypes.LeaveRoom;
    this.clientId = c.clientId;
    this.roomId = c.roomId;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class AcknowledgeCommand {
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.Acknowledge });
    this.commandId = c.commandId;
    this.type = CommandTypes.Acknowledge;
    this.clientId = c.clientId;
    this.roomId = c.roomId;
    this.seq = c.seq;
    this.negative = false;
    this.reason = c.reason ?? null;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class NegativeAcknowledgeCommand {
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.NegativeAcknowledge });
    this.commandId = c.commandId;
    this.type = CommandTypes.NegativeAcknowledge;
    this.clientId = c.clientId;
    this.roomId = c.roomId;
    this.seq = c.seq;
    this.negative = true;
    this.reason = c.reason ?? null;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class UpdateGeofenceCommand {
  /**
   * @param {object} c
   * @param {Array<{fenceId:string,lat:number,lon:number,radiusM:number}>} c.fences
   */
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.UpdateGeofence });
    this.commandId = c.commandId;
    this.type = CommandTypes.UpdateGeofence;
    this.clientId = c.clientId;
    this.roomId = c.roomId;
    this.fences = c.fences;
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

export class ResumeSessionCommand {
  /** @param {object} c @param {Array<{roomId:string,highestAckedSeq?:number}>} [c.rooms] */
  constructor(c) {
    requireCommand({ commandId: c.commandId, type: CommandTypes.ResumeSession });
    this.commandId = c.commandId;
    this.type = CommandTypes.ResumeSession;
    this.clientId = c.clientId;
    this.rooms = c.rooms ?? [];
    this.correlationId = c.correlationId ?? c.commandId;
    this.metadata = c.metadata ?? {};
  }
}

/** Haversine distance in metres. @private */
function haversineM(lat1, lon1, lat2, lon2) {
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
 * Factory for the PublishLocation handler. Validates business rules against
 * the vehicle aggregate and emits `LocationUpdated` plus any geofence
 * crossing events caused by the new fix.
 *
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @returns {(cmd: PublishLocationCommand) => Promise<DomainEvent[]>}
 */
export function makePublishLocationHandler({ vehicles }) {
  return async function handlePublishLocation(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    const correlationId = cmd.correlationId ?? cmd.commandId;

    vehicle.record(new LocationUpdatedEvent(cmd.clientId, cmd.payload, {
      causationId: cmd.commandId,
      correlationId,
      metadata: cmd.metadata,
    }));

    // Evaluate geofence crossings caused by this fix.
    const lat = cmd.payload.latitude;
    const lon = cmd.payload.longitude;
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      for (const [, fences] of vehicle.geofences) {
        for (const fence of fences ?? []) {
          const dist = haversineM(lat, lon, fence.lat, fence.lon);
          const inside = dist <= fence.radiusM;
          const wasInside = vehicle.insideGeofences.has(fence.fenceId);
          if (inside && !wasInside) {
            vehicle.record(new GeofenceEnteredEvent(cmd.clientId, fence, { latitude: lat, longitude: lon }, {
              causationId: cmd.commandId,
              correlationId,
              metadata: cmd.metadata,
            }));
          } else if (!inside && wasInside) {
            vehicle.record(new GeofenceExitedEvent(cmd.clientId, fence, { latitude: lat, longitude: lon }, {
              causationId: cmd.commandId,
              correlationId,
              metadata: cmd.metadata,
            }));
          }
        }
      }
    }

    return vehicles.save(vehicle);
  };
}

/**
 * Factory for JoinRoom. Re-joining a room the vehicle already occupies is a
 * no-op (the wire-level rejoin still refreshes the socket in the read model).
 *
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @param {AggregateRepository} [deps.roomsRepo]
 * @returns {(cmd: JoinRoomCommand) => Promise<DomainEvent[]>}
 */
export function makeJoinRoomHandler({ vehicles, roomsRepo }) {
  return async function handleJoinRoom(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    const events = [];
    const correlationId = cmd.correlationId ?? cmd.commandId;

    if (!vehicle.rooms.has(cmd.roomId)) {
      vehicle.record(new RoomJoinedEvent(cmd.clientId, cmd.roomId, {
        causationId: cmd.commandId,
        correlationId,
        metadata: cmd.metadata,
      }));
      events.push(...(await vehicles.save(vehicle)));
    }

    if (roomsRepo) {
      const room = await roomsRepo.get(cmd.roomId);
      room.record({
        ...createEvent({
          eventType: EventTypes.RoomJoined,
          aggregateId: cmd.roomId,
          aggregateType: AggregateTypes.Room,
          payload: { clientId: cmd.clientId, roomId: cmd.roomId },
          causationId: cmd.commandId,
          correlationId,
          metadata: cmd.metadata,
        }),
      });
      events.push(...(await roomsRepo.save(room)));
    }
    return events;
  };
}

/**
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @param {AggregateRepository} [deps.roomsRepo]
 * @returns {(cmd: LeaveRoomCommand) => Promise<DomainEvent[]>}
 */
export function makeLeaveRoomHandler({ vehicles, roomsRepo }) {
  return async function handleLeaveRoom(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    const events = [];
    const correlationId = cmd.correlationId ?? cmd.commandId;

    if (vehicle.rooms.has(cmd.roomId)) {
      vehicle.record(new RoomLeftEvent(cmd.clientId, cmd.roomId, {
        causationId: cmd.commandId,
        correlationId,
        metadata: cmd.metadata,
      }));
      events.push(...(await vehicles.save(vehicle)));
    }

    if (roomsRepo) {
      const room = await roomsRepo.get(cmd.roomId);
      room.record(createEvent({
        eventType: EventTypes.RoomLeft,
        aggregateId: cmd.roomId,
        aggregateType: AggregateTypes.Room,
        payload: { clientId: cmd.clientId, roomId: cmd.roomId },
        causationId: cmd.commandId,
        correlationId,
        metadata: cmd.metadata,
      }));
      events.push(...(await roomsRepo.save(room)));
    }
    return events;
  };
}

/**
 * Ack/Nack share one handler shape; nacks are audit-only.
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @returns {(cmd: AcknowledgeCommand|NegativeAcknowledgeCommand) => Promise<DomainEvent[]>}
 */
export function makeAcknowledgeHandler({ vehicles }) {
  return async function handleAcknowledge(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    const correlationId = cmd.correlationId ?? cmd.commandId;

    if (!cmd.negative && typeof cmd.seq === "number") {
      const acked = vehicle.ackedSeq.get(cmd.roomId) ?? 0;
      if (cmd.seq <= acked) return []; // duplicate/out-of-order ack: no event
    }

    vehicle.record(cmd.negative
      ? new MessageNackedEvent(cmd.clientId, cmd.roomId, cmd.seq, cmd.reason, {
          causationId: cmd.commandId,
          correlationId,
          metadata: cmd.metadata,
        })
      : new MessageAcknowledgedEvent(cmd.clientId, cmd.roomId, cmd.seq, {
          causationId: cmd.commandId,
          correlationId,
          metadata: cmd.metadata,
        }));
    return vehicles.save(vehicle);
  };
}

/**
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @returns {(cmd: UpdateGeofenceCommand) => Promise<DomainEvent[]>}
 */
export function makeUpdateGeofenceHandler({ vehicles }) {
  return async function handleUpdateGeofence(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    vehicle.record(new GeofenceUpdatedEvent(cmd.clientId, cmd.roomId, cmd.fences, {
      causationId: cmd.commandId,
      correlationId: cmd.correlationId ?? cmd.commandId,
      metadata: cmd.metadata,
    }));
    return vehicles.save(vehicle);
  };
}

/**
 * @param {object} deps
 * @param {AggregateRepository} deps.vehicles
 * @returns {(cmd: ResumeSessionCommand) => Promise<DomainEvent[]>}
 */
export function makeResumeSessionHandler({ vehicles }) {
  return async function handleResumeSession(cmd) {
    const vehicle = await vehicles.get(cmd.clientId);
    vehicle.record(new SessionResumedEvent(cmd.clientId, cmd.rooms, {
      causationId: cmd.commandId,
      correlationId: cmd.correlationId ?? cmd.commandId,
      metadata: cmd.metadata,
    }));
    return vehicles.save(vehicle);
  };
}

/**
 * Write-side entry point. Registers typed handlers and dispatches commands
 * with `commandId`-based idempotency: a repeated command returns the events
 * produced the first time without touching the store again.
 */
export class CommandHandler {
  /**
   * @param {object} [opts={}]
   * @param {number} [opts.dedupCacheSize=10000]
   */
  constructor({ dedupCacheSize = 10000 } = {}) {
    /** @type {Map<string, Function>} */
    this._handlers = new Map();
    /** @type {Map<string, DomainEvent[]>} commandId → committed events */
    this._dedup = new Map();
    this._dedupCacheSize = dedupCacheSize;
    /** @type {Set<(events: DomainEvent[]) => void>} */
    this._subscribers = new Set();

    this.commands_total = 0;
    this.commands_deduplicated_total = 0;
  }

  /**
   * Registers a handler for a command type.
   *
   * @param {string} commandType
   * @param {(cmd: object) => Promise<DomainEvent[]>} handler
   * @returns {void}
   */
  register(commandType, handler) {
    if (typeof handler !== "function") throw new TypeError("handler must be a function");
    this._handlers.set(commandType, handler);
  }

  /**
   * Subscribes to committed event batches (used to fan out to projections and
   * delivery reactions). Returns an unsubscribe function.
   *
   * @param {(events: DomainEvent[]) => void} fn
   * @returns {() => void}
   */
  subscribe(fn) {
    this._subscribers.add(fn);
    return () => this._subscribers.delete(fn);
  }

  /**
   * Dispatches a command. Duplicate `commandId`s are idempotently ignored.
   *
   * @param {Command} command
   * @returns {Promise<DomainEvent[]>} committed events (empty when duplicate/no-op)
   */
  async dispatch(command) {
    if (!command || typeof command.commandId !== "string") {
      throw new TypeError("command and command.commandId are required");
    }

    if (this._dedup.has(command.commandId)) {
      this.commands_deduplicated_total++;
      return this._dedup.get(command.commandId);
    }

    const handler = this._handlers.get(command.type);
    if (!handler) {
      throw new Error(`No handler registered for command type "${command.type}"`);
    }

    const events = (await handler(command)) ?? [];
    this.commands_total++;

    this._dedup.set(command.commandId, events);
    while (this._dedup.size > this._dedupCacheSize) {
      const oldest = this._dedup.keys().next().value;
      this._dedup.delete(oldest);
    }

    if (events.length > 0) {
      for (const subscriber of this._subscribers) {
        try {
          subscriber(events);
        } catch (err) {
          logger.error("Command subscriber failed", { error: err.message });
        }
      }
    }
    return events;
  }
}

// ─────────────────────────── projections ─────────────────────────────────────

/**
 * Storage contract used by built-in projections. Implemented in-memory by
 * {@link MemoryProjectionSink} and over PostgreSQL by {@link SqlProjectionSink}.
 *
 * @typedef {object} ProjectionSink
 */

/**
 * Maintains read models from the event log. Events are processed in
 * `(timestamp, eventId)` order so late-arriving out-of-order batches converge
 * deterministically. Each projection tracks its own checkpoint (last processed
 * eventId), making processing idempotent across restarts and replays.
 */
export class ProjectionManager {
  /**
   * @param {object} [opts={}]
   * @param {ProjectionSink} [opts.sink] - defaults to MemoryProjectionSink
   * @param {{ warn?: Function, error?: Function }} [opts.logger]
   */
  constructor({ sink, logger: log = logger } = {}) {
    this.sink = sink ?? new MemoryProjectionSink();
    this.log = log;
    /** @type {Array<{name:string,eventTypes:Set<string>|null,handle:Function}>} */
    this.projections = [];

    this.projection_lag_events = 0;
    this.projection_events_processed_total = 0;
    /** Highest event timestamp observed (ms epoch). */
    this._lastTimestampMs = 0;
  }

  /**
   * Registers a projection.
   *
   * @param {{name: string, eventTypes?: string[]|null, handle: (event: DomainEvent, sink: ProjectionSink) => Promise<void>}} projection
   * @returns {void}
   */
  register(projection) {
    if (!projection || typeof projection.name !== "string" || typeof projection.handle !== "function") {
      throw new TypeError("projection must expose name and handle()");
    }
    this.projections.push({
      name: projection.name,
      eventTypes: projection.eventTypes ? new Set(projection.eventTypes.map(baseEventType)) : null,
      handle: projection.handle.bind(projection),
    });
  }

  /**
   * Wires push-mode consumption: every committed command batch is projected.
   *
   * @param {CommandHandler} commandHandler
   * @returns {() => void} unsubscribe
   */
  subscribeTo(commandHandler) {
    return commandHandler.subscribe((events) => {
      this.processEvents(events).catch((err) => {
        this.log.error?.("Projection processing failed", { error: err.message });
      });
    });
  }

  /**
   * Projects a batch of events. Safe to call with overlapping batches:
   * per-projection checkpoints skip already-applied events.
   *
   * @param {DomainEvent[]} events
   * @returns {Promise<number>} number of newly applied event applications
   */
  async processEvents(events) {
    if (!Array.isArray(events) || events.length === 0) return 0;

    const ordered = [...events].sort(compareByTime);
    this.projection_lag_events += ordered.length;

    let applied = 0;
    for (const event of ordered) {
      const alreadyDone = await this._isProcessedByAll(event);
      if (alreadyDone) {
        this.projection_lag_events = Math.max(0, this.projection_lag_events - 1);
        continue;
      }
      for (const projection of this.projections) {
        if (projection.eventTypes && !projection.eventTypes.has(baseEventType(event.eventType))) continue;
        try {
          await projection.handle(event, this.sink);
          await this.sink.markEventProcessed?.(projection.name, event.eventId);
          await this.sink.setCheckpoint(projection.name, event.eventId, event.timestamp);
        } catch (err) {
          this.log.error?.(`Projection "${projection.name}" failed`, {
            eventId: event.eventId,
            error: err.message,
          });
        }
      }
      applied++;
      this.projection_events_processed_total++;
      this.projection_lag_events = Math.max(0, this.projection_lag_events - 1);
      const ts = Date.parse(event.timestamp);
      if (Number.isFinite(ts)) this._lastTimestampMs = Math.max(this._lastTimestampMs, ts);
    }
    return applied;
  }

  /**
   * An event is skipped only when EVERY registered projection already passed
   * its checkpoint beyond it (UUID v7 lexical comparison).
   * @private
   */
  async _isProcessedByAll(event) {
    if (this.projections.length === 0) return false;
    for (const projection of this.projections) {
      if (typeof this.sink.isEventProcessed === "function") {
        if (!(await this.sink.isEventProcessed(projection.name, event.eventId))) return false;
        continue;
      }
      const cp = await this.sink.getCheckpoint(projection.name);
      if (!cp || String(cp.lastEventId) < String(event.eventId)) return false;
    }
    return true;
  }
}

/** Default projection names. */
export const ProjectionNames = Object.freeze({
  VehicleState: "vehicle_state",
  RoomMembership: "room_membership",
  GeofenceViolations: "geofence_violations",
  Sequence: "sequence",
});

/** Events that advance a room's replay sequence counter. @private */
const ROOM_SEQUENCE_EVENT_TYPES = new Set(["location_update", "room_joined", "room_left"]);

/**
 * Read model: latest known state per vehicle (fast `getLatest` equivalent).
 */
export class VehicleStateProjection {
  constructor() { this.name = ProjectionNames.VehicleState; }

  async handle(event, sink) {
    // Room/fleet aggregates carry mirrored events; only vehicle-scoped
    // events drive the vehicle read model.
    if (event.aggregateType !== AggregateTypes.Vehicle) return;
    const base = baseEventType(event.eventType);
    if (base === "location_update") {
      const prev = (await sink.getVehicleState(event.aggregateId)) ?? {};
      if (prev.locationUpdatedAt && Date.parse(prev.locationUpdatedAt) > Date.parse(event.timestamp)) return;
      await sink.upsertVehicleState(event.aggregateId, {
        ...prev,
        clientId: event.aggregateId,
        latitude: event.payload.latitude,
        longitude: event.payload.longitude,
        altitude: event.payload.altitude ?? prev.altitude ?? null,
        accuracy: event.payload.accuracy ?? prev.accuracy ?? null,
        speed: event.payload.speed ?? prev.speed ?? null,
        locationUpdatedAt: event.timestamp,
        lastEventId: event.eventId,
        lastSequence: event.sequence,
      });
      return;
    }
    if (base === "room_joined" || base === "room_left") {
      const state = (await sink.getVehicleState(event.aggregateId)) ?? {};
      const rooms = new Set(Array.isArray(state.rooms) ? state.rooms : []);
      if (base === "room_joined") rooms.add(event.payload.roomId);
      else rooms.delete(event.payload.roomId);
      await sink.upsertVehicleState(event.aggregateId, {
        ...state,
        clientId: event.aggregateId,
        rooms: [...rooms],
        lastEventId: event.eventId,
        lastSequence: event.sequence ?? state.lastSequence,
      });
    }
  }
}

/**
 * Read model: current members per room (broadcast targeting).
 */
export class RoomMembershipProjection {
  constructor() { this.name = ProjectionNames.RoomMembership; }

  async handle(event, sink) {
    // Only vehicle-scoped join/leave events (aggregateId = clientId) mutate
    // membership; the mirrored room-aggregate events are ignored.
    if (event.aggregateType !== AggregateTypes.Vehicle) return;
    const base = baseEventType(event.eventType);
    if (base === "room_joined") {
      await sink.addRoomMember(event.payload.roomId, event.aggregateId, event.timestamp);
    } else if (base === "room_left") {
      await sink.removeRoomMember(event.payload.roomId, event.aggregateId);
    }
  }
}

/**
 * Compliance time-series of geofence crossings.
 */
export class GeofenceViolationProjection {
  constructor() { this.name = ProjectionNames.GeofenceViolations; }

  async handle(event, sink) {
    const base = baseEventType(event.eventType);
    if (base !== "geofence_entered" && base !== "geofence_exited") return;
    await sink.addGeofenceViolation({
      clientId: event.aggregateId,
      fenceId: event.payload.fenceId,
      roomId: event.payload.roomId ?? null,
      violationType: base === "geofence_entered" ? "entry" : "exit",
      timestamp: event.timestamp,
      correlationId: event.correlationId,
      eventId: event.eventId,
    });
  }
}

/**
 * Read model: per-aggregate and per-room sequence counters (replay support).
 */
export class SequenceProjection {
  constructor() { this.name = ProjectionNames.Sequence; }

  async handle(event, sink) {
    if (typeof event.sequence === "number") {
      await sink.setSequence(`aggregate:${event.aggregateId}`, event.sequence, event.eventId);
    }
    const roomId = event.payload?.roomId;
    if (
      roomId &&
      event.aggregateType === AggregateTypes.Vehicle &&
      ROOM_SEQUENCE_EVENT_TYPES.has(baseEventType(event.eventType))
    ) {
      await sink.bumpRoomSequence(String(roomId), event.eventId);
    }
  }
}

// ─────────────────────────── projection sinks ────────────────────────────────

/**
 * In-memory projection sink (default). Mirrors the SQL layout so tests run
 * the exact same projection code paths.
 *
 * @implements {ProjectionSink}
 */
export class MemoryProjectionSink {
  constructor() {
    this.vehicleStates = new Map();
    this.roomMembers = new Map();
    this.geofenceViolations = [];
    this.sequences = new Map();
    this.checkpoints = new Map();
    this.processedEvents = new Map();
  }

  async upsertVehicleState(clientId, state) {
    const prev = this.vehicleStates.get(clientId) ?? {};
    this.vehicleStates.set(clientId, { ...prev, ...state });
  }

  async getVehicleState(clientId) {
    return this.vehicleStates.get(clientId) ?? null;
  }

  async addRoomMember(roomId, clientId, joinedAt) {
    let members = this.roomMembers.get(roomId);
    if (!members) {
      members = new Map();
      this.roomMembers.set(roomId, members);
    }
    members.set(clientId, joinedAt);
  }

  async removeRoomMember(roomId, clientId) {
    this.roomMembers.get(roomId)?.delete(clientId);
  }

  async getRoomMembers(roomId) {
    const members = this.roomMembers.get(roomId);
    return members ? [...members.keys()] : [];
  }

  async addGeofenceViolation(violation) {
    this.geofenceViolations.push({ ...violation });
  }

  async getGeofenceViolations(filter = {}) {
    return this.geofenceViolations.filter((v) =>
      (!filter.clientId || v.clientId === filter.clientId) &&
      (!filter.violationType || v.violationType === filter.violationType)
    );
  }

  async setSequence(key, sequence, eventId) {
    const prev = this.sequences.get(key) ?? { sequence: 0 };
    if (sequence >= prev.sequence) {
      this.sequences.set(key, { sequence, eventId, updatedAt: new Date().toISOString() });
    }
  }

  async getSequence(key) {
    return this.sequences.get(key) ?? null;
  }

  async bumpRoomSequence(roomId, eventId) {
    const key = `room:${roomId}`;
    const prev = this.sequences.get(key) ?? { sequence: 0 };
    this.sequences.set(key, {
      sequence: prev.sequence + 1,
      eventId,
      updatedAt: new Date().toISOString(),
    });
  }

  async setCheckpoint(name, eventId, timestamp) {
    this.checkpoints.set(name, { lastEventId: eventId, updatedAt: timestamp ?? new Date().toISOString() });
  }

  async getCheckpoint(name) {
    return this.checkpoints.get(name) ?? null;
  }

  async markEventProcessed(name, eventId) {
    let ids = this.processedEvents.get(name);
    if (!ids) {
      ids = new Set();
      this.processedEvents.set(name, ids);
    }
    ids.add(eventId);
  }

  async isEventProcessed(name, eventId) {
    return this.processedEvents.get(name)?.has(eventId) ?? false;
  }
}

/**
 * PostgreSQL projection sink writing the materialised views:
 * `vehicle_current_state`, `room_membership`, `geofence_violations`,
 * `message_sequence` and `projection_checkpoints`. Uses the same lazy `pg`
 * import pattern as PostgresEventStore.
 *
 * @implements {ProjectionSink}
 */
export class SqlProjectionSink {
  constructor(config = {}) {
    this._connectionString = config.connectionString ?? process.env.DATABASE_URL;
    this._poolSize = config.poolSize ?? 10;
    this._pool = null;
    this._initPromise = null;
    this._closed = false;
  }

  async _init() {
    if (this._initPromise) return this._initPromise;
    this._initPromise = (async () => {
      const postgres = (await import("postgres")).default;
      const sql = postgres(this._connectionString, {
        max: this._poolSize,
        transform: { undefined: null },
      });
      this._pool = {
        query: async (text, values = []) => ({ rows: await sql.unsafe(text, values) }),
        connect: async () => ({
          query: async (queryText, queryValues = []) => ({ rows: await sql.unsafe(queryText, queryValues) }),
          release: () => {},
        }),
        end: () => sql.end(),
      };
      const client = await this._pool.connect();
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS vehicle_current_state (
            client_id          VARCHAR(255) PRIMARY KEY,
            rooms              TEXT[]       NOT NULL DEFAULT '{}',
            latitude           DOUBLE PRECISION,
            longitude          DOUBLE PRECISION,
            altitude           DOUBLE PRECISION,
            accuracy           DOUBLE PRECISION,
            speed              DOUBLE PRECISION,
            location_updated_at TIMESTAMPTZ,
            last_event_id      UUID,
            last_sequence      BIGINT,
            updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
          );
          CREATE INDEX IF NOT EXISTS vehicle_current_state_rooms_idx
            ON vehicle_current_state USING GIN (rooms);
          CREATE TABLE IF NOT EXISTS room_membership (
            room_id    VARCHAR(255) NOT NULL,
            client_id  VARCHAR(255) NOT NULL,
            joined_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (room_id, client_id)
          );
          CREATE TABLE IF NOT EXISTS geofence_violations (
            event_id       UUID PRIMARY KEY,
            client_id      VARCHAR(255) NOT NULL,
            fence_id       VARCHAR(255),
            room_id        VARCHAR(255),
            violation_type VARCHAR(10)  NOT NULL,
            occurred_at    TIMESTAMPTZ  NOT NULL,
            correlation_id UUID
          );
          CREATE INDEX IF NOT EXISTS geofence_violations_client_time_idx
            ON geofence_violations (client_id, occurred_at);
          CREATE TABLE IF NOT EXISTS message_sequence (
            seq_key    VARCHAR(320) PRIMARY KEY,
            sequence   BIGINT       NOT NULL DEFAULT 0,
            last_event_id UUID,
            updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
          );
          CREATE TABLE IF NOT EXISTS projection_events (
            projection_name VARCHAR(100) NOT NULL,
            event_id UUID NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (projection_name, event_id)
          );
          CREATE TABLE IF NOT EXISTS projection_checkpoints (
            name          VARCHAR(100) PRIMARY KEY,
            last_event_id UUID,
            updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
          );
        `);
      } finally {
        client.release();
      }
    })().catch((err) => {
      this._initPromise = null;
      throw err;
    });
    return this._initPromise;
  }

  async upsertVehicleState(clientId, state) {
    await this._init();
    await this._pool.query(
      `INSERT INTO vehicle_current_state
         (client_id, rooms, latitude, longitude, altitude, accuracy, speed,
          location_updated_at, last_event_id, last_sequence, updated_at)
       VALUES ($1, $2::text[], $3, $4, $5, $6, $7, $8, $9, $10, NOW())
       ON CONFLICT (client_id) DO UPDATE SET
         rooms = EXCLUDED.rooms,
         latitude = COALESCE(EXCLUDED.latitude, vehicle_current_state.latitude),
         longitude = COALESCE(EXCLUDED.longitude, vehicle_current_state.longitude),
         altitude = COALESCE(EXCLUDED.altitude, vehicle_current_state.altitude),
         accuracy = COALESCE(EXCLUDED.accuracy, vehicle_current_state.accuracy),
         speed = COALESCE(EXCLUDED.speed, vehicle_current_state.speed),
         location_updated_at = COALESCE(EXCLUDED.location_updated_at, vehicle_current_state.location_updated_at),
         last_event_id = COALESCE(EXCLUDED.last_event_id, vehicle_current_state.last_event_id),
         last_sequence = GREATEST(COALESCE(EXCLUDED.last_sequence, 0), COALESCE(vehicle_current_state.last_sequence, 0)),
         updated_at = NOW()`,
      [
        clientId,
        state.rooms ?? [],
        state.latitude ?? null,
        state.longitude ?? null,
        state.altitude ?? null,
        state.accuracy ?? null,
        state.speed ?? null,
        state.locationUpdatedAt ?? null,
        state.lastEventId ?? null,
        state.lastSequence ?? null,
      ]
    );
  }

  async getVehicleState(clientId) {
    await this._init();
    const r = await this._pool.query(
      `SELECT client_id, rooms, latitude, longitude, altitude, accuracy, speed,
              location_updated_at, last_event_id, last_sequence
       FROM vehicle_current_state WHERE client_id = $1`,
      [clientId]
    );
    if (r.rows.length === 0) return null;
    const row = r.rows[0];
    return {
      clientId: row.client_id,
      rooms: row.rooms ?? [],
      latitude: row.latitude,
      longitude: row.longitude,
      altitude: row.altitude,
      accuracy: row.accuracy,
      speed: row.speed,
      locationUpdatedAt: row.location_updated_at ? new Date(row.location_updated_at).toISOString() : null,
      lastEventId: row.last_event_id,
      lastSequence: row.last_sequence == null ? null : Number(row.last_sequence),
    };
  }

  async addRoomMember(roomId, clientId, joinedAt) {
    await this._init();
    await this._pool.query(
      `INSERT INTO room_membership (room_id, client_id, joined_at)
       VALUES ($1, $2, COALESCE($3::timestamptz, NOW()))
       ON CONFLICT (room_id, client_id) DO NOTHING`,
      [roomId, clientId, joinedAt ?? null]
    );
  }

  async removeRoomMember(roomId, clientId) {
    await this._init();
    await this._pool.query(
      `DELETE FROM room_membership WHERE room_id = $1 AND client_id = $2`,
      [roomId, clientId]
    );
  }

  async getRoomMembers(roomId) {
    await this._init();
    const r = await this._pool.query(
      `SELECT client_id FROM room_membership WHERE room_id = $1 ORDER BY joined_at ASC`,
      [roomId]
    );
    return r.rows.map((row) => row.client_id);
  }

  async addGeofenceViolation(v) {
    await this._init();
    await this._pool.query(
      `INSERT INTO geofence_violations
         (event_id, client_id, fence_id, room_id, violation_type, occurred_at, correlation_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (event_id) DO NOTHING`,
      [v.eventId, v.clientId, v.fenceId ?? null, v.roomId ?? null, v.violationType, v.timestamp, v.correlationId ?? null]
    );
  }

  async getGeofenceViolations(filter = {}) {
    await this._init();
    const conditions = [];
    const params = [];
    if (filter.clientId) {
      params.push(filter.clientId);
      conditions.push(`client_id = $${params.length}`);
    }
    if (filter.violationType) {
      params.push(filter.violationType);
      conditions.push(`violation_type = $${params.length}`);
    }
    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const r = await this._pool.query(
      `SELECT event_id, client_id, fence_id, room_id, violation_type, occurred_at, correlation_id
       FROM geofence_violations ${where} ORDER BY occurred_at ASC`,
      params
    );
    return r.rows.map((row) => ({
      eventId: row.event_id,
      clientId: row.client_id,
      fenceId: row.fence_id,
      roomId: row.room_id,
      violationType: row.violation_type,
      timestamp: new Date(row.occurred_at).toISOString(),
      correlationId: row.correlation_id,
    }));
  }

  async setSequence(key, sequence, eventId) {
    await this._init();
    await this._pool.query(
      `INSERT INTO message_sequence (seq_key, sequence, last_event_id, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (seq_key) DO UPDATE SET
         sequence = GREATEST(message_sequence.sequence, EXCLUDED.sequence),
         last_event_id = EXCLUDED.last_event_id,
         updated_at = NOW()`,
      [key, Math.trunc(sequence), eventId ?? null]
    );
  }

  async getSequence(key) {
    await this._init();
    const r = await this._pool.query(
      `SELECT sequence, last_event_id FROM message_sequence WHERE seq_key = $1`,
      [key]
    );
    if (r.rows.length === 0) return null;
    return {
      sequence: Number(r.rows[0].sequence),
      eventId: r.rows[0].last_event_id,
    };
  }

  async bumpRoomSequence(roomId, eventId) {
    await this._init();
    await this._pool.query(
      `INSERT INTO message_sequence (seq_key, sequence, last_event_id, updated_at)
       VALUES ($1, 1, $2, NOW())
       ON CONFLICT (seq_key) DO UPDATE SET
         sequence = message_sequence.sequence + 1,
         last_event_id = EXCLUDED.last_event_id,
         updated_at = NOW()`,
      [`room:${roomId}`, eventId ?? null]
    );
  }

  async setCheckpoint(name, eventId, timestamp) {
    await this._init();
    await this._pool.query(
      `INSERT INTO projection_checkpoints (name, last_event_id, updated_at)
       VALUES ($1, $2, COALESCE($3::timestamptz, NOW()))
       ON CONFLICT (name) DO UPDATE SET
         last_event_id = EXCLUDED.last_event_id,
         updated_at = NOW()`,
      [name, eventId, timestamp ?? null]
    );
  }

  async getCheckpoint(name) {
    await this._init();
    const r = await this._pool.query(
      `SELECT last_event_id FROM projection_checkpoints WHERE name = $1`,
      [name]
    );
    return r.rows.length > 0 ? { lastEventId: r.rows[0].last_event_id } : null;
  }

  async markEventProcessed(name, eventId) {
    await this._init();
    await this._pool.query(
      `INSERT INTO projection_events (projection_name, event_id)
       VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [name, eventId]
    );
  }

  async isEventProcessed(name, eventId) {
    await this._init();
    const r = await this._pool.query(
      `SELECT 1 FROM projection_events WHERE projection_name = $1 AND event_id = $2`,
      [name, eventId]
    );
    return r.rows.length > 0;
  }

  async close() {
    if (this._closed) return;
    this._closed = true;
    if (this._pool) {
      try { await this._pool.end(); } catch { /* noop */ }
      this._pool = null;
    }
  }
}

/**
 * Builds the standard projection set bound to a sink.
 *
 * @param {ProjectionManager} manager
 * @returns {void}
 */
export function registerDefaultProjections(manager) {
  manager.register(new VehicleStateProjection());
  manager.register(new RoomMembershipProjection());
  manager.register(new GeofenceViolationProjection());
  manager.register(new SequenceProjection());
}

// ─────────────────────────── wiring helper ───────────────────────────────────

/**
 * Assembles a complete event-sourcing stack: stores, repositories, command
 * handlers, and the projection pipeline wired to command commits.
 *
 * @param {object} [opts={}]
 * @param {EventStore} [opts.eventStore]      - Defaults to InMemoryEventStore.
 * @param {ProjectionSink} [opts.sink]        - Defaults to MemoryProjectionSink.
 * @param {number} [opts.snapshotEvery=1000]
 * @param {boolean} [opts.snapshotsEnabled=true]
 * @returns {{
 *   eventStore: EventStore,
 *   vehicles: AggregateRepository,
 *   roomsRepo: AggregateRepository,
 *   fleets: AggregateRepository,
 *   commands: CommandHandler,
 *   projections: ProjectionManager,
 * }}
 */
export function createEventSourcing(opts = {}) {
  const eventStore = opts.eventStore ?? new InMemoryEventStore();
  assertEventStore(eventStore);

  const vehicles = new AggregateRepository({
    eventStore,
    AggregateClass: VehicleAggregate,
    snapshotEvery: opts.snapshotEvery,
    snapshotsEnabled: opts.snapshotsEnabled,
  });
  const roomsRepo = new AggregateRepository({
    eventStore,
    AggregateClass: RoomAggregate,
    snapshotEvery: opts.snapshotEvery,
    snapshotsEnabled: opts.snapshotsEnabled,
  });
  const fleets = new AggregateRepository({
    eventStore,
    AggregateClass: FleetAggregate,
    snapshotEvery: opts.snapshotEvery,
    snapshotsEnabled: opts.snapshotsEnabled,
  });

  const commands = new CommandHandler();
  commands.register(CommandTypes.PublishLocation, makePublishLocationHandler({ vehicles }));
  commands.register(CommandTypes.JoinRoom, makeJoinRoomHandler({ vehicles, roomsRepo }));
  commands.register(CommandTypes.LeaveRoom, makeLeaveRoomHandler({ vehicles, roomsRepo }));
  commands.register(CommandTypes.Acknowledge, makeAcknowledgeHandler({ vehicles }));
  commands.register(CommandTypes.NegativeAcknowledge, makeAcknowledgeHandler({ vehicles }));
  commands.register(CommandTypes.UpdateGeofence, makeUpdateGeofenceHandler({ vehicles }));
  commands.register(CommandTypes.ResumeSession, makeResumeSessionHandler({ vehicles }));

  const projections = new ProjectionManager({ sink: opts.sink });
  registerDefaultProjections(projections);
  projections.subscribeTo(commands);

  return { eventStore, vehicles, roomsRepo, fleets, commands, projections };
}

// Backwards-friendly alias matching the issue's naming.
export { InMemoryEventStore as MemoryEventStore };

/** Re-export for callers wanting a uuid without another import. */
export { uuidv4 as generateCommandId };
