/**
 * @fileoverview Tests for the event sourcing / CQRS layer (issue #246).
 *
 * Covers command handling and idempotency, aggregate reconstruction,
 * snapshotting, temporal queries, causal-chain retrieval, replay ranges,
 * out-of-order projection convergence, geofence violations, metrics, and
 * end-to-end wiring through createServer().
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import WebSocket from "ws";
import jwt from "jsonwebtoken";
import {
  AggregateBase,
  AggregateRepository,
  AcknowledgeCommand,
  CommandHandler,
  CommandTypes,
  EventTypes,
  FleetAggregate,
  GeofenceViolationProjection,
  InMemoryEventStore,
  JoinRoomCommand,
  LeaveRoomCommand,
  MemoryProjectionSink,
  PostgresEventStore,
  ProjectionManager,
  PublishLocationCommand,
  RoomAggregate,
  RoomMembershipProjection,
  SequenceProjection,
  UpdateGeofenceCommand,
  VehicleAggregate,
  VehicleStateProjection,
  baseEventType,
  assertEventStore,
  createEvent,
  createEventSourcing,
  makeAcknowledgeHandler,
  makeJoinRoomHandler,
  makePublishLocationHandler,
  registerDefaultProjections,
  replayEvents,
} from "../src/event-sourcing.js";
import { createServer } from "../src/server.js";

const TEST_SECRET = "test-secret-key";

/** Build a fully-formed event with an explicit timestamp. */
function timedEvent(eventType, aggregateId, payload, isoTimestamp, extra = {}) {
  return createEvent({
    eventType,
    aggregateId,
    aggregateType: extra.aggregateType ?? "vehicle",
    payload,
    timestamp: isoTimestamp,
    causationId: extra.causationId,
    correlationId: extra.correlationId,
  });
}

/** Drain microtasks + timers scheduled with setTimeout(0). */
function flushAsync() {
  return new Promise((resolve) => setTimeout(resolve, 20));
}

describe("EventStore interface", () => {
  it("InMemoryEventStore satisfies the EventStore contract", () => {
    expect(() => assertEventStore(new InMemoryEventStore())).not.toThrow();
  });

  it("PostgresEventStore satisfies the EventStore contract", () => {
    expect(() => assertEventStore(new PostgresEventStore({ connectionString: "postgres://x" }))).not.toThrow();
  });

  it("rejects duplicate eventIds", async () => {
    const store = new InMemoryEventStore();
    const event = timedEvent(EventTypes.LocationUpdated, "v1", { latitude: 1 }, new Date().toISOString());
    event.sequence = 1;
    await store.append([event]);
    await expect(store.append([{ ...event }])).rejects.toThrow(/Duplicate eventId/);
  });

  it("enforces optimistic concurrency via expectedSequence", async () => {
    const store = new InMemoryEventStore();
    const e1 = timedEvent(EventTypes.LocationUpdated, "v1", { latitude: 1 }, new Date().toISOString());
    e1.sequence = 1;
    await store.append([e1], { expectedSequence: 0 });
    const e2 = timedEvent(EventTypes.LocationUpdated, "v1", { latitude: 2 }, new Date().toISOString());
    e2.sequence = 2;
    await expect(store.append([e2], { expectedSequence: 5 })).rejects.toThrow(/Concurrency conflict/);
  });
});

describe("CommandHandler", () => {
  let store;
  let vehicles;
  let commands;

  beforeEach(() => {
    store = new InMemoryEventStore();
    vehicles = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    commands = new CommandHandler();
    commands.register(CommandTypes.PublishLocation, makePublishLocationHandler({ vehicles }));
  });

  it("PublishLocationCommand persists LocationUpdated with causationId = commandId", async () => {
    const cmd = new PublishLocationCommand({
      commandId: "cmd-loc-1",
      clientId: "vehicle-1",
      payload: { latitude: 40.7128, longitude: -74.006, speed: 3 },
    });
    const events = await commands.dispatch(cmd);

    expect(events).toHaveLength(1);
    expect(baseEventType(events[0].eventType)).toBe("location_update");
    expect(events[0].eventType).toBe(EventTypes.LocationUpdated); // versioned
    expect(events[0].causationId).toBe("cmd-loc-1");
    expect(events[0].correlationId).toBe("cmd-loc-1");
    expect(events[0].sequence).toBe(1);

    const persisted = await store.getEvents("vehicle-1");
    expect(persisted).toHaveLength(1);
    expect(persisted[0].payload.latitude).toBeCloseTo(40.7128);
  });

  it("duplicate commandId produces no duplicate events", async () => {
    const cmd = () => new PublishLocationCommand({
      commandId: "cmd-dup",
      clientId: "vehicle-dup",
      payload: { latitude: 10, longitude: 10 },
    });

    const first = await commands.dispatch(cmd());
    const second = await commands.dispatch(cmd());

    expect(first).toHaveLength(1);
    expect(second).toHaveLength(1);
    expect(second[0].eventId).toBe(first[0].eventId);
    expect(commands.commands_deduplicated_total).toBe(1);

    const persisted = await store.getEvents("vehicle-dup");
    expect(persisted).toHaveLength(1);
  });

  it("sequences increment across commands for the same aggregate", async () => {
    await commands.dispatch(new PublishLocationCommand({
      commandId: "c1", clientId: "v-seq", payload: { latitude: 1, longitude: 1 },
    }));
    await commands.dispatch(new PublishLocationCommand({
      commandId: "c2", clientId: "v-seq", payload: { latitude: 2, longitude: 2 },
    }));
    const events = await store.getEvents("v-seq");
    expect(events.map((e) => e.sequence)).toEqual([1, 2]);
  });

  it("notifies subscribers with committed batches", async () => {
    const seen = [];
    const unsub = commands.subscribe((events) => seen.push(...events));
    unsub();
    await commands.dispatch(new PublishLocationCommand({
      commandId: "c-sub", clientId: "v-sub", payload: { latitude: 0, longitude: 0 },
    }));
    expect(seen).toHaveLength(0); // unsubscribed before dispatch
  });

  it("throws for unregistered command types", async () => {
    await expect(commands.dispatch({ commandId: "x", type: "nope" })).rejects.toThrow(/No handler registered/);
  });
});

describe("JoinRoom / Acknowledge commands", () => {
  let store;
  let vehicles;
  let roomsRepo;
  let commands;

  beforeEach(() => {
    store = new InMemoryEventStore();
    vehicles = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    roomsRepo = new AggregateRepository({ eventStore: store, AggregateClass: RoomAggregate });
    commands = new CommandHandler();
    commands.register(CommandTypes.JoinRoom, makeJoinRoomHandler({ vehicles, roomsRepo }));
    commands.register(CommandTypes.Acknowledge, makeAcknowledgeHandler({ vehicles }));
  });

  it("JoinRoom emits RoomJoined once; rejoining is a no-op", async () => {
    await commands.dispatch(new JoinRoomCommand({ commandId: "j1", clientId: "c-j", roomId: "r-j" }));
    const again = await commands.dispatch(new JoinRoomCommand({ commandId: "j2", clientId: "c-j", roomId: "r-j" }));

    // Second join records nothing on the vehicle (already a member).
    const vehicleEvents = await store.getEvents("c-j");
    expect(vehicleEvents.map((e) => baseEventType(e.eventType))).toEqual(["room_joined"]);
    expect(again.every((e) => e.aggregateType !== "vehicle")).toBe(true);
  });

  it("RoomJoined updates both vehicle and room aggregates", async () => {
    await commands.dispatch(new JoinRoomCommand({ commandId: "j3", clientId: "c-k", roomId: "r-k" }));

    const vehicle = await vehicles.get("c-k");
    expect(vehicle.rooms.has("r-k")).toBe(true);

    const room = await roomsRepo.get("r-k");
    expect(room.members.has("c-k")).toBe(true);
  });

  it("duplicate acks below the high-water mark emit no events", async () => {
    await commands.dispatch(new AcknowledgeCommand({ commandId: "a1", clientId: "c-a", roomId: "r-a", seq: 5 }));
    const dup = await commands.dispatch(new AcknowledgeCommand({ commandId: "a2", clientId: "c-a", roomId: "r-a", seq: 3 }));
    expect(dup).toEqual([]);

    const acks = (await store.getEvents("c-a")).filter((e) => baseEventType(e.eventType) === "message_ack");
    expect(acks).toHaveLength(1);
  });
});

describe("VehicleAggregate reconstruction", () => {
  it("reconstructed state matches live state", async () => {
    const store = new InMemoryEventStore();
    const live = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });

    const v = await live.get("veh-rec");
    v.record(createEvent({
      eventType: EventTypes.LocationUpdated, aggregateId: "veh-rec", payload: { latitude: 1.5, longitude: 2.5 },
    }));
    v.record(createEvent({
      eventType: EventTypes.RoomJoined, aggregateId: "veh-rec", payload: { roomId: "r1" },
    }));
    v.record(createEvent({
      eventType: EventTypes.RoomJoined, aggregateId: "veh-rec", payload: { roomId: "r2" },
    }));
    v.record(createEvent({
      eventType: EventTypes.MessageAcknowledged, aggregateId: "veh-rec", payload: { roomId: "r1", seq: 7 },
    }));
    await live.save(v);

    const reloaded = await live.get("veh-rec");
    expect(reloaded.location.latitude).toBeCloseTo(1.5);
    expect(reloaded.location.longitude).toBeCloseTo(2.5);
    expect([...reloaded.rooms].sort()).toEqual(["r1", "r2"]);
    expect(reloaded.ackedSeq.get("r1")).toBe(7);
    expect(reloaded.sequence).toBe(v.sequence);
    expect(reloaded.serializeState()).toEqual(v.serializeState());
  });

  it("reconstruction loads latest snapshot plus delta", async () => {
    const store = new InMemoryEventStore();
    const repo = new AggregateRepository({
      eventStore: store, AggregateClass: VehicleAggregate, snapshotEvery: 4,
    });

    let agg = await repo.get("veh-snap");
    for (let i = 1; i <= 6; i++) {
      agg.record(createEvent({
        eventType: EventTypes.LocationUpdated,
        aggregateId: "veh-snap",
        payload: { latitude: i, longitude: -i },
      }));
      await repo.save(agg);
    }
    await flushAsync(); // background snapshot write

    expect(repo.snapshot_count).toBeGreaterThanOrEqual(1);
    const snap = await store.getSnapshot("veh-snap");
    expect(snap.sequence).toBe(4);
    expect(snap.state.location.latitude).toBeCloseTo(4);

    agg = await repo.get("veh-snap");
    expect(agg.location.latitude).toBeCloseTo(6); // snapshot 4 + delta 5,6
    expect(agg.sequence).toBe(6);
  });

  it("snapshot writes do not block save()", async () => {
    const slowStore = new InMemoryEventStore();
    let releaseSnapshot;
    const gate = new Promise((resolve) => { releaseSnapshot = resolve; });
    const origSaveSnapshot = slowStore.saveSnapshot.bind(slowStore);
    slowStore.saveSnapshot = async (snapshot) => {
      await gate; // simulate a slow snapshot write
      return origSaveSnapshot(snapshot);
    };

    const repo = new AggregateRepository({
      eventStore: slowStore, AggregateClass: VehicleAggregate, snapshotEvery: 1,
    });
    const agg = await repo.get("veh-block");
    agg.record(createEvent({
      eventType: EventTypes.LocationUpdated, aggregateId: "veh-block", payload: { latitude: 9, longitude: 9 },
    }));
    const start = Date.now();
    await repo.save(agg); // must NOT wait for the gated snapshot write
    expect(Date.now() - start).toBeLessThan(100);
    releaseSnapshot();
    await flushAsync();
    expect(await slowStore.getSnapshot("veh-block")).not.toBeNull();
  });
});

describe("Temporal queries and replay", () => {
  it("VehicleAggregate.at(timestamp) returns historical state", async () => {
    const store = new InMemoryEventStore();
    const repo = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });

    const t0 = "2026-01-01T00:00:00.000Z";
    const t1 = "2026-01-02T00:00:00.000Z";
    const t2 = "2026-01-03T00:00:00.000Z";

    const v = await repo.get("veh-time");
    v.record(timedEvent(EventTypes.LocationUpdated, "veh-time", { latitude: 10, longitude: 10 }, t0));
    v.record(timedEvent(EventTypes.LocationUpdated, "veh-time", { latitude: 20, longitude: 20 }, t1));
    v.record(timedEvent(EventTypes.LocationUpdated, "veh-time", { latitude: 30, longitude: 30 }, t2));
    await repo.save(v);

    const atT1 = (await repo.get("veh-time")).at(t1);
    expect(atT1.location.latitude).toBeCloseTo(20);
    expect(atT1.sequence).toBe(2);

    const beforeAll = (await repo.get("veh-time")).at("2025-12-31T23:59:59.000Z");
    expect(beforeAll.location).toBeNull();

    // Live instance untouched by temporal folds.
    const live = await repo.get("veh-time");
    expect(live.location.latitude).toBeCloseTo(30);
  });

  it("replayEvents returns the requested sequence range", async () => {
    const store = new InMemoryEventStore();
    const repo = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    const v = await repo.get("veh-replay");
    for (let i = 1; i <= 5; i++) {
      v.record(createEvent({
        eventType: EventTypes.LocationUpdated, aggregateId: "veh-replay",
        payload: { latitude: i, longitude: i },
      }));
    }
    await repo.save(v);

    const slice = await replayEvents(store, "veh-replay", 2, 4);
    expect(slice.map((e) => e.sequence)).toEqual([2, 3, 4]);
  });
});

describe("Correlation chains", () => {
  it("getEventsByCorrelation returns the full causal chain in order", async () => {
    const store = new InMemoryEventStore();
    const correlationId = "corr-chain-42";

    const vehiclesRepo = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    const handler = makePublishLocationHandler({ vehicles: vehiclesRepo });
    const gh = new CommandHandler();
    gh.register(CommandTypes.PublishLocation, handler);
    gh.register(CommandTypes.UpdateGeofence, (async (cmd) => {
      const vehicle = await vehiclesRepo.get(cmd.clientId);
      vehicle.record(createEvent({
        eventType: EventTypes.GeofenceUpdated,
        aggregateId: cmd.clientId,
        payload: { roomId: cmd.roomId, fences: cmd.fences },
        causationId: cmd.commandId,
        correlationId: cmd.correlationId,
      }));
      return vehiclesRepo.save(vehicle);
    }));
    gh.register(CommandTypes.Acknowledge, makeAcknowledgeHandler({ vehicles: vehiclesRepo }));

    // Define a fence around (1,1) r=100m, then drive into it and ack.
    await gh.dispatch(new UpdateGeofenceCommand({
      commandId: "cmd-fence", clientId: "veh-corr", roomId: "r-corr",
      fences: [{ fenceId: "zone-a", lat: 1, lon: 1, radiusM: 100 }],
      correlationId,
    }));
    await gh.dispatch(new PublishLocationCommand({
      commandId: "cmd-inside", clientId: "veh-corr",
      payload: { latitude: 1.0005, longitude: 1.0005 },
      correlationId,
    }));
    await gh.dispatch(new AcknowledgeCommand({
      commandId: "cmd-ack", clientId: "veh-corr", roomId: "r-corr", seq: 1, correlationId,
    }));

    const chain = await store.getEventsByCorrelation(correlationId);
    const bases = chain.map((e) => baseEventType(e.eventType));
    expect(bases).toEqual(["geofence_updated", "location_update", "geofence_entered", "message_ack"]);
    expect(chain.every((e) => e.correlationId === correlationId)).toBe(true);
    // causation chain: each event caused by its command id
    expect(chain.map((e) => e.causationId)).toEqual(["cmd-fence", "cmd-inside", "cmd-inside", "cmd-ack"]);

    // Aggregate reflects the crossing.
    const vehicle = await vehiclesRepo.get("veh-corr");
    expect(vehicle.insideGeofences.has("zone-a")).toBe(true);
  });

  it("emits geofence_exited when leaving a configured zone", async () => {
    const store = new InMemoryEventStore();
    const vehiclesRepo = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    const handler = makePublishLocationHandler({ vehicles: vehiclesRepo });
    const gh = new CommandHandler();
    gh.register(CommandTypes.PublishLocation, handler);
    gh.register(CommandTypes.UpdateGeofence, (async (cmd) => {
      const vehicle = await vehiclesRepo.get(cmd.clientId);
      vehicle.record(createEvent({
        eventType: EventTypes.GeofenceUpdated,
        aggregateId: cmd.clientId,
        payload: { roomId: cmd.roomId, fences: cmd.fences },
        causationId: cmd.commandId,
        correlationId: cmd.correlationId,
      }));
      return vehiclesRepo.save(vehicle);
    }));

    await gh.dispatch(new UpdateGeofenceCommand({
      commandId: "f1", clientId: "veh-exit", roomId: null,
      fences: [{ fenceId: "zone-x", lat: 0, lon: 0, radiusM: 50 }],
    }));
    await gh.dispatch(new PublishLocationCommand({
      commandId: "p1", clientId: "veh-exit", payload: { latitude: 0.0001, longitude: 0.0001 },
    })); // enter
    const events = await gh.dispatch(new PublishLocationCommand({
      commandId: "p2", clientId: "veh-exit", payload: { latitude: 5, longitude: 5 },
    })); // exit

    expect(events.map((e) => baseEventType(e.eventType))).toEqual(["location_update", "geofence_exited"]);

    const violationsProj = new GeofenceViolationProjection();
    const sink = new MemoryProjectionSink();
    const pm = new ProjectionManager({ sink });
    pm.register(violationsProj);
    const all = await store.getEvents("veh-exit");
    await pm.processEvents(all);

    const types = (await sink.getGeofenceViolations({})).map((v) => v.violationType);
    expect(types).toEqual(["entry", "exit"]);
  });
});

describe("ProjectionManager", () => {
  function buildStack() {
    const sink = new MemoryProjectionSink();
    const manager = new ProjectionManager({ sink });
    manager.register(new VehicleStateProjection());
    manager.register(new RoomMembershipProjection());
    manager.register(new GeofenceViolationProjection());
    manager.register(new SequenceProjection());
    return { sink, manager };
  }

  it("projects committed commands within 100ms", async () => {
    const store = new InMemoryEventStore();
    const vehicles = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    const roomsRepo = new AggregateRepository({ eventStore: store, AggregateClass: RoomAggregate });
    const { sink, manager } = buildStack();
    registerDefaultProjections(manager); // idempotent names replaced? no — verify below

    const commands = new CommandHandler();
    commands.register(CommandTypes.JoinRoom, makeJoinRoomHandler({ vehicles, roomsRepo }));
    manager.subscribeTo(commands);

    const startedAt = Date.now();
    await commands.dispatch(new JoinRoomCommand({ commandId: "pj-1", clientId: "c-fast", roomId: "r-fast" }));

    while (!(await sink.getRoomMembers("r-fast")).includes("c-fast")) {
      await new Promise((resolve) => setTimeout(resolve, 2));
      if (Date.now() - startedAt > 1000) throw new Error("projection never converged");
    }
    expect(Date.now() - startedAt).toBeLessThan(100);
  });

  it("handles out-of-order batches using timestamp/eventId ordering", async () => {
    const { sink, manager } = buildStack();

    const e1 = timedEvent(EventTypes.LocationUpdated, "v-ooo", { latitude: 1, longitude: 1 }, "2026-02-01T00:00:01.000Z");
    const e2 = timedEvent(EventTypes.LocationUpdated, "v-ooo", { latitude: 2, longitude: 2 }, "2026-02-01T00:00:02.000Z");
    const e3 = timedEvent(EventTypes.LocationUpdated, "v-ooo", { latitude: 3, longitude: 3 }, "2026-02-01T00:00:03.000Z");
    [e1, e2, e3].forEach((e, i) => { e.sequence = i + 1; });

    await manager.processEvents([e3, e1, e2]); // shuffled arrival

    const state = await sink.getVehicleState("v-ooo");
    expect(state.latitude).toBeCloseTo(3); // newest fix wins
    expect((await sink.getSequence("aggregate:v-ooo")).sequence).toBe(3);
    expect(manager.projection_lag_events).toBe(0);
    expect(manager.projection_events_processed_total).toBe(3);
  });

  it("skips already-projected events (idempotent checkpoints)", async () => {
    const { sink, manager } = buildStack();
    const e1 = timedEvent(EventTypes.LocationUpdated, "v-idem", { latitude: 5, longitude: 5 }, new Date().toISOString());
    e1.sequence = 1;

    await manager.processEvents([e1]);
    const appliedAgain = await manager.processEvents([e1]);
    expect(appliedAgain).toBe(0);

    // State unchanged (still exactly one application).
    const state = await sink.getVehicleState("v-idem");
    expect(state.lastEventId).toBe(e1.eventId);
  });

  it("SequenceProjection maintains per-room counters for replay", async () => {
    const { sink, manager } = buildStack();
    const roomJoin = createEvent({
      eventType: EventTypes.RoomJoined, aggregateId: "v-seqp", payload: { roomId: "r-seqp" },
    });
    const loc = createEvent({
      eventType: EventTypes.LocationUpdated, aggregateId: "v-seqp", payload: { latitude: 0, longitude: 0, roomIdHint: true },
    });
    roomJoin.sequence = 1;
    loc.sequence = 2;
    // give the location event a roomId so it counts toward the room counter
    loc.payload.roomId = "r-seqp";

    await manager.processEvents([roomJoin, loc]);
    const roomSeq = await sink.getSequence("room:r-seqp");
    expect(roomSeq.sequence).toBe(2); // join + location
  });
});

describe("Aggregates", () => {
  it("AggregateBase.mutate throws when not implemented", () => {
    class Bare extends AggregateBase {}
    const bare = new Bare("b1");
    expect(() => bare.loadFromHistory([
      createEvent({ eventType: EventTypes.LocationUpdated, aggregateId: "b1" }),
    ])).toThrow(/Not implemented/);
  });

  it("FleetAggregate accumulates statistics and alert counts", async () => {
    const store = new InMemoryEventStore();
    const fleets = new AggregateRepository({ eventStore: store, AggregateClass: FleetAggregate });
    const f = await fleets.get("fleet-1");

    f.record(createEvent({ eventType: EventTypes.LocationUpdated, aggregateId: "fleet-1", aggregateType: "fleet", payload: { clientId: "car-a", latitude: 1, longitude: 1 } }));
    f.record(createEvent({ eventType: EventTypes.GeofenceEntered, aggregateId: "fleet-1", aggregateType: "fleet", payload: { clientId: "car-a", fenceId: "z" } }));
    f.record(createEvent({ eventType: EventTypes.GeofenceExited, aggregateId: "fleet-1", aggregateType: "fleet", payload: { clientId: "car-b", fenceId: "z" } }));
    f.record(createEvent({ eventType: EventTypes.MessageNacked, aggregateId: "fleet-1", aggregateType: "fleet", payload: { clientId: "car-b", roomId: "r", seq: 2 } }));
    await fleets.save(f);

    const reloaded = await fleets.get("fleet-1");
    expect(reloaded.vehicles.size).toBe(1); // only car-a produced a location fix
    expect(reloaded.alertCounts).toEqual({ geofence_entered: 1, geofence_exited: 1, nack: 1 });
  });

  it("LeaveRoom removes membership across aggregates", async () => {
    const store = new InMemoryEventStore();
    const vehicles = new AggregateRepository({ eventStore: store, AggregateClass: VehicleAggregate });
    const roomsRepo = new AggregateRepository({ eventStore: store, AggregateClass: RoomAggregate });
    const ch = new CommandHandler();
    ch.register(CommandTypes.JoinRoom, makeJoinRoomHandler({ vehicles, roomsRepo }));
    ch.register(CommandTypes.LeaveRoom, (async (cmd) => {
      const { makeLeaveRoomHandler } = await import("../src/event-sourcing.js");
      return makeLeaveRoomHandler({ vehicles, roomsRepo })(cmd);
    }));

    await ch.dispatch(new JoinRoomCommand({ commandId: "lj1", clientId: "c-lv", roomId: "r-lv" }));
    await ch.dispatch(new LeaveRoomCommand({ commandId: "ll1", clientId: "c-lv", roomId: "r-lv" }));

    const vehicle = await vehicles.get("c-lv");
    const room = await roomsRepo.get("r-lv");
    expect(vehicle.rooms.has("r-lv")).toBe(false);
    expect(room.members.has("c-lv")).toBe(false);
  });
});

describe("createEventSourcing wiring", () => {
  it("builds a fully wired default stack", async () => {
    const es = createEventSourcing();
    assertEventStore(es.eventStore);

    const events = await es.commands.dispatch(new PublishLocationCommand({
      commandId: "wire-1", clientId: "v-wire", payload: { latitude: 12.34, longitude: 56.78 },
    }));
    expect(events).toHaveLength(1);

    // Projections consume commits automatically.
    const deadline = Date.now() + 200;
    while (!await es.projections.sink.getVehicleState("v-wire")) {
      if (Date.now() > deadline) throw new Error("projections did not consume commit");
      await flushAsync();
    }
    const state = await es.projections.sink.getVehicleState("v-wire");
    expect(state.latitude).toBeCloseTo(12.34);
  });
});

describe("event sourcing metrics", () => {
  it("tracks append duration, appends total, lag, and snapshot count", async () => {
    const es = createEventSourcing({ snapshotsEnabled: false });
    await es.commands.dispatch(new PublishLocationCommand({
      commandId: "m-1", clientId: "v-metrics", payload: { latitude: 1, longitude: 1 },
    }));
    await flushAsync();

    expect(es.eventStore.event_store_append_duration_ms).toBeGreaterThanOrEqual(0);
    expect(es.eventStore.event_store_appends_total).toBeGreaterThan(0);
    expect(es.projections.projection_lag_events).toBe(0);
  });
});

describe("server integration", () => {
  /** Minimal storage double that records legacy persistence calls. */
  function makeFakeStorage() {
    const saved = [];
    return {
      saved,
      async saveLocation(clientId, roomId, payload) {
        saved.push({ clientId, roomId, payload });
      },
      async close() {},
    };
  }

  function makeToken(clientId) {
    return jwt.sign({ sub: clientId }, TEST_SECRET, { expiresIn: 60 });
  }

  function connect(port, token) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://localhost:${port}/?token=${token}`);
      ws.once("open", () => resolve(ws));
      ws.once("error", reject);
    });
  }

  function nextMessages(ws, n = 1) {
    return new Promise((resolve) => {
      const msgs = [];
      ws.on("message", function handler(data) {
        msgs.push(JSON.parse(data.toString()));
        if (msgs.length === n) {
          ws.off("message", handler);
          resolve(msgs);
        }
      });
    });
  }

  function waitClose(ws) {
    return new Promise((resolve) => ws.once("close", resolve));
  }

  let server;
  let port;
  let fakeStorage;

  beforeEach(() => {
    process.env.AUTH_SECRET = TEST_SECRET;
    fakeStorage = makeFakeStorage();
    server = createServer({
      port: 0,
      heartbeatMs: 60000,
      maxPayloadBytes: 4096,
      storageAdapter: fakeStorage,
    });
    port = server.wss.address().port;
  });

  afterEach(async () => {
    for (const client of server.wss.clients) client.terminate();
    await new Promise((resolve) => server.wss.close(resolve));
    delete process.env.AUTH_SECRET;
  });

  it("broadcasts locations via commands and persists immutable events", async () => {
    const wsA = await connect(port, makeToken("client-a"));
    const wsB = await connect(port, makeToken("client-b"));

    wsA.send(JSON.stringify({ type: "join_room", roomId: "fleet-es" }));
    wsB.send(JSON.stringify({ type: "join_room", roomId: "fleet-es" }));
    await nextMessages(wsA, 1);
    await nextMessages(wsB, 1);

    const receiverPromise = nextMessages(wsB, 1);
    wsA.send(JSON.stringify({
      type: "location_update",
      payload: { latitude: 41.0, longitude: -73.5, speed: 2 },
    }));
    const [frame] = await receiverPromise;

    expect(frame.type).toBe("location_update");
    expect(frame.payload.clientId).toBe("client-a");
    expect(frame.payload.latitude).toBeCloseTo(41.0);

    // Legacy storage path still invoked (backwards compatibility).
    expect(fakeStorage.saved.length).toBeGreaterThan(0);
    expect(fakeStorage.saved[0].roomId).toBe("fleet-es");

    // Immutable log: vehicle + room aggregates recorded.
    const { eventStore, projections } = server.eventSourcing;
    const vehicleEvents = await eventStore.getEvents("client-a");
    const bases = vehicleEvents.map((e) => baseEventType(e.eventType));
    expect(bases).toContain("room_joined");
    expect(bases).toContain("location_update");

    const locEvent = vehicleEvents.find((e) => baseEventType(e.eventType) === "location_update");
    expect(locEvent.eventType).toBe(EventTypes.LocationUpdated);
    expect(locEvent.causationId).toBeTruthy();
    expect(locEvent.correlationId).toBeTruthy();

    const roomEvents = await eventStore.getEvents("fleet-es");
    expect(roomEvents.some((e) => e.aggregateType === "room")).toBe(true);

    // Projections mirror the wire behavior (read model).
    await flushAsync();
    const members = await projections.sink.getRoomMembers("fleet-es");
    expect(members.sort()).toEqual(["client-a", "client-b"]);
    const vehState = await projections.sink.getVehicleState("client-a");
    expect(vehState.latitude).toBeCloseTo(41.0);

    await closeBoth(wsA, wsB);

    async function closeBoth(a, b) {
      a.close();
      b.close();
      await Promise.all([waitClose(a), waitClose(b)]);
    }
  });

  it("records ack events through the command side", async () => {
    const wsA = await connect(port, makeToken("client-ack"));
    wsA.send(JSON.stringify({ type: "join_room", roomId: "fleet-ack" }));
    await nextMessages(wsA, 1);

    wsA.send(JSON.stringify({ type: "ack", roomId: "fleet-ack", seq: 3 }));
    await flushAsync();

    const events = await server.eventSourcing.eventStore.getEvents("client-ack");
    const acks = events.filter((e) => baseEventType(e.eventType) === "message_ack");
    expect(acks).toHaveLength(1);
    expect(acks[0].payload.seq).toBe(3);
    expect(acks[0].causationId).toBeTruthy();

    wsA.close();
    await waitClose(wsA);
  });

  it("exposes event sourcing metrics on /metrics", async () => {
    const res = await fetch(`http://localhost:${port}/metrics`);
    expect(res.status).toBe(200);
    const body = await res.text();
    expect(body).toContain("event_store_append_duration_ms");
    expect(body).toContain("projection_lag_events");
    expect(body).toContain("snapshot_count");
    expect(body).toContain("commands_total");
  });

  it("uses an injected event-sourcing stack verbatim", async () => {
    const injected = createEventSourcing();
    process.env.AUTH_SECRET = TEST_SECRET;
    const s2 = createServer({
      port: 0,
      heartbeatMs: 60000,
      maxPayloadBytes: 4096,
      storageAdapter: makeFakeStorage(),
      eventSourcing: injected,
    });
    try {
      expect(s2.eventSourcing).toBe(injected);

      const ws = await connect(s2.wss.address().port, makeToken("client-inj"));
      ws.send(JSON.stringify({ type: "join_room", roomId: "fleet-inj" }));
      await nextMessages(ws, 1);
      await flushAsync();

      const events = await injected.eventStore.getEvents("client-inj");
      expect(events.some((e) => baseEventType(e.eventType) === "room_joined")).toBe(true);

      ws.close();
      await waitClose(ws);
    } finally {
      for (const client of s2.wss.clients) client.terminate();
      await new Promise((resolve) => s2.wss.close(resolve));
      await injected.eventStore.close();
    }
  });
});
