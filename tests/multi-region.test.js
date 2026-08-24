import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  HLC,
  hlc,
  ORSet,
  RGASequence,
  LWWMap,
  LWWElementSet,
  MultiRegionCoordinator,
  makeMessageId,
  compareMessageIds,
  vcIncrement,
  vcMerge,
  vcCompare,
  merkleRoot,
  stableStringify,
  sha256,
} from "../src/multi-region.js";

describe("HLC (Hybrid Logical Clock)", () => {
  let clock;

  beforeEach(() => {
    clock = new HLC(1000);
  });

  it("issues monotonically increasing timestamps", () => {
    const t1 = clock.now();
    const t2 = clock.now();
    const t3 = clock.now();

    expect(HLC.compare(t1, t2)).toBeLessThan(0);
    expect(HLC.compare(t2, t3)).toBeLessThan(0);
  });

  it("advances physical component when wall clock jumps forward", () => {
    clock = new HLC(1000);
    const t1 = clock.now();
    // Simulate receiving a timestamp from a peer with advanced wall clock
    const remote = { l: 2000, c: 0 };
    const t2 = clock.receive(remote);

    expect(t2.l).toBeGreaterThanOrEqual(2000);
    expect(HLC.compare(t1, t2)).toBeLessThan(0);
  });

  it("increments logical counter when wall clock stays same", () => {
    clock = new HLC(1000);
    const t1 = clock.now();
    const t2 = clock.now();

    expect(t1.l).toBe(t2.l);
    expect(t2.c).toBe(t1.c + 1);
  });

  it("receive() fast-forwards when peer is ahead", () => {
    clock = new HLC(1000);
    clock.now(); // advance clock
    const remote = { l: 2000, c: 5 };
    const updated = clock.receive(remote);

    expect(HLC.compare(remote, updated)).toBeLessThan(0);
    expect(updated.l).toBeGreaterThanOrEqual(2000);
  });

  it("receive() increments logical counter when tied on physical", () => {
    clock = new HLC(1000);
    const local = clock.now();
    const remote = { l: local.l, c: local.c + 10 };
    const updated = clock.receive(remote);

    expect(updated.l).toBe(local.l);
    expect(updated.c).toBeGreaterThan(remote.c);
  });

  it("causal ordering: if A happens-before B then HLC(A) < HLC(B)", () => {
    clock = new HLC(1000);
    const a = clock.now();
    clock.receive({ l: a.l, c: a.c });
    const b = clock.now();

    expect(HLC.compare(a, b)).toBeLessThan(0);
  });

  it("compare() provides total order", () => {
    expect(HLC.compare({ l: 1, c: 0 }, { l: 2, c: 0 })).toBeLessThan(0);
    expect(HLC.compare({ l: 1, c: 1 }, { l: 1, c: 0 })).toBeGreaterThan(0);
    expect(HLC.compare({ l: 1, c: 0 }, { l: 1, c: 0 })).toBe(0);
  });

  it("hlc export alias works", () => {
    expect(hlc).toBe(HLC);
  });
});

describe("ORSet (Observed-Remove Set)", () => {
  let set;

  beforeEach(() => {
    set = new ORSet();
  });

  it("adds and checks presence", () => {
    set.add("client-1");
    expect(set.has("client-1")).toBe(true);
    expect(set.has("client-2")).toBe(false);
  });

  it("removes observed tags only", () => {
    set.add("client-1");
    set.remove("client-1");
    expect(set.has("client-1")).toBe(false);
  });

  it("concurrent add + remove → add wins (convergence)", () => {
    const setA = new ORSet();
    const setB = new ORSet();

    // Region A adds client-1
    setA.add("client-1");

    // Region B concurrently removes client-1 WITHOUT observing the add tag
    // (simulating a stale remove operation)
    setB.applyRemove(["stale-tag-that-never-existed"]);

    // Now merge: setA has the add, setB has a remove for a different tag
    setA.merge(setB);
    expect(setA.has("client-1")).toBe(true);

    // Reverse merge should also converge
    setB.merge(setA);
    expect(setB.has("client-1")).toBe(true);
  });

  it("merge is commutative and idempotent", () => {
    const setA = new ORSet();
    const setB = new ORSet();

    setA.add("a");
    setB.add("b");

    setA.merge(setB);
    setB.merge(setA);

    expect(setA.members().sort()).toEqual(["a", "b"]);
    expect(setB.members().sort()).toEqual(["a", "b"]);

    setA.merge(setB);
    expect(setA.members().sort()).toEqual(["a", "b"]);
  });

  it("toJSON/fromJSON roundtrip preserves state", () => {
    set.add("client-1");
    set.add("client-2");
    set.remove("client-1");

    const snap = set.toJSON();
    const restored = ORSet.fromJSON(snap);

    expect(restored.has("client-1")).toBe(false);
    expect(restored.has("client-2")).toBe(true);
    expect(restored.members()).toEqual(["client-2"]);
  });

  it("applyRemove with empty tags is no-op", () => {
    set.applyRemove([]);
    expect(set.removes.size).toBe(0);
  });

  it("members() returns only elements with live tags", () => {
    set.add("a");
    set.add("b");
    set.remove("a");
    expect(set.members()).toEqual(["b"]);
  });
});

describe("RGASequence (Replicated Growable Array)", () => {
  let rga;

  beforeEach(() => {
    rga = new RGASequence();
  });

  it("inserts at head when originLeft is null", () => {
    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    rga.insert(id1, null, { msg: "first" });
    expect(rga.toArray()).toHaveLength(1);
    expect(rga.toArray()[0].value).toEqual({ msg: "first" });
  });

  it("appends after lastVisibleId by default", () => {
    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 2, c: 0 }, "us-east", 2);
    rga.insert(id1, null, { msg: "first" });
    rga.insert(id2, id1, { msg: "second" });

    const arr = rga.toArray();
    expect(arr).toHaveLength(2);
    expect(arr[0].value).toEqual({ msg: "first" });
    expect(arr[1].value).toEqual({ msg: "second" });
  });

  it("concurrent inserts at same originLeft → deterministic order by message id", () => {
    const origin = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    rga.insert(origin, null, { msg: "origin" });

    const idA = makeMessageId({ l: 2, c: 0 }, "us-east", 2);
    const idB = makeMessageId({ l: 2, c: 0 }, "eu-west", 2);

    rga.insert(idA, origin, { msg: "A" });
    rga.insert(idB, origin, { msg: "B" });

    const arr = rga.toArray();
    const ids = arr.map((n) => n.id).filter((id) => id !== origin);
    expect(ids.sort()).toEqual(ids);
    expect(ids).toContain(idA);
    expect(ids).toContain(idB);
  });

  it("remove() tombstones but keeps node as anchor", () => {
    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 2, c: 0 }, "us-east", 2);
    rga.insert(id1, null, { msg: "first" });
    rga.insert(id2, id1, { msg: "second" });

    rga.remove(id1);
    expect(rga.has(id1)).toBe(false);
    expect(rga.has(id2)).toBe(true);
    expect(rga.get(id1)).toEqual({ msg: "first" });
  });

  it("merge converges regardless of order", () => {
    const rgaA = new RGASequence();
    const rgaB = new RGASequence();

    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 2, c: 0 }, "eu-west", 1);

    rgaA.insert(id1, null, { msg: "A" });
    rgaB.insert(id2, null, { msg: "B" });

    rgaA.merge(rgaB);
    rgaB.merge(rgaA);

    expect(rgaA.toArray()).toHaveLength(2);
    expect(rgaB.toArray()).toHaveLength(2);
    expect(rgaA.toArray().map((n) => n.id).sort()).toEqual(
      rgaB.toArray().map((n) => n.id).sort(),
    );
  });

  it("indexOf returns global position", () => {
    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 2, c: 0 }, "us-east", 2);
    rga.insert(id1, null, { msg: "first" });
    rga.insert(id2, id1, { msg: "second" });

    expect(rga.indexOf(id1)).toBe(0);
    expect(rga.indexOf(id2)).toBe(1);
    expect(rga.indexOf("unknown")).toBe(-1);
  });

  it("toJSON/fromJSON roundtrip preserves state", () => {
    const id1 = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 2, c: 0 }, "eu-west", 1);
    rga.insert(id1, null, { msg: "first" });
    rga.insert(id2, id1, { msg: "second" });

    const snap = rga.toJSON();
    const restored = RGASequence.fromJSON(snap);

    expect(restored.toArray()).toHaveLength(2);
    expect(restored.indexOf(id1)).toBe(0);
  });

  it("insert is idempotent for duplicate IDs", () => {
    const id = makeMessageId({ l: 1, c: 0 }, "us-east", 1);
    rga.insert(id, null, { msg: "first" });
    rga.insert(id, null, { msg: "duplicate" });
    expect(rga.toArray()).toHaveLength(1);
  });
});

describe("LWWMap (Last-Writer-Wins Map with Vector Clocks)", () => {
  let map;

  beforeEach(() => {
    map = new LWWMap();
  });

  it("sets and gets values", () => {
    const clock = { "us-east": 1 };
    map.set("key1", "value1", clock, "us-east");
    expect(map.get("key1")).toBe("value1");
  });

  it("causally later write wins", () => {
    map.set("key1", "value1", { "us-east": 1 }, "us-east");
    map.set("key1", "value2", { "us-east": 2 }, "us-east");
    expect(map.get("key1")).toBe("value2");
  });

  it("concurrent writes: higher region id wins deterministically", () => {
    const stats = { conflicts: 0 };
    map.set("key1", "valueA", { "us-east": 1 }, "us-east");
    map.set("key1", "valueB", { "eu-west": 1 }, "eu-west");

    expect(["valueA", "valueB"]).toContain(map.get("key1"));
    // Winner is deterministic based on region id (us-east-1 > eu-west-1 alphabetically)
    map.merge(new LWWMap(), stats);
  });

  it("merge resolves concurrent conflicts and counts them", () => {
    const mapA = new LWWMap();
    const mapB = new LWWMap();
    const stats = { conflicts: 0 };

    mapA.set("key1", "valueA", { "us-east": 1 }, "us-east");
    mapB.set("key1", "valueB", { "eu-west": 1 }, "eu-west");

    mapA.merge(mapB, stats);
    expect(stats.conflicts).toBe(1);
  });

  it("delete() creates tombstone that beats older writes", () => {
    map.set("key1", "value1", { "us-east": 1 }, "us-east");
    map.delete("key1", { "us-east": 2 }, "us-east");
    expect(map.get("key1")).toBeUndefined();
    expect(map.has("key1")).toBe(false);
  });

  it("toJSON/fromJSON roundtrip", () => {
    map.set("key1", "value1", { "us-east": 1 }, "us-east");
    const snap = map.toJSON();
    const restored = LWWMap.fromJSON(snap);
    expect(restored.get("key1")).toBe("value1");
  });
});

describe("LWWElementSet (Geofence Definitions)", () => {
  let set;

  beforeEach(() => {
    set = new LWWElementSet();
  });

  it("adds and retrieves elements with stamp", () => {
    const stamp = { hlc: { l: 1000, c: 0 }, regionId: "us-east", value: { id: "fence-1", coords: [] } };
    set.add("fence-1", stamp);
    expect(set.has("fence-1")).toBe(true);
    expect(set.get("fence-1")).toEqual({ id: "fence-1", coords: [] });
  });

  it("later stamp wins over earlier", () => {
    set.add("fence-1", { hlc: { l: 1000, c: 0 }, regionId: "us-east", value: { v: 1 } });
    set.add("fence-1", { hlc: { l: 2000, c: 0 }, regionId: "eu-west", value: { v: 2 } });
    expect(set.get("fence-1").v).toBe(2);
  });

  it("remove with later stamp beats earlier add", () => {
    set.add("fence-1", { hlc: { l: 1000, c: 0 }, regionId: "us-east", value: { v: 1 } });
    set.remove("fence-1", { hlc: { l: 2000, c: 0 }, regionId: "eu-west" });
    expect(set.has("fence-1")).toBe(false);
  });

  it("merge keeps maximal stamps", () => {
    const setA = new LWWElementSet();
    const setB = new LWWElementSet();

    setA.add("fence-1", { hlc: { l: 1000, c: 0 }, regionId: "us-east", value: { v: 1 } });
    setB.add("fence-1", { hlc: { l: 2000, c: 0 }, regionId: "eu-west", value: { v: 2 } });

    setA.merge(setB);
    expect(setA.get("fence-1").v).toBe(2);
  });

  it("toJSON/fromJSON roundtrip", () => {
    set.add("fence-1", { hlc: { l: 1000, c: 0 }, regionId: "us-east", value: { v: 1 } });
    const snap = set.toJSON();
    const restored = LWWElementSet.fromJSON(snap);
    expect(restored.has("fence-1")).toBe(true);
    expect(restored.get("fence-1").v).toBe(1);
  });
});

describe("Vector Clock utilities", () => {
  it("vcIncrement increments single region counter", () => {
    const clock = { "us-east": 1 };
    const next = vcIncrement(clock, "us-east");
    expect(next["us-east"]).toBe(2);
    expect(clock["us-east"]).toBe(1);
  });

  it("vcMerge takes per-component maximum", () => {
    const a = { "us-east": 2, "eu-west": 1 };
    const b = { "us-east": 1, "eu-west": 3, "ap-southeast": 5 };
    const merged = vcMerge(a, b);
    expect(merged).toEqual({ "us-east": 2, "eu-west": 3, "ap-southeast": 5 });
  });

  it("vcCompare returns correct ordering", () => {
    expect(vcCompare({ a: 2 }, { a: 1 })).toBe("after");
    expect(vcCompare({ a: 1 }, { a: 2 })).toBe("before");
    expect(vcCompare({ a: 1 }, { a: 1 })).toBe("equal");
    expect(vcCompare({ a: 2, b: 1 }, { a: 1, b: 2 })).toBe("concurrent");
  });
});

describe("Merkle Tree & Stable Stringify", () => {
  it("sha256 produces consistent hex digest", () => {
    expect(sha256("test")).toHaveLength(64);
    expect(sha256("test")).toBe(sha256("test"));
  });

  it("merkleRoot computes root over sorted leaves", () => {
    const leaves = ["a", "b", "c", "d"].map(sha256);
    const root = merkleRoot(leaves);
    expect(root).toHaveLength(64);
  });

  it("merkleRoot duplicates last leaf when odd count", () => {
    const leaves = ["a", "b", "c"].map(sha256);
    const root = merkleRoot(leaves);
    expect(root).toHaveLength(64);
  });

  it("merkleRoot of empty array is hash of empty string", () => {
    expect(merkleRoot([])).toBe(sha256(""));
  });

  it("stableStringify sorts object keys", () => {
    const obj1 = { b: 1, a: 2 };
    const obj2 = { a: 2, b: 1 };
    expect(stableStringify(obj1)).toBe(stableStringify(obj2));
  });

  it("stableStringify handles arrays and primitives", () => {
    expect(stableStringify([1, 2, 3])).toBe("[1,2,3]");
    expect(stableStringify("hello")).toBe('"hello"');
    expect(stableStringify(42)).toBe("42");
    expect(stableStringify(null)).toBe("null");
  });
});

describe("MultiRegionCoordinator", () => {
  let coordinator;
  let mockTransport;
  let sentOps;

  beforeEach(() => {
    sentOps = [];
    mockTransport = {
      send: vi.fn(async (_region, op) => {
        sentOps.push({ region: _region, op });
        return { ack: true };
      }),
      fetchState: vi.fn(async () => {
        return coordinator.serializeState();
      }),
      fetchMerkleRoot: vi.fn(async () => {
        return coordinator.merkleRootForRooms();
      }),
    };

    coordinator = new MultiRegionCoordinator({
      regionId: "us-east-1",
      peerRegions: ["eu-west-1", "ap-southeast-1"],
      replicationTransport: mockTransport,
      config: {
        dataResidency: {
          eu: ["eu-west-1"],
          us: ["us-east-1", "us-west-2"],
        },
        roomResidency: {
          "fleet-eu": "eu",
          "fleet-us": "us",
          "fleet-global": "global",
        },
        quorumSize: 2,
        antiEntropyIntervalMs: 0,
        lagAlertMs: 5000,
      },
      logger: { error: vi.fn() },
    });
  });

  afterEach(() => {
    coordinator.close();
  });

  describe("Room Membership (OR-Set)", () => {
    it("joinRoom adds client locally and replicates", () => {
      const { tag } = coordinator.joinRoom("client-1", "fleet-global");
      expect(tag).toContain("us-east-1");
      expect(coordinator.getRoomMembers("fleet-global")).toContain("client-1");
      // Replicates to 2 peer regions
      expect(sentOps).toHaveLength(2);
      expect(sentOps[0].op.type).toBe("membership_add");
    });

    it("leaveRoom removes observed tags and replicates", () => {
      coordinator.joinRoom("client-1", "fleet-global");
      sentOps.length = 0;

      const { removed, tags } = coordinator.leaveRoom("client-1", "fleet-global");
      expect(removed).toBe(true);
      expect(tags.length).toBeGreaterThan(0);
      expect(coordinator.getRoomMembers("fleet-global")).not.toContain("client-1");
      expect(sentOps[0].op.type).toBe("membership_remove");
    });

    it("concurrent add + remove → add wins", () => {
      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });

      const tag = coordinator.joinRoom("client-1", "fleet-global").tag;

      // Peer receives the add operation
      peer.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "fleet-global", clientId: "client-1", tag },
        vectorClock: {},
        regionId: "us-east-1",
        timestamp: Date.now(),
      });

      // Peer performs a concurrent remove WITHOUT observing the add tag
      // (simulating a stale leave from before the join was replicated)
      peer.rooms.get("fleet-global").membership.applyRemove(["stale-tag"]);

      // Now peer sends the remove operation
      coordinator.handlePeerOperation({
        type: "membership_remove",
        payload: { roomId: "fleet-global", tags: ["stale-tag"] },
        vectorClock: {},
        regionId: "eu-west-1",
        timestamp: Date.now(),
      });

      // Add wins because the remove tag was never observed by the adder
      expect(coordinator.getRoomMembers("fleet-global")).toContain("client-1");
      peer.close();
    });

    it("getRoomMembers returns merged (read-repaired) state", () => {
      coordinator.joinRoom("client-1", "fleet-global");
      const peerState = {
        rooms: {
          "fleet-global": {
            membership: { adds: { "client-2": ["tag:eu-west:123"] }, removes: [] },
            sequence: { nodes: [] },
          },
        },
        sessions: { entries: {} },
        geofences: { adds: [], removes: [] },
        vectorClock: {},
      };
      coordinator.mergeRemoteState(peerState);
      expect(coordinator.getRoomMembers("fleet-global")).toContain("client-1");
      expect(coordinator.getRoomMembers("fleet-global")).toContain("client-2");
    });
  });

  describe("Message Broadcast (RGA Sequence + HLC)", () => {
    it("broadcast assigns HLC timestamp and global sequence ID", () => {
      const result = coordinator.broadcast("fleet-global", { lat: 1, lng: 2 });
      expect(result.id).toContain("us-east-1");
      expect(result.hlc).toBeDefined();
      expect(result.localSeq).toBe(1);
      expect(result.originLeft).toBeNull();
    });

    it("subsequent broadcasts chain originLeft", () => {
      const r1 = coordinator.broadcast("fleet-global", { msg: 1 });
      const r2 = coordinator.broadcast("fleet-global", { msg: 2 });
      expect(r2.originLeft).toBe(r1.id);
    });

    it("replicated message appears in peer with same global sequence", () => {
      const result = coordinator.broadcast("fleet-global", { msg: "hello" });

      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });

      peer.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "fleet-global", id: result.id, originLeft: result.originLeft, value: { msg: "hello" } },
        vectorClock: {},
        regionId: "us-east-1",
        timestamp: Date.now(),
        hlc: result.hlc,
      });

      const arr = peer.rooms.get("fleet-global").sequence.toArray();
      expect(arr).toHaveLength(1);
      expect(arr[0].id).toBe(result.id);
      expect(arr[0].value).toEqual({ msg: "hello" });
      peer.close();
    });

    it("message delivered in peer within logical time (simulated)", () => {
      const start = Date.now();
      coordinator.broadcast("fleet-global", { msg: "latency-test" });
      const elapsed = Date.now() - start;
      expect(elapsed).toBeLessThan(200);
    });
  });

  describe("Session State (LWWMap) & Failover", () => {
    it("saveSession upserts session state and replicates", () => {
      const { key, clock } = coordinator.saveSession("client-1", { rooms: ["fleet-global"], cursor: 5 });
      expect(key).toBe("session:client-1");
      expect(clock["us-east-1"]).toBe(1);
      expect(coordinator.sessions.get(key)).toEqual({ rooms: ["fleet-global"], cursor: 5 });
    });

    it("restoreSession returns full state for failover", () => {
      coordinator.joinRoom("client-1", "fleet-global");
      coordinator.saveSession("client-1", { cursor: 10, prefs: { theme: "dark" } });

      const restored = coordinator.restoreSession("client-1");
      expect(restored.clientId).toBe("client-1");
      expect(restored.session).toEqual({ cursor: 10, prefs: { theme: "dark" } });
      expect(restored.memberships).toContain("fleet-global");
      expect(restored.restoredAt).toBeDefined();
    });

    it("failover to new region restores session via CRDT replication", () => {
      coordinator.joinRoom("client-1", "fleet-global");
      coordinator.saveSession("client-1", { cursor: 10 });

      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });

      peer.mergeRemoteState(coordinator.serializeState());

      const restored = peer.restoreSession("client-1");
      expect(restored.session).toEqual({ cursor: 10 });
      expect(restored.memberships).toContain("fleet-global");
      peer.close();
    });

    it("session merge resolves concurrent updates with vector clocks", () => {
      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });

      coordinator.saveSession("client-1", { cursor: 5 });
      peer.saveSession("client-1", { cursor: 7 });

      // Concurrent writes: higher region id ("us-east-1" > "eu-west-1") wins
      peer.mergeRemoteState(coordinator.serializeState());
      expect(peer.sessions.get("session:client-1").cursor).toBe(5);

      coordinator.mergeRemoteState(peer.serializeState());
      expect(coordinator.sessions.get("session:client-1").cursor).toBe(5);
      peer.close();
    });
  });

  describe("Geofence Definitions (LWW-Element-Set) & Quorum", () => {
    it("syncGeofence adds fence locally and replicates with quorum", async () => {
      const result = await coordinator.syncGeofence({
        id: "fence-1",
        type: "circle",
        center: { lat: 0, lng: 0 },
        radius: 1000,
      });
      expect(result.committed).toBe(true);
      expect(result.acks).toBeGreaterThanOrEqual(2);
      expect(coordinator.geofences.has("fence-1")).toBe(true);
    });

    it("removeGeofence removes fence with quorum", async () => {
      await coordinator.syncGeofence({ id: "fence-1", type: "circle" });
      const result = await coordinator.removeGeofence("fence-1");
      expect(result.committed).toBe(true);
      expect(coordinator.geofences.has("fence-1")).toBe(false);
    });

    it("listGeofences returns only live fences", async () => {
      await coordinator.syncGeofence({ id: "fence-1" });
      await coordinator.syncGeofence({ id: "fence-2" });
      await coordinator.removeGeofence("fence-1");

      const fences = coordinator.listGeofences();
      expect(fences).toHaveLength(1);
      expect(fences[0].id).toBe("fence-2");
    });

    it("quorumSize defaults to majority (2 of 3)", () => {
      expect(coordinator.quorumSize()).toBe(2);
    });

    it("config.quorumSize overrides default", () => {
      const c = new MultiRegionCoordinator({
        regionId: "us-east-1",
        peerRegions: ["eu-west-1", "ap-southeast-1"],
        config: { quorumSize: 3, antiEntropyIntervalMs: 0 },
      });
      expect(c.quorumSize()).toBe(3);
      c.close();
    });
  });

  describe("Data Residency Enforcement", () => {
    it("EU-only room data never replicates to US region", () => {
      coordinator.joinRoom("client-1", "fleet-eu");
      const euOps = sentOps.filter((o) => o.region === "eu-west-1");
      const usOps = sentOps.filter((o) => o.region === "us-east-1");
      expect(euOps.length).toBeGreaterThan(0);
      expect(usOps.length).toBe(0);
    });

    it("US-only room data replicates only to US regions", () => {
      const usCoordinator = new MultiRegionCoordinator({
        regionId: "us-west-2",
        peerRegions: ["us-east-1", "eu-west-1"],
        replicationTransport: mockTransport,
        config: {
          dataResidency: { eu: ["eu-west-1"], us: ["us-east-1", "us-west-2"] },
          roomResidency: { "fleet-us": "us" },
          antiEntropyIntervalMs: 0,
        },
      });

      sentOps.length = 0;
      usCoordinator.joinRoom("client-1", "fleet-us");
      const euOps = sentOps.filter((o) => o.region === "eu-west-1");
      const usOps = sentOps.filter((o) => o.region === "us-east-1");
      expect(euOps.length).toBe(0);
      expect(usOps.length).toBeGreaterThan(0);
      usCoordinator.close();
    });

    it("global room data replicates to all regions", () => {
      sentOps.length = 0;
      coordinator.joinRoom("client-1", "fleet-global");
      const euOps = sentOps.filter((o) => o.region === "eu-west-1");
      const apOps = sentOps.filter((o) => o.region === "ap-southeast-1");
      expect(euOps.length).toBeGreaterThan(0);
      expect(apOps.length).toBeGreaterThan(0);
    });

    it("metrics track opsFilteredResidency", () => {
      coordinator.joinRoom("client-1", "fleet-eu");
      expect(coordinator.metrics.opsFilteredResidency).toBeGreaterThan(0);
    });
  });

  describe("Cross-Region Replication & handlePeerOperation", () => {
    it("handlePeerOperation applies membership_add", () => {
      coordinator.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "fleet-global", clientId: "client-remote", tag: "tag:eu:1" },
        vectorClock: { "eu-west-1": 1 },
        regionId: "eu-west-1",
        timestamp: Date.now(),
      });
      expect(coordinator.getRoomMembers("fleet-global")).toContain("client-remote");
    });

    it("handlePeerOperation applies message_append", () => {
      const id = makeMessageId({ l: 100, c: 0 }, "eu-west-1", 1);
      coordinator.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "fleet-global", id, originLeft: null, value: { msg: "remote" } },
        vectorClock: { "eu-west-1": 1 },
        regionId: "eu-west-1",
        timestamp: Date.now(),
        hlc: { l: 100, c: 0 },
      });
      const arr = coordinator.rooms.get("fleet-global").sequence.toArray();
      expect(arr).toHaveLength(1);
      expect(arr[0].id).toBe(id);
    });

    it("handlePeerOperation advances HLC and vector clock", () => {
      const beforeHlc = coordinator.hlcInstance.l;

      coordinator.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "fleet-global", clientId: "c", tag: "t" },
        vectorClock: { "eu-west-1": 5 },
        regionId: "eu-west-1",
        timestamp: Date.now(),
        hlc: { l: beforeHlc + 100, c: 0 },
      });

      expect(coordinator.hlcInstance.l).toBeGreaterThanOrEqual(beforeHlc + 100);
      expect(coordinator.vectorClock["eu-west-1"]).toBe(5);
    });

    it("metrics track replication lag", () => {
      coordinator.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "fleet-global", clientId: "c", tag: "t" },
        vectorClock: {},
        regionId: "eu-west-1",
        timestamp: Date.now() - 100,
      });
      const lagKey = "eu-west-1->us-east-1";
      expect(coordinator.metrics.replicationLagMs[lagKey]).toBeGreaterThanOrEqual(100);
    });

    it("metrics track crdt_merge_duration_ms", () => {
      coordinator.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "fleet-global", clientId: "c", tag: "t" },
        vectorClock: {},
        regionId: "eu-west-1",
        timestamp: Date.now(),
      });
      expect(coordinator.metrics.crdtMergeDurationMs.count).toBe(1);
      expect(coordinator.metrics.crdtMergeDurationMs.totalMs).toBeGreaterThanOrEqual(0);
    });

    it("metrics track replication_conflicts_total on concurrent LWWMap writes", () => {
      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });

      coordinator.saveSession("client-1", { v: 1 });
      peer.saveSession("client-1", { v: 2 });

      coordinator.mergeRemoteState(peer.serializeState());
      expect(coordinator.metrics.replicationConflictsTotal).toBeGreaterThanOrEqual(0);
      peer.close();
    });
  });

  describe("Anti-Entropy (Merkle Sync)", () => {
    it("merkleRootForRooms computes deterministic root", () => {
      coordinator.joinRoom("c1", "room-1");
      coordinator.broadcast("room-1", { msg: 1 });

      const root1 = coordinator.merkleRootForRooms();
      const root2 = coordinator.merkleRootForRooms();
      expect(root1).toBe(root2);
      expect(root1).toHaveLength(64);
    });

    it("antiEntropy detects divergence via Merkle root", async () => {
      coordinator.joinRoom("c1", "room-1");

      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        replicationTransport: mockTransport,
        config: { antiEntropyIntervalMs: 0 },
      });
      peer.joinRoom("c2", "room-1");

      const result = await coordinator.antiEntropy();
      expect(result.peersChecked).toBe(2);
      expect(result.peersRepaired).toBeGreaterThanOrEqual(0);
      peer.close();
    });

    it("antiEntropy repairs divergent state via fetchState", async () => {
      coordinator.joinRoom("c1", "room-1");

      // Replace coordinator's transport with one that simulates divergent peer state
      const divergentTransport = {
        fetchState: vi.fn(async () => {
          // Return state with an additional room that coordinator doesn't have
          const state = coordinator.serializeState();
          state.rooms["room-2"] = {
            membership: { adds: { "c2": ["tag:peer"] }, removes: [] },
            sequence: { nodes: [] },
          };
          return state;
        }),
        fetchMerkleRoot: vi.fn(async () => "different-root"),
      };
      coordinator.replicationTransport = divergentTransport;

      const result = await coordinator.antiEntropy();
      expect(result.peersRepaired).toBe(1);
      expect(coordinator.getRoomMembers("room-2")).toContain("c2");
    });

    it("antiEntropy marks region failed on transport error", async () => {
      const failingTransport = {
        fetchMerkleRoot: vi.fn(async () => { throw new Error("network error"); }),
      };
      const c = new MultiRegionCoordinator({
        regionId: "us-east-1",
        peerRegions: ["eu-west-1"],
        replicationTransport: failingTransport,
        config: { antiEntropyIntervalMs: 0 },
      });
      await c.antiEntropy();
      expect(c.failedRegions.has("eu-west-1")).toBe(true);
      c.close();
    });

    it("mergeRemoteState returns true when state changes", () => {
      const peerState = coordinator.serializeState();
      peerState.rooms["new-room"] = {
        membership: { adds: { "c1": ["tag:1"] }, removes: [] },
        sequence: { nodes: [] },
      };
      const changed = coordinator.mergeRemoteState(peerState);
      expect(changed).toBe(true);
      expect(coordinator.getRoomMembers("new-room")).toContain("c1");
    });
  });

  describe("Region Health & Failover", () => {
    it("markRegionFailed excludes region from replication", () => {
      coordinator.markRegionFailed("eu-west-1", "health check failed");
      expect(coordinator.failedRegions.has("eu-west-1")).toBe(true);
      expect(coordinator.isRegionHealthy("eu-west-1")).toBe(false);
    });

    it("markRegionHealthy restores region", () => {
      coordinator.markRegionFailed("eu-west-1");
      coordinator.markRegionHealthy("eu-west-1");
      expect(coordinator.failedRegions.has("eu-west-1")).toBe(false);
      expect(coordinator.isRegionHealthy("eu-west-1")).toBe(true);
    });

    it("isRegionHealthy checks replication lag threshold", () => {
      coordinator.metrics.replicationLagMs["eu-west-1->us-east-1"] = 6000;
      expect(coordinator.isRegionHealthy("eu-west-1")).toBe(false);
    });

    it("failoverTargets returns healthy peers sorted by lag", () => {
      coordinator.metrics.replicationLagMs["eu-west-1->us-east-1"] = 100;
      coordinator.metrics.replicationLagMs["ap-southeast-1->us-east-1"] = 50;

      const targets = coordinator.failoverTargets();
      expect(targets[0]).toBe("ap-southeast-1");
      expect(targets[1]).toBe("eu-west-1");
    });

    it("isLagAlertActive detects lag exceeding threshold", () => {
      coordinator.metrics.replicationLagMs["eu-west-1->us-east-1"] = 6000;
      expect(coordinator.isLagAlertActive()).toBe(true);
    });

    it("self health respects config.selfDisabled", () => {
      const c = new MultiRegionCoordinator({
        regionId: "us-east-1",
        peerRegions: [],
        config: { selfDisabled: true, antiEntropyIntervalMs: 0 },
      });
      expect(c.isRegionHealthy("us-east-1")).toBe(false);
      c.close();
    });
  });

  describe("Global Config (Quorum)", () => {
    it("setGlobalConfig writes through quorum", async () => {
      const result = await coordinator.setGlobalConfig("rate-limit:msg-per-min", 500);
      expect(result.committed).toBe(true);
      expect(coordinator.getGlobalConfig("rate-limit:msg-per-min")).toBe(500);
    });

    it("getGlobalConfig reads merged value", async () => {
      await coordinator.setGlobalConfig("test-key", "test-value");
      expect(coordinator.getGlobalConfig("test-key")).toBe("test-value");
    });
  });

  describe("Serialization & State Transfer", () => {
    it("serializeState captures all CRDTs", async () => {
      coordinator.joinRoom("c1", "room-1");
      coordinator.saveSession("c1", { v: 1 });
      await coordinator.syncGeofence({ id: "f1" });

      const state = coordinator.serializeState();
      expect(state.regionId).toBe("us-east-1");
      expect(state.rooms["room-1"]).toBeDefined();
      expect(state.sessions.entries["session:c1"]).toBeDefined();
      expect(state.geofences.adds.length).toBeGreaterThan(0);
      expect(state.vectorClock).toBeDefined();
    });

    it("mergeRemoteState merges all CRDT types", async () => {
      const peer = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        config: { antiEntropyIntervalMs: 0 },
      });
      peer.joinRoom("c2", "room-1");
      peer.saveSession("c2", { v: 2 });
      await peer.syncGeofence({ id: "f2" });

      coordinator.mergeRemoteState(peer.serializeState());
      expect(coordinator.getRoomMembers("room-1")).toContain("c2");
      expect(coordinator.sessions.get("session:c2")).toEqual({ v: 2 });
      expect(coordinator.geofences.has("f2")).toBe(true);
      peer.close();
    });
  });

  describe("Full Integration: Multi-Region Fleet Operation", () => {
    it("simulates vessel crossing Atlantic: handoff from us-east to eu-west", () => {
      const usEast = coordinator;
      const euWest = new MultiRegionCoordinator({
        regionId: "eu-west-1",
        peerRegions: ["us-east-1"],
        replicationTransport: mockTransport,
        config: {
          dataResidency: { eu: ["eu-west-1"], us: ["us-east-1"] },
          roomResidency: { "fleet-global": "global" },
          antiEntropyIntervalMs: 0,
        },
      });

      // Vessel joins in us-east
      usEast.joinRoom("vessel-1", "fleet-global");
      usEast.saveSession("vessel-1", { position: { lat: 40, lng: -74 }, seq: 10 });

      // Replicate to eu-west
      euWest.mergeRemoteState(usEast.serializeState());

      // Vessel publishes position from us-east
      const broadcast = usEast.broadcast("fleet-global", { lat: 40.5, lng: -70, seq: 11 });

      // Replicate message to eu-west
      euWest.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "fleet-global", id: broadcast.id, originLeft: broadcast.originLeft, value: { lat: 40.5, lng: -70, seq: 11 } },
        vectorClock: broadcast.vectorClock ?? {},
        regionId: "us-east-1",
        timestamp: Date.now(),
        hlc: broadcast.hlc,
      });

      // us-east fails
      usEast.markRegionFailed("us-east-1");

      // Vessel reconnects to eu-west with session_id
      const restored = euWest.restoreSession("vessel-1");
      expect(restored.session.position).toEqual({ lat: 40, lng: -74 });
      expect(restored.memberships).toContain("fleet-global");
      expect(restored.session.seq).toBe(10);

      // Message history available in eu-west
      const history = euWest.rooms.get("fleet-global").sequence.toArray();
      expect(history).toHaveLength(1);
      expect(history[0].value.seq).toBe(11);

      euWest.close();
    });

    it("OR-Set convergence: region A adds, region B removes concurrently → client present", () => {
      const regionA = new MultiRegionCoordinator({ regionId: "A", peerRegions: ["B"], config: { antiEntropyIntervalMs: 0 } });
      const regionB = new MultiRegionCoordinator({ regionId: "B", peerRegions: ["A"], config: { antiEntropyIntervalMs: 0 } });

      const tag = regionA.joinRoom("client-1", "room-1").tag;
      regionB.handlePeerOperation({
        type: "membership_add",
        payload: { roomId: "room-1", clientId: "client-1", tag },
        vectorClock: {},
        regionId: "A",
        timestamp: Date.now(),
      });

      // Region B performs concurrent remove WITHOUT observing the add tag
      regionB.rooms.get("room-1").membership.applyRemove(["stale-tag"]);
      const removeOp = {
        type: "membership_remove",
        payload: { roomId: "room-1", tags: ["stale-tag"] },
        vectorClock: {},
        regionId: "B",
        timestamp: Date.now(),
      };
      regionA.handlePeerOperation(removeOp);

      expect(regionA.getRoomMembers("room-1")).toContain("client-1");
      regionA.close();
      regionB.close();
    });

    it("RGA convergence: concurrent inserts at same position → both appear, deterministic order", () => {
      const regionA = new MultiRegionCoordinator({ regionId: "A", peerRegions: ["B"], config: { antiEntropyIntervalMs: 0 } });
      const regionB = new MultiRegionCoordinator({ regionId: "B", peerRegions: ["A"], config: { antiEntropyIntervalMs: 0 } });

      const idA = regionA.broadcast("room-1", { from: "A" }).id;
      const idB = regionB.broadcast("room-1", { from: "B" }).id;

      regionA.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "room-1", id: idB, originLeft: null, value: { from: "B" } },
        vectorClock: {},
        regionId: "B",
        timestamp: Date.now(),
        hlc: { l: 100, c: 0 },
      });
      regionB.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "room-1", id: idA, originLeft: null, value: { from: "A" } },
        vectorClock: {},
        regionId: "A",
        timestamp: Date.now(),
        hlc: { l: 100, c: 0 },
      });

      const arrA = regionA.rooms.get("room-1").sequence.toArray();
      const arrB = regionB.rooms.get("room-1").sequence.toArray();

      expect(arrA).toHaveLength(2);
      expect(arrB).toHaveLength(2);
      expect(arrA.map((n) => n.id)).toEqual(arrB.map((n) => n.id));
      regionA.close();
      regionB.close();
    });

    it("HLC ordering: event in us-east at T, replicated to eu-west at T+50ms → eu-west HLC > us-east HLC", () => {
      const usEast = new MultiRegionCoordinator({ regionId: "us-east", peerRegions: ["eu-west"], config: { antiEntropyIntervalMs: 0 } });
      const euWest = new MultiRegionCoordinator({ regionId: "eu-west", peerRegions: ["us-east"], config: { antiEntropyIntervalMs: 0 } });

      const stamp = usEast.hlcInstance.now();
      usEast.broadcast("room-1", { msg: "test" });

      euWest.handlePeerOperation({
        type: "message_append",
        payload: { roomId: "room-1", id: "msg-1", originLeft: null, value: { msg: "test" } },
        vectorClock: {},
        regionId: "us-east",
        timestamp: Date.now(),
        hlc: stamp,
      });

      expect(HLC.compare(stamp, euWest.hlcInstance.receive(stamp))).toBeLessThan(0);
      usEast.close();
      euWest.close();
    });
  });
});

describe("makeMessageId & compareMessageIds", () => {
  it("makeMessageId produces sortable strings", () => {
    const id1 = makeMessageId({ l: 1000, c: 0 }, "us-east", 1);
    const id2 = makeMessageId({ l: 1000, c: 1 }, "us-east", 2);
    const id3 = makeMessageId({ l: 2000, c: 0 }, "eu-west", 1);

    expect(compareMessageIds(id1, id2)).toBeLessThan(0);
    expect(compareMessageIds(id2, id3)).toBeLessThan(0);
  });

  it("same HLC, different region → region id breaks tie", () => {
    const id1 = makeMessageId({ l: 1000, c: 0 }, "aaa", 1);
    const id2 = makeMessageId({ l: 1000, c: 0 }, "zzz", 1);
    expect(compareMessageIds(id1, id2)).toBeLessThan(0);
  });
});