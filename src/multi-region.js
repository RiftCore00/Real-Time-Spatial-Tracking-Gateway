/**
 * @fileoverview Multi-region active-active coordination with conflict-free
 * geo-replication (issue 24).
 *
 * This module implements the building blocks for running gateway fleets in
 * several regions at once:
 *
 * - `HLC` — hybrid logical clocks for causal ordering across regions.
 * - `ORSet` — observed-remove set used for room membership (concurrent
 *   add + remove resolves to add-wins, which is safe for presence).
 * - `RGASequence` — replicated growable array giving every message a single,
 *   globally agreed position even when two regions insert concurrently.
 * - `LWWMap` — last-writer-wins map resolved with vector clocks, used for
 *   client session state so a reconnecting client lands on a region that
 *   already knows its state (zero-downtime failover).
 * - `LWWElementSet` — last-update-wins element set for geofence definitions.
 * - `MultiRegionCoordinator` — wires the CRDTs together, replicates operations
 *   to peer regions through an injected transport, enforces data-residency
 *   policy at the replication layer, runs quorum writes for critical metadata,
 *   and periodically reconciles divergence via Merkle-tree anti-entropy.
 *
 * Everything is dependency-free: the CRDTs and the HLC are implemented from
 * scratch and the replication transport is an abstract interface (`send`,
 * `fetchState`, optional `fetchMerkleRoot`) so deployments can back it with
 * WebSockets, gRPC, AWS Global Accelerator, Cloudflare Tunnel or dedicated
 * fiber without touching this code.
 */

import { createHash } from "node:crypto";

// ─── defaults ─────────────────────────────────────────────────────────────────

const DEFAULT_ANTI_ENTROPY_INTERVAL_MS = 15000;
const DEFAULT_LAG_ALERT_MS = 5000;
const RESIDENCY_GLOBAL = "global";

// ─── hybrid logical clock ─────────────────────────────────────────────────────

/**
 * A timestamp produced by {@link HLC}.
 *
 * @typedef {object} HLCTimestamp
 * @property {number} l - Millisecond component (physical wall clock, lamport-adjusted).
 * @property {number} c - Logical counter disambiguating events in the same millisecond.
 */

/**
 * Hybrid logical clock combining wall-clock time with a Lamport counter.
 *
 * Timestamps are totally ordered: if event A happens-before event B (in real
 * time or through message causality) then `HLC.compare(A, B) < 0`. Receiving a
 * timestamp from a peer whose clock is ahead fast-forwards the local clock, so
 * replication itself can never move a region backwards.
 */
export class HLC {
  /**
   * @param {number} [initialWallMs] Initial physical reading, mainly for tests.
   */
  constructor(initialWallMs = 0) {
    /** Physical component of the clock. @private */
    this.l = initialWallMs;
    /** Logical counter for events sharing the same physical value. @private */
    this.c = 0;
  }

  /**
   * Issues a fresh timestamp for a local event.
   * @returns {HLCTimestamp}
   */
  now() {
    const wall = Date.now();
    if (wall > this.l) {
      this.l = wall;
      this.c = 0;
    } else {
      this.c += 1;
    }
    return { l: this.l, c: this.c };
  }

  /**
   * Observes a timestamp received from a peer, keeping causality monotonic.
   *
   * @param {HLCTimestamp} remote - The peer's timestamp attached to its event.
   * @returns {HLCTimestamp} The new local reading, strictly after `remote`.
   */
  receive(remote) {
    const wall = Date.now();
    if (remote.l > this.l && remote.l > wall) {
      this.l = remote.l;
      this.c = remote.c + 1;
    } else if (wall > this.l) {
      this.l = wall;
      this.c = 0;
    } else if (this.l > remote.l) {
      this.c += 1;
    } else {
      // Local reading ties the remote one: break the tie logically.
      this.c = Math.max(this.c, remote.c) + 1;
    }
    return { l: this.l, c: this.c };
  }

  /**
   * Total order over timestamps. Never returns 0 for distinct timestamps.
   *
   * @param {HLCTimestamp} a
   * @param {HLCTimestamp} b
   * @returns {number} Negative when `a < b`, positive when `a > b`.
   */
  static compare(a, b) {
    if (a.l !== b.l) return a.l - b.l;
    return a.c - b.c;
  }
}

/** Convenience alias matching the issue's `hlc.*` naming. */
export const hlc = HLC;

// ─── vector clocks ────────────────────────────────────────────────────────────

/**
 * Vector clock mapping `regionId → counter`. Used by {@link LWWMap} and stamped
 * onto every replicated operation.
 *
 * @typedef {Record<string, number>} VectorClock
 */

/**
 * Returns a copy of `clock` with the counter for `regionId` incremented.
 *
 * @param {VectorClock} clock
 * @param {string} regionId
 * @returns {VectorClock}
 */
export function vcIncrement(clock, regionId) {
  return { ...clock, [regionId]: (clock[regionId] ?? 0) + 1 };
}

/**
 * Merges two vector clocks taking the per-component maximum.
 *
 * @param {VectorClock} a
 * @param {VectorClock} b
 * @returns {VectorClock}
 */
export function vcMerge(a, b) {
  const out = { ...a };
  for (const [region, counter] of Object.entries(b)) {
    out[region] = Math.max(out[region] ?? 0, counter);
  }
  return out;
}

/**
 * @typedef {"equal"|"before"|"after"|"concurrent"} VCOrder
 */

/**
 * Compares two vector clocks.
 *
 * @param {VectorClock} a
 * @param {VectorClock} b
 * @returns {VCOrder}
 */
export function vcCompare(a, b) {
  let aGreater = false;
  let bGreater = false;
  const regions = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const region of regions) {
    const av = a[region] ?? 0;
    const bv = b[region] ?? 0;
    if (av > bv) aGreater = true;
    else if (bv > av) bGreater = true;
  }
  if (aGreater && bGreater) return "concurrent";
  if (aGreater) return "after";
  if (bGreater) return "before";
  return "equal";
}

// ─── OR-Set ───────────────────────────────────────────────────────────────────

/**
 * Observed-Remove set.
 *
 * Every `add()` attaches a unique tag; `remove()` only retires the tags the
 * remover has actually observed. Two regions that concurrently add and remove
 * the same element converge to "present", because the concurrent add carries a
 * tag the remover never saw. This makes OR-Set ideal for room membership:
 * a stale leave can never evict a fresh join.
 */
export class ORSet {
  constructor() {
    /** element → set of live add-tags. @private */
    this.adds = new Map();
    /** Retired tags. @private */
    this.removes = new Set();
  }

  /**
   * Adds an element under a unique tag (generated when omitted).
   *
   * @param {string} elem
   * @param {string} [tag]
   * @returns {string} The tag backing this addition.
   */
  add(elem, tag) {
    const resolved =
      tag ?? `tag:${process.hrtime.bigint()}:${Math.random().toString(36).slice(2, 10)}`;
    let tags = this.adds.get(elem);
    if (!tags) {
      tags = new Set();
      this.adds.set(elem, tags);
    }
    tags.add(resolved);
    return resolved;
  }

  /**
   * Removes every currently-observed tag for the element.
   *
   * @param {string} elem
   * @returns {string[]} The retired tags (empty when the element was absent).
   */
  remove(elem) {
    const tags = this.adds.get(elem);
    const retired = [];
    if (!tags) return retired;
    for (const tag of tags) {
      this.removes.add(tag);
      retired.push(tag);
    }
    return retired;
  }

  /**
   * Whether the element is present (has at least one unretired add-tag).
   *
   * @param {string} elem
   * @returns {boolean}
   */
  has(elem) {
    const tags = this.adds.get(elem);
    if (!tags) return false;
    for (const tag of tags) {
      if (!this.removes.has(tag)) return true;
    }
    return false;
  }

  /**
   * Applies a replicated add. Idempotent for duplicate tags.
   *
   * @param {string} elem
   * @param {string} tag
   */
  applyAdd(elem, tag) {
    this.add(elem, tag);
  }

  /**
   * Applies a replicated remove of specific observed tags.
   *
   * @param {string[]} tags
   */
  applyRemove(tags) {
    for (const tag of tags) this.removes.add(tag);
  }

  /**
   * All present elements in insertion order.
   *
   * @returns {string[]}
   */
  members() {
    const out = [];
    for (const [elem, tags] of this.adds) {
      for (const tag of tags) {
        if (!this.removes.has(tag)) {
          out.push(elem);
          break;
        }
      }
    }
    return out;
  }

  /**
   * Merges a peer's state. Converges regardless of call order.
   *
   * @param {ORSet} other
   */
  merge(other) {
    for (const [elem, tags] of other.adds) {
      for (const tag of tags) this.applyAdd(elem, tag);
    }
    for (const tag of other.removes) this.removes.add(tag);
  }

  /**
   * Serializable snapshot for anti-entropy exchange.
   *
   * @returns {{ adds: Record<string, string[]>, removes: string[] }}
   */
  toJSON() {
    const adds = {};
    for (const [elem, tags] of this.adds) adds[elem] = [...tags];
    return { adds, removes: [...this.removes] };
  }

  /**
   * Restores from a snapshot produced by {@link ORSet#toJSON}.
   *
   * @param {{ adds?: Record<string, string[]>, removes?: string[] }} snap
   * @returns {ORSet}
   */
  static fromJSON(snap) {
    const set = new ORSet();
    for (const [elem, tags] of Object.entries(snap?.adds ?? {})) {
      for (const tag of tags) set.add(elem, tag);
    }
    set.applyRemove(snap?.removes ?? []);
    return set;
  }
}

// ─── RGA sequence ─────────────────────────────────────────────────────────────

/**
 * Deterministic total order over message identifiers. Identifiers encode
 * `{ hlcTimestamp, regionId, localSeq }`; sorting them yields the same order
 * in every region.
 *
 * @param {string} a Message id.
 * @param {string} b Message id.
 * @returns {number}
 */
export function compareMessageIds(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Builds a globally-sortable id from an HLC stamp, originating region and
 * per-region sequence number. Fixed-width numeric fields keep lexicographic
 * order equal to numeric order.
 *
 * @param {HLCTimestamp} stamp
 * @param {string} regionId
 * @param {number} localSeq
 * @returns {string}
 */
export function makeMessageId(stamp, regionId, localSeq) {
  const l = String(stamp.l).padStart(15, "0");
  const c = String(stamp.c).padStart(6, "0");
  const seq = String(localSeq).padStart(12, "0");
  return `${l}:${c}:${regionId}:${seq}`;
}

/**
 * Replicated Growable Array for room message ordering.
 *
 * Nodes are inserted after an explicit predecessor (`originLeft`, `null`
 * meaning "after the virtual head"). Children of the same predecessor are
 * ordered by their message id, which is identical everywhere, so two regions
 * inserting at "position 5" at the same time both appear and interleave in one
 * deterministic order. Removal tombstones instead of deleting so concurrent
 * inserts anchored on a removed node still converge.
 */
export class RGASequence {
  constructor() {
    /** id → node. @private */
    this.nodes = new Map();
  }

  /**
   * Inserts a node after `originLeft`.
   *
   * @param {string} id Globally unique message id (see {@link makeMessageId}).
   * @param {string|null} originLeft Predecessor id or `null` for head.
   * @param {*} value Application payload stored on the node.
   * @param {boolean} [tombstoned] Insert already removed (replay case).
   * @returns {{ id: string, originLeft: string|null, value: *, tombstoned: boolean }}
   */
  insert(id, originLeft, value, tombstoned = false) {
    const existing = this.nodes.get(id);
    if (existing) {
      existing.tombstoned = existing.tombstoned || tombstoned;
      return existing;
    }
    const node = { id, originLeft: originLeft ?? null, value, tombstoned };
    this.nodes.set(id, node);
    return node;
  }

  /**
   * Tombstones a node while keeping it as an anchor for descendants.
   *
   * @param {string} id
   * @returns {boolean} Whether the node existed.
   */
  remove(id) {
    const node = this.nodes.get(id);
    if (!node) return false;
    node.tombstoned = true;
    return true;
  }

  /**
   * @param {string} id
   * @returns {boolean} Whether a visible (non-tombstoned) node exists.
   */
  has(id) {
    const node = this.nodes.get(id);
    return Boolean(node && !node.tombstoned);
  }

  /**
   * @param {string} id
   * @returns {*} The payload of the node, visible or not.
   */
  get(id) {
    return this.nodes.get(id)?.value;
  }

  /**
   * Visible nodes in the converged global order.
   *
   * @returns {Array<{ id: string, value: *, index: number }>}
   */
  toArray() {
    const out = [];
    for (const node of this.traverse()) {
      if (!node.tombstoned) {
        out.push({ id: node.id, value: node.value, index: out.length });
      }
    }
    return out;
  }

  /**
   * Zero-based global position of a visible node, or `-1` when unknown/hidden.
   *
   * @param {string} id
   * @returns {number}
   */
  indexOf(id) {
    let index = 0;
    for (const node of this.traverse()) {
      if (node.tombstoned) continue;
      if (node.id === id) return index;
      index += 1;
    }
    return -1;
  }

  /**
   * Id of the last visible node, used as the default insertion origin so
   * broadcasts append at the end of the global order.
   *
   * @returns {string|null}
   */
  lastVisibleId() {
    const items = this.toArray();
    return items.length > 0 ? items[items.length - 1].id : null;
  }

  /**
   * Deterministic DFS traversal: children of each node sorted by message id.
   *
   * @yields {{ id: string, originLeft: string|null, value: *, tombstoned: boolean }}
   */
  *traverse() {
    const children = new Map();
    for (const node of this.nodes.values()) {
      const key = node.originLeft ?? "\u0000head";
      let bucket = children.get(key);
      if (!bucket) {
        bucket = [];
        children.set(key, bucket);
      }
      bucket.push(node);
    }
    for (const bucket of children.values()) {
      bucket.sort((a, b) => compareMessageIds(a.id, b.id));
    }
    const stack = [...(children.get("\u0000head") ?? [])].reverse();
    while (stack.length > 0) {
      const node = stack.pop();
      yield node;
      const next = children.get(node.id);
      if (next) {
        for (let i = next.length - 1; i >= 0; i -= 1) stack.push(next[i]);
      }
    }
  }

  /**
   * Merges a peer's nodes. Grow-only, hence commutative and idempotent.
   *
   * @param {RGASequence} other
   */
  merge(other) {
    for (const node of other.nodes.values()) {
      this.insert(node.id, node.originLeft, node.value, node.tombstoned);
    }
  }

  /**
   * Serializable snapshot for anti-entropy exchange.
   *
   * @returns {{ nodes: Array<{ id: string, originLeft: string|null, value: *, tombstoned: boolean }> }}
   */
  toJSON() {
    return { nodes: [...this.nodes.values()] };
  }

  /**
   * Restores from a snapshot produced by {@link RGASequence#toJSON}.
   *
   * @param {{ nodes?: Array<{ id: string, originLeft: string|null, value: *, tombstoned?: boolean }> }} snap
   * @returns {RGASequence}
   */
  static fromJSON(snap) {
    const rga = new RGASequence();
    for (const node of snap?.nodes ?? []) {
      rga.insert(node.id, node.originLeft, node.value, Boolean(node.tombstoned));
    }
    return rga;
  }
}

// ─── LWW-Map (vector clocks) ──────────────────────────────────────────────────

/**
 * Last-writer-wins map resolved with vector clocks.
 *
 * Writes carry the vector clock at write time. When states merge:
 * - a causally-after clock replaces the older entry;
 * - concurrent clocks conflict → deterministic winner is the entry whose
 *   writer region id sorts higher (counted in metrics as a conflict).
 *
 * Used for client session data: whichever region the client fails over to, the
 * merged map contains the most recent session state.
 */
export class LWWMap {
  constructor() {
    /** key → entry. @private */
    this.entries = new Map();
  }

  /**
   * Records a write locally.
   *
   * @param {string} key
   * @param {*} value
   * @param {VectorClock} clock Vector clock snapshot at write time.
   * @param {string} writer Originating region id.
   */
  set(key, value, clock, writer) {
    this.#apply(key, { value, clock, writer, deleted: false });
  }

  /**
   * Records a delete (the tombstone keeps its clock for ordering).
   *
   * @param {string} key
   * @param {VectorClock} clock
   * @param {string} writer
   */
  delete(key, clock, writer) {
    this.#apply(key, { value: undefined, clock, writer, deleted: true });
  }

  /**
   * @param {string} key
   * @returns {*} Current value, or `undefined` when absent/deleted.
   */
  get(key) {
    const entry = this.entries.get(key);
    return entry && !entry.deleted ? entry.value : undefined;
  }

  /**
   * @param {string} key
   * @returns {boolean}
   */
  has(key) {
    const entry = this.entries.get(key);
    return Boolean(entry && !entry.deleted);
  }

  /**
   * Resolves and applies any incoming entry (local write or remote merge).
   *
   * @param {string} key
   * @param {{ value: *, clock: VectorClock, writer: string, deleted: boolean }} incoming
   * @returns {boolean} Whether the incoming entry won.
   * @private
   */
  #apply(key, incoming) {
    const current = this.entries.get(key);
    if (!current || LWWMap.#winsOver(incoming, current)) {
      this.entries.set(key, { ...incoming });
      return true;
    }
    return false;
  }

  /**
   * Deterministic winner rule between two entries.
   *
   * @param {{ clock: VectorClock, writer: string }} a
   * @param {{ clock: VectorClock, writer: string }} b
   * @returns {boolean} Whether `a` supersedes `b`.
   * @private
   */
  static #winsOver(a, b) {
    const order = vcCompare(a.clock, b.clock);
    if (order === "after") return true;
    if (order === "before" || order === "equal") return false;
    // Concurrent writes: deterministic tie-break on writer region id.
    return a.writer > b.writer;
  }

  /**
   * Merges every entry from a peer map, counting concurrent conflicts.
   *
   * @param {LWWMap} other
   * @param {{ conflicts?: number }} [statsOut] Incremented per conflicting key.
   */
  merge(other, statsOut) {
    for (const [key, entry] of other.entries) {
      const current = this.entries.get(key);
      if (current && vcCompare(entry.clock, current.clock) === "concurrent") {
        if (statsOut) statsOut.conflicts = (statsOut.conflicts ?? 0) + 1;
      }
      this.#apply(key, entry);
    }
  }

  /**
   * Serializable snapshot for anti-entropy exchange.
   *
   * @returns {{ entries: Record<string, { value: *, clock: VectorClock, writer: string, deleted: boolean }> }}
   */
  toJSON() {
    const entries = {};
    for (const [key, entry] of this.entries) entries[key] = entry;
    return { entries };
  }

  /**
   * Restores from a snapshot produced by {@link LWWMap#toJSON}.
   *
   * @param {{ entries?: Record<string, { value: *, clock: VectorClock, writer: string, deleted: boolean }> }} snap
   * @returns {LWWMap}
   */
  static fromJSON(snap) {
    const map = new LWWMap();
    for (const [key, entry] of Object.entries(snap?.entries ?? {})) {
      map.#apply(key, { ...entry });
    }
    return map;
  }
}

// ─── LWW-Element-Set (geofences) ──────────────────────────────────────────────

/**
 * Compares two stamps ({@link HLCTimestamp} plus writer region) totally.
 *
 * @param {{ hlc: HLCTimestamp, regionId: string }} a
 * @param {{ hlc: HLCTimestamp, regionId: string }} b
 * @returns {number}
 */
function compareStamps(a, b) {
  const byHlc = HLC.compare(a.hlc, b.hlc);
  if (byHlc !== 0) return byHlc;
  return a.regionId < b.regionId ? -1 : a.regionId > b.regionId ? 1 : 0;
}

/**
 * Last-update-wins element set used for geofence definitions.
 *
 * Admin edits are rare and effectively serialized, so plain LWW semantics are
 * sufficient: the add/remove carrying the greater stamp wins. Critical fence
 * mutations additionally go through quorum acknowledgement at the coordinator
 * level to prevent split-brain during partitions.
 */
export class LWWElementSet {
  constructor() {
    /** elem → stamp (+ value). @private */
    this.adds = new Map();
    /** elem → stamp. @private */
    this.removes = new Map();
  }

  /**
   * @param {string} elem
   * @param {{ hlc: HLCTimestamp, regionId: string, value?: * }} stamp
   */
  add(elem, stamp) {
    const current = this.adds.get(elem);
    if (!current || compareStamps(stamp, current) > 0) this.adds.set(elem, stamp);
  }

  /**
   * @param {string} elem
   * @param {{ hlc: HLCTimestamp, regionId: string }} stamp
   */
  remove(elem, stamp) {
    const current = this.removes.get(elem);
    if (!current || compareStamps(stamp, current) > 0) this.removes.set(elem, stamp);
  }

  /**
   * Element present iff its best add stamp beats its best remove stamp.
   *
   * @param {string} elem
   * @returns {boolean}
   */
  has(elem) {
    const addStamp = this.adds.get(elem);
    if (!addStamp) return false;
    const removeStamp = this.removes.get(elem);
    if (!removeStamp) return true;
    return compareStamps(addStamp, removeStamp) > 0;
  }

  /**
   * @param {string} elem
   * @returns {*} The winning definition, or `undefined`.
   */
  get(elem) {
    return this.adds.get(elem)?.value;
  }

  /**
   * Merges a peer set keeping the maximal stamp per side.
   *
   * @param {LWWElementSet} other
   */
  merge(other) {
    for (const [elem, stamp] of other.adds) this.add(elem, stamp);
    for (const [elem, stamp] of other.removes) this.remove(elem, stamp);
  }

  /**
   * Serializable snapshot for anti-entropy exchange.
   *
   * @returns {{ adds: Array<[string, object]>, removes: Array<[string, object]> }}
   */
  toJSON() {
    return {
      adds: [...this.adds.entries()],
      removes: [...this.removes.entries()],
    };
  }

  /**
   * Restores from a snapshot produced by {@link LWWElementSet#toJSON}.
   *
   * @param {{ adds?: Array<[string, object]>, removes?: Array<[string, object]> }} snap
   * @returns {LWWElementSet}
   */
  static fromJSON(snap) {
    const set = new LWWElementSet();
    for (const [elem, stamp] of snap?.adds ?? []) set.add(elem, stamp);
    for (const [elem, stamp] of snap?.removes ?? []) set.remove(elem, stamp);
    return set;
  }
}

// ─── Merkle tree ──────────────────────────────────────────────────────────────

/**
 * SHA-256 hex digest of a string.
 *
 * @param {string} data
 * @returns {string}
 */
export function sha256(data) {
  return createHash("sha256").update(data).digest("hex");
}

/**
 * Builds a binary Merkle root over leaf hashes (duplicated when odd).
 * Empty input hashes to the digest of the empty string.
 *
 * @param {string[]} leaves Hex digests, sorted by the caller.
 * @returns {string}
 */
export function merkleRoot(leaves) {
  if (leaves.length === 0) return sha256("");
  let level = [...leaves];
  while (level.length > 1) {
    const next = [];
    for (let i = 0; i < level.length; i += 2) {
      const left = level[i];
      const right = level[i + 1] ?? left;
      next.push(sha256(left + right));
    }
    level = next;
  }
  return level[0];
}

/**
 * Canonical JSON stringify (sorted object keys) so identical CRDT states hash
 * identically regardless of key insertion order.
 *
 * @param {*} value
 * @returns {string}
 */
export function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

// ─── operation protocol ───────────────────────────────────────────────────────

/**
 * Wire format exchanged between regions.
 *
 * @typedef {object} ReplicationOp
 * @property {"membership_add"|"membership_remove"|"message_append"|"message_remove"|
 *            "session_update"|"session_delete"|"geofence_upsert"|"geofence_remove"|
 *            "global_config"} type
 * @property {object} payload Type-specific fields.
 * @property {VectorClock} vectorClock Sender's vector-clock snapshot.
 * @property {string} regionId Originating region.
 * @property {number} timestamp Sender's `Date.now()` at emit time (lag metric).
 * @property {HLCTimestamp} [hlc] Hybrid logical timestamp of the event.
 * @property {string} [residency] Data-residency zone ("eu", "us", … or "global").
 */

// ─── multi-region coordinator ─────────────────────────────────────────────────

/**
 * Coordinates cross-region replication for one region's gateway.
 *
 * Local mutations update the region-local CRDTs immediately and fan out a
 * {@link ReplicationOp} to every eligible peer through the injected transport.
 * Remote operations arrive via {@link MultiRegionCoordinator#handlePeerOperation}.
 * Room membership and messages replicate best-effort (CRDTs resolve conflicts),
 * while geofence definitions and global config use quorum acknowledgement so a
 * partitioned minority cannot mutate critical metadata.
 *
 * The transport interface is deliberately minimal so any dedicated link works:
 *
 * ```txt
 * send(toRegion, op)          → Promise<{ ack: boolean }>   op delivery, resolves on ack
 * fetchState(fromRegion)      → Promise<object>             full CRDT snapshot pull
 * fetchMerkleRoot?(fromRegion)→ Promise<string|null>        optional fast divergence check
 * ```
 */
export class MultiRegionCoordinator {
  /**
   * @param {object} options
   * @param {string} options.regionId This region's identifier (e.g. "us-east-1").
   * @param {string[]} [options.peerRegions] Peer region ids.
   * @param {object} [options.replicationTransport] See class docs.
   * @param {object} [options.crdtRegistry] Optional shared CRDT instances:
   *   `{ rooms?: Map<string, { membership: ORSet, sequence: RGASequence }>,
   *   sessions?: LWWMap, geofences?: LWWElementSet }`.
   * @param {object} [options.config]
   * @param {Record<string, string[]>} [options.config.dataResidency] zone → allowed regions.
   * @param {Record<string, string>} [options.config.roomResidency] roomId → zone.
   * @param {number} [options.config.quorumSize] Overrides the computed majority.
   * @param {number} [options.config.antiEntropyIntervalMs] 0 disables the timer.
   * @param {number} [options.config.lagAlertMs] Lag threshold flagged unhealthy (>5 s default).
   * @param {{ error?: Function }} [options.logger] Optional sink for replication errors.
   */
  constructor({
    regionId,
    peerRegions = [],
    replicationTransport = null,
    crdtRegistry = null,
    config = {},
    logger = null,
  }) {
    if (!regionId || typeof regionId !== "string") {
      throw new Error("MultiRegionCoordinator requires a non-empty regionId");
    }

    /** @type {string} */
    this.regionId = regionId;
    /** @type {string[]} */
    this.peerRegions = [...peerRegions];
    /** @type {object|null} */
    this.replicationTransport = replicationTransport;
    /** @type {object} */
    this.config = config;
    /** @type {{ error?: Function }|null} */
    this.logger = logger;

    /** @private */ this.hlcInstance = new HLC();
    /** @private @type {VectorClock} */
    this.vectorClock = {};
    /** @private */ this.localSeq = 0;

    /** @private @type {Map<string, { membership: ORSet, sequence: RGASequence }>} */
    this.rooms = crdtRegistry?.rooms ?? new Map();
    /** @private @type {LWWMap} */
    this.sessions = crdtRegistry?.sessions ?? new LWWMap();
    /** @private @type {LWWElementSet} */
    this.geofences = crdtRegistry?.geofences ?? new LWWElementSet();

    /** Region ids marked failed (health/failover integration). @private */
    this.failedRegions = new Set();

    /**
     * Runtime metrics (Prometheus-friendly): `replication_lag_ms`,
     * `replication_conflicts_total`, `crdt_merge_duration_ms`.
     *
     * @type {{ replicationLagMs: Record<string, number>, replicationConflictsTotal: number,
     *          crdtMergeDurationMs: { count: number, totalMs: number, maxMs: number },
     *          opsReplicated: number, opsReceived: number, opsFilteredResidency: number,
     *          quorumWritesTotal: number, quorumWriteFailuresTotal: number,
     *          antiEntropy: { runs: number, divergencesDetected: number, peersRepaired: number },
     *          regionHealth: Record<string, boolean> }}
     */
    this.metrics = {
      replicationLagMs: {},
      replicationConflictsTotal: 0,
      crdtMergeDurationMs: { count: 0, totalMs: 0, maxMs: 0 },
      opsReplicated: 0,
      opsReceived: 0,
      opsFilteredResidency: 0,
      quorumWritesTotal: 0,
      quorumWriteFailuresTotal: 0,
      antiEntropy: { runs: 0, divergencesDetected: 0, peersRepaired: 0 },
      regionHealth: {},
    };

    /** @private */ this.antiEntropyTimer = null;
    const interval = config.antiEntropyIntervalMs ?? DEFAULT_ANTI_ENTROPY_INTERVAL_MS;
    if (interval > 0 && typeof setInterval === "function") {
      this.antiEntropyTimer = setInterval(() => {
        void this.antiEntropy().catch(() => {});
      }, interval);
      if (typeof this.antiEntropyTimer.unref === "function") this.antiEntropyTimer.unref();
    }
  }

  // ── rooms & membership ──

  /**
   * Returns (creating if needed) the CRDT pair for a room.
   *
   * @param {string} roomId
   * @returns {{ membership: ORSet, sequence: RGASequence }}
   */
  ensureRoom(roomId) {
    let room = this.rooms.get(roomId);
    if (!room) {
      room = { membership: new ORSet(), sequence: new RGASequence() };
      this.rooms.set(roomId, room);
    }
    return room;
  }

  /**
   * Joins a client to a room locally and replicates the add to peers.
   *
   * @param {string} clientId
   * @param {string} roomId
   * @param {{ residency?: string }} [opts] Residency zone override.
   * @returns {{ tag: string }}
   */
  joinRoom(clientId, roomId, opts = {}) {
    const room = this.ensureRoom(roomId);
    const tag = `${this.regionId}:${process.hrtime.bigint()}:${Math.random().toString(36).slice(2, 10)}`;
    room.membership.add(clientId, tag);
    this.replicate({
      type: "membership_add",
      payload: { roomId, clientId, tag },
      residency: opts.residency ?? this.roomResidencyZone(roomId),
    });
    return { tag };
  }

  /**
   * Removes a client from a room locally (observed tags only) and replicates.
   * Concurrent joins elsewhere win because they carry unseen tags.
   *
   * @param {string} clientId
   * @param {string} roomId
   * @param {{ residency?: string }} [opts]
   * @returns {{ removed: boolean, tags: string[] }}
   */
  leaveRoom(clientId, roomId, opts = {}) {
    const room = this.ensureRoom(roomId);
    const tags = room.membership.remove(clientId);
    if (tags.length > 0) {
      this.replicate({
        type: "membership_remove",
        payload: { roomId, clientId, tags },
        residency: opts.residency ?? this.roomResidencyZone(roomId),
      });
    }
    return { removed: tags.length > 0, tags };
  }

  /**
   * Present members of a room. Because membership operations and anti-entropy
   * repairs are folded straight into the local OR-Set, this view already
   * reflects the merged (read-repaired) state.
   *
   * @param {string} roomId
   * @returns {string[]}
   */
  getRoomMembers(roomId) {
    return this.ensureRoom(roomId).membership.members();
  }

  /**
   * Publishes a message to a room: assigns the causal HLC stamp, appends to the
   * room's RGA (one global position everywhere) and replicates the operation.
   *
   * @param {string} roomId
   * @param {*} message JSON-safe payload.
   * @param {{ residency?: string, originLeft?: string|null }} [opts]
   * @returns {{ id: string, hlc: HLCTimestamp, localSeq: number, originLeft: string|null }}
   */
  broadcast(roomId, message, opts = {}) {
    const room = this.ensureRoom(roomId);
    const stamp = this.hlcInstance.now();
    this.localSeq += 1;
    const id = makeMessageId(stamp, this.regionId, this.localSeq);
    const originLeft =
      opts.originLeft !== undefined ? opts.originLeft : room.sequence.lastVisibleId();
    room.sequence.insert(id, originLeft, message);
    this.bumpVectorClock();
    this.replicate({
      type: "message_append",
      payload: { roomId, id, originLeft, value: message },
      hlc: stamp,
      residency: opts.residency ?? this.roomResidencyZone(roomId),
    });
    return { id, hlc: stamp, localSeq: this.localSeq, originLeft };
  }

  // ── session state (failover support) ──

  /**
   * Upserts client session state (rooms, ack cursors, rate-limit windows…).
   *
   * @param {string} clientId
   * @param {*} state JSON-safe session blob.
   * @param {{ residency?: string }} [opts]
   * @returns {{ key: string, clock: VectorClock }}
   */
  saveSession(clientId, state, opts = {}) {
    const clock = this.bumpVectorClock();
    const key = `session:${clientId}`;
    this.sessions.set(key, state, clock, this.regionId);
    this.replicate({
      type: "session_update",
      payload: { key, value: state },
      residency: opts.residency ?? RESIDENCY_GLOBAL,
    });
    return { key, clock };
  }

  /**
   * Full failover read for a client: everything the new region needs to resume
   * without further round-trips — the session blob plus room memberships
   * replicated from the failed region.
   *
   * @param {string} clientId
   * @returns {{ clientId: string, session: *, memberships: string[], restoredAt: number }}
   */
  restoreSession(clientId) {
    const memberships = [];
    for (const [roomId, room] of this.rooms) {
      if (room.membership.has(clientId)) memberships.push(roomId);
    }
    return {
      clientId,
      session: this.sessions.get(`session:${clientId}`),
      memberships,
      restoredAt: Date.now(),
    };
  }

  // ── geofences & global config (quorum writes) ──

  /**
   * Quorum-replicated geofence upsert. Requires `quorumSize` acknowledgements
   * (self included) before reporting committed, preventing split-brain edits
   * of critical metadata during partitions.
   *
   * @param {{ id: string, [key: string]: * }} fence Fence definition including `id`.
   * @returns {Promise<{ committed: boolean, acks: number, quorum: number }>}
   */
  async syncGeofence(fence) {
    const stamp = { hlc: this.hlcInstance.now(), regionId: this.regionId };
    this.geofences.add(fence.id, { ...stamp, value: fence });
    this.bumpVectorClock();
    return this.quorumReplicate({
      type: "geofence_upsert",
      payload: { fenceId: fence.id, definition: fence, stamp },
      hlc: stamp.hlc,
      residency: RESIDENCY_GLOBAL,
    });
  }

  /**
   * Quorum-replicated geofence removal.
   *
   * @param {string} fenceId
   * @returns {Promise<{ committed: boolean, acks: number, quorum: number }>}
   */
  async removeGeofence(fenceId) {
    const stamp = { hlc: this.hlcInstance.now(), regionId: this.regionId };
    this.geofences.remove(fenceId, stamp);
    this.bumpVectorClock();
    return this.quorumReplicate({
      type: "geofence_remove",
      payload: { fenceId, stamp },
      hlc: stamp.hlc,
      residency: RESIDENCY_GLOBAL,
    });
  }

  /**
   * Quorum-replicated mutation of global config (rate limits, room limits…)
   * for admin-server operations that must not diverge across regions.
   *
   * @param {string} key Config name (e.g. "rate-limit:messages-per-min").
   * @param {*} value New value.
   * @returns {Promise<{ committed: boolean, acks: number, quorum: number }>}
   */
  async setGlobalConfig(key, value) {
    const clock = this.bumpVectorClock();
    const storageKey = `global-config:${key}`;
    this.sessions.set(storageKey, value, clock, this.regionId);
    return this.quorumReplicate({
      type: "global_config",
      payload: { key: storageKey, value },
      residency: RESIDENCY_GLOBAL,
    });
  }

  /**
   * Reads a global config value previously written via {@link setGlobalConfig}.
   *
   * @param {string} key
   * @returns {*}
   */
  getGlobalConfig(key) {
    return this.sessions.get(`global-config:${key}`);
  }

  /**
   * Live geofence definitions known locally.
   *
   * @returns {Array<{ id: string, definition: * }>}
   */
  listGeofences() {
    const out = [];
    for (const [fenceId] of this.geofences.adds) {
      if (this.geofences.has(fenceId)) {
        out.push({ id: fenceId, definition: this.geofences.get(fenceId) });
      }
    }
    return out;
  }

  // ── replication plumbing ──

  /**
   * Increments and returns this region's vector-clock entry.
   *
   * @returns {VectorClock} Post-increment snapshot.
   * @private
   */
  bumpVectorClock() {
    this.vectorClock = vcIncrement(this.vectorClock, this.regionId);
    return this.vectorClock;
  }

  /**
   * Residency zone governing a room (config lookup, defaulting to "global").
   *
   * @param {string} roomId
   * @returns {string}
   */
  roomResidencyZone(roomId) {
    return this.config.roomResidency?.[roomId] ?? RESIDENCY_GLOBAL;
  }

  /**
   * Whether `regionId` may receive data classified under `zone`.
   *
   * @param {string} regionId
   * @param {string} zone
   * @returns {boolean}
   */
  canReplicateTo(regionId, zone) {
    const policy = this.config.dataResidency?.[zone];
    if (!policy) return true;
    return policy.includes(regionId);
  }

  /**
   * Fans a best-effort operation out to all healthy peers permitted by data
   * residency. Delivery failures are swallowed — CRDT convergence is restored
   * later by anti-entropy.
   *
   * @param {Omit<ReplicationOp, "vectorClock"|"regionId"|"timestamp">} partial
   * @returns {void}
   * @private
   */
  replicate(partial) {
    if (!this.replicationTransport) return;
    const op = /** @type {ReplicationOp} */ ({
      ...partial,
      vectorClock: this.vectorClock,
      regionId: this.regionId,
      timestamp: Date.now(),
    });
    for (const peer of this.peerRegions) {
      if (this.failedRegions.has(peer)) continue;
      if (!this.canReplicateTo(peer, op.residency ?? RESIDENCY_GLOBAL)) {
        this.metrics.opsFilteredResidency += 1;
        continue;
      }
      this.metrics.opsReplicated += 1;
      void Promise.resolve(this.replicationTransport.send(peer, op)).catch((err) => {
        this.logger?.error?.(`replication to ${peer} failed: ${err?.message ?? err}`);
      });
    }
  }

  /**
   * Sends `op` to every healthy peer and awaits acknowledgements for a quorum
   * decision. Self always counts as one ack (the write is applied locally).
   *
   * @param {Omit<ReplicationOp, "vectorClock"|"regionId"|"timestamp">} partial
   * @returns {Promise<{ committed: boolean, acks: number, quorum: number }>}
   * @private
   */
  async quorumReplicate(partial) {
    this.metrics.quorumWritesTotal += 1;
    const quorum = this.quorumSize();
    const op = /** @type {ReplicationOp} */ ({
      ...partial,
      vectorClock: this.vectorClock,
      regionId: this.regionId,
      timestamp: Date.now(),
    });

    let acks = 1; // self
    await Promise.all(
      this.peerRegions.map(async (peer) => {
        if (this.failedRegions.has(peer)) return;
        try {
          const result = await this.replicationTransport.send(peer, op);
          if (result?.ack) acks += 1;
        } catch (err) {
          this.logger?.error?.(`quorum send to ${peer} failed: ${err?.message ?? err}`);
        }
      }),
    );

    const committed = acks >= quorum;
    if (!committed) this.metrics.quorumWriteFailuresTotal += 1;
    return { committed, acks, quorum };
  }

  /**
   * Majority over self + peers (2 of 3 regions), or the configured override.
   *
   * @returns {number}
   */
  quorumSize() {
    if (this.config.quorumSize) return this.config.quorumSize;
    return Math.floor((this.peerRegions.length + 1) / 2) + 1;
  }

  /**
   * Applies a remote operation to the local CRDTs. Idempotent: replays and
   * out-of-order delivery are absorbed by CRDT merge rules. Also folds the
   * sender's vector clock into ours and advances the HLC so future local
   * events stay causally after everything observed so far.
   *
   * @param {ReplicationOp} op
   * @returns {{ applied: boolean, type: string }}
   */
  handlePeerOperation(op) {
    if (!op || typeof op.type !== "string") return { applied: false, type: "invalid" };

    // Replication lag metric: receive time minus sender emit time.
    if (op.regionId && typeof op.timestamp === "number") {
      const lag = Date.now() - op.timestamp;
      const key = `${op.regionId}->${this.regionId}`;
      this.metrics.replicationLagMs[key] = Math.max(
        this.metrics.replicationLagMs[key] ?? 0,
        lag,
      );
    }

    if (op.hlc) this.hlcInstance.receive(op.hlc);
    if (op.vectorClock) this.vectorClock = vcMerge(this.vectorClock, op.vectorClock);

    const started = process.hrtime.bigint();
    switch (op.type) {
      case "membership_add": {
        const { roomId, clientId, tag } = op.payload;
        this.ensureRoom(roomId).membership.applyAdd(clientId, tag);
        break;
      }
      case "membership_remove": {
        const { roomId, tags } = op.payload;
        this.ensureRoom(roomId).membership.applyRemove(tags ?? []);
        break;
      }
      case "message_append": {
        const { roomId, id, originLeft, value } = op.payload;
        this.ensureRoom(roomId).sequence.insert(id, originLeft, value);
        break;
      }
      case "message_remove": {
        const { roomId, id } = op.payload;
        this.ensureRoom(roomId).sequence.remove(id);
        break;
      }
      case "session_update": {
        const { key, value } = op.payload;
        this.sessions.set(key, value, op.vectorClock ?? {}, op.regionId);
        break;
      }
      case "session_delete": {
        const { key } = op.payload;
        this.sessions.delete(key, op.vectorClock ?? {}, op.regionId);
        break;
      }
      case "geofence_upsert": {
        const { fenceId, definition, stamp } = op.payload;
        this.geofences.add(fenceId, { ...stamp, value: definition });
        break;
      }
      case "geofence_remove": {
        const { fenceId, stamp } = op.payload;
        this.geofences.remove(fenceId, stamp);
        break;
      }
      case "global_config": {
        const { key, value } = op.payload;
        this.sessions.set(key, value, op.vectorClock ?? {}, op.regionId);
        break;
      }
      default:
        return { applied: false, type: op.type };
    }

    this.recordMergeDuration(started);
    this.metrics.opsReceived += 1;
    return { applied: true, type: op.type };
  }

  /**
   * Records `crdt_merge_duration_ms` for an apply/merge section.
   *
   * @param {bigint} startedNanos `process.hrtime.bigint()` captured at start.
   * @returns {void}
   * @private
   */
  recordMergeDuration(startedNanos) {
    const ms = Number(process.hrtime.bigint() - startedNanos) / 1e6;
    const bucket = this.metrics.crdtMergeDurationMs;
    bucket.count += 1;
    bucket.totalMs += ms;
    bucket.maxMs = Math.max(bucket.maxMs, ms);
  }

  // ── anti-entropy ──

  /**
   * Computes this region's Merkle root over all rooms' CRDT state (leaves are
   * canonical-JSON state hashes sorted by room id).
   *
   * @returns {string}
   */
  merkleRootForRooms() {
    const leaves = [...this.rooms.keys()]
      .sort()
      .map((roomId) => {
        const room = this.rooms.get(roomId);
        return sha256(
          stableStringify({
            membership: room.membership.toJSON(),
            sequence: room.sequence.toJSON(),
          }),
        );
      });
    return merkleRoot(leaves);
  }

  /**
   * Serializable full state for transport-based snapshot pulls.
   *
   * @returns {{ regionId: string, rooms: Record<string, { membership: object, sequence: object }>,
   *            sessions: object, geofences: object, vectorClock: VectorClock }}
   */
  serializeState() {
    const rooms = {};
    for (const [roomId, room] of this.rooms) {
      rooms[roomId] = {
        membership: room.membership.toJSON(),
        sequence: room.sequence.toJSON(),
      };
    }
    return {
      regionId: this.regionId,
      rooms,
      sessions: this.sessions.toJSON(),
      geofences: this.geofences.toJSON(),
      vectorClock: this.vectorClock,
    };
  }

  /**
   * Merges a peer snapshot into the local CRDTs (read repair).
   *
   * @param {{ rooms?: Record<string, { membership: object, sequence: object }>,
   *          sessions?: object, geofences?: object, vectorClock?: VectorClock }} snap
   * @returns {boolean} Whether anything changed locally.
   */
  mergeRemoteState(snap) {
    const started = process.hrtime.bigint();
    const before = stableStringify(this.serializeState());
    let changed = false;

    for (const [roomId, roomSnap] of Object.entries(snap?.rooms ?? {})) {
      const room = this.ensureRoom(roomId);
      const membershipBefore = stableStringify(room.membership.toJSON());
      const sequenceBefore = stableStringify(room.sequence.toJSON());
      room.membership.merge(ORSet.fromJSON(roomSnap.membership));
      room.sequence.merge(RGASequence.fromJSON(roomSnap.sequence));
      if (
        stableStringify(room.membership.toJSON()) !== membershipBefore ||
        stableStringify(room.sequence.toJSON()) !== sequenceBefore
      ) {
        changed = true;
      }
    }

    const stats = { conflicts: 0 };
    this.sessions.merge(LWWMap.fromJSON(snap?.sessions ?? {}), stats);
    this.geofences.merge(LWWElementSet.fromJSON(snap?.geofences ?? {}));
    if (stats.conflicts > 0) this.metrics.replicationConflictsTotal += stats.conflicts;
    if (snap?.vectorClock) this.vectorClock = vcMerge(this.vectorClock, snap.vectorClock);

    if (stableStringify(this.serializeState()) !== before) changed = true;
    this.recordMergeDuration(started);
    return changed;
  }

  /**
   * One anti-entropy round against every healthy peer: compares Merkle roots of
   * room state and pulls/merges a full snapshot when they diverge.
   *
   * @returns {Promise<{ peersChecked: number, peersRepaired: number }>}
   */
  async antiEntropy() {
    this.metrics.antiEntropy.runs += 1;
    if (!this.replicationTransport) return { peersChecked: 0, peersRepaired: 0 };

    let peersChecked = 0;
    let peersRepaired = 0;

    for (const peer of this.peerRegions) {
      if (this.failedRegions.has(peer)) continue;
      let diverged = true;
      try {
        const remoteRoot = await this.replicationTransport.fetchMerkleRoot?.(peer);
        if (remoteRoot != null) diverged = remoteRoot !== this.merkleRootForRooms();
      } catch (err) {
        this.markRegionFailed(peer, `anti-entropy probe failed: ${err?.message ?? err}`);
        continue;
      }
      peersChecked += 1;
      if (!diverged) continue;

      // Divergence detected (or peer did not expose roots): pull and repair.
      this.metrics.antiEntropy.divergencesDetected += 1;
      try {
        const snap = await this.replicationTransport.fetchState(peer);
        if (snap && this.mergeRemoteState(snap)) {
          peersRepaired += 1;
          this.metrics.antiEntropy.peersRepaired += 1;
        }
      } catch (err) {
        this.logger?.error?.(`anti-entropy fetchState(${peer}) failed: ${err?.message ?? err}`);
      }
    }

    return { peersChecked, peersRepaired };
  }

  // ── region health / failover ──

  /**
   * Marks a region failed (health check or lag alert): excluded from further
   * replication targets until recovered.
   *
   * @param {string} regionId
   * @param {string} [reason]
   * @returns {void}
   */
  markRegionFailed(regionId, reason) {
    this.failedRegions.add(regionId);
    this.metrics.regionHealth[regionId] = false;
    if (reason) this.logger?.error?.(`region ${regionId} marked failed: ${reason}`);
  }

  /**
   * Marks a region recovered and eligible for replication again.
   *
   * @param {string} regionId
   * @returns {void}
   */
  markRegionHealthy(regionId) {
    this.failedRegions.delete(regionId);
    this.metrics.regionHealth[regionId] = true;
  }

  /**
   * @param {string} regionId
   * @returns {boolean} Whether the region accepts traffic/replication.
   */
  isRegionHealthy(regionId) {
    if (regionId === this.regionId) return !this.config.selfDisabled;
    if (this.failedRegions.has(regionId)) return false;
    const lagKey = `${regionId}->${this.regionId}`;
    if ((this.metrics.replicationLagMs[lagKey] ?? 0) > this.lagAlertMs()) return false;
    return this.metrics.regionHealth[regionId] !== false;
  }

  /**
   * Ordered failover targets: healthy peers sorted by lowest known lag.
   *
   * @returns {string[]}
   */
  failoverTargets() {
    return this.peerRegions
      .filter((peer) => this.isRegionHealthy(peer))
      .sort((a, b) => this.knownLagFrom(a) - this.knownLagFrom(b));
  }

  /**
   * Known inbound lag contributed by a region (0 when never measured).
   *
   * @param {string} regionId
   * @returns {number}
   * @private
   */
  knownLagFrom(regionId) {
    return this.metrics.replicationLagMs[`${regionId}->${this.regionId}`] ?? 0;
  }

  /**
   * Lag alert threshold (default 5 s per issue spec).
   *
   * @returns {number}
   */
  lagAlertMs() {
    return this.config.lagAlertMs ?? DEFAULT_LAG_ALERT_MS;
  }

  /**
   * True when any recorded inbound lag exceeds the alert threshold.
   *
   * @returns {boolean}
   */
  isLagAlertActive() {
    return Object.values(this.metrics.replicationLagMs).some(
      (lag) => lag > this.lagAlertMs(),
    );
  }

  // ── lifecycle ──

  /** Stops the periodic anti-entropy timer. */
  close() {
    if (this.antiEntropyTimer) {
      clearInterval(this.antiEntropyTimer);
      this.antiEntropyTimer = null;
    }
  }
}

export default MultiRegionCoordinator;
