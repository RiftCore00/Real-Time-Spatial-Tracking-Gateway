/**
 * @fileoverview Encrypted session state for connection migration and resumption.
 *
 * A client's session state is serialised, deflated and sealed with AES-256-GCM.
 * The resulting blob IS the `session_id` the client presents on reconnect, so a
 * different gateway instance can restore room memberships, sequence numbers,
 * geofence state and rate-limit windows without extra round-trips.
 *
 * Storage holds the same blob under `session:<clientId>` with a sliding TTL.
 * A blob only resumes while its storage entry is still live, which makes
 * `delete()` and TTL expiry authoritative even though the blob is self-contained.
 *
 * Redis is optional: without it the manager keeps an in-memory map with expiry
 * timestamps, so single-instance deployments need no extra infrastructure.
 */

import { createCipheriv, createDecipheriv, hkdfSync, randomBytes } from "node:crypto";
import { deflateSync, inflateSync } from "node:zlib";
import { logger as defaultLogger } from "./logger.js";

const ALGORITHM = "aes-256-gcm";
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;
const HKDF_INFO = "session-key";
const MAX_BLOB_BYTES = 16384;
const KEY_PREFIX = "session:";
const DEFAULT_TTL_MS = 3600000;
const DEFAULT_DEBOUNCE_MS = 500;
const MIN_SWEEP_MS = 1000;
const MAX_SWEEP_MS = 60000;

/**
 * @typedef {object} SessionRoomState
 * @property {string} roomId
 * @property {number} highestAckedSeq - Highest sequence number the client ACKed.
 * @property {number} highestReceivedSeq - Highest sequence number sent to the client.
 * @property {string[]} geofenceInsideSet - Fence IDs the client is currently inside.
 */

/**
 * Session state is opaque to this class: it is serialised as-is and never
 * validated beyond needing a `clientId` to locate the storage entry.
 *
 * @typedef {object} SessionState
 * @property {string} clientId
 * @property {number} [protocolVersion]
 * @property {object} [authIdentity] - JWT claims or mTLS device identity.
 * @property {SessionRoomState[]} [rooms]
 * @property {{ messageWindow?: number[], connectionWindow?: number[] }} [rateLimitState]
 * @property {{ ip?: string, userAgent?: string, connectedAt?: number, lastActivityAt?: number }} [metadata]
 */

/**
 * Minimal subset of the node-redis v4 client used by this module.
 *
 * @typedef {object} SessionStore
 * @property {(key: string, value: string, opts: { PX: number }) => Promise<unknown>} set
 * @property {(key: string) => Promise<string|null>} get
 * @property {(key: string) => Promise<unknown>} del
 */

/** @typedef {"success"|"decrypt_failed"|"expired"|"mismatch"|"new_session"} ResumptionResult */

/**
 * Marks a timer as non-blocking so a pending save never holds the event loop
 * open. Fake timers may omit `unref`, hence the guard.
 *
 * @param {any} timer
 * @returns {any} The same timer.
 */
function unrefTimer(timer) {
  if (timer && typeof timer.unref === "function") timer.unref();
  return timer;
}

/**
 * Decodes one base64 master key and stretches it into an AES-256 key.
 *
 * @param {string} keyId - Key name, used only in error messages.
 * @param {unknown} value - Base64-encoded 32-byte master key.
 * @returns {Buffer} 32-byte derived key.
 */
function deriveKey(keyId, value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`encryptionKey["${keyId}"] must be a non-empty base64 string`);
  }
  const master = Buffer.from(value, "base64");
  if (master.length !== KEY_BYTES) {
    throw new RangeError(
      `encryptionKey["${keyId}"] must decode to ${KEY_BYTES} bytes, got ${master.length}`
    );
  }
  return Buffer.from(hkdfSync("sha256", master, "", HKDF_INFO, KEY_BYTES));
}

/**
 * Normalises the `encryptionKey` option into a `keyId → derived key` map.
 * Accepts a bare base64 key (bound to `keyId`), a JSON string, or an object.
 *
 * @param {unknown} encryptionKey
 * @param {string} keyId - Key used to seal new blobs; must exist in the result.
 * @returns {Map<string, Buffer>}
 */
function parseKeyMap(encryptionKey, keyId) {
  if (encryptionKey == null) {
    throw new TypeError(
      "encryptionKey is required: a 32-byte base64 key, or a { keyId: base64key } map"
    );
  }

  let source = encryptionKey;
  if (typeof source === "string") {
    const trimmed = source.trim();
    if (trimmed.length === 0) throw new TypeError("encryptionKey must not be empty");
    if (trimmed.startsWith("{")) {
      try {
        source = JSON.parse(trimmed);
      } catch (err) {
        throw new TypeError(`encryptionKey looks like JSON but does not parse: ${err.message}`);
      }
    } else {
      source = { [keyId]: trimmed };
    }
  }

  if (typeof source !== "object" || Array.isArray(source)) {
    throw new TypeError("encryptionKey must be a base64 string, a JSON string, or an object");
  }

  const entries = Object.entries(source);
  if (entries.length === 0) throw new TypeError("encryptionKey holds no keys");

  const keys = new Map();
  for (const [id, value] of entries) keys.set(id, deriveKey(id, value));

  if (!keys.has(keyId)) {
    throw new RangeError(
      `keyId "${keyId}" is absent from encryptionKey (have: ${[...keys.keys()].join(", ")})`
    );
  }
  return keys;
}

/**
 * In-memory stand-in for Redis with per-entry expiry timestamps.
 *
 * @implements {SessionStore}
 */
class MemoryStore {
  constructor() {
    /** @type {Map<string, { value: string, expiresAt: number }>} */
    this._entries = new Map();
  }

  /**
   * @param {string} key
   * @param {string} value
   * @param {{ PX: number }} opts - Millisecond TTL, matching node-redis.
   */
  async set(key, value, { PX }) {
    this._entries.set(key, { value, expiresAt: Date.now() + PX });
  }

  /**
   * @param {string} key
   * @returns {Promise<string|null>} Null when absent or expired.
   */
  async get(key) {
    const entry = this._entries.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= Date.now()) {
      this._entries.delete(key);
      return null;
    }
    return entry.value;
  }

  /** @param {string} key */
  async del(key) {
    this._entries.delete(key);
  }

  /** Drops every expired entry so idle sessions do not leak. */
  sweep() {
    const now = Date.now();
    for (const [key, entry] of this._entries) {
      if (entry.expiresAt <= now) this._entries.delete(key);
    }
  }

  /** @type {number} */
  get size() {
    return this._entries.size;
  }
}

/**
 * Seals, stores and restores per-client session state.
 */
export class SessionManager {
  /**
   * @param {object} options
   * @param {SessionStore|null} [options.redis] - node-redis v4 style client.
   *   Omit for the in-memory fallback.
   * @param {string|object} options.encryptionKey - 32-byte base64 key, or a
   *   `{ keyId: base64key }` map (object or JSON string) for key rotation.
   * @param {number} [options.ttlMs=3600000] - Sliding TTL of a stored session.
   * @param {string} [options.keyId="v1"] - Key that seals new blobs.
   * @param {number} [options.debounceMs=500] - Debounce window per client.
   * @param {{ error: Function }} [options.logger] - Sink for background failures.
   */
  constructor({
    redis = null,
    encryptionKey,
    ttlMs = DEFAULT_TTL_MS,
    keyId = "v1",
    debounceMs = DEFAULT_DEBOUNCE_MS,
    logger = defaultLogger,
  } = {}) {
    if (typeof keyId !== "string" || keyId.length === 0) {
      throw new TypeError("keyId must be a non-empty string");
    }
    if (keyId.includes(".")) {
      throw new TypeError('keyId must not contain "." — it separates blob fields');
    }
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new RangeError("ttlMs must be a positive number of milliseconds");
    }
    if (!Number.isFinite(debounceMs) || debounceMs < 0) {
      throw new RangeError("debounceMs must be a non-negative number of milliseconds");
    }

    this._keys = parseKeyMap(encryptionKey, keyId);
    this._keyId = keyId;
    this._ttlMs = ttlMs;
    this._debounceMs = debounceMs;
    this._logger = logger;

    this._redis = redis ?? null;
    this._memory = this._redis ? null : new MemoryStore();
    /** @type {any} */
    this._sweepTimer = null;
    if (this._memory) {
      const period = Math.max(MIN_SWEEP_MS, Math.min(ttlMs, MAX_SWEEP_MS));
      this._sweepTimer = unrefTimer(setInterval(() => this._memory.sweep(), period));
    }

    /** @type {Map<string, any>} clientId → pending save timer */
    this._debounceTimers = new Map();
    /** @type {Map<string, () => SessionState>} clientId → latest state provider */
    this._debounceProviders = new Map();

    this._counters = {
      success: 0,
      decrypt_failed: 0,
      expired: 0,
      mismatch: 0,
      new_session: 0,
    };
    this._stateSizeBytes = 0;
  }

  /**
   * @private
   * @returns {SessionStore}
   */
  _storage() {
    return this._redis ?? this._memory;
  }

  /**
   * Seals `state` and stores it under `session:<clientId>`, refreshing the TTL.
   *
   * @param {string} clientId
   * @param {SessionState} state - Opaque session state; must be serialisable.
   * @returns {Promise<string>} The blob to hand back as the client's `session_id`.
   * @throws {RangeError} When the blob exceeds 16 KB.
   */
  async save(clientId, state) {
    if (typeof clientId !== "string" || clientId.length === 0) {
      throw new TypeError("clientId must be a non-empty string");
    }
    if (state == null || typeof state !== "object") {
      throw new TypeError("state must be an object");
    }

    const compressed = deflateSync(Buffer.from(JSON.stringify(state), "utf8"));
    const iv = randomBytes(IV_BYTES);
    const cipher = createCipheriv(ALGORITHM, this._keys.get(this._keyId), iv);
    const ciphertext = Buffer.concat([cipher.update(compressed), cipher.final()]);
    const blob = [
      this._keyId,
      iv.toString("base64"),
      ciphertext.toString("base64"),
      cipher.getAuthTag().toString("base64"),
    ].join(".");

    this._stateSizeBytes = Buffer.byteLength(blob, "utf8");
    if (this._stateSizeBytes > MAX_BLOB_BYTES) {
      throw new RangeError(
        `session blob for ${clientId} is ${this._stateSizeBytes} bytes, over the ${MAX_BLOB_BYTES} byte limit`
      );
    }

    await this._storage().set(KEY_PREFIX + clientId, blob, { PX: this._ttlMs });
    return blob;
  }

  /**
   * Opens a `session_id` blob and confirms the session is still live.
   *
   * Never throws: every failure (bad format, unknown key, tampering, expiry,
   * explicit delete) returns null and bumps the matching counter. A storage
   * read error counts as `expired` because liveness cannot be proven.
   *
   * @param {string} sessionId - The blob returned by `save()`.
   * @param {{ countSuccess?: boolean }} [options] - Pass `countSuccess: false`
   *   when the caller still has its own checks (e.g. identity binding) and
   *   records the final outcome itself.
   * @returns {Promise<SessionState|null>}
   */
  async load(sessionId, { countSuccess = true } = {}) {
    const state = this._open(sessionId);
    if (!state) {
      this._counters.decrypt_failed++;
      return null;
    }

    let stored;
    try {
      stored = await this._storage().get(KEY_PREFIX + state.clientId);
    } catch (err) {
      this._logger?.error?.("session store read failed", {
        clientId: state.clientId,
        error: err.message,
      });
      this._counters.expired++;
      return null;
    }

    if (stored == null) {
      this._counters.expired++;
      return null;
    }

    if (countSuccess) this._counters.success++;
    return state;
  }

  /**
   * Parses and decrypts a blob. The key named by the blob prefix is tried
   * first, then every other key so blobs sealed before a rotation still open.
   *
   * @private
   * @param {unknown} sessionId
   * @returns {SessionState|null}
   */
  _open(sessionId) {
    if (typeof sessionId !== "string" || sessionId.length === 0) return null;

    const parts = sessionId.split(".");
    if (parts.length !== 4) return null;

    const [blobKeyId, ivB64, ciphertextB64, tagB64] = parts;
    const iv = Buffer.from(ivB64, "base64");
    const ciphertext = Buffer.from(ciphertextB64, "base64");
    const tag = Buffer.from(tagB64, "base64");
    if (iv.length !== IV_BYTES || tag.length !== TAG_BYTES || ciphertext.length === 0) {
      return null;
    }

    const candidates = [];
    if (this._keys.has(blobKeyId)) candidates.push(this._keys.get(blobKeyId));
    for (const [id, key] of this._keys) {
      if (id !== blobKeyId) candidates.push(key);
    }

    for (const key of candidates) {
      try {
        const decipher = createDecipheriv(ALGORITHM, key, iv);
        decipher.setAuthTag(tag);
        const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
        const state = JSON.parse(inflateSync(plaintext).toString("utf8"));
        // An authentic blob without a clientId cannot be matched to storage.
        if (state == null || typeof state !== "object" || typeof state.clientId !== "string") {
          return null;
        }
        return state;
      } catch {
        // Wrong key or corrupt payload — fall through to the next candidate.
      }
    }
    return null;
  }

  /**
   * Coalesces rapid state changes into one save per `debounceMs` per client.
   * `stateProvider` runs when the timer fires, so the newest state is captured.
   *
   * @param {string} clientId
   * @param {() => SessionState} stateProvider
   * @returns {Promise<void>} Resolves once the save is scheduled, not stored.
   */
  async debouncedSave(clientId, stateProvider) {
    if (typeof clientId !== "string" || clientId.length === 0) {
      throw new TypeError("clientId must be a non-empty string");
    }
    if (typeof stateProvider !== "function") {
      throw new TypeError("stateProvider must be a function returning the latest state");
    }

    this._cancelDebounce(clientId);
    this._debounceProviders.set(clientId, stateProvider);
    this._debounceTimers.set(
      clientId,
      unrefTimer(
        setTimeout(() => {
          this._debounceTimers.delete(clientId);
          const provider = this._debounceProviders.get(clientId);
          this._debounceProviders.delete(clientId);
          if (provider) this._runSave(clientId, provider);
        }, this._debounceMs)
      )
    );
  }

  /**
   * Runs a pending save. Failures are logged rather than thrown so a
   * background timer never produces an unhandled rejection.
   *
   * @private
   * @param {string} clientId
   * @param {() => SessionState} stateProvider
   * @returns {Promise<string|null>} The blob, or null on failure.
   */
  _runSave(clientId, stateProvider) {
    let state;
    try {
      state = stateProvider();
    } catch (err) {
      this._logger?.error?.("session stateProvider threw", { clientId, error: err.message });
      return Promise.resolve(null);
    }
    return this.save(clientId, state).catch((err) => {
      this._logger?.error?.("session save failed", { clientId, error: err.message });
      return null;
    });
  }

  /**
   * @private
   * @param {string} clientId
   * @returns {(() => SessionState)|null} The cancelled provider, if any.
   */
  _cancelDebounce(clientId) {
    const timer = this._debounceTimers.get(clientId);
    if (timer) clearTimeout(timer);
    this._debounceTimers.delete(clientId);
    const provider = this._debounceProviders.get(clientId) ?? null;
    this._debounceProviders.delete(clientId);
    return provider;
  }

  /**
   * Fires a client's pending debounced save immediately.
   *
   * @param {string} clientId
   * @returns {Promise<string|null>} The blob, or null when nothing was pending.
   */
  async flush(clientId) {
    if (!this._debounceTimers.has(clientId)) {
      this._cancelDebounce(clientId);
      return null;
    }
    const provider = this._cancelDebounce(clientId);
    return provider ? this._runSave(clientId, provider) : null;
  }

  /**
   * Fires every pending save — call before closing connections on shutdown.
   *
   * @returns {Promise<Array<string|null>>} One blob per flushed client.
   */
  async flushAll() {
    const pending = [...this._debounceTimers.keys()];
    return Promise.all(pending.map((clientId) => this.flush(clientId)));
  }

  /**
   * Drops a client's stored session and cancels any pending save.
   *
   * @param {string} clientId
   * @returns {Promise<void>}
   */
  async delete(clientId) {
    if (typeof clientId !== "string" || clientId.length === 0) {
      throw new TypeError("clientId must be a non-empty string");
    }
    this._cancelDebounce(clientId);
    await this._storage().del(KEY_PREFIX + clientId);
  }

  /**
   * Clears every timer so the process (or a test run) can exit. Idempotent.
   *
   * @returns {Promise<void>}
   */
  async close() {
    for (const clientId of [...this._debounceTimers.keys()]) {
      this._cancelDebounce(clientId);
    }
    if (this._sweepTimer) {
      clearInterval(this._sweepTimer);
      this._sweepTimer = null;
    }
  }

  /**
   * Counts a resumption outcome decided outside this class, such as an
   * identity `mismatch` or a `new_session`.
   *
   * @param {ResumptionResult} result
   */
  recordResumption(result) {
    if (!Object.hasOwn(this._counters, result)) {
      throw new RangeError(
        `unknown resumption result "${result}" (expected: ${Object.keys(this._counters).join("|")})`
      );
    }
    this._counters[result]++;
  }

  /**
   * Snapshot of the issue-18 metrics. Mutating it does not affect the manager.
   *
   * @type {{ session_resumption_total: Record<ResumptionResult, number>, session_state_size_bytes: number }}
   */
  get metrics() {
    return {
      session_resumption_total: { ...this._counters },
      session_state_size_bytes: this._stateSizeBytes,
    };
  }
}
