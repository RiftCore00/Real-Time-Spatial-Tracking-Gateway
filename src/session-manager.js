import crypto from "node:crypto";
import zlib from "node:zlib";

const DEFAULT_TTL_MS = 3600000;
const DEBOUNCE_MS = 500;
const MAX_BLOB_SIZE = 16384;

/**
 * @typedef {Object} SessionRoom
 * @property {string} roomId
 * @property {number} highestAckedSeq
 * @property {number} highestReceivedSeq
 * @property {string[]} geofenceInsideSet
 */

/**
 * @typedef {Object} RateLimitState
 * @property {number[]} messageWindow
 * @property {number[]} connectionWindow
 */

/**
 * @typedef {Object} SessionMetadata
 * @property {string} ip
 * @property {string} userAgent
 * @property {number} connectedAt
 * @property {number} lastActivityAt
 */

/**
 * @typedef {Object} SessionState
 * @property {string} clientId
 * @property {number} protocolVersion
 * @property {Object} authIdentity
 * @property {SessionRoom[]} rooms
 * @property {RateLimitState} rateLimitState
 * @property {SessionMetadata} metadata
 */

/**
 * @typedef {Object} SessionManagerOptions
 * @property {Object} [redis] - Optional Redis client with get/set/del methods
 * @property {string|Object} encryptionKey - Base64 encoded key or { keyId: base64key } map
 * @property {number} [ttlMs] - Session TTL in milliseconds
 * @property {string} [keyId] - Current key identifier for encryption
 * @property {number} [debounceMs] - Debounce interval for saves
 */

/**
 * Derives an AES-256 key from a master key using HKDF.
 *
 * @param {Buffer} masterKey
 * @param {string} info
 * @returns {Buffer}
 */
function deriveKey(masterKey, info) {
  return crypto.hkdfSync("sha256", masterKey, Buffer.alloc(0), info, 32);
}

/**
 * Resolves the raw key bytes for a given key ID.
 *
 * @param {string|Object} encryptionKey
 * @param {string} keyId
 * @returns {Buffer|null}
 */
function resolveKey(encryptionKey, keyId) {
  if (typeof encryptionKey === "string") {
    return Buffer.from(encryptionKey, "base64");
  }
  if (typeof encryptionKey === "object" && encryptionKey[keyId]) {
    return Buffer.from(encryptionKey[keyId], "base64");
  }
  return null;
}

/**
 * Encrypts a session state blob using AES-256-GCM.
 *
 * @param {Buffer} plaintext
 * @param {Buffer} key
 * @param {string} keyId
 * @returns {string} Encrypted blob in format: keyId.base64iv.base64ciphertext.base64tag
 */
function encryptBlob(plaintext, key, keyId) {
  const derivedKey = deriveKey(key, `session-key-${keyId}`);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", derivedKey, iv);
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${keyId}.${iv.toString("base64")}.${ciphertext.toString("base64")}.${tag.toString("base64")}`;
}

/**
 * Decrypts a session state blob.
 *
 * @param {string} blob
 * @param {string|Object} encryptionKey
 * @returns {Buffer|null} Decrypted plaintext or null if decryption fails
 */
function decryptBlob(blob, encryptionKey) {
  const parts = blob.split(".");
  if (parts.length !== 4) return null;

  const [keyId, ivB64, ciphertextB64, tagB64] = parts;
  const key = resolveKey(encryptionKey, keyId);
  if (!key) return null;

  try {
    const derivedKey = deriveKey(key, `session-key-${keyId}`);
    const iv = Buffer.from(ivB64, "base64");
    const ciphertext = Buffer.from(ciphertextB64, "base64");
    const tag = Buffer.from(tagB64, "base64");
    const decipher = crypto.createDecipheriv("aes-256-gcm", derivedKey, iv);
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch {
    return null;
  }
}

/**
 * SessionManager handles encrypted session state persistence for WebSocket
 * connection migration and resumption.
 *
 * Supports both Redis-backed distributed storage and in-memory fallback
 * for single-instance mode.
 *
 * @example
 * const sm = new SessionManager({ encryptionKey: "base64key..." });
 * await sm.save("client-1", { clientId: "client-1", rooms: [] });
 * const state = await sm.load("client-1");
 */
export class SessionManager {
  /** @type {Object|null} */
  #redis;

  /** @type {string|Object} */
  #encryptionKey;

  /** @type {number} */
  #ttlMs;

  /** @type {string} */
  #keyId;

  /** @type {number} */
  #debounceMs;

  /** @type {Map<string, NodeJS.Timeout>} */
  #timers;

  /** @type {Map<string, SessionState>} */
  #pendingStates;

  /** @type {Map<string, { state: SessionState, expiresAt: number }>} */
  #localCache;

  /** @type {NodeJS.Timeout|null} */
  #cleanupInterval;

  /**
   * @param {SessionManagerOptions} options
   */
  constructor({ redis, encryptionKey, ttlMs, keyId, debounceMs } = {}) {
    this.#redis = redis ?? null;
    this.#encryptionKey = encryptionKey ?? process.env.SESSION_ENCRYPTION_KEY ?? "";
    this.#ttlMs = ttlMs ?? DEFAULT_TTL_MS;
    this.#keyId = keyId ?? "v1";
    this.#debounceMs = debounceMs ?? DEBOUNCE_MS;
    this.#timers = new Map();
    this.#pendingStates = new Map();
    this.#localCache = new Map();
    this.#cleanupInterval = null;

    if (!this.#redis) {
      this.#cleanupInterval = setInterval(() => {
        this.#evictExpired();
      }, Math.min(this.#ttlMs, 60000));
      if (this.#cleanupInterval.unref) {
        this.#cleanupInterval.unref();
      }
    }
  }

  /**
   * Removes expired entries from the local in-memory cache.
   * @private
   */
  #evictExpired() {
    const now = Date.now();
    for (const [clientId, entry] of this.#localCache) {
      if (entry.expiresAt <= now) {
        this.#localCache.delete(clientId);
      }
    }
  }

  /**
   * Compresses and encrypts a session state, then stores it.
   *
   * @param {string} clientId
   * @param {SessionState} state
   * @returns {Promise<void>}
   */
  async save(clientId, state) {
    const plaintext = Buffer.from(JSON.stringify(state), "utf8");
    const compressed = zlib.deflateSync(plaintext, { level: 6 });

    if (compressed.length > MAX_BLOB_SIZE) {
      throw new Error(`Session blob exceeds ${MAX_BLOB_SIZE} bytes after compression`);
    }

    const blob = encryptBlob(compressed, this.#resolveEncryptionKey(), this.#keyId);
    const expiresAt = Date.now() + this.#ttlMs;
    const ttlSeconds = Math.ceil(this.#ttlMs / 1000);

    if (this.#redis) {
      await this.#redis.set(`session:${clientId}`, blob, "EX", ttlSeconds);
    } else {
      this.#localCache.set(clientId, { state, expiresAt });
    }
  }

  /**
   * Loads and decrypts a session state by its client ID.
   *
   * @param {string} sessionId - The client ID used as session identifier
   * @returns {Promise<SessionState|null>}
   */
  async load(sessionId) {
    if (this.#redis) {
      const blob = await this.#redis.get(`session:${sessionId}`);
      if (!blob) return null;

      const decrypted = decryptBlob(blob, this.#encryptionKey);
      if (!decrypted) return null;

      try {
        const decompressed = zlib.inflateSync(decrypted);
        const json = JSON.parse(decompressed.toString("utf8"));
        if (json.clientId !== sessionId) return null;
        return json;
      } catch {
        return null;
      }
    }

    const entry = this.#localCache.get(sessionId);
    if (!entry) return null;
    if (entry.expiresAt <= Date.now()) {
      this.#localCache.delete(sessionId);
      return null;
    }
    return entry.state;
  }

  /**
   * Deletes a session from storage.
   *
   * @param {string} clientId
   * @returns {Promise<void>}
   */
  async delete(clientId) {
    this.#clearDebounce(clientId);
    if (this.#redis) {
      await this.#redis.del(`session:${clientId}`);
    } else {
      this.#localCache.delete(clientId);
    }
  }

  /**
   * Saves immediately without debouncing. Used for graceful shutdown.
   *
   * @param {string} clientId
   * @param {SessionState} state
   * @returns {Promise<void>}
   */
  async saveImmediate(clientId, state) {
    this.#clearDebounce(clientId);
    await this.save(clientId, state);
  }

  /**
   * Debounced save that coalesces rapid state changes.
   *
   * @param {string} clientId
   * @param {SessionState} state
   * @returns {void}
   */
  debouncedSave(clientId, state) {
    const existing = this.#timers.get(clientId);
    if (existing) clearTimeout(existing);
    this.#pendingStates.set(clientId, state);
    this.#timers.set(clientId, setTimeout(() => {
      this.#timers.delete(clientId);
      const pending = this.#pendingStates.get(clientId);
      this.#pendingStates.delete(clientId);
      if (pending) {
        this.save(clientId, pending).catch(() => {});
      }
    }, this.#debounceMs));
  }

  /**
   * Clears a pending debounce timer for a client.
   *
   * @param {string} clientId
   * @private
   */
  #clearDebounce(clientId) {
    const timer = this.#timers.get(clientId);
    if (timer) {
      clearTimeout(timer);
      this.#timers.delete(clientId);
    }
    this.#pendingStates.delete(clientId);
  }

  /**
   * Returns the number of pending debounced saves.
   * @returns {number}
   */
  get pendingSaves() {
    return this.#timers.size;
  }

  /**
   * Returns the number of sessions in local cache (single-instance mode).
   * @returns {number}
   */
  get cachedSessions() {
    return this.#localCache.size;
  }

  /**
   * Flushes all pending debounced saves immediately.
   * @returns {Promise<void>}
   */
  async flushPending() {
    const entries = [];
    for (const [clientId, timer] of this.#timers) {
      clearTimeout(timer);
      const pending = this.#pendingStates.get(clientId);
      entries.push({ clientId, state: pending });
    }
    this.#timers.clear();
    this.#pendingStates.clear();
    for (const { clientId, state } of entries) {
      if (state) {
        await this.save(clientId, state).catch(() => {});
      }
    }
  }

  /**
   * Cleans up resources. Clears timers and intervals.
   * @returns {void}
   */
  destroy() {
    for (const timer of this.#timers.values()) {
      clearTimeout(timer);
    }
    this.#timers.clear();
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval);
      this.#cleanupInterval = null;
    }
  }

  /**
   * Resolves the encryption key to a Buffer.
   *
   * @returns {Buffer}
   * @private
   */
  #resolveEncryptionKey() {
    if (typeof this.#encryptionKey === "string" && this.#encryptionKey) {
      return Buffer.from(this.#encryptionKey, "base64");
    }
    if (typeof this.#encryptionKey === "object" && this.#encryptionKey[this.#keyId]) {
      return Buffer.from(this.#encryptionKey[this.#keyId], "base64");
    }
    return crypto.randomBytes(32);
  }
}
