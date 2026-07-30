/**
 * Per-client sliding-window rate limiter.
 *
 * Tracks message timestamps in a 1-second window for each client.
 * Clients that exceed the configured limit have their messages dropped
 * and receive an error frame.
 *
 * Memory is bounded: stale entries are proactively evictable via cleanup()
 * and check() uses batch filter() instead of O(n) shift().
 */

/**
 * Creates a rate limiter that allows at most `maxPerSecond` messages per
 * client per second using a sliding window algorithm.
 *
 * @param {number} [maxPerSecond] - Max messages per second per client.
 *   Defaults to MAX_MESSAGES_PER_SECOND env var, or 100.
 * @returns {{ check: (clientId: string) => boolean, remove: (clientId: string) => void, cleanup: () => void, size: number }}
 */
export function createRateLimiter(maxPerSecond) {
  const envLimit = Number(process.env.MAX_MESSAGES_PER_SECOND) || 100;
  const limit = maxPerSecond ?? envLimit;
  /** @type {Map<string, number[]>} clientId → sorted array of message timestamps (ms) */
  const windows = new Map();

  return {
    /**
     * Returns true if the client is within the rate limit, false if exceeded.
     * Side effect: records the current timestamp for the client.
     *
     * @param {string} clientId
     * @returns {boolean}
     */
    check(clientId) {
      const now = Date.now();
      const cutoff = now - 1000;
      let timestamps = windows.get(clientId);
      if (!timestamps) {
        timestamps = [];
        windows.set(clientId, timestamps);
      }
      // Batch-evict entries outside the 1-second window (O(k) where k = expired entries)
      if (timestamps.length > 0 && timestamps[0] <= cutoff) {
        const filtered = timestamps.filter(t => t > cutoff);
        windows.set(clientId, filtered);
        timestamps = filtered;
      }
      if (timestamps.length >= limit) return false;
      timestamps.push(now);
      return true;
    },

    /**
     * Removes the rate-limit state for a client (call on disconnect).
     *
     * @param {string} clientId
     */
    remove(clientId) {
      windows.delete(clientId);
    },

    /**
     * Iterates all tracked clients and evicts stale entries.
     * Keys with no remaining timestamps within the window are removed entirely.
     * This is safe to call from a periodic timer (e.g. every 10s).
     */
    cleanup() {
      const cutoff = Date.now() - 1000;
      for (const [clientId, timestamps] of windows) {
        const filtered = timestamps.filter(t => t > cutoff);
        if (filtered.length === 0) {
          windows.delete(clientId);
        } else {
          windows.set(clientId, filtered);
        }
      }
    },

    /**
     * Returns the number of distinct clients currently tracked by this limiter.
     * @returns {number}
     */
    get size() {
      return windows.size;
    },
