/**
 * Per-IP connection rate limiter using a sliding 60-second window.
 * Memory is bounded: stale entries are proactively evictable via cleanup()
 * and check() uses batch filter() instead of O(n) shift().
 *
 * @param {number} [maxPerMinute]
 * @returns {{ check: (ip: string) => boolean, cleanup: (ip: string) => void }}
 */
export function createConnRateLimiter(maxPerMinute) {
  const limit = maxPerMinute ?? (Number(process.env.MAX_CONNECTIONS_PER_IP ?? process.env.CONN_RATE_LIMIT) || 30);
  /** @type {Map<string, number[]>} */
  const windows = new Map();

  return {
    /**
     * Returns true if the connection is allowed, false if rate-limited.
     * Side effect: records the current timestamp for the IP.
     *
     * @param {string} ip
     * @returns {boolean}
     */
    check(ip) {
      const now = Date.now();
      const cutoff = now - 60_000;
      let timestamps = windows.get(ip);
      if (!timestamps) {
        timestamps = [];
        windows.set(ip, timestamps);
      }
      // Batch-evict entries outside the 60-second window (O(k) where k = expired entries)
      if (timestamps.length > 0 && timestamps[0] <= cutoff) {
        const filtered = timestamps.filter(t => t > cutoff);
        windows.set(ip, filtered);
        timestamps = filtered;
      }
      if (timestamps.length >= limit) return false;
      timestamps.push(now);
      return true;
    },

    /**
     * Removes the rate-limit state for an IP (call on disconnect).
     * Prevents unbounded growth of the windows Map over time.
     *
     * @param {string} ip
     */
    cleanup(ip) {
      windows.delete(ip);
    },
  };
}
