/**
 * Per-client sliding-window rate limiter.
 *
 * Tracks message timestamps in a 1-second window for each client.
 * Clients that exceed the configured limit have their messages dropped
 * and receive an error frame.
 */

/**
 * Creates a rate limiter that allows at most `maxPerSecond` messages per
 * client per second using a sliding window algorithm.
 *
 * @param {number} [maxPerSecond] - Max messages per second per client.
 *   Defaults to MAX_MESSAGES_PER_SECOND env var, or 100.
 * @returns {{ check: (clientId: string) => boolean, remove: (clientId: string) => void }}
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
      // Remove entries outside the 1-second window
      while (timestamps.length > 0 && timestamps[0] <= cutoff) {
        timestamps.shift();
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
  };
}
