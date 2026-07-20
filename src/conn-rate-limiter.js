/**
 * Per-IP connection rate limiter using a sliding 60-second window.
 * @param {number} [maxPerMinute]
 * @returns {{ check: (ip: string) => boolean }}
 */
export function createConnRateLimiter(maxPerMinute) {
  const limit = maxPerMinute ?? (Number(process.env.MAX_CONNECTIONS_PER_IP ?? process.env.CONN_RATE_LIMIT) || 30);
  /** @type {Map<string, number[]>} */
  const windows = new Map();

  return {
    check(ip) {
      const now = Date.now();
      const cutoff = now - 60_000;
      let timestamps = windows.get(ip);
      if (!timestamps) {
        timestamps = [];
        windows.set(ip, timestamps);
      }
      while (timestamps.length > 0 && timestamps[0] <= cutoff) {
        timestamps.shift();
      }
      if (timestamps.length >= limit) return false;
      timestamps.push(now);
      return true;
    },
  };
}
