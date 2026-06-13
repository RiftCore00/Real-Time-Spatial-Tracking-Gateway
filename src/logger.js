const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

/**
 * Returns the current time as an ISO 8601 string.
 * @returns {string} ISO 8601 timestamp.
 */
function timestamp() {
  return new Date().toISOString();
}

/**
 * Safely serializes a log entry to JSON. Falls back to a safe representation
 * when the entry contains non-serializable values (circular refs, BigInt, etc.).
 *
 * @param {object} entry - The log entry object.
 * @returns {string} JSON string.
 */
function serialize(entry) {
  try {
    return JSON.stringify(entry);
  } catch {
    return JSON.stringify({
      time: entry.time,
      level: entry.level,
      msg: entry.msg,
      serializeError: "meta contained non-serializable values",
    });
  }
}

/**
 * Creates a structured logger bound to the given minimum log level.
 *
 * @param {string} [level] - Minimum severity level ("debug"|"info"|"warn"|"error").
 *   Defaults to the `LOG_LEVEL` environment variable, falling back to "info".
 * @returns {{ debug: Function, info: Function, warn: Function, error: Function }}
 *
 * @example
 * const log = createLogger("debug");
 * log.info("hello", { clientId: "abc" });
 */
export function createLogger(level) {
  const current = LEVELS[level ?? process.env.LOG_LEVEL] ?? LEVELS.info;

  /**
   * @param {"debug"|"info"|"warn"|"error"} lvl
   * @param {string} msg
   * @param {Record<string, unknown>} [meta={}]
   */
  function emit(lvl, msg, meta = {}) {
    if (LEVELS[lvl] < current) return;
    const entry = { time: timestamp(), level: lvl, msg, ...meta };
    const dest = lvl === "error" ? process.stderr : process.stdout;
    dest.write(serialize(entry) + "\n");
  }

  return {
    /** @param {string} msg @param {Record<string, unknown>} [meta] */
    debug: (msg, meta) => emit("debug", msg, meta),
    /** @param {string} msg @param {Record<string, unknown>} [meta] */
    info: (msg, meta) => emit("info", msg, meta),
    /** @param {string} msg @param {Record<string, unknown>} [meta] */
    warn: (msg, meta) => emit("warn", msg, meta),
    /** @param {string} msg @param {Record<string, unknown>} [meta] */
    error: (msg, meta) => emit("error", msg, meta),
  };
}

/** Default logger instance using LOG_LEVEL env var (falls back to "info"). */
export const logger = createLogger();
