const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const CURRENT = LEVELS[process.env.LOG_LEVEL] ?? LEVELS.info;

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
    const safe = {
      time: entry.time,
      level: entry.level,
      msg: entry.msg,
      serializeError: "meta contained non-serializable values",
    };
    return JSON.stringify(safe);
  }
}

/**
 * Writes a structured JSON log entry to stdout (or stderr for errors).
 * Entries below the active log level are silently discarded.
 *
 * @param {"debug"|"info"|"warn"|"error"} level - Severity level.
 * @param {string} msg - Human-readable message.
 * @param {Record<string, unknown>} [meta={}] - Additional fields merged into the entry.
 */
function emit(level, msg, meta = {}) {
  if (LEVELS[level] < CURRENT) return;
  const entry = { time: timestamp(), level, msg, ...meta };
  const dest = level === "error" ? process.stderr : process.stdout;
  dest.write(serialize(entry) + "\n");
}

/**
 * Structured logger with four severity levels.
 * Each method accepts an optional `meta` object whose fields are spread
 * into the emitted JSON log entry.
 *
 * @example
 * logger.info("Client connected", { clientId: "abc" });
 * // → {"time":"...","level":"info","msg":"Client connected","clientId":"abc"}
 */
export const logger = {
  /** @param {string} msg @param {Record<string, unknown>} [meta] */
  debug: (msg, meta) => emit("debug", msg, meta),
  /** @param {string} msg @param {Record<string, unknown>} [meta] */
  info: (msg, meta) => emit("info", msg, meta),
  /** @param {string} msg @param {Record<string, unknown>} [meta] */
  warn: (msg, meta) => emit("warn", msg, meta),
  /** @param {string} msg @param {Record<string, unknown>} [meta] */
  error: (msg, meta) => emit("error", msg, meta),
};
