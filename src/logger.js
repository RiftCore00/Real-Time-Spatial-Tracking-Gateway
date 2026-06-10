const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const CURRENT = LEVELS[process.env.LOG_LEVEL] ?? LEVELS.info;

function timestamp() {
  return new Date().toISOString();
}

function emit(level, msg, meta = {}) {
  if (LEVELS[level] < CURRENT) return;
  const entry = { time: timestamp(), level, msg, ...meta };
  const dest = level === "error" ? process.stderr : process.stdout;
  dest.write(JSON.stringify(entry) + "\n");
}

export const logger = {
  debug: (msg, meta) => emit("debug", msg, meta),
  info: (msg, meta) => emit("info", msg, meta),
  warn: (msg, meta) => emit("warn", msg, meta),
  error: (msg, meta) => emit("error", msg, meta),
};
