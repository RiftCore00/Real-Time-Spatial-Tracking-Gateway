import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// We need to test logger in isolation, controlling LOG_LEVEL
describe("logger", () => {
  let stdoutSpy, stderrSpy;

  beforeEach(() => {
    stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function importLogger(level) {
    vi.resetModules();
    if (level !== undefined) process.env.LOG_LEVEL = level;
    else delete process.env.LOG_LEVEL;
    const { logger } = await import("../src/logger.js");
    return logger;
  }

  it("writes info to stdout as valid JSON with time/level/msg", async () => {
    const logger = await importLogger("info");
    logger.info("hello world");
    expect(stdoutSpy).toHaveBeenCalledOnce();
    const line = stdoutSpy.mock.calls[0][0];
    const entry = JSON.parse(line);
    expect(entry.level).toBe("info");
    expect(entry.msg).toBe("hello world");
    expect(typeof entry.time).toBe("string");
  });

  it("writes error to stderr", async () => {
    const logger = await importLogger("info");
    logger.error("boom");
    expect(stderrSpy).toHaveBeenCalledOnce();
    const entry = JSON.parse(stderrSpy.mock.calls[0][0]);
    expect(entry.level).toBe("error");
    expect(entry.msg).toBe("boom");
  });

  it("suppresses levels below current level", async () => {
    const logger = await importLogger("warn");
    logger.debug("ignored");
    logger.info("also ignored");
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  it("does not suppress levels at or above current level", async () => {
    const logger = await importLogger("warn");
    logger.warn("shown");
    logger.error("also shown");
    expect(stdoutSpy).toHaveBeenCalledOnce(); // warn → stdout
    expect(stderrSpy).toHaveBeenCalledOnce(); // error → stderr
  });

  it("spreads meta fields into the log entry", async () => {
    const logger = await importLogger("info");
    logger.info("with meta", { clientId: "abc", roomId: "r1" });
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(entry.clientId).toBe("abc");
    expect(entry.roomId).toBe("r1");
  });

  it("defaults to info level when LOG_LEVEL is not set", async () => {
    const logger = await importLogger(undefined);
    logger.debug("hidden");
    expect(stdoutSpy).not.toHaveBeenCalled();
    logger.info("visible");
    expect(stdoutSpy).toHaveBeenCalledOnce();
  });

  it("writes warn to stdout", async () => {
    const logger = await importLogger("info");
    logger.warn("careful");
    expect(stdoutSpy).toHaveBeenCalledOnce();
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(entry.level).toBe("warn");
  });

  it("time field is a valid ISO 8601 string", async () => {
    const logger = await importLogger("info");
    logger.info("time check");
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(() => new Date(entry.time)).not.toThrow();
    expect(new Date(entry.time).toISOString()).toBe(entry.time);
  });
});
