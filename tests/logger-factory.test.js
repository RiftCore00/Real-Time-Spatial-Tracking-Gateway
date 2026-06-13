import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createLogger } from "../src/logger.js";

describe("createLogger factory", () => {
  let stdoutSpy, stderrSpy;

  beforeEach(() => {
    stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("creates a logger at the specified level", () => {
    const log = createLogger("warn");
    log.debug("hidden");
    log.info("hidden");
    expect(stdoutSpy).not.toHaveBeenCalled();
    log.warn("visible");
    expect(stdoutSpy).toHaveBeenCalledOnce();
  });

  it("creates a debug-level logger that emits all levels", () => {
    const log = createLogger("debug");
    log.debug("d");
    log.info("i");
    log.warn("w");
    log.error("e");
    expect(stdoutSpy).toHaveBeenCalledTimes(3); // debug, info, warn
    expect(stderrSpy).toHaveBeenCalledTimes(1); // error
  });

  it("each createLogger call is independent", () => {
    const quiet = createLogger("error");
    const verbose = createLogger("debug");
    verbose.info("from verbose");
    quiet.info("from quiet — suppressed");
    expect(stdoutSpy).toHaveBeenCalledOnce();
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(entry.msg).toBe("from verbose");
  });

  it("falls back to info when no level provided and LOG_LEVEL unset", () => {
    delete process.env.LOG_LEVEL;
    const log = createLogger();
    log.debug("hidden");
    expect(stdoutSpy).not.toHaveBeenCalled();
    log.info("shown");
    expect(stdoutSpy).toHaveBeenCalledOnce();
  });
});
