import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

describe("logger error handling (non-serializable meta)", () => {
  let stdoutSpy;

  beforeEach(() => {
    stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function importLogger() {
    vi.resetModules();
    delete process.env.LOG_LEVEL;
    const { logger } = await import("../src/logger.js");
    return logger;
  }

  it("handles circular references in meta without throwing", async () => {
    const logger = await importLogger();
    const circular = {};
    circular.self = circular;
    expect(() => logger.info("circular", circular)).not.toThrow();
    expect(stdoutSpy).toHaveBeenCalledOnce();
  });

  it("falls back gracefully when meta has circular reference", async () => {
    const logger = await importLogger();
    const circular = {};
    circular.self = circular;
    logger.info("circular", circular);
    const line = stdoutSpy.mock.calls[0][0];
    const entry = JSON.parse(line);
    expect(entry.level).toBe("info");
    expect(entry.msg).toBe("circular");
    expect(entry.serializeError).toMatch(/non-serializable/);
  });

  it("handles BigInt in meta without throwing", async () => {
    const logger = await importLogger();
    expect(() => logger.info("bigint meta", { val: BigInt(9007199254740991) })).not.toThrow();
    expect(stdoutSpy).toHaveBeenCalledOnce();
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(entry.serializeError).toMatch(/non-serializable/);
  });

  it("still writes valid JSON when meta is serializable", async () => {
    const logger = await importLogger();
    logger.info("normal", { foo: "bar" });
    const entry = JSON.parse(stdoutSpy.mock.calls[0][0]);
    expect(entry.foo).toBe("bar");
    expect(entry.serializeError).toBeUndefined();
  });
});
