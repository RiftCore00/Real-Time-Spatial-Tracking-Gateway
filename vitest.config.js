import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.test.js"],
    coverage: {
      provider: "v8",
      reporter: ["text", "lcov"],
      // Exclude postgres.js from coverage: it requires a live PostgreSQL
      // database and its tests are intentionally skipped in CI when
      // DATABASE_URL is not set. Coverage is validated via integration
      // tests in storage-postgres.test.js when DATABASE_URL is available.
      exclude: ["src/storage/postgres.js"],
      thresholds: {
        branches: 80,
        functions: 85,
        lines: 85,
        statements: 85,
      },
    },
  },
});
