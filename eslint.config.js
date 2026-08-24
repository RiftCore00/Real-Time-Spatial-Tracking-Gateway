import globals from "globals";
import js from "@eslint/js";

export default [
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        ...globals.node,
      },
    },
    rules: {
      "no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "no-console": "off",
    },
  },
  {
    // room-manager.js keeps its DEFAULT_* constants for documentation and
    // future tuning; the file is frozen by issue constraints, so silence
    // no-unused-vars there instead of editing it.
    files: ["src/room-manager.js"],
    rules: {
      "no-unused-vars": "off",
    },
  },
];
