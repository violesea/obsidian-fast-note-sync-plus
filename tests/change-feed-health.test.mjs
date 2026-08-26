import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "change_feed_health.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
  JSON,
  Date,
}, { filename: sourcePath });
const H = module.exports;

// Contract: every fallback increments both the incident and lifetime counts,
// preserves the reason, and alerts only once after three consecutive failures.
let state = H.createChangeFeedHealthState();
state = H.recordChangeFeedFallback(state, "stale_cursor", 100);
state = H.recordChangeFeedFallback(state, "timeout", 200);
assert.equal(state.consecutiveFallbacks, 2);
assert.equal(state.totalFallbacks, 2);
assert.equal(state.lastReason, "timeout");
assert.equal(H.shouldAlertChangeFeedFallback(state), false);
state = H.recordChangeFeedFallback(state, "unauthorized", 300);
assert.equal(state.consecutiveFallbacks, 3);
assert.equal(H.shouldAlertChangeFeedFallback(state), true);
state = H.markChangeFeedFallbackAlerted(state);
assert.equal(H.shouldAlertChangeFeedFallback(state), false);

// Contract: a successful catch-up clears the consecutive incident and allows a
// future incident to alert again without losing the lifetime total.
state = H.recordChangeFeedSuccess(state, 400);
assert.equal(state.consecutiveFallbacks, 0);
assert.equal(state.totalFallbacks, 3);
assert.equal(state.alerted, false);

const restored = H.parseChangeFeedHealthState(H.serializeChangeFeedHealthState(state));
assert.deepEqual(restored, state);
assert.equal(H.parseChangeFeedHealthState("not-json").totalFallbacks, 0);

console.log("change-feed-health.test.mjs: all scenarios passed");
