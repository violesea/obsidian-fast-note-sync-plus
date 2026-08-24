import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_progress_tracker.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const timers = new Map();
let nextTimer = 1;
const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => id === "../utils/helpers" ? { dump: () => undefined } : (() => { throw new Error(`Unexpected require: ${id}`); })(),
  module,
  exports: module.exports,
  console,
  Map,
  Set,
  window: {
    setTimeout(callback, delay) {
      const id = nextTimer++;
      timers.set(id, { callback, delay });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
  },
}, { filename: sourcePath });

const { SyncProgressTracker } = module.exports;
const tracker = new SyncProgressTracker();
const pageAcks = [];
tracker.onPageComplete = (_type, pageIndex) => pageAcks.push(pageIndex);
tracker.reset(["note"]);
tracker.recordPageProgress("note", 0, 1, false);
tracker.recordCompleted("note", 0);
assert.deepEqual(pageAcks, [0]);

const staleTimer = Array.from(timers.values()).find((timer) => timer.delay === 15000);
assert.ok(staleTimer, "a stagnation timer should be scheduled after a page ACK");

// Contract: a transport close invalidates delayed callbacks from the old
// context even if the callback was already dequeued by the host runtime.
tracker.clearStagnationTimers();
staleTimer.callback();
assert.deepEqual(pageAcks, [0]);

console.log("sync-progress-tracker.test.mjs: all scenarios passed");
