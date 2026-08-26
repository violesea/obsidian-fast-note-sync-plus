import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "incremental_scan_progress.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
}, { filename: sourcePath });

const { createIncrementalScanProgress } = module.exports;

// Contract: each processed entry advances one shared denominator and reaches
// 100% on the last entry, even when different scanner branches report it.
{
  const updates = [];
  const progress = createIncrementalScanProgress(3, (value) => updates.push(value));
  progress.step();
  progress.step();
  progress.step();
  assert.deepEqual(JSON.parse(JSON.stringify(updates)), [
    { processed: 1, total: 3, percent: 33 },
    { processed: 2, total: 3, percent: 66 },
    { processed: 3, total: 3, percent: 100 },
  ]);
}

// Contract: an empty incremental batch must not leave the UI at its initial 0%.
{
  const updates = [];
  const progress = createIncrementalScanProgress(0, (value) => updates.push(value));
  progress.completeEmpty();
  assert.deepEqual(JSON.parse(JSON.stringify(updates)), [{ processed: 0, total: 0, percent: 100 }]);
}

console.log("incremental-scan-progress.test.mjs: all scenarios passed");
