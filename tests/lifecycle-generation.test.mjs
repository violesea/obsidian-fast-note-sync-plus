import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "lifecycle_generation.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, { module, exports: module.exports }, { filename: sourcePath });
const { LifecycleGeneration } = module.exports;

const lifecycle = new LifecycleGeneration();
const first = lifecycle.begin();
assert.equal(lifecycle.isCurrent(first), true);

// Contract: unloading invalidates every callback captured by the old plugin instance.
lifecycle.invalidate();
assert.equal(lifecycle.isCurrent(first), false);

const second = lifecycle.begin();
assert.notEqual(second, first);
assert.equal(lifecycle.isCurrent(first), false);
assert.equal(lifecycle.isCurrent(second), true);

console.log("lifecycle-generation.test.mjs: all scenarios passed");
