import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_state.ts");
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
vm.runInNewContext(transpiled, { module, exports: module.exports, window: { clearInterval() {} } }, { filename: sourcePath });
const { SyncState } = module.exports;

const state = new SyncState();

// Contract: ownership is acquired before any asynchronous scan work begins.
assert.equal(state.tryBeginSync("first"), true);
assert.equal(state.isSyncing, true);
assert.equal(state.activeSyncContext, "first");

// Contract: a second auth/reconnect event cannot create a second logical round.
assert.equal(state.tryBeginSync("second"), false);
assert.equal(state.activeSyncContext, "first");

state.activeSyncContext = null;
state.isSyncing = false;
assert.equal(state.tryBeginSync("third"), true);
assert.equal(state.activeSyncContext, "third");

console.log("sync-state-session.test.mjs: all scenarios passed");
