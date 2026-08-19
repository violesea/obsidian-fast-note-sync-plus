import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "auth_sync_coordinator.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

const policySourcePath = path.join(root, "src", "lib", "sync", "sync_trigger_policy.ts");
const policySource = fs.readFileSync(policySourcePath, "utf8");
const policyTranspiled = ts.transpileModule(policySource, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: policySourcePath,
}).outputText;
const policyModule = { exports: {} };
vm.runInNewContext(policyTranspiled, {
  module: policyModule,
  exports: policyModule.exports,
}, { filename: policySourcePath });

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => {
    if (id === "./sync_trigger_policy") return policyModule.exports;
    throw new Error(`Unexpected require: ${id}`);
  },
  module,
  exports: module.exports,
  console,
}, { filename: sourcePath });

const { AuthSyncCoordinator } = module.exports;

let currentConnection = 1;
let activeSync = false;
let incrementalAvailable = false;
let manualOnly = false;
let pendingRequest = null;
let readyResolve;
const ready = new Promise((resolve) => { readyResolve = resolve; });
const started = [];
let resumed = 0;
const logs = [];

const coordinator = new AuthSyncCoordinator({
  isCurrentConnection: (connectionId) => connectionId === currentConnection,
  waitUntilReady: async (isCurrent) => {
    const readyResult = await ready;
    return readyResult && isCurrent();
  },
  hasActiveSync: () => activeSync,
  canUseIncremental: () => incrementalAvailable,
  manualSyncEnabled: () => manualOnly,
  getPendingRequest: () => pendingRequest,
  consumePendingRequest: () => { pendingRequest = null; },
  resumeActiveSync: () => { resumed++; },
  startSync: (decision) => { started.push(decision); },
  log: (message) => { logs.push(message); },
});

// Contract: an older auth callback that is still waiting for startup readiness
// cannot launch a second full scan after a newer socket becomes current.
const firstDispatch = coordinator.dispatch(1);
currentConnection = 2;
const secondDispatch = coordinator.dispatch(2);
readyResolve(true);
await Promise.all([firstDispatch, secondDispatch]);
assert.equal(started.length, 1);
assert.equal(started[0].isLoadLastTime, false);

// Contract: a duplicate auth message for one physical connection is ignored.
await coordinator.dispatch(2);
assert.equal(started.length, 1);

// Contract: an authenticated reconnect resumes the active logical round and
// never starts a new scan.
activeSync = true;
currentConnection = 3;
await coordinator.dispatch(3);
assert.equal(resumed, 1);
assert.equal(started.length, 1);

// Contract: a later reconnect with a durable baseline starts incremental catch-up.
activeSync = false;
incrementalAvailable = true;
currentConnection = 4;
await coordinator.dispatch(4);
assert.equal(started.length, 2);
assert.equal(started[1].isLoadLastTime, true);

assert.ok(logs.some((message) => message.includes("full")));
console.log("auth-sync-coordinator.test.mjs: all scenarios passed");
