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
const { SyncState, getPostSendSyncPhase } = module.exports;

// Contract: a send that races with a physical close stays resumable instead of
// entering monitoring with no authenticated transport.
assert.equal(getPostSendSyncPhase(true, true), "waiting-connection");
assert.equal(getPostSendSyncPhase(false, false), "waiting-connection");
assert.equal(getPostSendSyncPhase(false, true), "monitoring");

const state = new SyncState();

// Contract: ownership is acquired before any asynchronous scan work begins.
assert.equal(state.tryBeginSync("first"), true);
assert.equal(state.isSyncing, true);
assert.equal(state.activeSyncContext, "first");

// Contract: a second auth/reconnect event cannot create a second logical round.
assert.equal(state.tryBeginSync("second"), false);
assert.equal(state.activeSyncContext, "first");

// Contract: a physical transport close marks the logical round for context
// rotation; old page ownership and upload ACK state must not survive the
// replacement connection.
state.transportResetPending = true;
state.pendingFilePushPageIndex.set("assets/old.bin", 3);
state.pendingFileUploadAcks.add("assets/old.bin");
assert.equal(state.activeSyncContext, "first");
assert.equal(state.transportResetPending, true);
state.activeSyncContext = "replacement";
state.transportResetPending = false;
state.pendingFilePushPageIndex.clear();
state.pendingFileUploadAcks.clear();
assert.equal(state.activeSyncContext, "replacement");
assert.equal(state.transportResetPending, false);
assert.equal(state.pendingFilePushPageIndex.size, 0);
assert.equal(state.pendingFileUploadAcks.size, 0);

state.activeSyncContext = null;
state.isSyncing = false;
assert.equal(state.tryBeginSync("third"), true);
assert.equal(state.activeSyncContext, "third");

console.log("sync-state-session.test.mjs: all scenarios passed");
