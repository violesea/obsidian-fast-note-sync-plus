import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_trigger_policy.ts");
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
vm.runInNewContext(transpiled, { module, exports: module.exports }, { filename: sourcePath });
const { decideSyncAfterAuthentication } = module.exports;
const decide = (input) => JSON.parse(JSON.stringify(decideSyncAfterAuthentication(input)));

// Contract: an active logical round survives transport re-authentication.
assert.deepEqual(
  decide({
    hasActiveSync: true,
    canUseIncremental: false,
    manualSyncEnabled: false,
    pendingRequest: null,
  }),
  { kind: "resume-active" },
);

// Contract: the first automatic round is the one calibration/full scan.
assert.deepEqual(
  decide({
    hasActiveSync: false,
    canUseIncremental: false,
    manualSyncEnabled: false,
    pendingRequest: null,
  }),
  { kind: "start", isLoadLastTime: false, syncMode: "auto", reason: "initial-full" },
);

// Contract: a later reconnect catches up through the server watermark without
// enumerating the whole local vault again.
assert.deepEqual(
  decide({
    hasActiveSync: false,
    canUseIncremental: true,
    manualSyncEnabled: false,
    pendingRequest: null,
  }),
  { kind: "start", isLoadLastTime: true, syncMode: "auto", reason: "reconnect-incremental" },
);

// Contract: an explicit full request wins over the automatic reconnect policy.
assert.deepEqual(
  decide({
    hasActiveSync: false,
    canUseIncremental: true,
    manualSyncEnabled: true,
    pendingRequest: { type: "full", mode: "note" },
  }),
  { kind: "start", isLoadLastTime: false, syncMode: "note", reason: "explicit-request" },
);

// Contract: manual-only mode never turns a reconnect into an automatic sync.
assert.deepEqual(
  decide({
    hasActiveSync: false,
    canUseIncremental: true,
    manualSyncEnabled: true,
    pendingRequest: null,
  }),
  { kind: "none", reason: "manual-mode" },
);

console.log("sync-trigger-policy.test.mjs: all scenarios passed");
