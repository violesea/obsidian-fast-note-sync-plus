import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_completion_gate.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
  },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
}, { filename: sourcePath });

const { canCompleteSync } = module.exports;
const ready = {
  allSyncDone: true,
  allDownloadsComplete: true,
  bufferCleared: true,
  isSyncRequesting: false,
  syncPhase: "monitoring",
  activeUnprocessedCount: 0,
  pendingNoteModifies: 0,
  pendingUploadHashes: 0,
  pendingConfigModifies: 0,
  pendingFileUploadAcks: 0,
  pendingNoteDeleteAcks: 0,
  pendingFileDeleteAcks: 0,
  pendingConfigDeleteAcks: 0,
  pendingNoteRenames: 0,
  pendingFileRenames: 0,
  pendingDeleteNotePaths: 0,
  pendingDeleteFilePaths: 0,
  pendingDeleteFolderPaths: 0,
  pendingDeleteConfigPaths: 0,
  syncPageAckOutbox: 0,
  activeUploads: 0,
};

// Contract: a clean protocol round is complete only when every independent
// transport, journal, ACK, rename, delete, and upload source is drained.
assert.equal(canCompleteSync(ready), true);

const blockingFields = [
  "allSyncDone",
  "allDownloadsComplete",
  "bufferCleared",
  "isSyncRequesting",
  "activeUnprocessedCount",
  "pendingNoteModifies",
  "pendingUploadHashes",
  "pendingConfigModifies",
  "pendingFileUploadAcks",
  "pendingNoteDeleteAcks",
  "pendingFileDeleteAcks",
  "pendingConfigDeleteAcks",
  "pendingNoteRenames",
  "pendingFileRenames",
  "pendingDeleteNotePaths",
  "pendingDeleteFilePaths",
  "pendingDeleteFolderPaths",
  "pendingDeleteConfigPaths",
  "syncPageAckOutbox",
  "activeUploads",
];

for (const field of blockingFields) {
  const blocked = { ...ready };
  if (field === "allSyncDone" || field === "allDownloadsComplete" || field === "bufferCleared") {
    blocked[field] = false;
  } else if (field === "isSyncRequesting") {
    blocked[field] = true;
  } else {
    blocked[field] = 1;
  }
  assert.equal(canCompleteSync(blocked), false, `${field} must block completion`);
}

// Contract: the scan phase cannot be mistaken for the monitoring phase even
// when all queues happen to be empty at that instant.
assert.equal(canCompleteSync({ ...ready, syncPhase: "scanning" }), false);

console.log("sync-completion-gate.test.mjs: all scenarios passed");
