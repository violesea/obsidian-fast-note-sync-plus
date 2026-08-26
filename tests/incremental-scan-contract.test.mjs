import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "operator.ts");
const source = fs.readFileSync(sourcePath, "utf8");

// The implementation intentionally keeps the vault-wide scan for full and
// metadata-recovery rounds. These structural contracts pin the branch that
// must stay event/journal-only for ordinary incremental rounds.
const incrementalBranchStart = source.indexOf(
  "if (fastIncremental) {\n        const processed = await scanIncrementalVaultEntries",
);
assert.notEqual(incrementalBranchStart, -1, "incremental scan branch must remain explicit");
const fullBranchStart = source.indexOf("      } else {", incrementalBranchStart);
assert.notEqual(fullBranchStart, -1, "full scan fallback branch must remain explicit");
const incrementalBranch = source.slice(incrementalBranchStart, fullBranchStart);

// Contract: ordinary incremental sync never enumerates the whole vault.
assert.equal(incrementalBranch.includes("getAllLoadedFiles()"), false);
assert.equal(incrementalBranch.includes("collectIncrementalReconciliation"), false);
assert.equal(incrementalBranch.includes("scanIncrementalVaultEntries"), true);

// Contract: fast incremental config sync cannot create an empty full-scan
// candidate set and then compare it against every tracked config path.
assert.match(
  source,
  /const configPaths = plugin\.settings\.configSyncEnabled && shouldSyncConfigs && !fastIncremental\n\s+\? await configAllPaths\(/,
);
assert.match(
  source,
  /if \(plugin\.settings\.configSyncEnabled && shouldSyncConfigs && !fastIncremental && plugin\.settings\.offlineDeleteSyncEnabled\)/,
);
assert.match(
  source,
  /else if \(plugin\.settings\.configSyncEnabled && shouldSyncConfigs && !fastIncremental && isLoadLastTime\)/,
);

// Contract: folder creation is a send barrier. NoteSync and FileSync must be
// scheduled only after FolderSync has drained its end signal and page ACKs.
const requestStart = source.indexOf("export const handleRequestSend");
assert.notEqual(requestStart, -1, "request dispatcher must remain present");
const folderSend = source.indexOf("await sendSyncInBatches(", requestStart);
const folderBarrier = source.indexOf('await waitForSyncTypeDrain(plugin, "folder"', folderSend);
const noteSend = source.indexOf("jobs.push(sendSyncInBatches(", folderBarrier);
const fileSend = source.indexOf("jobs.push(sendSyncInBatches(", noteSend + 1);
assert.ok(folderSend > requestStart, "FolderSync must be dispatched");
assert.ok(folderBarrier > folderSend, "FolderSync must have a drain barrier");
assert.ok(noteSend > folderBarrier, "NoteSync must follow the folder barrier");
assert.ok(fileSend > noteSend, "FileSync must follow the folder barrier");

console.log("incremental-scan-contract.test.mjs: all scenarios passed");
