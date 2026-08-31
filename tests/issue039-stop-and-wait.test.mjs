import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

// ISSUE-039 contracts — stop-and-wait hardening. Each assertion must fail if
// the corresponding protection is removed:
// 1. The WS URL must not contain the pv=2/pb experimental handshake params.
// 2. The auth negotiation block must never adopt offered pipeline windows —
//    both windows pinned to 0 (stop-and-wait).
// 3. The ClientInfo fallback must also pin windows to 0 (no adoption).
// 4. Post-auth must not flush old-connection page ACKs into the new socket.

const root = path.resolve(import.meta.dirname, "..");

// ── Source-level contract checks (fast, exact) ──────────────────────────────
const wsManagerSource = fs.readFileSync(path.join(root, "src", "lib", "sync", "websocket_manager.ts"), "utf8");

// 1. Handshake: negotiation params must be gone.
assert.ok(
  !wsManagerSource.includes('"&pv=2&pb="'),
  "websocket_manager must not construct the pv=2/pb handshake parameter",
);
assert.ok(
  !wsManagerSource.includes("negotiationParams"),
  "websocket_manager must not append any negotiation params to the WS URL",
);

// 2. Auth negotiation: windows pinned, not adopted.
assert.ok(
  /ISSUE-039[\s\S]{0,400}pipelineWindowUp = 0;/.test(wsManagerSource),
  "auth negotiation block must pin pipelineWindowUp to 0 (ISSUE-039 marker present)",
);
assert.ok(
  /pipelineWindowDown = 0;\s*\n\s*this\.plugin\.syncState\.negotiated = negotiated;/.test(wsManagerSource),
  "auth negotiation block must pin pipelineWindowDown to 0 right before recording negotiated",
);
assert.ok(
  !/if \(typeof nego\.pipelineWindowUp === "number"\)/.test(wsManagerSource),
  "auth negotiation must not conditionally adopt offered window values",
);

// 3. Post-auth must not flush old page ACKs into the new socket.
assert.ok(
  !/startKeepAlive\(\);[\s\S]{0,120}flushSyncPageAcks\(\);/.test(wsManagerSource),
  "post-auth path must not call flushSyncPageAcks (old-connection ACKs stay in the outbox)",
);

// 4. ClientInfo fallback pins windows too.
const versionManagerSource = fs.readFileSync(path.join(root, "src", "lib", "utils", "version_manager.ts"), "utf8");
assert.ok(
  !versionManagerSource.includes("data.pipelineWindowUp"),
  "version_manager must not adopt window values from ClientInfo data",
);
assert.ok(
  /ISSUE-039[\s\S]{0,300}pipelineWindowUp = 0;/.test(versionManagerSource),
  "version_manager fallback must pin pipelineWindowUp to 0 (ISSUE-039 marker present)",
);
assert.ok(
  /pipelineWindowDown = 0;/.test(versionManagerSource),
  "version_manager fallback must pin pipelineWindowDown to 0",
);

// ── Behavioral check: the WS URL builder with a live-style plugin mock ──────
const transpiled = ts.transpileModule(wsManagerSource, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: path.join(root, "src", "lib", "sync", "websocket_manager.ts"),
}).outputText;

// websocket_manager pulls a wide dependency graph; a full vm harness is over
// weight here, so extract the getWsUrl body via a marker scan on the built
// output instead — assert the compiled artifact cannot re-introduce pv=2.
assert.ok(
  !/&pv=2/.test(transpiled),
  "compiled websocket_manager must not embed &pv=2 anywhere",
);

// 5. Completion gate: success timestamps blocked by failures.
const operatorSource = fs.readFileSync(path.join(root, "src", "lib", "sync", "operator.ts"), "utf8");
assert.ok(
  /roundSucceeded && plugin\.expectedSyncCount > 0 && !plugin\.localStorageManager\.getMetadata\("isInitSync"\)/.test(operatorSource),
  "isInitSync must only advance on a whole-round commit (ISSUE-039 gate)",
);
assert.ok(
  /if \(roundSucceeded\) \{\s*plugin\.localStorageManager\.setMetadata\("lastSyncSuccessTime", Date\.now\(\)\);/.test(operatorSource),
  "lastSyncSuccessTime must only advance on a whole-round commit (ISSUE-039 gate)",
);

// 5.1 Round commit is atomic: failures/guard skips abort the incremental
// manager, and scanDelta survives until the entire round commits.
assert.ok(
  /const roundSucceeded = totalFailed === 0 && !offlineGuardSkippedThisRound;/.test(operatorSource),
  "round success must combine write failures and the offline guard",
);
assert.ok(
  /if \(roundSucceeded\) \{[\s\S]{0,360}incrementalScanManager\?\.completeSync\(\);[\s\S]{0,120}await clearScanDelta\(plugin\);[\s\S]{0,240}\} else \{[\s\S]{0,240}incrementalScanManager\?\.failSync\(\);/.test(operatorSource),
  "baseline and scanDelta must commit only on whole-round success; failures must abort",
);
assert.ok(
  operatorSource.indexOf("await clearScanDelta(plugin);") < operatorSource.indexOf('plugin.isSyncing = false;', operatorSource.indexOf("await clearScanDelta(plugin);")),
  "scanDelta removal must settle before a new sync round is allowed",
);
const syncEndWrapperStart = operatorSource.indexOf("async function receiveSyncEndWrapper");
const syncEndWrapperEnd = operatorSource.indexOf("export const handleRequestSend", syncEndWrapperStart);
assert.ok(syncEndWrapperStart >= 0 && syncEndWrapperEnd > syncEndWrapperStart, "SyncEnd wrapper must be locatable");
assert.equal(
  operatorSource.slice(syncEndWrapperStart, syncEndWrapperEnd).includes("clearScanDelta(plugin)"),
  false,
  "a single note/file SyncEnd must not clear the shared scan checkpoint",
);

// 6. File download integrity gate: assembly hash mismatch fails the session
//    without allowing any per-item watermark advance.
const operatorFileSource = fs.readFileSync(path.join(root, "src", "lib", "sync", "operator_file.ts"), "utf8");
assert.ok(
  operatorFileSource.includes("Hash mismatch after assembly"),
  "download completion must verify assembled bytes against the server-declared hash",
);
assert.ok(
  operatorFileSource.includes("Read-back size mismatch after write"),
  "download completion must read back the written file and compare size",
);
{
  const gatePos = operatorFileSource.indexOf("Read-back size mismatch after write");
  assert.ok(gatePos >= 0, "download read-back gate must be locatable");
  assert.doesNotMatch(
    operatorFileSource,
    /setMetadata\("lastFileSyncTime", session\.lastTime\)/,
    "file handlers must leave lastFileSyncTime to the whole-round completion gate",
  );
}
{
  const updateStart = operatorFileSource.indexOf("export const receiveFileSyncUpdate");
  const updateEnd = operatorFileSource.indexOf("export const receiveFileSyncDelete", updateStart);
  assert.ok(updateStart >= 0 && updateEnd > updateStart, "file update handler must be locatable");
  assert.doesNotMatch(
    operatorFileSource.slice(updateStart, updateEnd),
    /setMetadata\("lastFileSyncTime", data\.lastTime\)/,
    "file metadata arrival must not advance the watermark before chunk materialization",
  );
  const endStart = operatorFileSource.indexOf("export const receiveFileSyncEnd");
  const endEnd = operatorFileSource.indexOf("export const checkAndUploadAttachments", endStart);
  assert.ok(endStart >= 0 && endEnd > endStart, "file SyncEnd handler must be locatable");
  assert.doesNotMatch(
    operatorFileSource.slice(endStart, endEnd),
    /setMetadata\("lastFileSyncTime", syncData\.lastTime\)/,
    "FileSyncEnd must remain transport evidence until the whole-round gate commits its watermark",
  );
}

// 7. Mid-scan full-map flush must be gated (2.5.19): both flush paths check
//    the scanning phase and defer to the scanDelta JSONL instead.
const hashManagerSource = fs.readFileSync(path.join(root, "src", "lib", "storage", "file_hash_manager.ts"), "utf8");
assert.ok(
  (hashManagerSource.match(/syncPhase === "scanning"/g) || []).length >= 2,
  "flush() and flushAsync() must both gate full-map persistence during scans",
);
assert.ok(
  hashManagerSource.includes("appendScanDelta"),
  "cold-build checkpoints must persist via scanDelta JSONL appends, not full-map serialization",
);
assert.ok(
  /if \(!await appendScanDelta\(this\.plugin, "note", pendingCheckpointEntries\)\) \{[\s\S]{0,180}return false;[\s\S]{0,120}pendingCheckpointEntries\.clear\(\);/.test(hashManagerSource),
  "cold-build checkpoints must retain their batch when adapter append fails",
);
assert.ok(
  !/if \(hashMapChangedSinceCheckpoint\) \{\s*\n\s*this\.saveHashMapToStorage\(\);/.test(hashManagerSource),
  "cold-build checkpoint must not call saveHashMapToStorage (full-map serialization) anymore",
);

// 8. A process-reload checkpoint is a write-ahead barrier, not a best-effort
// side effect. The scan loop must await delta/cursor persistence and must not
// clear its in-memory batch through a fire-and-forget append.
assert.equal(
  operatorSource.includes("void appendScanDelta"),
  false,
  "operator scan checkpoints must never fire-and-forget scanDelta writes",
);
assert.ok(
  /if \(!await appendScanDelta\(plugin, kind, checkpoint\)\) return false;/.test(operatorSource)
    && /await checkpointScannedHashes\(plugin, "note", plugin\.scannedNoteHashes\)/.test(operatorSource)
    && /await checkpointScannedHashes\(plugin, "file", plugin\.scannedFileHashes\)/.test(operatorSource),
  "operator scan checkpoints must await durable append for both note and file batches",
);
assert.ok(
  /await appendScanProgress\(plugin,/.test(operatorSource),
  "operator scan checkpoints must persist a reload cursor after hash deltas",
);
assert.ok(
  /restoredScanProgress/.test(operatorSource),
  "operator scan must restore visible progress instead of restarting at zero",
);

console.log("issue039-stop-and-wait: all contracts hold");
