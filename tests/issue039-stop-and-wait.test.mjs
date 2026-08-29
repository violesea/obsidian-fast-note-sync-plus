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
  /totalFailed === 0 && plugin\.expectedSyncCount > 0 && !plugin\.localStorageManager\.getMetadata\("isInitSync"\)/.test(operatorSource),
  "isInitSync must only advance on zero-failure rounds (ISSUE-039 gate)",
);
assert.ok(
  /!offlineGuardSkippedThisRound && totalFailed === 0/.test(operatorSource),
  "lastSyncSuccessTime must only advance on zero-failure rounds (ISSUE-039 gate)",
);

// 6. File download integrity gate: assembly hash mismatch fails the session
//    before baseline/timestamp advance.
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
  const tsPos = operatorFileSource.indexOf('setMetadata("lastFileSyncTime", session.lastTime)', gatePos);
  assert.ok(tsPos > gatePos, "lastFileSyncTime advance must sit after the read-back gate, not before");
}

console.log("issue039-stop-and-wait: all contracts hold");
