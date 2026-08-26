import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");

function load(relative) {
  const sourcePath = path.join(root, relative);
  const transpiled = ts.transpileModule(fs.readFileSync(sourcePath, "utf8"), {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
    },
    fileName: sourcePath,
  }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(transpiled, { module, exports: module.exports }, { filename: sourcePath });
  return module.exports;
}

const {
  decideWrite,
  shouldCheckPrecondition,
  createWritePreconditionCounters,
  countWriteDecision,
} = load("src/lib/sync/write_precondition.ts");

// Contract: with no confirmed baseline there is nothing to protect, so the
// first upload of a path must never be reported as a conflict.
{
  const d = decideWrite({ localHash: "L", baseHash: null, serverHash: "S" });
  assert.equal(d.kind, "upload");
  assert.equal(d.reason, "no-baseline");
}

// Contract: an unreadable server fails open. A probe failure must degrade to
// the pre-M7 behaviour, never stall the upload and never fabricate a conflict.
{
  const d = decideWrite({ localHash: "L", baseHash: "B", serverHash: null });
  assert.equal(d.kind, "upload");
  assert.equal(d.reason, "precondition-unavailable");
}

// Contract: the server still holds exactly what this device last had ACKed,
// so this device owns the next version and may overwrite.
{
  const d = decideWrite({ localHash: "L", baseHash: "B", serverHash: "B" });
  assert.equal(d.kind, "upload");
  assert.equal(d.reason, "server-matches-baseline");
}

// Contract: the server already holds our exact content. Sending it again is
// pure write amplification, so the round is skipped instead.
{
  const d = decideWrite({ localHash: "L", baseHash: "B", serverHash: "L" });
  assert.equal(d.kind, "skip");
  assert.equal(d.reason, "local-matches-server");
}

// Contract: THE defect this module exists for. The server moved since our
// baseline and our content differs from it, so both sides changed. The upload
// must be withheld; overwriting here is the silent lost update.
{
  const d = decideWrite({ localHash: "L", baseHash: "B", serverHash: "OTHER" });
  assert.equal(d.kind, "conflict");
  assert.equal(d.reason, "server-moved-and-local-diverged");
}

// Contract: the probe costs one round trip, so it is only spent when a
// baseline exists and local content has actually diverged from it.
{
  assert.equal(shouldCheckPrecondition({ enabled: true, baseHash: "B", localHash: "L" }), true);
  assert.equal(shouldCheckPrecondition({ enabled: false, baseHash: "B", localHash: "L" }), false);
  assert.equal(shouldCheckPrecondition({ enabled: true, baseHash: null, localHash: "L" }), false);
  assert.equal(shouldCheckPrecondition({ enabled: true, baseHash: "L", localHash: "L" }), false);
}

// Contract: counters separate a real conflict from a fail-open probe failure,
// so a rise in withheld uploads is never confused with an unreachable server.
{
  let c = createWritePreconditionCounters();
  c = countWriteDecision(c, decideWrite({ localHash: "L", baseHash: "B", serverHash: "OTHER" }));
  c = countWriteDecision(c, decideWrite({ localHash: "L", baseHash: "B", serverHash: null }));
  c = countWriteDecision(c, decideWrite({ localHash: "L", baseHash: "B", serverHash: "L" }));
  c = countWriteDecision(c, decideWrite({ localHash: "L", baseHash: "B", serverHash: "B" }));
  // Spread into this realm: the module runs in a vm context, so its objects
  // carry a different Object.prototype and would fail a strict deep compare.
  assert.deepEqual({ ...c }, { checked: 4, conflicts: 1, unavailable: 1, skipped: 1 });
}

console.log("write-precondition.test.mjs: all scenarios passed");
