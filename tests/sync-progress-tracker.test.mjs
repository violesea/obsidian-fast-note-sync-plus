import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_progress_tracker.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const timers = new Map();
let nextTimer = 1;
const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => id === "../utils/helpers" ? { dump: () => undefined } : (() => { throw new Error(`Unexpected require: ${id}`); })(),
  module,
  exports: module.exports,
  console,
  Map,
  Set,
  window: {
    setTimeout(callback, delay) {
      const id = nextTimer++;
      timers.set(id, { callback, delay });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
  },
}, { filename: sourcePath });

const { SyncProgressTracker } = module.exports;
const tracker = new SyncProgressTracker();
const pageAcks = [];
tracker.onPageComplete = (_type, pageIndex) => pageAcks.push(pageIndex);
tracker.reset(["note"]);
tracker.recordPageProgress("note", 0, 1, false);
tracker.recordCompleted("note", 0);
assert.deepEqual(pageAcks, [0]);

// Contract: a stalled page ACK is retried at most three times, then the
// transport-recovery callback is raised instead of resending forever.
const stalledTracker = new SyncProgressTracker();
const stalledAcks = [];
const stalledEvents = [];
stalledTracker.onPageComplete = (_type, pageIndex) => stalledAcks.push(pageIndex);
stalledTracker.onPageAckStalled = (type, pageIndex, retries) => stalledEvents.push({ type, pageIndex, retries });
stalledTracker.reset(["note"]);
stalledTracker.recordHashProgress(100);
stalledTracker.recordUploadComplete("note");
stalledTracker.setDownloadTotal("note", 1);
stalledTracker.recordPageProgress("note", 0, 1, false);
stalledTracker.recordCompleted("note", 0);
assert.deepEqual(stalledAcks, [0]);
for (let retry = 0; retry < 3; retry++) {
  const latestTimer = Array.from(timers.values()).filter((timer) => timer.delay === 15000).at(-1);
  assert.ok(latestTimer, "each ACK retry should schedule the next stagnation check");
  latestTimer.callback();
}
assert.equal(stalledAcks.length, 4, "initial ACK plus three bounded retries");
const exhaustedTimer = Array.from(timers.values()).filter((timer) => timer.delay === 15000).at(-1);
assert.ok(exhaustedTimer, "the exhausted retry should still have a final timer callback");
exhaustedTimer.callback();
assert.deepEqual(stalledEvents, [{ type: "note", pageIndex: 0, retries: 3 }]);
assert.equal(stalledAcks.length, 4, "no ACK is sent after the retry budget is exhausted");

// Contract: an incomplete round never emits a 100% progress signal.
const incompleteTracker = new SyncProgressTracker();
const incompleteProgress = [];
incompleteTracker.onProgressChange = (pct) => incompleteProgress.push(pct);
incompleteTracker.reset(["note"]);
incompleteTracker.recordHashProgress(50);
incompleteTracker.markIncomplete();
assert.ok(incompleteProgress.at(-1) < 100);

const staleTimer = Array.from(timers.values()).find((timer) => timer.delay === 15000);
assert.ok(staleTimer, "a stagnation timer should be scheduled after a page ACK");

// Contract: a transport close invalidates delayed callbacks from the old
// context even if the callback was already dequeued by the host runtime.
tracker.clearStagnationTimers();
staleTimer.callback();
assert.deepEqual(pageAcks, [0]);

// Contract: replacing the physical transport must discard old page/ACK state
// without erasing the hash phase or allowing the visible percentage to move
// backwards.
tracker.reset(["note"]);
tracker.recordHashProgress(100);
tracker.recordUploadComplete("note");
tracker.setDownloadTotal("note", 100);
tracker.recordPageProgress("note", 0, 100, false);
tracker.setInitialAckSent("note", true);
for (let i = 0; i < 60; i++) tracker.recordCompleted("note", 0);
const beforeTransportRetry = tracker.getOverallPct();
tracker.resetForTransportRetry(["note"]);
assert.equal(tracker.getPhase(), "upload");
assert.equal(tracker.getDetailText().includes("哈希计算"), false);
assert.ok(
  tracker.getOverallPct() >= beforeTransportRetry,
  "transport retry must not move visible progress backwards",
);
assert.equal(tracker.isInitialAckSent("note"), false);
assert.equal(tracker.getTypeTaskTotal("note"), 0);

// Contract: a safety-required recovery round keeps the logical progress floor
// while exposing a new hash phase for the re-preparation work.
tracker.recordHashProgress(100);
tracker.recordUploadComplete("note");
tracker.setDownloadTotal("note", 100);
tracker.recordPageProgress("note", 0, 100, false);
for (let i = 0; i < 60; i++) tracker.recordCompleted("note", 0);
const beforeRecoveryRound = tracker.getOverallPct();
tracker.resetForRecoveredRound(["note"]);
assert.equal(tracker.getPhase(), "hash");
assert.ok(
  tracker.getOverallPct() >= beforeRecoveryRound,
  "recovery preparation must keep the logical progress floor",
);

// Contract: a genuinely new logical sync round still starts from zero.
tracker.reset(["note"]);
assert.equal(tracker.getPhase(), "hash");
assert.equal(tracker.getOverallPct(), 0);

console.log("sync-progress-tracker.test.mjs: all scenarios passed");
