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
// Contract: a stalled page ACK is nudged at most once, then the
// transport-recovery callback is raised. A resent ACK makes the server rewind
// its send window and reflood pages the client already has (2026-08-27 live
// evidence), so the nudge budget is minimal and rotation happens well inside
// the server's 75s no-pong deadline.
const latestTimer = Array.from(timers.values()).filter((timer) => timer.delay === 15000).at(-1);
assert.ok(latestTimer, "the ACK retry should schedule the next stagnation check");
latestTimer.callback();
assert.equal(stalledAcks.length, 2, "initial ACK plus exactly one bounded retry");
const exhaustedTimer = Array.from(timers.values()).filter((timer) => timer.delay === 15000).at(-1);
assert.ok(exhaustedTimer, "the exhausted retry should still have a final timer callback");
exhaustedTimer.callback();
assert.deepEqual(stalledEvents, [{ type: "note", pageIndex: 0, retries: 1 }]);
assert.equal(stalledAcks.length, 2, "no ACK is sent after the retry budget is exhausted");

// Contract: failures on the very first page leave no previous ACK to resend.
// After one quiet window the tracker must escalate the unfinished page instead
// of returning forever; the caller can then terminate the round as failed
// without falsely ACKing that page.
{
  const firstPageTracker = new SyncProgressTracker();
  const firstPageAcks = [];
  const firstPageStalls = [];
  firstPageTracker.onPageComplete = (_type, pageIndex) => firstPageAcks.push(pageIndex);
  firstPageTracker.onPageAckStalled = (type, pageIndex, retries) => firstPageStalls.push({ type, pageIndex, retries });
  firstPageTracker.reset(["note"]);
  firstPageTracker.recordHashProgress(100);
  firstPageTracker.recordUploadComplete("note");
  firstPageTracker.setDownloadTotal("note", 2);
  firstPageTracker.recordPageProgress("note", 0, 2, false);
  // Live 3.6.1 detail payloads omit pageIndex and therefore exercise the
  // legacy accounting branch even though a Page control message exists.
  firstPageTracker.recordCompleted("note");
  const timer = Array.from(timers.values()).filter((item) => item.delay === 15000).at(-1);
  assert.ok(timer, "an unfinished first page should schedule a bounded stagnation check");
  timer.callback();
  assert.deepEqual(firstPageAcks, [], "the failed first page is never ACKed");
  assert.deepEqual(firstPageStalls, [{ type: "note", pageIndex: 0, retries: 0 }]);
}

// Contract: forceCloseType closes a stalled type's accounting with the shortfall
// returned as failures, letting the round terminate instead of looping forever
// (2026-08-27 iPad: same batch of files re-synced endlessly because a stuck type
// could never satisfy isTypeFullyDone, so the baseline never advanced).
{
  const closeTracker = new SyncProgressTracker();
  closeTracker.reset(["note"]);
  closeTracker.recordHashProgress(100);
  closeTracker.recordUploadComplete("note");
  closeTracker.setDownloadTotal("note", 10);
  closeTracker.recordPageProgress("note", 0, 10, true);
  // only 7 of 10 received items complete
  for (let i = 0; i < 7; i++) closeTracker.recordCompleted("note", 0);
  assert.equal(closeTracker.isTypeFullyDone("note"), false, "stalled type is not done before close-out");
  assert.equal(closeTracker.hasReceivedAnyPages("note"), true);
  const shortfall = closeTracker.forceCloseType("note");
  assert.equal(shortfall, 3, "shortfall of uncompleted items is reported");
  assert.equal(closeTracker.isTypeFullyDone("note"), true, "type closes out after forceCloseType");
  assert.equal(closeTracker.hasReceivedAnyPages("note"), true, "close-out keeps received history");
  // idempotent second close reports zero further shortfall
  assert.equal(closeTracker.forceCloseType("note"), 0);
  console.log("forceCloseType contract ok");
}

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
