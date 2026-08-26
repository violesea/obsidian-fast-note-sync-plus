import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "stable_capture.ts");
const module = { exports: {} };
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
  Promise,
  Map,
  setTimeout,
}, { filename: sourcePath });

const {
  DEFAULT_STABILITY_WINDOW_MS,
  StableCaptureCoordinator,
  captureStableSnapshot,
} = module.exports;

const stat = { size: 10, mtime: 100, ctime: 1 };

// Contract: a capture waits for the quiet window, hashes before and after the
// window, and returns the second read only when both samples agree.
{
  const waits = [];
  const reads = [];
  const hashes = [];
  let statReads = 0;
  const result = await captureStableSnapshot({
    stat: async () => { statReads++; return { ...stat }; },
    read: async () => {
      const value = `content-${reads.length + 1}`;
      reads.push(value);
      return value;
    },
    hash: async (value) => {
      hashes.push(value);
      return "same-hash";
    },
    wait: async (milliseconds) => { waits.push(milliseconds); },
  });

  assert.equal(result.value, "content-2");
  assert.equal(result.hash, "same-hash");
  assert.equal(statReads, 4);
  assert.deepEqual(reads, ["content-1", "content-2"]);
  assert.deepEqual(hashes, ["content-1", "content-2"]);
  assert.deepEqual(waits, [DEFAULT_STABILITY_WINDOW_MS]);
}

// Contract: a file that changes during the quiet window is discarded without
// performing a second content read.
{
  let statReads = 0;
  let readCount = 0;
  const result = await captureStableSnapshot({
    stat: async () => {
      statReads++;
      return statReads === 3 ? { ...stat, mtime: 101 } : { ...stat };
    },
    read: async () => { readCount++; return "content"; },
    hash: async () => "hash",
    wait: async () => undefined,
  });

  assert.equal(result, null);
  assert.equal(readCount, 1);
}

// Contract: a same-size/same-mtime rewrite is still rejected by the double
// hash check.
{
  let readCount = 0;
  const result = await captureStableSnapshot({
    stat: async () => ({ ...stat }),
    read: async () => `content-${++readCount}`,
    hash: async (value) => value,
    wait: async () => undefined,
  });

  assert.equal(result, null);
  assert.equal(readCount, 2);
}

// Contract: concurrent events for one path share one capture and are cleared
// after completion so a later event can start a fresh capture.
{
  const coordinator = new StableCaptureCoordinator();
  let taskCalls = 0;
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const task = async () => {
    taskCalls++;
    await gate;
    return { value: undefined, hash: "hash", stat };
  };

  const first = coordinator.capture("vault:path", task);
  const second = coordinator.capture("vault:path", task);
  assert.equal(first, second);
  assert.equal(taskCalls, 0);
  release();
  await first;
  await coordinator.capture("vault:path", task);
  assert.equal(taskCalls, 2);
}

// Contract: when the shared capture is unstable, an event that arrived during
// its quiet window starts exactly one follow-up quiet window instead of being
// lost with the first null result.
{
  const coordinator = new StableCaptureCoordinator();
  let taskCalls = 0;
  const task = async () => {
    taskCalls++;
    return taskCalls === 1 ? null : { value: undefined, hash: "stable", stat };
  };

  const first = coordinator.capture("vault:unstable", task);
  const second = coordinator.capture("vault:unstable", task);
  assert.equal(first, second);
  const result = await first;
  assert.equal(result.hash, "stable");
  assert.equal(taskCalls, 2);
}

console.log("stable-capture.test.mjs: all scenarios passed");
