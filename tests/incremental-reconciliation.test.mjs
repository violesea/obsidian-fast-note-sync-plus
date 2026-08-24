import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "incremental_reconciliation.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
  require: () => { throw new Error("incremental_reconciliation should have no runtime dependencies"); },
}, { filename: sourcePath });

const { collectIncrementalReconciliationEntries } = module.exports;

const assertEntriesEqual = (actual, expected) => {
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), expected);
};

function makeIndex({ local = new Map(), baseline = new Map(), folders = new Set() } = {}) {
  return {
    getValidHash: (entryPath, mtime, size) => {
      const cached = local.get(entryPath);
      return cached && cached.mtime === mtime && cached.size === size ? cached.hash : null;
    },
    getPathHash: (entryPath) => baseline.get(entryPath) ?? null,
    getAllPaths: () => [...baseline.keys()],
    getFolderMtime: (entryPath) => folders.has(entryPath) ? 1 : null,
    getAllFolderPaths: () => [...folders],
  };
}

const includeAll = {
  dirtyKeys: new Set(),
  isPathExcluded: () => false,
  isFolderPathExcluded: () => false,
};

// Contract: a local note created while vault events were unavailable must be
// returned even though the durable dirty journal is empty.
{
  const entries = collectIncrementalReconciliationEntries([
    { path: "小红书/宠物/每日内容/2026-08/2026-08-24_狗狗中暑别急着灌水，先做这5步.md", kind: "note", mtime: 10, size: 4 },
  ], makeIndex(), includeAll);
  assertEntriesEqual(entries, [{
    kind: "note",
    operation: "modify",
    path: "小红书/宠物/每日内容/2026-08/2026-08-24_狗狗中暑别急着灌水，先做这5步.md",
    version: 0,
    forceHash: false,
  }]);
}

// Contract: an unchanged, fully baselined note must not be re-uploaded.
{
  const entries = collectIncrementalReconciliationEntries(
    [{ path: "stable.md", kind: "note", mtime: 10, size: 4 }],
    makeIndex({
      local: new Map([["stable.md", { hash: "same", mtime: 10, size: 4 }]]),
      baseline: new Map([["stable.md", "same"]]),
    }),
    includeAll,
  );
  assertEntriesEqual(entries, []);
}

// Contract: a metadata change or a local/server hash mismatch is a candidate,
// while the valid local hash may be reused by the scanner without rereading it.
{
  const entries = collectIncrementalReconciliationEntries([
    { path: "changed.md", kind: "note", mtime: 11, size: 4 },
    { path: "local-newer.md", kind: "note", mtime: 10, size: 4 },
  ], makeIndex({
    local: new Map([
      ["changed.md", { hash: "old", mtime: 10, size: 4 }],
      ["local-newer.md", { hash: "new", mtime: 10, size: 4 }],
    ]),
    baseline: new Map([
      ["changed.md", "old"],
      ["local-newer.md", "old"],
    ]),
  }), includeAll);
  assertEntriesEqual(entries, [
    { kind: "note", operation: "modify", path: "changed.md", version: 0, forceHash: false },
    { kind: "note", operation: "modify", path: "local-newer.md", version: 0, forceHash: false },
  ]);
}

// Contract: a missed delete is surfaced from the server-confirmed baseline,
// but an explicit durable delete event remains the sole owner of that path.
{
  const entries = collectIncrementalReconciliationEntries([], makeIndex({
    baseline: new Map([["gone.md", "hash"], ["gone.bin", "hash"]]),
  }), includeAll);
  assertEntriesEqual(entries, [
    { kind: "note", operation: "delete", path: "gone.md", version: 0 },
    { kind: "file", operation: "delete", path: "gone.bin", version: 0 },
  ]);

  const suppressed = collectIncrementalReconciliationEntries([], makeIndex({
    baseline: new Map([["gone.md", "hash"]]),
  }), { ...includeAll, dirtyKeys: new Set(["note:gone.md"]) });
  assertEntriesEqual(suppressed, []);
}

// Contract: ignored paths are not resurrected as local uploads/deletes.
{
  const entries = collectIncrementalReconciliationEntries([
    { path: "remote-write.md", kind: "note", mtime: 10, size: 4 },
  ], makeIndex({ baseline: new Map([["remote-write.md", "old"]]) }), {
    ...includeAll,
    isIgnoredPath: (entryPath) => entryPath === "remote-write.md",
  });
  assertEntriesEqual(entries, []);
}

console.log("incremental-reconciliation.test.mjs: all scenarios passed");
