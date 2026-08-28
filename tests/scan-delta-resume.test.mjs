import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

// Contracts under test — the scan delta checkpoint:
// Mid-scan hash checkpoints were memory-only, so a mid-scan process kill lost
// everything and each iOS reload restarted the full hash walk from zero
// (the "switch app → reload → full hash scan" doom loop, observed on iPad
// 2026-08-28). The delta sidecar must:
// 1. Append checkpoint batches durably (JSONL, one line per hashed entry).
// 2. Reload them into the hash manager as IN-MEMORY cache candidates only
//    (no flush), split by kind, surviving corrupt lines.
// 3. Be cleared after the final durable commit.
// 4. Never throw: IO failures degrade to the old restart-from-zero behavior.

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "scan_delta.ts");
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
const requireStub = (id) => {
  switch (id) {
    case "obsidian":
      return { normalizePath: (v) => v };
    case "../utils/helpers":
      return { getPluginDir: () => ".obsidian/plugins/fast-note-sync", dump: () => undefined };
    case "../../main":
      return { default: class {} };
    default:
      throw new Error(`Unexpected require: ${id}`);
  }
};

vm.runInNewContext(transpiled, {
  require: requireStub,
  module,
  exports: module.exports,
  console,
}, { filename: sourcePath });

const { appendScanDelta, loadScanDelta, clearScanDelta } = module.exports;

const makePlugin = () => {
  const store = new Map();
  const bulkCalls = [];
  return {
    store,
    plugin: {
      app: {
        vault: {
          adapter: {
            exists: async (p) => store.has(p),
            read: async (p) => store.get(p) ?? "",
            append: async (p, data) => { store.set(p, (store.get(p) ?? "") + data); },
            remove: async (p) => { store.delete(p); },
          },
        },
      },
      fileHashManager: {
        bulkSetFromScanned: (entries, flush) => { bulkCalls.push({ size: entries.size, flush }); },
      },
    },
    bulkCalls,
  };
};

// 1. Append persists durably; reload feeds the hash manager in-memory (flush=false).
{
  const { plugin, bulkCalls } = makePlugin();
  const batch1 = new Map([["a/one.md", { hash: "h1", mtime: 1, size: 10, ctime: 2 }]]);
  const batch2 = new Map([
    ["a/two.md", { hash: "h2", mtime: 3, size: 20, ctime: 4 }],
    ["bin/pic.png", { hash: "h3", mtime: 5, size: 30, ctime: 6 }],
  ]);
  await appendScanDelta(plugin, "note", batch1);
  await appendScanDelta(plugin, "note", batch2);
  await appendScanDelta(plugin, "file", new Map([["bin/pic.png", { hash: "h3", mtime: 5, size: 30, ctime: 6 }]]));

  const loaded = await loadScanDelta(plugin);
  assert.equal(loaded, 4, "all appended entries reload");
  assert.equal(bulkCalls.length, 2, "note and file batches fed separately");
  assert.ok(bulkCalls.every((c) => c.flush === false), "preload is in-memory only (no flush)");
  assert.equal(bulkCalls[0].size, 3, "note batch has three entries (kind decides, not extension)");
  assert.equal(bulkCalls[1].size, 1, "file batch has one entry");
  console.log("scenario 1 ok: append durable, reload in-memory per kind");
}

// 2. Corrupt lines are skipped; surviving entries still load.
{
  const { plugin } = makePlugin();
  const payload = [
    JSON.stringify({ k: "note", p: "good.md", h: "hh", m: 1, s: 1, c: 1 }),
    "<<<corrupt>>>",
    JSON.stringify({ k: "note", p: "good2.md", h: "hh2", m: 2, s: 2, c: 2 }),
  ].join("\n") + "\n";
  plugin.app.vault.adapter.append(".obsidian/plugins/fast-note-sync/scanDelta.jsonl", payload);
  const loaded = await loadScanDelta(plugin);
  assert.equal(loaded, 2, "corrupt line skipped, valid lines load");
  console.log("scenario 2 ok: corrupt lines tolerated");
}

// 3. Clear removes the sidecar after the durable commit.
{
  const { plugin } = makePlugin();
  await appendScanDelta(plugin, "note", new Map([["a/one.md", { hash: "h1", mtime: 1, size: 1, ctime: 1 }]]));
  await clearScanDelta(plugin);
  assert.equal(await loadScanDelta(plugin), 0, "cleared sidecar loads nothing");
  console.log("scenario 3 ok: clear after durable commit");
}

// 4. IO failure never throws (degrades to restart-from-zero).
{
  const broken = {
    app: { vault: { adapter: {
      exists: async () => { throw new Error("disk gone"); },
      read: async () => { throw new Error("disk gone"); },
      append: async () => { throw new Error("disk gone"); },
      remove: async () => { throw new Error("disk gone"); },
    } } },
    fileHashManager: { bulkSetFromScanned: () => undefined },
  };
  await assert.doesNotReject(() => appendScanDelta(broken, "note", new Map([["x", { hash: "h", mtime: 1, size: 1 }]])));
  await assert.doesNotReject(() => loadScanDelta(broken));
  await assert.doesNotReject(() => clearScanDelta(broken));
  console.log("scenario 4 ok: IO failures degrade silently");
}

console.log("scan-delta-resume: all scenarios passed");
