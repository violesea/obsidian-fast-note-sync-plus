import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "incremental_scan_manager.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

class FakeMirror {
  constructor(plugin, fileName) {
    this.plugin = plugin;
    this.path = `${plugin.app.vault.configDir}/plugins/${plugin.manifest.id}/${fileName}`;
    this.pending = null;
  }

  async read() {
    return this.plugin.app.vault.adapter.files.get(this.path) ?? null;
  }

  scheduleWrite(data) {
    this.pending = data;
  }

  flush() {
    if (this.pending !== null) {
      this.plugin.app.vault.adapter.files.set(this.path, this.pending);
      this.pending = null;
    }
  }

  async flushAsync() {
    this.flush();
  }
}

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => {
    if (id === "../utils/helpers") return { LocalStateFileMirror: FakeMirror, dump: () => undefined };
    if (id === "./background_activity_gate") return { waitForForeground: async () => true };
    throw new Error(`Unexpected require: ${id}`);
  },
  module,
  exports: module.exports,
  console,
}, { filename: sourcePath });

const { IncrementalScanManager, incrementalEntryKey } = module.exports;

function makePlugin(localStorage = new Map(), mirroredFiles = new Map()) {
  return {
    manifest: { id: "fast-note-sync" },
    app: {
      loadLocalStorage: (key) => localStorage.get(key) ?? null,
      saveLocalStorage: (key, value) => {
        if (value === null || value === undefined) localStorage.delete(key);
        else localStorage.set(key, String(value));
      },
      vault: {
        configDir: ".obsidian",
        getName: () => "TestVault",
        adapter: { files: mirroredFiles },
      },
    },
  };
}

function readState(localStorage) {
  return JSON.parse(localStorage.get("fns-incrementalScanState"));
}

// Contract: an event created after a sync snapshot must survive completion of that snapshot.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  assert.equal(manager.canUseIncrementalSync(true), false, "首次使用 fork 时应先做一次校准扫描");
  manager.markModified("note", "same.md");
  const snapshot = manager.beginSync();
  manager.markProcessed(snapshot.entries.map((entry) => incrementalEntryKey(entry.kind, entry.path)));
  manager.markModified("note", "same.md");
  manager.completeSync(false);
  const state = readState(localStorage);
  assert.equal(Object.values(state.entries).length, 1);
  assert.equal(Object.values(state.entries)[0].operation, "modify");
  assert.equal(Object.values(state.entries)[0].version > snapshot.entries[0].version, true);
}

// Contract: an entry whose path scan failed remains queued for a later retry.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markModified("file", "attachments/a.bin");
  manager.beginSync();
  manager.completeSync(false);
  assert.equal(Object.values(readState(localStorage).entries).length, 1);
}

// Contract: rename recovery records both the old deletion and the new path update.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markRenamed("note", "old.md", "folder/new.md");
  const entries = Object.values(readState(localStorage).entries);
  assert.deepEqual(
    entries.map(({ kind, operation, path: entryPath }) => ({ kind, operation, path: entryPath })),
    [
      { kind: "note", operation: "delete", path: "old.md" },
      { kind: "note", operation: "modify", path: "folder/new.md" },
    ],
  );
}

// Contract: a successful full reconciliation establishes a baseline and drains the queue.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markModified("note", "a.md");
  manager.beginSync(true);
  manager.completeSync();
  assert.deepEqual(readState(localStorage).entries, {});
  assert.equal(manager.canUseIncrementalSync(false), true);
  manager.requestFullReconcile();
  assert.equal(manager.canUseIncrementalSync(false), false);
  manager.beginSync(true);
  manager.completeSync();
  assert.equal(manager.canUseIncrementalSync(false), true);
}

// Contract: an event arriving during a full reconciliation remains queued for the next run.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markInitialSyncComplete();
  manager.beginSync(true);
  manager.markModified("note", "edited-during-full.md");
  manager.completeSync();
  const entries = Object.values(readState(localStorage).entries);
  assert.deepEqual(entries.map(({ kind, operation, path: entryPath }) => ({ kind, operation, path: entryPath })), [
    { kind: "note", operation: "modify", path: "edited-during-full.md" },
  ]);
}

// Contract: an ACK for an older sent version cannot consume a newer local edit.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markModified("note", "versioned.md");
  manager.markSent("note", "versioned.md");
  manager.markModified("note", "versioned.md");
  assert.equal(manager.acknowledge("note", "versioned.md"), "stale");
  assert.equal(Object.values(readState(localStorage).entries).length, 1);

  manager.markSent("note", "versioned.md");
  assert.equal(manager.acknowledge("note", "versioned.md"), "acked");
  assert.equal(Object.values(readState(localStorage).entries).length, 0);
}

// Contract: an interrupted full reconciliation must not fall back to an
// event-only sync after restart, even if the previous round had succeeded.
{
  const localStorage = new Map();
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  manager.markInitialSyncComplete();
  assert.equal(manager.canUseIncrementalSync(true), true);

  manager.beginSync(true);
  const interrupted = JSON.parse(localStorage.get("fns-incrementalScanState"));
  assert.equal(interrupted.serverBaselineReady, false);
  assert.equal(interrupted.completedInitialSync, false);

  const reloaded = new IncrementalScanManager(makePlugin(localStorage));
  await reloaded.initialize();
  assert.equal(reloaded.canUseIncrementalSync(true), false);
  assert.equal(reloaded.canUseMetadataReconciliation(), true);
}

// Contract: old vault-scoped storage and the file mirror both migrate to the stable key.
{
  const migratedState = JSON.stringify({
    schema: 1,
    nextVersion: 2,
    completedInitialSync: true,
    needsFullReconcile: false,
    entries: {},
  });
  const localStorage = new Map([["fns-TestVault-incrementalScanState", migratedState]]);
  const manager = new IncrementalScanManager(makePlugin(localStorage));
  await manager.initialize();
  const migrated = JSON.parse(localStorage.get("fns-incrementalScanState"));
  assert.equal(migrated.completedInitialSync, true);
  assert.equal(migrated.serverBaselineReady, true);
  assert.equal(manager.canUseIncrementalSync(false), true);

  const mirroredFiles = new Map([
    [".obsidian/plugins/fast-note-sync/incrementalScanState.json", migratedState],
  ]);
  const mirrorStorage = new Map();
  const mirrorManager = new IncrementalScanManager(makePlugin(mirrorStorage, mirroredFiles));
  await mirrorManager.initialize();
  const mirrored = JSON.parse(mirrorStorage.get("fns-incrementalScanState"));
  assert.equal(mirrored.completedInitialSync, true);
  assert.equal(mirrored.serverBaselineReady, true);
  assert.equal(mirrorManager.canUseIncrementalSync(false), true);
}

console.log("incremental-scan-manager.test.mjs: all scenarios passed");
