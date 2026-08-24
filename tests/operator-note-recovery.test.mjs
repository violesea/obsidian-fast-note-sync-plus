import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "operator_note.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

class TAbstractFile {}
class TFile extends TAbstractFile {}

const hashContent = (content) => content.length === 0 ? "0" : `h:${content}`;
const module = { exports: {} };
const requireStub = (id) => {
  if (id === "obsidian") return { TFile, TAbstractFile, normalizePath: (value) => value };
  if (id === "../utils/types") return {};
  if (id === "../utils/helpers") {
    return {
      hashContent,
      hashContentAsync: async (content) => hashContent(content),
      dump: () => undefined,
      dumpError: () => undefined,
      isPathExcluded: () => false,
      getSafeCtime: () => 1,
      vaultDelete: async () => undefined,
      checkAndNotifyCaseConflict: () => false,
      getPluginDir: () => ".obsidian/plugins/fast-note-sync",
    };
  }
  if (id === "./sync_log_manager") return {
    SyncLogManager: { getInstance: () => ({ addLog: () => undefined }) },
  };
  if (id === "../../main") return {};
  if (id === "./background_activity_gate") return { waitForForeground: async () => true };
  throw new Error(`Unexpected require: ${id}`);
};

vm.runInNewContext(transpiled, {
  require: requireStub,
  module,
  exports: module.exports,
  console,
  Promise,
  Set,
  Map,
  window: {
    setTimeout(callback) {
      callback();
      return 1;
    },
  },
}, { filename: sourcePath });

const { receiveNoteSyncModify, receiveNoteUpload, repairSuspiciousEmptyNotes } = module.exports;

function makeFile(pathName, content = "keep") {
  const file = new TFile();
  file.path = pathName;
  file.stat = { mtime: 1, ctime: 1, size: content.length };
  file.content = content;
  return file;
}

function makePlugin(file, apiNote) {
  const calls = { fetched: 0, modified: 0, created: 0, completed: [], released: 0, setHash: [] };
  const plugin = {
    settings: { syncEnabled: true, readonlySyncEnabled: false, vault: "TestVault" },
    api: {
      getNoteContent: async () => {
        calls.fetched++;
        return apiNote;
      },
    },
    app: {
      vault: {
        getFileByPath: (pathName) => pathName === file.path ? file : null,
        getFolderByPath: () => ({ path: "folder" }),
        read: async (target) => target.content,
        modify: async (target, content, options) => {
          calls.modified++;
          target.content = content;
          target.stat.size = content.length;
          if (options?.mtime) target.stat.mtime = options.mtime;
          if (options?.ctime) target.stat.ctime = options.ctime;
        },
        create: async () => { calls.created++; },
      },
    },
    lockManager: { withLock: async (_pathName, callback) => callback() },
    fileHashManager: {
      getPathHash: () => null,
      getValidHash: () => null,
      getZeroLengthNoteHashEntries: () => [],
      setFileHash: (...args) => calls.setHash.push(args),
    },
    lastSyncMtime: new Map(),
    pendingNoteModifies: new Map(),
    pendingNoteDeleteAcks: new Set(),
    syncState: {
      conflictedPaths: new Set(),
      newConflictedPathsThisRound: new Set(),
      pendingNotePushPageIndex: new Map(),
    },
    localStorageManager: {
      getMetadata: () => 0,
      setMetadata: () => undefined,
      savePending: () => undefined,
      setConflictedPaths: () => undefined,
    },
    statusBarManager: { updateConflictBadge: () => undefined },
    noteSyncTasks: { failed: 0 },
    addIgnoredFile: () => undefined,
    removeIgnoredFile: () => undefined,
    pendingNotePushPageIndex: new Map(),
    concurrencyLimiter: {
      waitForSlot: async () => undefined,
      releaseSlot: () => { calls.released++; },
      releaseFifoSlot: () => undefined,
    },
    recordSyncCompleted: (_type, pageIndex) => calls.completed.push(pageIndex),
    websocket: { SendMessage: async () => "closed" },
    getClientName: () => "test",
    manifest: { version: "test" },
  };
  return { plugin, calls };
}

const incoming = (contentHash, content = "") => ({
  vault: "TestVault",
  path: "notes/example.md",
  pathHash: "path-hash",
  action: "modify",
  content,
  contentHash,
  ctime: 1,
  mtime: 2,
  lastTime: 0,
  pageIndex: 4,
});

// Contract: an incomplete payload with a non-empty hash is materialized through
// the canonical HTTP endpoint and the verified content is written locally.
{
  const file = makeFile("notes/example.md");
  const { plugin, calls } = makePlugin(file, {
    path: file.path,
    pathHash: "path-hash",
    content: "restored",
    contentHash: "h:restored",
    ctime: 1,
    mtime: 2,
    lastTime: 0,
  });
  await receiveNoteSyncModify(incoming("h:restored"), plugin);
  assert.equal(calls.fetched, 1);
  assert.equal(file.content, "restored");
  assert.deepEqual(calls.completed, [4]);
  assert.equal(plugin.noteSyncTasks.failed, 0);
}

// Contract: a non-empty but truncated payload follows the same recovery path;
// the client must not trust a body merely because it is non-empty.
{
  const file = makeFile("notes/example.md");
  const { plugin, calls } = makePlugin(file, {
    path: file.path,
    pathHash: "path-hash",
    content: "restored",
    contentHash: "h:restored",
    ctime: 1,
    mtime: 2,
    lastTime: 0,
  });
  await receiveNoteSyncModify(incoming("h:restored", "partial"), plugin);
  assert.equal(calls.fetched, 1);
  assert.equal(file.content, "restored");
  assert.deepEqual(calls.completed, [4]);
}

// Contract: fallback failure preserves the existing file and does not advance
// the page ACK watermark, so a later round can retry the item.
{
  const file = makeFile("notes/example.md", "keep-me");
  const { plugin, calls } = makePlugin(file, null);
  await receiveNoteSyncModify(incoming("h:restored"), plugin);
  assert.equal(file.content, "keep-me");
  assert.equal(calls.modified, 0);
  assert.deepEqual(calls.completed, []);
  assert.equal(plugin.noteSyncTasks.failed, 1);
}

// Contract: a vault adapter that returns a different body after modify must
// not advance the hash baseline or page ACK watermark.
{
  const file = makeFile("notes/example.md", "keep-me");
  const { plugin, calls } = makePlugin(file, {
    path: file.path,
    pathHash: "path-hash",
    content: "restored",
    contentHash: "h:restored",
    ctime: 1,
    mtime: 2,
    lastTime: 0,
  });
  const modify = plugin.app.vault.modify;
  plugin.app.vault.modify = async (target, _content, options) => modify(target, "corrupt", options);
  await receiveNoteSyncModify(incoming("h:restored"), plugin);
  assert.equal(file.content, "corrupt");
  assert.deepEqual(calls.completed, []);
  assert.equal(plugin.noteSyncTasks.failed, 1);
  assert.deepEqual(calls.setHash, []);
}

// Contract: a real empty note (hash of the empty string) does not trigger a
// network fallback and is still accepted as a successful sync item.
{
  const file = makeFile("notes/example.md", "old");
  const { plugin, calls } = makePlugin(file, null);
  await receiveNoteSyncModify(incoming("0"), plugin);
  assert.equal(calls.fetched, 0);
  assert.equal(file.content, "");
  assert.deepEqual(calls.completed, [4]);
}

// Contract: a zero-byte file whose durable hash claims non-empty content is
// repaired from the canonical note endpoint before the next sync scan.
{
  const file = makeFile("notes/example.md", "");
  const { plugin, calls } = makePlugin(file, {
    path: file.path,
    pathHash: "path-hash",
    content: "restored",
    contentHash: "h:restored",
    ctime: 1,
    mtime: 2,
    lastTime: 0,
  });
  plugin.fileHashManager.getZeroLengthNoteHashEntries = () => [{
    path: file.path,
    hash: "h:restored",
    mtime: 1,
    size: 0,
    ctime: 1,
  }];
  plugin.fileHashManager.getPathHash = () => "h:restored";
  const repaired = await repairSuspiciousEmptyNotes(plugin);
  assert.equal(repaired, 1);
  assert.equal(calls.fetched, 1);
  assert.equal(file.content, "restored");
  assert.deepEqual(calls.completed, []);
}

// Contract: NeedPush transport failure releases the slot but retains the
// pending hash and does not falsely complete the server download page.
{
  const file = makeFile("notes/example.md", "local");
  const { plugin, calls } = makePlugin(file, null);
  const pageIndex = 9;
  plugin.syncState.pendingNotePushPageIndex.set(file.path, pageIndex);
  await receiveNoteUpload({ path: file.path, pageIndex }, plugin);
  assert.equal(plugin.pendingNoteModifies.has(file.path), true);
  assert.equal(plugin.syncState.pendingNotePushPageIndex.has(file.path), false);
  assert.equal(calls.released, 1);
  assert.deepEqual(calls.completed, []);
}

console.log("operator-note-recovery.test.mjs: all scenarios passed");
