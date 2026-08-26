import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");

function transpile(sourcePath) {
  return ts.transpileModule(fs.readFileSync(sourcePath, "utf8"), {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
    },
    fileName: sourcePath,
  }).outputText;
}

function loadModule(relative, context) {
  const sourcePath = path.join(root, relative);
  const module = { exports: {} };
  vm.runInNewContext(transpile(sourcePath), { module, exports: module.exports, ...context },
    { filename: sourcePath });
  return module.exports;
}

// The precondition module is loaded for real. Stubbing it here would let the
// wiring pass while the decision table is wrong, which is the exact failure
// mode this suite exists to prevent.
const precondition = loadModule("src/lib/sync/write_precondition.ts", {});

class TAbstractFile {}
class TFile extends TAbstractFile {}

const hashContent = (content) => (content.length === 0 ? "0" : `h:${content}`);

let serverState = null;   // { contentHash, content } or null for unreachable
let serverCalls = [];

const operatorPath = path.join(root, "src", "lib", "sync", "operator_note.ts");
const operatorModule = { exports: {} };

const requireStub = (id) => {
  if (id === "obsidian") return { TFile, TAbstractFile, normalizePath: (v) => v };
  if (id === "../utils/types") return {};
  if (id === "../utils/helpers") return {
    hashContent,
    hashContentAsync: async (content) => hashContent(content),
    dump: () => undefined,
    dumpError: () => undefined,
    isPathExcluded: () => false,
    getSafeCtime: () => 1,
    vaultDelete: async () => undefined,
    checkAndNotifyCaseConflict: () => false,
    getPluginDir: () => ".obsidian/plugins/fast-note-sync",
    showSyncNotice: (message) => { notices.push(message); },
  };
  if (id === "./sync_log_manager") return {
    SyncLogManager: { getInstance: () => ({ addLog: () => undefined, addOrUpdateLog: () => undefined }) },
  };
  if (id === "../../main") return {};
  if (id === "./background_activity_gate") return { waitForForeground: async () => true };
  if (id === "./stable_capture") return {
    captureStableSnapshot: async (options) => {
      const value = await options.read();
      return { value, hash: await options.hash(value), stat: { size: value.length, mtime: 7, ctime: 1 } };
    },
    stableCaptureCoordinator: { capture: async (_key, task) => task() },
  };
  if (id === "../api/http_api_service") return {
    HttpApiService: class {
      async getNoteContent(notePath) {
        serverCalls.push(notePath);
        return serverState;
      }
    },
  };
  if (id === "./write_precondition") return precondition;
  throw new Error(`Unexpected require: ${id}`);
};

const notices = [];

vm.runInNewContext(transpile(operatorPath), {
  require: requireStub,
  module: operatorModule,
  exports: operatorModule.exports,
  console,
  Promise, Set, Map, JSON, Date, Math,
  Uint8Array, ArrayBuffer, Blob, DataView, TextDecoder,
  window: { setTimeout: (callback) => { callback(); return 1; } },
}, { filename: operatorPath });

const { noteModify } = operatorModule.exports;

function makePlugin({ localContent, baseHash }) {
  const written = new Map();
  const plugin = {
    settings: {
      vault: "V",
      syncEnabled: true,
      readonlySyncEnabled: false,
      writePreconditionEnabled: true,
    },
    sent: [],
    baselineWrites: [],
    written,
    app: {
      vault: {
        read: async () => localContent,
        adapter: {
          stat: async () => ({ size: localContent.length, mtime: 7, ctime: 1 }),
          exists: async (p) => written.has(p),
          mkdir: async (p) => { written.set(p, "<dir>"); },
          write: async (p, data) => { written.set(p, data); },
        },
        getFileByPath: () => null,
      },
    },
    isIgnoredFile: () => false,
    addIgnoredFile: () => undefined,
    removeIgnoredFile: () => undefined,
    fileHashManager: {
      getPathHash: () => baseHash,
      getValidHash: () => null,
      setFileHash: (p, h) => { plugin.baselineWrites.push([p, h]); },
    },
    lastSyncMtime: new Map(),
    pendingNoteModifies: new Map(),
    pendingNoteDeleteAcks: new Set(),
    lockManager: { withLock: async (_p, task) => task() },
    localStorageManager: { savePending: () => undefined },
    incrementalScanManager: { markSent: () => undefined, acknowledge: () => undefined },
    concurrencyLimiter: { waitForSlot: async () => undefined, releaseSlot: () => undefined },
    syncState: { conflictedPaths: new Set() },
    websocket: {
      SendMessage: async (action, data) => { plugin.sent.push([action, data.path]); return "sent"; },
    },
  };
  return plugin;
}

function makeFile(p) {
  const file = new TFile();
  file.path = p;
  file.stat = { size: 1, mtime: 7, ctime: 1 };
  return file;
}

const NOTE = "notes/a.md";

// Contract: THE defect. The server moved since this device's baseline and the
// local content differs from it. The upload must be withheld, both versions
// preserved, and the path registered as conflicted.
{
  serverState = { contentHash: "h:server-edit", content: "server-edit" };
  serverCalls = [];
  notices.length = 0;
  const plugin = makePlugin({ localContent: "local-edit", baseHash: "h:ancestor" });
  await noteModify(makeFile(NOTE), plugin);

  assert.deepEqual(plugin.sent, [], "a diverged path must not be uploaded");
  assert.deepEqual(serverCalls, [NOTE]);
  assert.ok(plugin.syncState.conflictedPaths.has(NOTE));
  const remote = [...plugin.written.entries()].find(([k]) => k.endsWith(".remote.md"));
  const base = [...plugin.written.entries()].find(([k]) => k.endsWith(".base.md"));
  assert.equal(remote?.[1], "server-edit", "the server version must be preserved");
  assert.equal(base?.[1], "local-edit", "the local version must be preserved");
  assert.equal(notices.length, 1, "the user must be told once");
}

// Contract: the server still holds this device's baseline, so this device owns
// the next version and the upload proceeds as before.
{
  serverState = { contentHash: "h:ancestor", content: "ancestor" };
  serverCalls = [];
  const plugin = makePlugin({ localContent: "local-edit", baseHash: "h:ancestor" });
  await noteModify(makeFile(NOTE), plugin);
  assert.deepEqual(plugin.sent, [["NoteModify", NOTE]]);
  assert.equal(plugin.syncState.conflictedPaths.size, 0);
}

// Contract: an unreachable server fails open. The guard must never convert a
// probe failure into a stalled upload.
{
  serverState = null;
  serverCalls = [];
  const plugin = makePlugin({ localContent: "local-edit", baseHash: "h:ancestor" });
  await noteModify(makeFile(NOTE), plugin);
  assert.deepEqual(serverCalls, [NOTE]);
  assert.deepEqual(plugin.sent, [["NoteModify", NOTE]], "probe failure must not block the upload");
}

// Contract: the server already holds our exact content, so the round is
// skipped and the baseline advances to the state just read back from it.
{
  serverState = { contentHash: "h:local-edit", content: "local-edit" };
  serverCalls = [];
  const plugin = makePlugin({ localContent: "local-edit", baseHash: "h:ancestor" });
  await noteModify(makeFile(NOTE), plugin);
  assert.deepEqual(plugin.sent, [], "re-sending content the server already has is write amplification");
  assert.deepEqual(plugin.baselineWrites, [[NOTE, "h:local-edit"]]);
}

// Contract: with no baseline the guard costs nothing. A first upload must not
// spend a round trip and must not be reported as a conflict.
{
  serverState = { contentHash: "h:whatever", content: "whatever" };
  serverCalls = [];
  const plugin = makePlugin({ localContent: "local-edit", baseHash: null });
  await noteModify(makeFile(NOTE), plugin);
  assert.deepEqual(serverCalls, [], "no baseline means no probe");
  assert.deepEqual(plugin.sent, [["NoteModify", NOTE]]);
}

// Contract: the kill switch removes both the probe and the guard, restoring
// the pre-M7 upload path exactly.
{
  serverState = { contentHash: "h:server-edit", content: "server-edit" };
  serverCalls = [];
  const plugin = makePlugin({ localContent: "local-edit", baseHash: "h:ancestor" });
  plugin.settings.writePreconditionEnabled = false;
  await noteModify(makeFile(NOTE), plugin);
  assert.deepEqual(serverCalls, []);
  assert.deepEqual(plugin.sent, [["NoteModify", NOTE]]);
}

console.log("note-write-precondition.test.mjs: all scenarios passed");
