import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "operator_file.ts");
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

const module = { exports: {} };
const requireStub = (id) => {
  switch (id) {
    case "obsidian":
      return {
        TFile,
        TAbstractFile,
        Platform: { isMobile: true },
        normalizePath: (value) => value,
      };
    case "../utils/helpers":
      return {
        hashContent: () => "hash",
        hashArrayBuffer: async () => "hash",
        getPluginDir: () => ".obsidian/plugins/fast-note-sync",
        dump: () => undefined,
        dumpError: () => undefined,
        sleep: async () => undefined,
        isPathExcluded: () => false,
        getSafeCtime: () => 1,
        isLargeBinarySyncRisk: () => false,
        describeBinarySyncLimit: () => "50 MB",
        showSyncNotice: () => undefined,
        checkAndNotifyCaseConflict: () => false,
        logMemorySnapshot: () => undefined,
        hashFileAsync: async () => "hash",
        vaultDelete: async () => undefined,
      };
    case "../storage/file_cloud_preview":
      return { FileCloudPreview: { isRestrictedType: () => false } };
    case "./sync_log_manager":
      return { SyncLogManager: { getInstance: () => ({ addOrUpdateLog: () => undefined, addLog: () => undefined }) } };
    case "../api/http_api_service":
      return { HttpApiService: class {} };
    default:
      throw new Error(`Unexpected require: ${id}`);
  }
};

vm.runInNewContext(transpiled, {
  require: requireStub,
  module,
  exports: module.exports,
  console,
  ArrayBuffer,
  Blob,
  DataView,
  TextDecoder,
  TextEncoder,
  window: { setTimeout, clearTimeout },
  setTimeout,
  clearTimeout,
}, { filename: sourcePath });

const {
  clearUploadQueue,
  receiveFileUpload,
  receiveFileUploadAck,
  receiveFileUploadSessionNotFound,
  resetFileDownloadSessions,
} = module.exports;
const file = new TFile();
file.path = "assets/test.bin";
file.extension = "bin";
file.stat = { size: 1, mtime: 1, ctime: 1 };

const pendingSlots = [];
let readBinaryCalls = 0;
let releasedSlots = 0;
const plugin = {
  settings: {
    syncEnabled: true,
    readonlySyncEnabled: false,
    binarySyncLimitEnabled: true,
    attachmentSyncLimit: 50,
  },
  app: {
    vault: {
      getName: () => "test-vault",
      getFileByPath: () => file,
      readBinary: async () => {
        readBinaryCalls++;
        throw new Error("old upload must not read after transport reset");
      },
      adapter: {
        exists: async () => true,
        rmdir: async () => undefined,
      },
    },
    loadLocalStorage: () => null,
    saveLocalStorage: () => undefined,
  },
  syncState: {
    pendingFilePushPageIndex: new Map(),
    pendingFileUploadAcks: new Set(),
  },
  concurrencyLimiter: {
    waitForSlot: () => new Promise((resolve) => pendingSlots.push(resolve)),
    releaseSlot: () => { releasedSlots++; },
  },
  totalChunksToUpload: 0,
  uploadedChunksCount: 0,
  pendingUploadHashes: new Map(),
  localStorageManager: { savePending: () => undefined },
  fileHashManager: { setFileHash: () => undefined, setLocalFileHash: () => undefined },
  recordSyncCompleted: () => undefined,
  websocket: { SendBinary: async () => "closed" },
};

await receiveFileUpload({
  path: file.path,
  pathHash: "hash",
  ctime: 1,
  mtime: 1,
  sessionId: "session-1",
  chunkSize: 1,
  pageIndex: 0,
}, plugin);

assert.equal(pendingSlots.length, 1);
plugin.syncState.pendingFileUploadAcks.add(file.path);
clearUploadQueue(plugin);
pendingSlots[0]();
await new Promise((resolve) => setTimeout(resolve, 0));

// Contract: a transport reset cancels a queued upload before it reads the
// attachment or sends an old session to the replacement WebSocket.
assert.equal(readBinaryCalls, 0);
assert.equal(releasedSlots, 1);
assert.equal(plugin.syncState.pendingFileUploadAcks.size, 0);

// Contract: a repeated FileUploadAck is idempotent. It must not release the
// same upload slot or advance the same sync page twice.
let uploadAckSetHashCalls = 0;
let uploadAckReleaseCalls = 0;
const uploadAckPages = [];
const uploadAckPlugin = {
  app: {
    vault: {
      getName: () => "test-vault",
      getFileByPath: () => file,
    },
  },
  pendingUploadHashes: new Map([[file.path, "content-hash"]]),
  syncState: {
    pendingFilePushPageIndex: new Map([[file.path, 7]]),
    pendingFileUploadAcks: new Set([file.path]),
  },
  fileHashManager: {
    setFileHash: () => { uploadAckSetHashCalls++; },
  },
  localStorageManager: {
    savePending: () => undefined,
    getMetadata: () => 0,
    setMetadata: () => undefined,
  },
  concurrencyLimiter: {
    releaseSlot: () => { uploadAckReleaseCalls++; },
  },
  recordSyncCompleted: (_type, pageIndex) => { uploadAckPages.push(pageIndex); },
};
receiveFileUploadAck({ path: file.path, pathHash: "hash", lastTime: 3 }, uploadAckPlugin);
receiveFileUploadAck({ path: file.path, pathHash: "hash", lastTime: 3 }, uploadAckPlugin);
assert.equal(uploadAckSetHashCalls, 1);
assert.equal(uploadAckReleaseCalls, 1);
assert.deepEqual(uploadAckPages, [7]);

// Contract: download sessions owned by the dead socket are discarded before a
// retry, so stale chunks cannot keep completion detection stuck forever.
const resetSlots = [];
const resetDirs = [];
const resetPlugin = {
  ...plugin,
  fileDownloadSessions: new Map([
    ["temp_assets/a.bin", {
      path: "assets/a.bin",
      ctime: 1,
      mtime: 1,
      lastTime: 0,
      sessionId: "",
      totalChunks: 2,
      size: 2,
      tempDir: ".obsidian/plugins/fast-note-sync/temp-chunks/init-a",
      initialSlotKey: "download_assets/a.bin",
    }],
    ["session-b", {
      path: "assets/b.bin",
      ctime: 1,
      mtime: 1,
      lastTime: 0,
      sessionId: "session-b",
      totalChunks: 2,
      size: 2,
      initialSlotKey: "download_assets/b.bin",
    }],
    ["session-c", {
      path: "assets/c.bin",
      ctime: 1,
      mtime: 1,
      lastTime: 0,
      sessionId: "session-c",
      totalChunks: 2,
      size: 2,
      tempDir: ".obsidian/plugins/fast-note-sync/temp-chunks/session-c",
      initialSlotKey: "download_assets/c.bin",
    }],
  ]),
  concurrencyLimiter: {
    waitForSlot: async () => undefined,
    releaseSlot: (key) => resetSlots.push(key),
  },
};
resetPlugin.app.vault.adapter = {
  exists: async () => true,
  rmdir: async (path) => resetDirs.push(path),
};
resetFileDownloadSessions(resetPlugin);
assert.equal(resetPlugin.fileDownloadSessions.size, 0);
assert.deepEqual(resetSlots.sort(), ["download_assets/a.bin", "download_assets/b.bin", "download_assets/c.bin"]);
await new Promise((resolve) => setTimeout(resolve, 0));
assert.deepEqual(resetDirs, [".obsidian/plugins/fast-note-sync/temp-chunks/session-c"]);

// Contract: a server-driven upload may cache the local content hash before the
// binary transfer completes, but it must not advance the server baseline until
// FileUploadAck arrives.
let localHashCalls = 0;
let serverHashCalls = 0;
let closedReleaseCalls = 0;
const uploadPlugin = {
  settings: {
    syncEnabled: true,
    readonlySyncEnabled: false,
    binarySyncLimitEnabled: true,
    attachmentSyncLimit: 50,
  },
  app: {
    vault: {
      getName: () => "test-vault",
      getFileByPath: () => file,
      readBinary: async () => new ArrayBuffer(1),
    },
    loadLocalStorage: () => null,
    saveLocalStorage: () => undefined,
  },
  syncState: {
    pendingFilePushPageIndex: new Map(),
    pendingFileUploadAcks: new Set(),
  },
  concurrencyLimiter: {
    waitForSlot: async () => undefined,
    releaseSlot: () => { closedReleaseCalls++; },
  },
  totalChunksToUpload: 0,
  uploadedChunksCount: 0,
  pendingUploadHashes: new Map(),
  localStorageManager: { savePending: () => undefined },
  fileHashManager: {
    setLocalFileHash: () => { localHashCalls++; },
    setFileHash: () => { serverHashCalls++; },
  },
  fileSyncTasks: { failed: 0 },
  recordSyncCompleted: () => undefined,
  websocket: { SendBinary: async () => "closed" },
};

await receiveFileUpload({
  path: file.path,
  pathHash: "hash-local-only",
  ctime: 1,
  mtime: 1,
  sessionId: "session-local-only",
  chunkSize: 1,
  pageIndex: 0,
}, uploadPlugin);
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(localHashCalls, 1);
assert.equal(serverHashCalls, 0);
assert.equal(closedReleaseCalls, 1);

// Contract: a repeated 463 for the same upload session is idempotent. It must
// release the slot and count the failed task only once.
let release463Calls = 0;
let completed463 = 0;
const pendingSlotResolvers = [];
const sessionPlugin = {
  ...uploadPlugin,
  concurrencyLimiter: {
    waitForSlot: () => new Promise((resolve) => pendingSlotResolvers.push(resolve)),
    releaseSlot: () => { release463Calls++; },
  },
  fileSyncTasks: { failed: 0 },
  recordSyncCompleted: () => { completed463++; },
  websocket: { SendBinary: async () => "closed" },
};

await receiveFileUpload({
  path: file.path,
  pathHash: "hash-463",
  ctime: 1,
  mtime: 1,
  sessionId: "session-463",
  chunkSize: 1,
  pageIndex: 0,
}, sessionPlugin);
assert.equal(pendingSlotResolvers.length, 1);
receiveFileUploadSessionNotFound("session-463", sessionPlugin);
receiveFileUploadSessionNotFound("session-463", sessionPlugin);
assert.equal(release463Calls, 1);
assert.equal(sessionPlugin.fileSyncTasks.failed, 1);
assert.equal(completed463, 1);
pendingSlotResolvers[0]();
await new Promise((resolve) => setTimeout(resolve, 0));

console.log("upload-queue-cancel.test.mjs: all scenarios passed");
