import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "operator_file.ts");
const source = fs.readFileSync(sourcePath, "utf8");

// operator_file uses window timers for retry/backoff. Keep these tests
// deterministic while retaining Node's real timers for microtask yielding.
let fakeNow = 0;
let nextTimerId = 1;
const fakeTimers = new Map();
const fakeSetTimeout = (callback, delay = 0) => {
  const id = nextTimerId++;
  fakeTimers.set(id, { callback, due: fakeNow + Math.max(0, delay) });
  return id;
};
const fakeClearTimeout = (id) => {
  fakeTimers.delete(id);
};
const flushMicrotasks = async () => {
  await Promise.resolve();
  await Promise.resolve();
};
const advanceTimers = async (milliseconds) => {
  fakeNow += milliseconds;
  while (true) {
    const due = [...fakeTimers.entries()]
      .filter(([, timer]) => timer.due <= fakeNow)
      .sort(([, left], [, right]) => left.due - right.due);
    if (due.length === 0) break;
    for (const [id, timer] of due) {
      if (!fakeTimers.delete(id)) continue;
      timer.callback();
      await flushMicrotasks();
    }
  }
};

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
    case "./background_activity_gate":
      return { waitForForeground: async () => true };
    case "./cloud_preview_reconciliation":
      return {
        advanceCloudPreviewCheckState: (state, pathName) => ({ ...state, nextPath: pathName }),
        completeCloudPreviewCheckState: (state) => ({ ...state, complete: true }),
        parseCloudPreviewCheckState: (_raw, mode) => ({ schema: 1, mode, nextPath: "", complete: false, updatedAt: 0 }),
        serializeCloudPreviewCheckState: (state) => JSON.stringify(state),
      };
    case "./stable_capture":
      return {
        captureStableSnapshot: async () => ({ hash: "hash", stat: { size: 1, mtime: 1, ctime: 1 } }),
        stableCaptureCoordinator: { capture: async (_key, task) => task() },
      };
    case "./sync_feature_policy":
      return { isCloudPreviewRuntimeEnabled: () => false };
    case "./vault_folder":
      return { createVaultFolderIdempotent: async () => "existing" };
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
  window: { setTimeout: fakeSetTimeout, clearTimeout: fakeClearTimeout },
  setTimeout,
  clearTimeout,
}, { filename: sourcePath });

const {
  clearUploadQueue,
  abortAllFileOperations,
  resetFileOperations,
  getActiveUploadCount,
  receiveFileUpload,
  receiveFileUploadAck,
  receiveFileUploadSessionNotFound,
  fileDeleteByPath,
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
  lockManager: { withLock: async (_path, task) => task() },
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
  lockManager: { withLock: async (_path, task) => task() },
  fileHashManager: {
    setLocalFileHash: () => { localHashCalls++; },
    setFileHash: () => { serverHashCalls++; },
    removeFileHash: () => undefined,
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

// Contract: a 463 invalidates only the current binary session. The logical
// upload remains pending, reissues FileUploadCheck with bounded retry, and
// does not count a failure before the replacement session is acknowledged.
let release463Calls = 0;
let completed463 = [];
let binarySendCalls = 0;
let resolveFirstBinarySend;
const retryChecks = [];
const sessionPlugin = {
  ...uploadPlugin,
  concurrencyLimiter: {
    waitForSlot: async () => undefined,
    releaseSlot: () => { release463Calls++; },
  },
  localStorageManager: {
    savePending: () => undefined,
    getMetadata: () => 0,
    setMetadata: () => undefined,
  },
  fileSyncTasks: { failed: 0 },
  recordSyncCompleted: (_type, pageIndex) => { completed463.push(pageIndex); },
  websocket: {
    SendMessage: async (action, payload) => {
      retryChecks.push({ action, payload });
      return "sent";
    },
    SendBinary: async (_frame, _prefix, before, after) => {
      if (before?.()) return "cancelled";
      binarySendCalls++;
      if (binarySendCalls === 1) {
        return new Promise((resolve) => { resolveFirstBinarySend = resolve; });
      }
      after?.();
      return "sent";
    },
  },
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
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(binarySendCalls, 1);
receiveFileUploadSessionNotFound("session-463", sessionPlugin);
receiveFileUploadSessionNotFound("session-463", sessionPlugin);
assert.equal(release463Calls, 1);
assert.equal(sessionPlugin.fileSyncTasks.failed, 0);
assert.deepEqual(completed463, []);
resolveFirstBinarySend("cancelled");
await new Promise((resolve) => setTimeout(resolve, 0));
await advanceTimers(500);
assert.equal(retryChecks.length, 1);
assert.equal(retryChecks[0].action, "FileUploadCheck");

await receiveFileUpload({
  path: file.path,
  pathHash: "hash-463",
  ctime: 1,
  mtime: 1,
  sessionId: "session-463-new",
  chunkSize: 1,
  pageIndex: 0,
}, sessionPlugin);
await new Promise((resolve) => setTimeout(resolve, 0));

// The retry is one logical task: total chunks are not double-counted and the
// old session's repeated 463 cannot cancel the replacement session.
assert.equal(binarySendCalls, 2);
assert.equal(sessionPlugin.totalChunksToUpload, 1);
assert.equal(sessionPlugin.fileSyncTasks.failed, 0);
assert.deepEqual(completed463, []);
receiveFileUploadSessionNotFound("session-463", sessionPlugin);
receiveFileUploadAck({ path: file.path, pathHash: "hash-463", lastTime: 3 }, sessionPlugin);
assert.equal(release463Calls, 2);
assert.equal(sessionPlugin.syncState.pendingFileUploadAcks.size, 0);
assert.equal(sessionPlugin.fileSyncTasks.failed, 0);
assert.deepEqual(completed463, [0]);

// Contract: a retry response timeout is bounded. Three FileUploadCheck
// attempts are allowed; after the third unanswered response the logical task
// is recorded as failed and every completion marker is drained exactly once.
let timeoutReleaseCalls = 0;
const timeoutChecks = [];
const timeoutCompleted = [];
const timeoutPlugin = {
  ...uploadPlugin,
  lockManager: { withLock: async (_path, task) => task() },
  concurrencyLimiter: {
    waitForSlot: async () => undefined,
    releaseSlot: () => { timeoutReleaseCalls++; },
  },
  localStorageManager: {
    savePending: () => undefined,
    getMetadata: () => 0,
    setMetadata: () => undefined,
  },
  pendingUploadHashes: new Map(),
  syncState: {
    pendingFilePushPageIndex: new Map(),
    pendingFileUploadAcks: new Set(),
  },
  fileSyncTasks: { failed: 0 },
  recordSyncCompleted: (_type, pageIndex) => { timeoutCompleted.push(pageIndex); },
  websocket: {
    SendMessage: async (action, payload) => {
      timeoutChecks.push({ action, payload });
      return "sent";
    },
    SendBinary: async (_frame, _prefix, before, after) => {
      if (before?.()) return "cancelled";
      after?.();
      return "sent";
    },
  },
};
await receiveFileUpload({
  path: file.path,
  pathHash: "hash-timeout",
  ctime: 1,
  mtime: 1,
  sessionId: "session-timeout",
  chunkSize: 1,
  pageIndex: 4,
}, timeoutPlugin);
await flushMicrotasks();
receiveFileUploadSessionNotFound("session-timeout", timeoutPlugin);
assert.equal(getActiveUploadCount(), 1);

await advanceTimers(500);
assert.equal(timeoutChecks.length, 1);
await advanceTimers(5000);
await advanceTimers(1000);
assert.equal(timeoutChecks.length, 2);
await advanceTimers(5000);
await advanceTimers(2000);
assert.equal(timeoutChecks.length, 3);
await advanceTimers(5000);
assert.equal(timeoutPlugin.fileSyncTasks.failed, 1);
assert.deepEqual(timeoutCompleted, [4]);
assert.equal(timeoutPlugin.syncState.pendingFileUploadAcks.size, 0);
assert.equal(timeoutPlugin.pendingUploadHashes.size, 0);
assert.equal(getActiveUploadCount(), 0);
assert.equal(timeoutReleaseCalls, 1);

// Contract: deleting a path cancels a scheduled session retry, so no retry
// check or delete request is emitted after the local file has disappeared.
let deleteRetryChecks = 0;
const deletePlugin = {
  ...uploadPlugin,
  lastSyncPathDeleted: new Set(),
  concurrencyLimiter: {
    waitForSlot: async () => undefined,
    releaseSlot: () => undefined,
  },
  localStorageManager: {
    savePending: () => undefined,
    getMetadata: () => 0,
    setMetadata: () => undefined,
  },
  pendingUploadHashes: new Map(),
  syncState: {
    pendingFilePushPageIndex: new Map(),
    pendingFileUploadAcks: new Set(),
  },
  websocket: {
    SendMessage: async () => {
      deleteRetryChecks++;
      return "sent";
    },
    SendBinary: async (_frame, _prefix, before, after) => {
      if (before?.()) return "cancelled";
      after?.();
      return "sent";
    },
  },
};
await receiveFileUpload({
  path: file.path,
  pathHash: "hash-delete-retry",
  ctime: 1,
  mtime: 1,
  sessionId: "session-delete-retry",
  chunkSize: 1,
  pageIndex: 5,
}, deletePlugin);
await flushMicrotasks();
receiveFileUploadSessionNotFound("session-delete-retry", deletePlugin);
await fileDeleteByPath(file.path, deletePlugin);
await advanceTimers(500);
assert.equal(deleteRetryChecks, 0);
assert.equal(deletePlugin.syncState.pendingFileUploadAcks.size, 0);
assert.equal(getActiveUploadCount(), 0);

// Contract: abortAllFileOperations clears retrying and queued state, releases
// acquired resources, and leaves no timer capable of sending on the old socket.
const abortPlugin = {
  ...deletePlugin,
  pendingUploadHashes: new Map(),
  syncState: {
    pendingFilePushPageIndex: new Map(),
    pendingFileUploadAcks: new Set(),
  },
};
await receiveFileUpload({
  path: file.path,
  pathHash: "hash-abort",
  ctime: 1,
  mtime: 1,
  sessionId: "session-abort",
  chunkSize: 1,
  pageIndex: 6,
}, abortPlugin);
await flushMicrotasks();
receiveFileUploadSessionNotFound("session-abort", abortPlugin);
assert.equal(getActiveUploadCount(), 1);
abortAllFileOperations(abortPlugin);
await advanceTimers(10000);
assert.equal(getActiveUploadCount(), 0);
assert.equal(abortPlugin.syncState.pendingFileUploadAcks.size, 0);
assert.equal(abortPlugin.totalChunksToUpload, 0);
resetFileOperations();

console.log("upload-queue-cancel.test.mjs: all scenarios passed");
