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

const { clearUploadQueue, receiveFileUpload } = module.exports;
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
    },
    loadLocalStorage: () => null,
    saveLocalStorage: () => undefined,
  },
  syncState: { pendingFilePushPageIndex: new Map() },
  concurrencyLimiter: {
    waitForSlot: () => new Promise((resolve) => pendingSlots.push(resolve)),
    releaseSlot: () => { releasedSlots++; },
  },
  totalChunksToUpload: 0,
  uploadedChunksCount: 0,
  pendingUploadHashes: new Map(),
  localStorageManager: { savePending: () => undefined },
  fileHashManager: { setFileHash: () => undefined },
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
clearUploadQueue();
pendingSlots[0]();
await new Promise((resolve) => setTimeout(resolve, 0));

// Contract: a transport reset cancels a queued upload before it reads the
// attachment or sends an old session to the replacement WebSocket.
assert.equal(readBinaryCalls, 0);
assert.equal(releasedSlots, 1);

console.log("upload-queue-cancel.test.mjs: all scenarios passed");
