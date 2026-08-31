import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");

function transpile(sourcePath) {
  const source = fs.readFileSync(sourcePath, "utf8");
  return ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
    },
    fileName: sourcePath,
  }).outputText;
}

function loadCloudPreviewStateModule() {
  const sourcePath = path.join(root, "src", "lib", "sync", "cloud_preview_reconciliation.ts");
  const module = { exports: {} };
  vm.runInNewContext(transpile(sourcePath), {
    module,
    exports: module.exports,
    JSON,
    Date,
  }, { filename: sourcePath });
  return module.exports;
}

const stateModule = loadCloudPreviewStateModule();
const operatorPath = path.join(root, "src", "lib", "sync", "operator_file.ts");
const operatorModule = { exports: {} };
const TFile = class {};

function loadOperatorDependency(id) {
  if (id === "./cloud_preview_reconciliation") return stateModule;
  throw new Error(`Unexpected dependency: ${id}`);
}

const requireStub = (id) => {
  if (id === "obsidian") return {
    TFile,
    TAbstractFile: class {},
    normalizePath: (value) => value,
    Platform: { isMobile: false },
  };
  if (id === "../utils/types") return {};
  if (id === "../utils/helpers") return {
    hashContent: (value) => `hash:${value}`,
    hashArrayBuffer: () => "array-hash",
    getPluginDir: () => ".obsidian/plugins/fast-note-sync",
    dump: () => undefined,
    dumpError: () => undefined,
    sleep: async () => undefined,
    isPathExcluded: () => false,
    getSafeCtime: () => 1,
    isLargeBinarySyncRisk: () => false,
    describeBinarySyncLimit: () => "test limit",
    showSyncNotice: () => undefined,
    checkAndNotifyCaseConflict: () => false,
    logMemorySnapshot: () => undefined,
    hashFileAsync: async () => "file-hash",
    vaultDelete: async () => undefined,
  };
  if (id === "../storage/file_cloud_preview") return {
    FileCloudPreview: { isRestrictedType: (extension) => [".jpg", ".png", ".pdf"].includes(extension) },
  };
  if (id === "./sync_log_manager") return {
    SyncLogManager: { getInstance: () => ({ addOrUpdateLog: () => undefined }) },
  };
  if (id === "../api/http_api_service") return {
    HttpApiService: class {
      constructor(plugin) { this.plugin = plugin; }
      async getFileInfo(filePath) {
        this.plugin.calls.info.push(filePath);
        if (this.plugin.failPath === filePath) throw new Error("temporary network failure");
        if (this.plugin.missingPaths.has(filePath)) return null;
        return { path: filePath };
      }
    },
  };
  if (id === "../../main") return {};
  if (id === "./background_activity_gate") return { waitForForeground: async () => true };
  if (id === "./vault_folder") return { createVaultFolderIdempotent: async () => "existing" };
  if (id === "./stable_capture") return {
    captureStableSnapshot: async () => ({ hash: "file-hash", stat: { size: 10, mtime: 1, ctime: 1 } }),
    stableCaptureCoordinator: { capture: async () => ({ hash: "file-hash", stat: { size: 10, mtime: 1, ctime: 1 } }) },
  };
  if (id === "./sync_feature_policy") return {
    isCloudPreviewRuntimeEnabled: (settings) => settings.cloudPreviewEnabled === true,
  };
  if (id === "./cloud_preview_reconciliation") return loadOperatorDependency(id);
  throw new Error(`Unexpected dependency: ${id}`);
};

vm.runInNewContext(transpile(operatorPath), {
  require: requireStub,
  module: operatorModule,
  exports: operatorModule.exports,
  console,
  Promise,
  Map,
  Set,
  Uint8Array,
  ArrayBuffer,
  Blob,
  DataView,
  TextDecoder,
  JSON,
  Date,
  Math,
  window: { setTimeout: (callback) => { callback(); return 1; } },
}, { filename: operatorPath });

const { checkAndUploadAttachments } = operatorModule.exports;

function makeFile(filePath) {
  const file = new TFile();
  file.path = filePath;
  file.extension = path.posix.extname(filePath).slice(1);
  file.stat = { size: 10, mtime: 1, ctime: 1 };
  return file;
}

function makePlugin(files) {
  let state = "";
  const plugin = {
    settings: {
      cloudPreviewEnabled: true,
      cloudPreviewTypeRestricted: false,
      readonlySyncEnabled: false,
    },
    calls: { getFiles: 0, info: [], writes: [], uploads: [] },
    app: { vault: { getFiles: () => { plugin.calls.getFiles++; return files; } } },
    lockManager: { withLock: async (_path, task) => task() },
    addIgnoredFile: () => undefined,
    removeIgnoredFile: () => undefined,
    fileHashManager: {
      getPathHash: () => null,
      getValidHash: () => null,
    },
    lastSyncMtime: new Map(),
    pendingFileDeleteAcks: new Set(),
    pendingUploadHashes: new Map(),
    concurrencyLimiter: { waitForSlot: async () => undefined },
    websocket: {
      SendMessage: async (_action, data) => {
        plugin.calls.uploads.push(data.path);
        return "sent";
      },
    },
    localStorageManager: {
      getMetadata: () => state,
      setMetadata: (_key, value) => { state = value; plugin.calls.writes.push(value); },
      savePending: () => undefined,
    },
    failPath: null,
    missingPaths: new Set(),
  };
  plugin.settings.syncEnabled = true;
  return plugin;
}

// Contract: the initial cloud-preview reconciliation is durable and never
// performs another vault enumeration after it reaches the complete state.
{
  const plugin = makePlugin([makeFile("b/image.jpg"), makeFile("a/image.jpg")]);
  // This is the post-mapping result of the real { code: 0, details:
  // "record not found" } envelope returned by /api/file/info.
  plugin.missingPaths = new Set(["a/image.jpg", "b/image.jpg"]);
  await checkAndUploadAttachments(plugin);
  assert.deepEqual(plugin.calls.info, ["a/image.jpg", "b/image.jpg"]);
  assert.deepEqual(plugin.calls.uploads, ["a/image.jpg", "b/image.jpg"]);
  assert.equal(plugin.calls.getFiles, 1);
  const completedState = JSON.parse(plugin.calls.writes.at(-1));
  assert.equal(completedState.complete, true);

  await checkAndUploadAttachments(plugin);
  assert.equal(plugin.calls.getFiles, 1);
  assert.deepEqual(plugin.calls.info, ["a/image.jpg", "b/image.jpg"]);
}

// Contract: sync rounds that finish concurrently share one repair pass and do
// not enumerate or query the same attachment set twice.
{
  const plugin = makePlugin([makeFile("a/image.jpg"), makeFile("b/image.jpg")]);
  const first = checkAndUploadAttachments(plugin);
  const second = checkAndUploadAttachments(plugin);
  await Promise.all([first, second]);
  assert.equal(plugin.calls.getFiles, 1);
  assert.deepEqual(plugin.calls.info, ["a/image.jpg", "b/image.jpg"]);
}

// Contract: a failed check keeps the cursor before the failed path; the next
// invocation resumes there instead of rechecking the already-confirmed prefix.
{
  const plugin = makePlugin([makeFile("a/image.jpg"), makeFile("b/image.jpg"), makeFile("c/image.jpg")]);
  plugin.failPath = "b/image.jpg";
  await checkAndUploadAttachments(plugin);
  assert.deepEqual(plugin.calls.info, ["a/image.jpg", "b/image.jpg"]);
  const pausedState = JSON.parse(plugin.calls.writes.at(-1));
  assert.equal(pausedState.complete, false);
  assert.equal(pausedState.nextPath, "a/image.jpg");

  plugin.failPath = null;
  await checkAndUploadAttachments(plugin);
  assert.deepEqual(plugin.calls.info, ["a/image.jpg", "b/image.jpg", "b/image.jpg", "c/image.jpg"]);
  const resumedState = JSON.parse(plugin.calls.writes.at(-1));
  assert.equal(resumedState.complete, true);
}

console.log("cloud-preview-reconciliation.test.mjs: all scenarios passed");
