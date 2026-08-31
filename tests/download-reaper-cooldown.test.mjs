import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

// Contracts under test:
// 1. A download session with no activity beyond the reaper age is failed, removed
//    from the session map, and its path enters the cooldown list. (Orphaned
//    sessions block allDownloadsComplete forever — the endless same-batch
//    re-sync loop observed on iPad 2026-08-27.)
// 2. A session with recent activity is left alone.
// 3. While a path is cooling down, isDownloadCoolingDown() is true and expires
//    after the deadline.

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
    case "../utils/types":
      return {};
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
  window: { setTimeout, clearTimeout },
  setTimeout,
  clearTimeout,
}, { filename: sourcePath });

const { reapStaleFileDownloadSessions, isDownloadCoolingDown } = module.exports;

const makePlugin = () => ({
  settings: { syncEnabled: true },
  app: {
    vault: {
      getName: () => "test-vault",
      adapter: { exists: async () => false, rmdir: async () => undefined },
    },
    loadLocalStorage: () => null,
    saveLocalStorage: () => undefined,
  },
  fileDownloadSessions: new Map(),
  pendingFileChunks: new Map(),
  downloadCooldownPaths: new Map(),
  fileSyncTasks: { failed: 0 },
  recordSyncCompleted: () => undefined,
  concurrencyLimiter: { releaseSlot: () => undefined },
});

const session = (sessionId, path, lastActivityAt) => ({
  path,
  ctime: 1,
  mtime: 1,
  lastTime: 0,
  sessionId,
  totalChunks: 2,
  size: 8,
  pageIndex: 0,
  initialSlotKey: `download_${path}`,
  lastActivityAt,
});

// Scenario 1: stale session is reaped; fresh session survives; the failed path
// enters cooldown.
{
  const plugin = makePlugin();
  const now = Date.now();
  plugin.fileDownloadSessions.set("stale-1", session("stale-1", "a/stale.bin", now - 300000));
  plugin.fileDownloadSessions.set("fresh-1", session("fresh-1", "a/fresh.bin", now - 1000));

  const reaped = await reapStaleFileDownloadSessions(plugin, 120000);
  assert.equal(reaped, 1, "only the stale session is reaped");
  assert.equal(plugin.fileDownloadSessions.size, 1, "fresh session survives");
  assert.ok(plugin.fileDownloadSessions.has("fresh-1"));
  assert.equal(plugin.fileSyncTasks.failed, 1, "reaped session counted as failure");
  assert.ok(isDownloadCoolingDown(plugin, "a/stale.bin"), "reaped path enters cooldown");
  assert.equal(isDownloadCoolingDown(plugin, "a/fresh.bin"), false, "fresh path not in cooldown");
  console.log("scenario 1 ok: stale session reaped, fresh survives, cooldown set");
}

// Scenario 2: cooldown expires and the path becomes eligible for retry.
{
  const plugin = makePlugin();
  plugin.downloadCooldownPaths.set("a/stale.bin", Date.now() - 1);
  assert.equal(isDownloadCoolingDown(plugin, "a/stale.bin"), false, "expired cooldown no longer blocks");
  assert.equal(plugin.downloadCooldownPaths.has("a/stale.bin"), false, "expired entry cleaned up");
  console.log("scenario 2 ok: cooldown expiry unblocks retry");
}

// Scenario 3: sessions created before this patch (no lastActivityAt) are reaped
// immediately — they are exactly the orphans this feature targets.
{
  const plugin = makePlugin();
  plugin.fileDownloadSessions.set("legacy-1", { ...session("legacy-1", "a/legacy.bin", undefined), lastActivityAt: undefined });
  const reaped = await reapStaleFileDownloadSessions(plugin, 120000);
  assert.equal(reaped, 1, "legacy session without activity stamp is reaped");
  assert.equal(plugin.fileDownloadSessions.size, 0);
  console.log("scenario 3 ok: legacy unstamped session reaped");
}

console.log("download-reaper-cooldown: all scenarios passed");
