import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

// Contract under test: a binary file chunk that arrives BEFORE its download
// session finishes registering must be buffered and replayed once registration
// completes. The live failure (2026-08-27, .19) was: the announcement handler
// awaits temp-dir cleanup before inserting the session id, the first chunk
// loses the lookup race, gets dropped, and the file never materializes.

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
      return {
        SyncLogManager: {
          getInstance: () => ({
            addOrUpdateLog: () => undefined,
            addLog: () => undefined,
          }),
        },
      };
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

const { receiveFileSyncChunkDownload, handleFileChunkDownload } = module.exports;

const flushMicrotasks = async () => {
  for (let i = 0; i < 8; i++) await Promise.resolve();
};

const SESSION_ID = "11111111-2222-3333-4444-555555555555";
const makeFrame = (sessionId, chunkIndex, payload) => {
  const encoder = new TextEncoder();
  const idBytes = encoder.encode(sessionId);
  assert.equal(idBytes.length, 36, "session id must be 36 ascii bytes for the frame layout");
  const buf = new ArrayBuffer(40 + payload.byteLength);
  new Uint8Array(buf).set(idBytes, 0);
  const view = new DataView(buf, 36, 4);
  view.setUint32(0, chunkIndex, false);
  new Uint8Array(buf).set(new Uint8Array(payload), 40);
  return buf;
};

const makePlugin = () => {
  const adapterWrites = [];
  return {
    plugin: {
      settings: { syncEnabled: true },
      isSyncing: false,
      app: {
        vault: {
          getName: () => "test-vault",
          adapter: {
            exists: async () => false,
            mkdir: async () => undefined,
            rmdir: async () => undefined,
            writeBinary: async (p, data) => { adapterWrites.push([p, data]); },
          },
        },
        loadLocalStorage: () => null,
        saveLocalStorage: () => undefined,
      },
      fileDownloadSessions: new Map(),
      pendingFileChunks: new Map(),
      totalChunksToDownload: 0,
      downloadedChunksCount: 0,
    },
    adapterWrites,
  };
};

const announcement = {
  path: "原料池/wx/ early-chunk.jpg",
  pathHash: "ph",
  contentHash: "hash",
  ctime: 1,
  mtime: 1,
  sessionId: SESSION_ID,
  totalChunks: 2,
  size: 8, // small + isMobile => in-memory chunk storage, no temp dir chain
  chunkSize: 4,
  pageIndex: 0,
};

// Scenario 1: chunk arrives before the session is registered — it must be
// buffered, not dropped.
{
  const { plugin } = makePlugin();
  const frame = makeFrame(SESSION_ID, 0, new Uint8Array([1, 2, 3, 4]).buffer);
  await handleFileChunkDownload(frame, plugin);
  await flushMicrotasks();
  assert.equal(plugin.pendingFileChunks.size, 1, "early chunk must be buffered");
  assert.equal(plugin.pendingFileChunks.get(SESSION_ID).length, 1, "exactly one frame buffered");
  assert.equal(plugin.fileDownloadSessions.size, 0, "no session registered yet");
  assert.equal(plugin.downloadedChunksCount, 0, "buffered chunk is not counted as downloaded");
  console.log("scenario 1 ok: early chunk buffered, not dropped");
}

// Scenario 2: the announcement registers the session and replays the buffered
// chunk into it.
{
  const { plugin } = makePlugin();
  const frame = makeFrame(SESSION_ID, 0, new Uint8Array([9, 8, 7, 6]).buffer);
  await handleFileChunkDownload(frame, plugin);
  await flushMicrotasks();

  await receiveFileSyncChunkDownload(announcement, plugin);
  await flushMicrotasks();

  assert.ok(plugin.fileDownloadSessions.has(SESSION_ID), "session registered by announcement");
  assert.equal(plugin.pendingFileChunks.size, 0, "pending buffer drained after registration");
  const session = plugin.fileDownloadSessions.get(SESSION_ID);
  assert.ok(session.chunks?.has(0), "replayed chunk 0 landed in the session");
  assert.equal(plugin.downloadedChunksCount, 1, "replayed chunk counted once");
  console.log("scenario 2 ok: buffered chunk replayed into the registered session");
}

// Scenario 3: a chunk for a session that never registers must not buffer
// without bound (cap of 64 frames per session).
{
  const { plugin } = makePlugin();
  for (let i = 0; i < 70; i++) {
    await handleFileChunkDownload(makeFrame(SESSION_ID, i % 2, new Uint8Array([0, 0, 0, 1]).buffer), plugin);
    await flushMicrotasks();
  }
  assert.equal(plugin.pendingFileChunks.get(SESSION_ID).length, 64, "per-session buffer is capped at 64 frames");
  console.log("scenario 3 ok: unbounded buffering prevented");
}

console.log("file-chunk-early-buffer: all scenarios passed");
