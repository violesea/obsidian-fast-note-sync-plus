// Runtime contracts for change-feed page guards and failed materialization.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "change_feed.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const logicPath = path.join(root, "src", "lib", "sync", "change_feed_logic.ts");
const logicSource = fs.readFileSync(logicPath, "utf8");
const logicTranspiled = ts.transpileModule(logicSource, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: logicPath,
}).outputText;

class FakeTFile {
  constructor(pathName) {
    this.path = pathName;
    this.stat = { size: 0 };
  }
}

class FakeTFolder {
  constructor(pathName) {
    this.path = pathName;
    this.children = [];
  }
}

function loadChangeFeed(requestUrl, deleteResult = true) {
  const module = { exports: {} };
  vm.runInNewContext(transpiled, {
    require: (id) => {
      if (id === "obsidian") {
        return {
          Platform: { isIosApp: false, isTablet: false, isAndroidApp: false, isDesktopApp: true },
          TFile: FakeTFile,
          TFolder: FakeTFolder,
          normalizePath: (value) => value,
          requestUrl,
        };
      }
      if (id === "../utils/helpers") {
        return {
          dump: () => undefined,
          hashArrayBuffer: async () => "binary-hash",
          hashContent: () => "path-hash",
          hashContentAsync: async () => "remote-hash",
          hashFileAsync: async () => "local-hash",
          isPathExcluded: () => false,
          LocalStateFileMirror: class {
            constructor(plugin) {
              this.plugin = plugin;
            }
            async read() {
              return this.plugin.__cursorMirror ?? null;
            }
            scheduleWrite(value) {
              this.plugin.__cursorMirror = value;
            }
            flush() {}
          },
        };
      }
      if (id === "../utils/types") return { CLIENT_TYPE: "ObsidianPlugin" };
      if (id === "./operator_note") return { receiveNoteSyncDelete: async () => deleteResult };
      if (id === "./operator_file") return { receiveFileSyncDelete: async () => deleteResult };
      if (id === "./background_activity_gate") return { requireForeground: async () => undefined };
      if (id === "./vault_folder") return {
        createVaultFolderIdempotent: async (vault, folderPath) => {
          await vault.createFolder(folderPath);
          return "created";
        },
      };
      if (id === "./sync_feature_policy") return {
        isChangeFeedRuntimeEnabled: () => false,
        isCloudPreviewRuntimeEnabled: () => false,
      };
      if (id === "./change_feed_logic") {
        const logicModule = { exports: {} };
        vm.runInNewContext(logicTranspiled, {
          module: logicModule,
          exports: logicModule.exports,
          Date,
          Math,
        }, { filename: logicPath });
        return logicModule.exports;
      }
      if (id === "../../main") return {};
      throw new Error(`Unexpected require: ${id}`);
    },
    module,
    exports: module.exports,
    console,
    Date,
    Math,
    Promise,
    window: { setTimeout: () => 0 },
  }, { filename: sourcePath });
  return module.exports;
}

function makeCatchUpPlugin(cursorState) {
  let setRevCalls = 0;
  let completeRepairCalls = 0;
  const plugin = {
    settings: {
      sidecarUrl: "http://127.0.0.1:9100",
      sidecarToken: "test-token",
      vault: "New-World",
      cloudPreviewEnabled: false,
      cloudPreviewTypeRestricted: false,
    },
    changeFeedDeviceId: "ipad-device",
    changeFeedCursor: {
      get: () => cursorState,
      initialize: async () => undefined,
      setRev: (rev) => {
        setRevCalls++;
        cursorState.rev = rev;
      },
      completeRepair: () => { completeRepairCalls++; },
    },
    syncState: { transportResetPending: false, activeSyncContext: "", conflictedPaths: new Set() },
    pendingNoteModifies: new Map(),
    localStorageManager: {
      getMetadata: () => 0,
      setMetadata: () => undefined,
    },
    app: {
      vault: {
        getAbstractFileByPath: () => null,
        getFileByPath: () => null,
      },
    },
    fileHashManager: { getPathHash: () => null },
    api: { getNoteContent: async () => null },
    manifest: { version: "2.5.10" },
    getClientName: () => "iPad",
  };
  return { plugin, getSetRevCalls: () => setRevCalls, getCompleteRepairCalls: () => completeRepairCalls };
}

const pageRequests = [];
const changeFeed = loadChangeFeed(async ({ url }) => {
  const since = Number(new URL(url).searchParams.get("since_rev"));
  pageRequests.push(since);
  return {
    status: 200,
    json: {
      ok: true,
      data: {
        changes: [{ rev: since + 1, type: "folder", action: "create", path: `folder-${since + 1}` }],
        next_rev: since + 1,
        has_more: true,
        safe_rev: since + 100,
        min_available_rev: 0,
        collapsed: true,
        scanned: 1,
      },
    },
  };
});
const pageCursor = {
  schema: 2,
  deviceId: "ipad-device",
  vault: "New-World",
  rev: 460000,
  noteWatermarkMs: 0,
  fileWatermarkMs: 0,
  updatedAt: 0,
  repairPending: true,
};
const pageHarness = makeCatchUpPlugin(pageCursor);
const pageResult = await changeFeed.runChangeFeedCatchUp(pageHarness.plugin);
assert.equal(pageResult.ok, false);
assert.equal(pageResult.reason, "page_limit");
assert.equal(pageRequests.length, 40);
assert.equal(pageHarness.getSetRevCalls(), 40);
assert.equal(pageHarness.getCompleteRepairCalls(), 0);

// Contract: a non-advancing page is detected immediately rather than being
// fetched repeatedly until the page budget is exhausted.
const stalledFeed = loadChangeFeed(async () => ({
  status: 200,
  json: {
    ok: true,
    data: {
      changes: [{ rev: 460000, type: "folder", action: "create", path: "stalled" }],
      next_rev: 460000,
      has_more: true,
      safe_rev: 460001,
      min_available_rev: 0,
      collapsed: true,
      scanned: 1,
    },
  },
}));
const stalledCursor = { ...pageCursor };
const stalledHarness = makeCatchUpPlugin(stalledCursor);
const stalledResult = await stalledFeed.runChangeFeedCatchUp(stalledHarness.plugin);
assert.equal(stalledResult.ok, false);
assert.equal(stalledResult.reason, "cursor_stalled");
assert.equal(stalledHarness.getSetRevCalls(), 0);

// Contract: a delete handler that could not mutate the local vault must keep
// the page cursor and repair marker unchanged. A swallowed delete exception is
// not evidence that the remote tombstone was projected.
const failedDeleteFeed = loadChangeFeed(async ({ url }) => {
  const since = Number(new URL(url).searchParams.get("since_rev"));
  return {
    status: 200,
    json: {
      ok: true,
      data: {
        changes: [{
          rev: since + 1,
          type: "note",
          action: "delete",
          path: "deleted-but-still-local.md",
          content_hash: "",
          path_hash: "path-hash",
          size: 0,
          mtime: 1,
          ctime: 1,
        }],
        next_rev: since + 1,
        has_more: false,
        safe_rev: since + 1,
        min_available_rev: 0,
        collapsed: true,
        scanned: 1,
      },
    },
  };
}, false);
const failedDeleteCursor = { ...pageCursor, rev: 465000, repairPending: true };
const failedDeleteHarness = makeCatchUpPlugin(failedDeleteCursor);
failedDeleteHarness.plugin.app.vault.getAbstractFileByPath = () => ({ path: "deleted-but-still-local.md" });
const failedDeleteResult = await failedDeleteFeed.runChangeFeedCatchUp(failedDeleteHarness.plugin);
assert.equal(failedDeleteResult.ok, false);
assert.equal(failedDeleteResult.reason, "materialization_failed");
assert.equal(failedDeleteResult.failures, 1);
assert.equal(failedDeleteHarness.getSetRevCalls(), 0);
assert.equal(failedDeleteHarness.getCompleteRepairCalls(), 0);

// Contract: two callers for the same plugin cannot register/poll the same
// cursor concurrently. The second caller must share the in-flight operation.
let concurrentRequests = 0;
const concurrentFeed = loadChangeFeed(async ({ url }) => {
  if (url.includes("/vault/changes")) concurrentRequests++;
  await new Promise((resolve) => setTimeout(resolve, 10));
  const since = Number(new URL(url).searchParams.get("since_rev"));
  return {
    status: 200,
    json: {
      ok: true,
      data: {
        changes: [],
        next_rev: since,
        has_more: false,
        safe_rev: since,
        min_available_rev: 0,
        collapsed: true,
        scanned: 0,
      },
    },
  };
});
const concurrentCursor = { ...pageCursor, rev: 470000, repairPending: false };
const concurrentHarness = makeCatchUpPlugin(concurrentCursor);
const concurrentResults = await Promise.all([
  concurrentFeed.runChangeFeedCatchUp(concurrentHarness.plugin, "same-context"),
  concurrentFeed.runChangeFeedCatchUp(concurrentHarness.plugin, "same-context"),
]);
assert.equal(concurrentRequests, 1, "same-context catch-up must be single-flight");
assert.equal(concurrentResults[0].ok, true, JSON.stringify(concurrentResults));
assert.equal(concurrentResults[1].ok, true, JSON.stringify(concurrentResults));

// Contract: localStorage and the mirror are replicas. Initialization chooses
// the highest cursor and merges monotonic watermarks/repair state.
const cursorLocalStorage = new Map([
  ["fns-changeFeedCursor", JSON.stringify({
    schema: 2,
    deviceId: "ipad-device",
    vault: "New-World",
    rev: 470010,
    noteWatermarkMs: 10,
    fileWatermarkMs: 20,
    updatedAt: 10,
    repairPending: false,
  })],
]);
const cursorPlugin = {
  changeFeedDeviceId: "ipad-device",
  settings: { vault: "New-World" },
  __cursorMirror: JSON.stringify({
    schema: 2,
    deviceId: "ipad-device",
    vault: "New-World",
    rev: 470020,
    noteWatermarkMs: 30,
    fileWatermarkMs: 15,
    updatedAt: 20,
    repairPending: true,
  }),
  app: {
    loadLocalStorage: (key) => cursorLocalStorage.get(key) ?? null,
    saveLocalStorage: (key, value) => {
      if (value === null || value === undefined) cursorLocalStorage.delete(key);
      else cursorLocalStorage.set(key, String(value));
    },
  },
};
const CursorStore = concurrentFeed.ChangeFeedCursorStore;
const cursorStore = new CursorStore(cursorPlugin);
const cursorInit = cursorStore.initialize();
const cursorInitAgain = cursorStore.initialize();
assert.equal(cursorInit, cursorInitAgain, "cursor initialization must be single-flight");
await cursorInit;
assert.equal(cursorStore.get().rev, 470020);
assert.equal(cursorStore.get().noteWatermarkMs, 30);
assert.equal(cursorStore.get().fileWatermarkMs, 20);
assert.equal(cursorStore.get().repairPending, true);
cursorStore.adopt("ipad-device", "New-World", 470000);
assert.equal(cursorStore.get().rev, 470020, "late register result must not move cursor backwards");
cursorStore.setRev(470010);
assert.equal(cursorStore.get().rev, 470020, "stale page result must not move cursor backwards");
cursorStore.adopt("ipad-device", "New-World", 470030);
assert.equal(cursorStore.get().rev, 470030);

// Contract: a failed note write removes only folders created by that call.
const changeFeedForWrite = loadChangeFeed(async () => ({ status: 200, json: {} }));
const folders = new Map();
const deletedFolders = [];
const writePlugin = {
  settings: { cloudPreviewEnabled: false, cloudPreviewTypeRestricted: false },
  app: {
    fileManager: {
      trashFile: async (folder) => {
        deletedFolders.push(folder.path);
        folders.delete(folder.path);
      },
    },
    vault: {
      getFolderByPath: (folderPath) => folders.get(folderPath) ?? null,
      getFileByPath: () => null,
      getAbstractFileByPath: () => null,
      createFolder: async (folderPath) => {
        const parts = folderPath.split("/");
        for (let index = 0; index < parts.length; index++) {
          const candidate = parts.slice(0, index + 1).join("/");
          if (!folders.has(candidate)) folders.set(candidate, new FakeTFolder(candidate));
        }
      },
      create: async () => { throw new Error("simulated note write failure"); },
    },
  },
  fileHashManager: { setFileHash: () => undefined },
  lastSyncMtime: new Map(),
  addIgnoredFile: () => undefined,
  removeIgnoredFile: () => undefined,
};

await assert.rejects(
  changeFeedForWrite.applyRemoteNote(writePlugin, "daily/reports/failed.md", {
    content: "body",
    contentHash: "remote-hash",
    mtime: 1,
    ctime: 1,
  }),
  /simulated note write failure/,
);
assert.deepEqual(deletedFolders, ["daily/reports", "daily"]);

// Contract: a binary materialization must verify the server-advertised hash
// before it writes bytes or records a local baseline.
const binaryPlugin = {
  settings: { cloudPreviewEnabled: false, cloudPreviewTypeRestricted: false },
  app: {
    fileManager: { trashFile: async () => undefined },
    vault: {
      getFolderByPath: () => null,
      getFileByPath: () => null,
      getAbstractFileByPath: () => null,
      createBinary: async () => { throw new Error("write must not run after hash failure"); },
    },
  },
  api: { getFileBinary: async () => new ArrayBuffer(4) },
  fileHashManager: { setFileHash: () => { throw new Error("baseline must not advance"); } },
  lastSyncMtime: new Map(),
  addIgnoredFile: () => { throw new Error("ignored set must not be touched after preflight failure"); },
  removeIgnoredFile: () => undefined,
};
await assert.rejects(
  changeFeedForWrite.applyRemoteFile(binaryPlugin, {
    path: "daily/reports/failed.bin",
    path_hash: "path-hash",
    content_hash: "wrong-hash",
    size: 4,
    mtime: 1,
    ctime: 1,
  }),
  /binary content hash mismatch/,
);

// Contract: a pre-existing empty folder is never deleted by another failed
// materialization attempt.
folders.set("existing", new FakeTFolder("existing"));
await assert.rejects(
  changeFeedForWrite.applyRemoteNote(writePlugin, "existing/failed.md", {
    content: "body",
    contentHash: "remote-hash",
    mtime: 1,
    ctime: 1,
  }),
  /simulated note write failure/,
);
assert.deepEqual(deletedFolders, ["daily/reports", "daily"]);

console.log("change-feed-runtime-guards.test.mjs: all scenarios passed");
