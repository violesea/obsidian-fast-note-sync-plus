// Runtime contract test for change-feed page acknowledgement.
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

const sidecarChanges = [{
  rev: 460574,
  type: "note",
  action: "modify",
  changed_fields: "content,mtime",
  path: "工作/GE/想法/日常想法.md",
  content_hash: "remote-hash",
  path_hash: "path-hash",
  size: 3090,
  mtime: 1787407712367,
  ctime: 1787407712367,
}];

class FakeTFile {}
const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => {
    if (id === "obsidian") {
      return {
        Platform: { isIosApp: false, isTablet: false, isAndroidApp: false, isDesktopApp: true },
        TFile: FakeTFile,
        normalizePath: (value) => value,
        requestUrl: async () => ({
          status: 200,
          json: {
            ok: true,
            data: {
              changes: sidecarChanges,
              next_rev: 460575,
              has_more: false,
              safe_rev: 464990,
              min_available_rev: 312482,
              collapsed: true,
              scanned: 1,
            },
          },
        }),
      };
    }
    if (id === "../utils/helpers") {
      return {
        dump: () => undefined,
        hashContent: () => "path-hash",
        hashContentAsync: async () => "local-hash",
        hashFileAsync: async () => "local-hash",
        isPathExcluded: () => false,
        LocalStateFileMirror: class {},
      };
    }
    if (id === "../utils/types") return { CLIENT_TYPE: "ObsidianPlugin" };
    if (id === "./operator_note") return { receiveNoteSyncDelete: async () => undefined };
    if (id === "./operator_file") return { receiveFileSyncDelete: async () => undefined };
    if (id === "./background_activity_gate") return { requireForeground: async () => undefined };
    if (id === "./change_feed_logic") {
      const logicPath = path.join(root, "src", "lib", "sync", "change_feed_logic.ts");
      const logicSource = fs.readFileSync(logicPath, "utf8");
      const logicTranspiled = ts.transpileModule(logicSource, {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
        fileName: logicPath,
      }).outputText;
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

const { runChangeFeedCatchUp } = module.exports;
const cursorState = {
  schema: 2,
  deviceId: "ipad-device",
  vault: "New-World",
  rev: 459990,
  noteWatermarkMs: 0,
  fileWatermarkMs: 0,
  updatedAt: 0,
  repairPending: true,
};
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
    setRev: () => { setRevCalls++; },
    completeRepair: () => { completeRepairCalls++; },
  },
  syncState: { transportResetPending: false, activeSyncContext: "", conflictedPaths: new Set() },
  pendingNoteModifies: new Set(),
  app: {
    vault: {
      getAbstractFileByPath: () => ({ path: sidecarChanges[0].path }),
      getFileByPath: () => null,
    },
  },
  fileHashManager: { getPathHash: () => null },
  api: { getNoteContent: async () => null },
  manifest: { version: "2.5.4" },
};

const result = await runChangeFeedCatchUp(plugin);
assert.equal(result.ok, false);
assert.equal(result.reason, "materialization_failed");
// Contract: a failed materialization must not acknowledge the page or clear
// the one-time replay marker; the next foreground sync must retry it.
assert.equal(setRevCalls, 0);
assert.equal(completeRepairCalls, 0);

console.log("change-feed-runtime.test.mjs: all scenarios passed");
