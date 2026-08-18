import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "utils", "events_manager.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

const listeners = new Map();
const documentStub = { visibilityState: "hidden" };
const windowStub = {
  addEventListener(name, listener) {
    listeners.set(name, listener);
  },
  removeEventListener(name, listener) {
    if (listeners.get(name) === listener) listeners.delete(name);
  },
  setTimeout,
  clearTimeout,
};

const module = { exports: {} };
vm.runInNewContext(
  transpiled,
  {
    require: (id) => {
      switch (id) {
        case "obsidian":
          return {
            TAbstractFile: class TAbstractFile {},
            TFile: class TFile {},
            TFolder: class TFolder {},
            Menu: class Menu {},
            MenuItem: class MenuItem {},
            normalizePath: (value) => value,
          };
        case "../sync/operator_note":
          return {
            noteModify: () => undefined,
            noteDelete: () => undefined,
            noteRename: () => undefined,
            noteDeleteByPath: () => undefined,
          };
        case "../sync/operator_file":
          return {
            fileModify: () => undefined,
            fileDelete: () => undefined,
            fileRename: () => undefined,
            fileDeleteByPath: () => undefined,
          };
        case "../sync/operator_folder":
          return {
            folderModify: () => undefined,
            folderDelete: () => undefined,
            folderRename: () => undefined,
          };
        case "../../views/note-history/history-modal":
          return { NoteHistoryModal: class NoteHistoryModal {} };
        case "../../views/share-modal":
          return { ShareModal: class ShareModal {} };
        case "../utils/helpers":
        case "./helpers":
          return {
            dump: () => undefined,
            isPathInConfigSyncDirs: () => true,
            isPathExcluded: () => false,
            configIsPathExcluded: () => false,
          };
        case "../../main":
          return {};
        case "../../i18n/lang":
          return { $: (key) => key };
        default:
          throw new Error(`Unexpected require: ${id}`);
      }
    },
    module,
    exports: module.exports,
    console,
    window: windowStub,
    activeDocument: documentStub,
    setTimeout,
    clearTimeout,
  },
  { filename: sourcePath },
);

const { EventManager } = module.exports;
let unregisterCount = 0;
let reconnectCount = 0;
let shareRefreshCount = 0;
let unload;

const plugin = {
  fileHashManager: { isReady: () => true },
  app: {
    vault: { on: () => ({}) },
    workspace: { on: () => ({}) },
  },
  registerEvent: () => undefined,
  register: (callback) => {
    unload = callback;
  },
  settings: {
    manualSyncEnabled: false,
    readonlySyncEnabled: false,
    configSyncEnabled: false,
  },
  websocket: {
    unRegister: () => {
      unregisterCount += 1;
    },
    triggerReconnect: () => {
      reconnectCount += 1;
    },
  },
  shareIndicatorManager: {
    syncWithServer: async () => {
      shareRefreshCount += 1;
    },
  },
};

const manager = new EventManager(plugin);
manager.registerEvents();

// Contract: background transitions never close the sync connection.
assert.equal(listeners.has("blur"), false);
listeners.get("visibilitychange")();
assert.equal(unregisterCount, 0);
assert.equal(reconnectCount, 0);

// Contract: returning to the foreground retries the connection and refreshes state.
documentStub.visibilityState = "visible";
listeners.get("visibilitychange")();
assert.equal(reconnectCount, 1);
assert.equal(shareRefreshCount, 1);

listeners.get("focus")();
assert.equal(reconnectCount, 2);

unload();
assert.equal(listeners.size, 0);

console.log("background-sync.test.mjs: all scenarios passed");
