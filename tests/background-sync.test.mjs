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
const platformStub = { isMobile: true };
const timers = new Map();
let nextTimerId = 0;
const windowStub = {
  addEventListener(name, listener) {
    listeners.set(name, listener);
  },
  removeEventListener(name, listener) {
    if (listeners.get(name) === listener) listeners.delete(name);
  },
  setTimeout(callback, delay) {
    const id = ++nextTimerId;
    timers.set(id, { callback, delay });
    return id;
  },
  clearTimeout(id) {
    timers.delete(id);
  },
};

function flushTimers() {
  const pending = [...timers.values()];
  timers.clear();
  for (const timer of pending) timer.callback();
}

const module = { exports: {} };
vm.runInNewContext(
  transpiled,
  {
    require: (id) => {
      switch (id) {
        case "obsidian":
          return {
            TAbstractFile: class TAbstractFile {},
            Platform: platformStub,
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
let forceReconnectCount = 0;
let backgroundedCount = 0;
let networkLostCount = 0;
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
    isOpen: true,
    ws: { readyState: 1 },
    unRegister: () => {
      unregisterCount += 1;
    },
    triggerReconnect: () => {
      reconnectCount += 1;
    },
    forceReconnect: () => {
      forceReconnectCount += 1;
    },
    noteBackgrounded: () => {
      backgroundedCount += 1;
    },
    noteNetworkLost: () => {
      networkLostCount += 1;
    },
    recoverAfterResume: () => {
      if (platformStub.isMobile) {
        plugin.websocket.forceReconnect();
      } else if (!plugin.websocket.isOpen) {
        plugin.websocket.triggerReconnect();
      }
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
assert.equal(timers.size, 0);
assert.equal(backgroundedCount, 1);

// Contract: going offline does not unregister the socket or clear its retry lifecycle.
listeners.get("offline")();
assert.equal(unregisterCount, 0);
assert.equal(reconnectCount, 0);
assert.equal(networkLostCount, 1);

// Contract: returning to the foreground refreshes state and forces a fresh
// mobile socket because the WebView may retain a stale OPEN object.
documentStub.visibilityState = "visible";
listeners.get("visibilitychange")();
assert.equal(reconnectCount, 0);
assert.equal(shareRefreshCount, 1);

// Contract: focus, visibility, and online events in one resume transition are
// debounced into one recovery decision.
listeners.get("focus")();
listeners.get("online")();
assert.equal(timers.size, 1);
flushTimers();
assert.equal(reconnectCount, 0);
assert.equal(forceReconnectCount, 1);

// Contract: even when the client still reports OPEN, a mobile resume performs
// exactly one forced replacement for the coalesced event burst.
plugin.websocket.isOpen = false;
documentStub.visibilityState = "hidden";
listeners.get("visibilitychange")();
assert.equal(backgroundedCount, 2);
documentStub.visibilityState = "visible";
listeners.get("focus")();
listeners.get("visibilitychange")();
listeners.get("online")();
flushTimers();
assert.equal(reconnectCount, 0);
assert.equal(forceReconnectCount, 2);

// Contract: a desktop resume keeps a healthy socket untouched.
platformStub.isMobile = false;
plugin.websocket.isOpen = true;
listeners.get("focus")();
flushTimers();
assert.equal(reconnectCount, 0);
assert.equal(forceReconnectCount, 2);

unload();
assert.equal(listeners.size, 0);

console.log("background-sync.test.mjs: all scenarios passed");
