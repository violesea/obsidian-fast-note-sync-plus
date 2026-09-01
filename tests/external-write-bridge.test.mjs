import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

// Contracts under test — the external-write bridge:
// Writes made outside Obsidian (shell/agent/cron) never fire vault 'modify'
// events, so incremental sync never sees them (verified live 2026-08-28: a
// full night of pipeline output produced zero uploads). The bridge feeds
// filesystem-level 'raw' events for non-config paths into the regular
// note/file change path, with these hard safety properties:
// 1. An indexed external .md write bridges to noteModify; non-md to fileModify.
// 2. Echo suppression: paths in ignoredFiles (the plugin's own materialization
//    writes) never bridge — re-checked at flush time too.
// 3. Paths not indexed as TFile (deleted, dotfiles, folders) never bridge —
//    external deletions must never propagate as deletions (INV-1/F-3).
// 4. Excluded paths never bridge.
// 5. Config-dir paths keep the original config-channel semantics (no content
//    bridging when config sync is disabled).

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

let fakeNow = 0;
let nextTimerId = 1;
const timers = new Map();
const fakeSetTimeout = (cb, delay = 0) => {
  const id = nextTimerId++;
  timers.set(id, { cb, due: fakeNow + Math.max(0, delay) });
  return id;
};
const fakeClearTimeout = (id) => timers.delete(id);
const advanceTimers = async (ms) => {
  fakeNow += ms;
  for (;;) {
    const due = [...timers.entries()].filter(([, t]) => t.due <= fakeNow).sort((a, b) => a[1].due - b[1].due);
    if (due.length === 0) break;
    for (const [id, t] of due) {
      if (!timers.delete(id)) continue;
      t.cb();
      await Promise.resolve();
      await Promise.resolve();
    }
  }
};

class TAbstractFile {}
class TFile extends TAbstractFile { constructor(p) { super(); this.path = p; this.stat = { size: 1, mtime: 1, ctime: 1 }; } }
class TFolder extends TAbstractFile { constructor(p) { super(); this.path = p; } }

const calls = { noteModify: [], fileModify: [], dirtyJournal: [], configRaw: [] };

const module = { exports: {} };
const requireStub = (id) => {
  switch (id) {
    case "obsidian":
      return { Platform: { isMobile: false, isDesktop: true }, TAbstractFile, TFile, TFolder, Menu: class {}, MenuItem: class {}, normalizePath: (v) => v };
    case "../sync/operator_note":
      return {
        noteModify: async (file, _p, _a) => { calls.noteModify.push(file.path); return true; },
        noteDelete: async () => true,
        noteRename: async () => true,
        noteDeleteByPath: async () => true,
      };
    case "../sync/operator_file":
      return {
        fileModify: async (file, _p, _a) => { calls.fileModify.push(file.path); return true; },
        fileDelete: async () => true,
        fileRename: async () => true,
        fileDeleteByPath: async () => true,
      };
    case "../sync/operator_folder":
      return {
        folderModify: async () => true,
        folderDelete: async () => true,
        folderRename: async () => true,
      };
    case "../../views/note-history/history-modal":
      return { NoteHistoryModal: class {} };
    case "./helpers":
      return {
        dump: () => undefined,
        isPathInConfigSyncDirs: (p, plugin) => p.startsWith(".obsidian/"),
        isPathExcluded: (p, plugin) => (plugin.__excludedPaths || []).includes(p),
        configIsPathExcluded: () => false,
      };
    case "../../views/share-modal":
      return { ShareModal: class {} };
    case "../../i18n/lang":
      return { $: (k) => k };
    case "../../main":
      return { default: class {} };
    default:
      throw new Error(`Unexpected require: ${id}`);
  }
};

vm.runInNewContext(transpiled, {
  require: requireStub,
  module,
  exports: module.exports,
  console,
  window: { setTimeout: fakeSetTimeout, clearTimeout: fakeClearTimeout, addEventListener: () => undefined },
  setTimeout: fakeSetTimeout,
  clearTimeout: fakeClearTimeout,
  navigator: { onLine: true },
}, { filename: sourcePath });

const { EventManager } = module.exports;

const makePlugin = (overrides = {}) => ({
  settings: { manualSyncEnabled: false, readonlySyncEnabled: false, configSyncEnabled: false, syncUpdateDelay: 0, ...overrides.settings },
  app: {
    vault: {
      getAbstractFileByPath: (p) => (overrides.indexedPaths || new Map()).get(p) ?? null,
      on: () => () => undefined,
      offref: () => undefined,
    },
    workspace: { on: () => () => undefined, offref: () => undefined },
  },
  ignoredFiles: new Set(overrides.ignored || []),
  ignoredConfigFiles: new Set(),
  incrementalScanManager: { markModified: (kind, p) => calls.dirtyJournal.push(`${kind}:${p}`) },
  configManager: { handleRawEvent: async (p) => calls.configRaw.push(p) },
  websocket: { isAuth: true },
  lockManager: { withLock: async (_k, task) => task() },
  fileHashManager: { isReady: () => true },
  registerEvent: () => undefined,
  registerDomEvent: () => undefined,
  register: () => undefined,
  __excludedPaths: overrides.excluded || [],
});

const mdPath = "琅琊阁/report/dasikou/2026-08-28/外部写入.md";
const indexed = new Map([[mdPath, new TFile(mdPath)], ["data/bin附件.png", new TFile("data/bin附件.png")]]);

// 1. External .md write bridges to noteModify; non-md bridges to fileModify.
{
  const em = new EventManager(makePlugin({ indexedPaths: indexed }));
  em["watchRaw"](mdPath);
  assert.ok(calls.dirtyJournal.includes(`note:${mdPath}`), "external md write is durably journaled before debounce");
  await advanceTimers(600);
  assert.deepEqual(calls.noteModify, [mdPath], "external md write bridges to noteModify");
  em["watchRaw"]("data/bin附件.png");
  assert.ok(calls.dirtyJournal.includes("file:data/bin附件.png"), "external file write is durably journaled before debounce");
  await advanceTimers(600);
  assert.deepEqual(calls.fileModify, ["data/bin附件.png"], "external non-md write bridges to fileModify");
  console.log("scenario 1 ok: md/non-md external writes bridge");
}

// 2. Echo suppression, both at enqueue and at flush.
{
  const em = new EventManager(makePlugin({ indexedPaths: indexed, ignored: [mdPath] }));
  em["watchRaw"](mdPath);
  await advanceTimers(700);
  assert.equal(calls.noteModify.length, 1, "echo-suppressed path never bridges (only scenario-1 call remains)");
  console.log("scenario 2 ok: ignoredFiles echo suppression");
}

// 3. Not indexed as TFile (deleted / dotfile / folder) → never bridges.
{
  const em = new EventManager(makePlugin({ indexedPaths: indexed }));
  em["watchRaw"]("已删除/不存在的文件.md");
  em["watchRaw"](".DS_Store");
  em["watchRaw"]("某个目录");
  await advanceTimers(700);
  assert.equal(calls.noteModify.length, 1, "unindexed paths never bridge (external deletions must not propagate)");
  console.log("scenario 3 ok: unindexed/deleted/dotfile paths skipped");
}

// 4. Excluded paths never bridge.
{
  const em = new EventManager(makePlugin({ indexedPaths: indexed, excluded: [mdPath] }));
  em["watchRaw"](mdPath);
  await advanceTimers(700);
  assert.equal(calls.noteModify.length, 1, "excluded path never bridges");
  console.log("scenario 4 ok: exclusion rules honored");
}

// 5. Config paths keep config-channel semantics (no content bridge), and with
//    config sync enabled they journal + dispatch through the config manager.
{
  const em = new EventManager(makePlugin({ indexedPaths: new Map([[".obsidian/appearance.json", new TFile(".obsidian/appearance.json")]]), settings: { configSyncEnabled: true } }));
  em["watchRaw"](".obsidian/appearance.json");
  await advanceTimers(400);
  assert.equal(calls.noteModify.length, 1, "config path does not go through the content bridge");
  assert.ok(calls.dirtyJournal.includes("config:.obsidian/appearance.json"), "config path journaled in config channel");
  assert.deepEqual(calls.configRaw, [".obsidian/appearance.json"], "config path dispatched via config manager");
  console.log("scenario 5 ok: config paths keep config-channel semantics");
}

// 6. A reload can cancel the debounce timer, but it must not erase the event:
// the next authenticated incremental round replays the durable dirty entry.
{
  const beforeModify = calls.noteModify.length;
  const em = new EventManager(makePlugin({ indexedPaths: indexed }));
  em["watchRaw"](mdPath);
  assert.ok(calls.dirtyJournal.includes(`note:${mdPath}`), "reload-safe journal entry exists immediately");
  em.stop();
  await advanceTimers(700);
  assert.equal(calls.noteModify.length, beforeModify, "cancelled debounce does not perform a late live upload");
  console.log("scenario 6 ok: reload cancels live task but preserves durable journal");
}

console.log("external-write-bridge: all scenarios passed");
