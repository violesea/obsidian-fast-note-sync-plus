import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "storage", "file_hash_manager.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

class FakeMirror {
  constructor(plugin, fileName) {
    this.files = plugin.__mirroredFiles;
    this.path = plugin.app.vault.configDir + "/plugins/" + plugin.manifest.id + "/" + fileName;
    this.pending = null;
  }

  async read() {
    return this.files.get(this.path) ?? null;
  }

  scheduleWrite(data) {
    this.pending = data;
  }

  flush() {
    if (this.pending !== null) {
      this.files.set(this.path, this.pending);
      this.pending = null;
    }
  }

  async flushAsync() {
    this.flush();
  }
}

// 真实 scan_delta 模块（transpile 自源码），append/load/clear 均指向共享 scanDeltaStore；
// plugin 参数由被测对象在调用时传入（fileHashManager 方法在真实对象上，delta 直接写 store）
const realScanDelta = (() => {
  const mod = { exports: {} };
  const s = fs.readFileSync(path.join(root, "src", "lib", "sync", "scan_delta.ts"), "utf8");
  const out = ts.transpileModule(s, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, esModuleInterop: true }, fileName: "scan_delta.ts" }).outputText;
  vm.runInNewContext(out, {
    require: (id) => id === "obsidian" ? { normalizePath: (v) => v } : { getPluginDir: () => ".obsidian/plugins/fast-note-sync", dump: (...a) => console.error("[delta-dump]", String(a[0]).slice(0,110), a[1] && a[1].message ? a[1].message : "") },
    module: mod, exports: mod.exports, console,
  }, { filename: "scan_delta.ts" });
  return mod.exports;
})();

function makeModule() {
  const module = { exports: {} };
  vm.runInNewContext(transpiled, {
    require: (id) => {
      if (id === "../utils/helpers") {
        return {
          hashContentAsync: async (content) => "hash:" + content,
          hashFileAsync: async (_app, filePath) => "hash:" + filePath,
          dump: () => undefined,
          isPathExcluded: () => false,
          showSyncNotice: () => ({ setMessage: () => undefined, hide: () => undefined }),
          isLargeBinarySyncRisk: () => false,
          describeBinarySyncLimit: () => "limit",
          logMemorySnapshot: () => undefined,
          debounce: () => () => undefined,
          LocalStateFileMirror: FakeMirror,
        };
      }
      if (id === "../../main") return {};
      if (id === "../sync/background_activity_gate") {
        return {
          isBackgroundActivityClosedError: () => false,
          requireForeground: async () => undefined,
          waitForForeground: async () => true,
        };
      }
      if (id === "../sync/scan_delta") {
        // 转发到真实 scan_delta（共享 scanDeltaStore）：冷建检查点写入 delta 的
        // 行为必须真实生效，重启断言才有意义——吞掉写入会让"从断点恢复"变空谈。
        return realScanDelta;
      }
      throw new Error("Unexpected require: " + id);
    },
    module,
    exports: module.exports,
    console,
    window: {
      setTimeout(callback) {
        callback();
        return 1;
      },
      clearTimeout: () => undefined,
    },
  }, { filename: sourcePath });
  return module.exports.FileHashManager;
}

const files = Array.from({ length: 5100 }, (_, index) => ({
  path: "notes/" + index + ".md",
  extension: "md",
  stat: { mtime: index + 1, size: 10, ctime: index + 1 },
}));
const contentByPath = new Map(files.map((file) => [file.path, "content-" + file.path]));
const localStorage = new Map();
const mirroredFiles = new Map();
let hashCalls = 0;
let shouldInterrupt = true;
let hashMapSaveCalls = 0;
// scanDelta JSONL 的内存替身（2.5.19 起冷建检查点写入这里而非全图 localStorage）
const scanDeltaStore = new Map();
let scanDeltaPreloaded = 0;

function makePlugin() {
  return {
    manifest: { id: "fast-note-sync" },
    __mirroredFiles: mirroredFiles,
    fileHashManager: { bulkSetFromScanned: (entries) => { scanDeltaPreloaded += entries.size; } },
    app: {
      vault: {
        configDir: ".obsidian",
        getFiles: () => files,
        getName: () => "TestVault",
        read: async (file) => {
          hashCalls += 1;
          return contentByPath.get(file.path);
        },
        // 2.5.19：冷建检查点把新增哈希追加到 scanDelta JSONL（vault 外持久化），
        // 取代原先的全图 localStorage 写。测试镜像该行为：追加到内存 store，
        // 重启场景由下一个 makePlugin 共享同一 store 断言"从断点恢复"。
        adapter: {
          exists: async (p) => scanDeltaStore.has(p),
          read: async (p) => scanDeltaStore.get(p) ?? "",
          append: async (p, data) => { scanDeltaStore.set(p, (scanDeltaStore.get(p) ?? "") + data); },
          remove: async (p) => { scanDeltaStore.delete(p); },
        },
      },
      loadLocalStorage: (key) => localStorage.get(key) ?? null,
      saveLocalStorage: (key, value) => {
        if (key === "fns-fileHashMap") hashMapSaveCalls++;
        if (value === null || value === undefined) localStorage.delete(key);
        else localStorage.set(key, String(value));
      },
    },
  };
}

// Contract: if the process disappears after a checkpoint, restarting reuses
// completed metadata entries and hashes only the unfinished suffix.
// Re-evaluate with a window that interrupts exactly at the first 5000-entry
// checkpoint, after the checkpoint has already been persisted.
// The first module above is non-interrupting, so run the actual interruption in
// a dedicated evaluation context.
const interruptedSource = transpiled;
const interruptedModule = { exports: {} };
vm.runInNewContext(interruptedSource, {
  require: (id) => {
    if (id === "../utils/helpers") {
      return {
        hashContentAsync: async (content) => "hash:" + content,
        hashFileAsync: async (_app, filePath) => "hash:" + filePath,
        dump: () => undefined,
        isPathExcluded: () => false,
        showSyncNotice: () => ({ setMessage: () => undefined, hide: () => undefined }),
        isLargeBinarySyncRisk: () => false,
        describeBinarySyncLimit: () => "limit",
        logMemorySnapshot: () => undefined,
        debounce: () => () => undefined,
        LocalStateFileMirror: FakeMirror,
      };
    }
    if (id === "../../main") return {};
    if (id === "../sync/background_activity_gate") {
      return {
        isBackgroundActivityClosedError: () => false,
        requireForeground: async () => undefined,
        waitForForeground: async () => true,
      };
    }
    if (id === "../sync/scan_delta") {
      return realScanDelta;
    }
    throw new Error("Unexpected require: " + id);
  },
  module: interruptedModule,
  exports: interruptedModule.exports,
  console,
  window: {
    setTimeout(callback) {
      if (shouldInterrupt && hashCalls >= 5000) {
        shouldInterrupt = false;
        throw new Error("simulated iOS process termination");
      }
      callback();
      return 1;
    },
    clearTimeout: () => undefined,
  },
}, { filename: sourcePath });
const InterruptedFileHashManager = interruptedModule.exports.FileHashManager;
const interruptedManager = new InterruptedFileHashManager(makePlugin());
await assert.rejects(() => interruptedManager.initialize(), /simulated iOS process termination/);
assert.equal(hashCalls, 5000);
assert.equal(JSON.parse(localStorage.get("fns-fileHashBuildState")).phase, "building");

const ReloadedFileHashManager = makeModule();
const resumed = new ReloadedFileHashManager(makePlugin());
await resumed.initialize();
assert.equal(hashCalls, 5100);
assert.equal(resumed.getBuildStats().phase, "ready");
assert.equal(resumed.getBuildStats().cacheHits, 5000);

// Contract: bulk scan updates do not synchronously serialize the complete
// hash map; an explicit flush still persists the latest local cache.
const savesBeforeBulk = hashMapSaveCalls;
resumed.bulkSetFromScanned(new Map([["notes/checkpoint.md", { hash: "checkpoint", mtime: 98, size: 9 }]]), false);
assert.equal(hashMapSaveCalls, savesBeforeBulk, "中间扫描 checkpoint 不应触发完整哈希表落盘");
assert.equal(resumed.getValidHash("notes/checkpoint.md", 98, 9), "checkpoint");
resumed.bulkSetFromScanned(new Map([["notes/0.md", { hash: "old", mtime: 99, size: 10 }]]));
resumed.bulkSetFromScanned(new Map([["notes/0.md", { hash: "new", mtime: 99, size: 11, ctime: 7 }]]));
assert.equal(hashMapSaveCalls, savesBeforeBulk);
resumed.flush();
assert.equal(hashMapSaveCalls, savesBeforeBulk + 1);

// Contract: a same-mtime rewrite with a different size/hash must replace the
// cache entry; timestamp equality alone is not a valid change detector.
assert.equal(resumed.getValidHash("notes/0.md", 99, 11, 7), "new");
assert.equal(resumed.getValidHash("notes/0.md", 99, 11, 8), null, "ctime 变化时不能误用旧缓存");

console.log("file-hash-checkpoint.test.mjs: all scenarios passed");
