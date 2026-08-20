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
          debounce: (fn) => fn,
          LocalStateFileMirror: FakeMirror,
        };
      }
      if (id === "../../main") return {};
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

const files = Array.from({ length: 60 }, (_, index) => ({
  path: "notes/" + index + ".md",
  extension: "md",
  stat: { mtime: index + 1, size: 10, ctime: index + 1 },
}));
const contentByPath = new Map(files.map((file) => [file.path, "content-" + file.path]));
const localStorage = new Map();
const mirroredFiles = new Map();
let hashCalls = 0;
let shouldInterrupt = true;

function makePlugin() {
  return {
    manifest: { id: "fast-note-sync" },
    __mirroredFiles: mirroredFiles,
    app: {
      vault: {
        configDir: ".obsidian",
        getFiles: () => files,
        getName: () => "TestVault",
        read: async (file) => {
          hashCalls += 1;
          return contentByPath.get(file.path);
        },
      },
      loadLocalStorage: (key) => localStorage.get(key) ?? null,
      saveLocalStorage: (key, value) => {
        if (value === null || value === undefined) localStorage.delete(key);
        else localStorage.set(key, String(value));
      },
    },
  };
}

// Contract: if the process disappears after a checkpoint, restarting reuses
// completed metadata entries and hashes only the unfinished suffix.
// Re-evaluate with a window that interrupts exactly at the first 50-entry
// progress yield, after the checkpoint has already been persisted.
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
        debounce: (fn) => fn,
        LocalStateFileMirror: FakeMirror,
      };
    }
    if (id === "../../main") return {};
    throw new Error("Unexpected require: " + id);
  },
  module: interruptedModule,
  exports: interruptedModule.exports,
  console,
  window: {
    setTimeout(callback) {
      if (shouldInterrupt) {
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
assert.equal(hashCalls, 50);
assert.equal(JSON.parse(localStorage.get("fns-fileHashBuildState")).phase, "building");

const ReloadedFileHashManager = makeModule();
const resumed = new ReloadedFileHashManager(makePlugin());
await resumed.initialize();
assert.equal(hashCalls, 60);
assert.equal(resumed.getBuildStats().phase, "ready");
assert.equal(resumed.getBuildStats().cacheHits, 50);

// Contract: a same-mtime rewrite with a different size/hash must replace the
// cache entry; timestamp equality alone is not a valid change detector.
resumed.bulkSetFromScanned(new Map([["notes/0.md", { hash: "old", mtime: 99, size: 10 }]]));
resumed.bulkSetFromScanned(new Map([["notes/0.md", { hash: "new", mtime: 99, size: 11, ctime: 7 }]]));
assert.equal(resumed.getValidHash("notes/0.md", 99, 11, 7), "new");
assert.equal(resumed.getValidHash("notes/0.md", 99, 11, 8), null, "ctime 变化时不能误用旧缓存");

console.log("file-hash-checkpoint.test.mjs: all scenarios passed");
