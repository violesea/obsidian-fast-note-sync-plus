import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "storage", "local_storage_manager.ts");
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

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => {
    if (id === "../utils/helpers") {
      return { hashContent: () => "hash", dump: () => undefined, LocalStateFileMirror: FakeMirror };
    }
    if (id === "../sync/operator_config") return { configModify: () => undefined };
    if (id === "../../main") return {};
    throw new Error("Unexpected require: " + id);
  },
  module,
  exports: module.exports,
  console,
}, { filename: sourcePath });

const { LocalStorageManager } = module.exports;
const localStorage = new Map();
const mirroredFiles = new Map();

function makePlugin() {
  return {
    manifest: { id: "fast-note-sync" },
    settings: { vault: "RemoteVault" },
    __mirroredFiles: mirroredFiles,
    app: {
      vault: {
        configDir: ".obsidian",
        getName: () => "LocalVault",
      },
      loadLocalStorage: (key) => localStorage.get(key) ?? null,
      saveLocalStorage: (key, value) => {
        if (value === null || value === undefined) localStorage.delete(key);
        else localStorage.set(key, String(value));
      },
    },
  };
}

// Contract: sync cursors and initialization state survive localStorage eviction,
// while credentials never enter the vault mirror.
const first = new LocalStorageManager(makePlugin());
await first.initializeMirror();
first.setMetadata("isInitSync", true);
first.setMetadata("lastNoteSyncTime", 123);
first.setMetadata("lastFileSyncTime", 456);
first.setMetadata("apiToken", "secret-must-not-be-mirrored");
first.flush();

const mirrorPath = ".obsidian/plugins/fast-note-sync/syncMetadata.json";
const mirrorRaw = mirroredFiles.get(mirrorPath);
assert.equal(mirrorRaw.includes("secret-must-not-be-mirrored"), false);

localStorage.clear();
const restored = new LocalStorageManager(makePlugin());
await restored.initializeMirror();
assert.equal(restored.getMetadata("isInitSync"), true);
assert.equal(restored.getMetadata("lastNoteSyncTime"), 123);
assert.equal(restored.getMetadata("lastFileSyncTime"), 456);

console.log("sync-metadata-mirror.test.mjs: all scenarios passed");
