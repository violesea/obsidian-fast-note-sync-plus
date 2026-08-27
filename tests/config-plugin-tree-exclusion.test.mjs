import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

// Contract under test: the entire .obsidian/plugins/ tree must be hard-excluded
// from the config sync channel. Plugin binaries are per-device software, never
// vault content. The live failure (2026-08-27, .19): a same-id backup plugin
// directory was uploaded as setting entries and then resurrected on every
// config sync round, hijacking which plugin copy Obsidian loaded (ISSUE-026).

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "utils", "helpers.ts");
const source = fs.readFileSync(sourcePath, "utf8");

const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
const requireStub = (id) => {
  switch (id) {
    case "obsidian":
      return {
        Notice: class {},
        normalizePath: (v) => v,
        TFolder: class {},
        Platform: { isMobile: false },
        App: class {},
        PluginManifest: {},
      };
    case "../../i18n/lang":
      return { $: (k) => k };
    case "../../main":
      return { default: class {} };
    case "../sync/sync_log_manager":
      return { SyncLogManager: { getInstance: () => ({ addLog: () => undefined }) } };
    case "../sync/background_activity_gate":
      return {
        requireForeground: async () => true,
        waitForForeground: async () => true,
      };
    case "../helpers_obsidian_bypass":
      return {
        nativeFetch: async () => { throw new Error("not needed"); },
        vaultDelete: async () => undefined,
        dump: () => undefined,
        dumpError: () => undefined,
        setLogEnabled: () => undefined,
        logLevel: "off",
      };
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
  Uint8Array,
  TextDecoder,
  TextEncoder,
  setTimeout,
  clearTimeout,
}, { filename: sourcePath });

const { configIsPathExcluded } = module.exports;

const plugin = {
  settings: { syncExcludeFolders: "", syncExcludeWhitelist: "" },
  app: {
    vault: {
      configDir: ".obsidian",
      getName: () => "test-vault",
    },
  },
  manifest: { id: "fast-note-sync", dir: "fast-note-sync" },
};

// 1. Plugin binaries anywhere under the plugins tree are excluded, including
//    same-id backup copies with arbitrary directory names.
assert.equal(configIsPathExcluded(".obsidian/plugins/fast-note-sync/main.js", plugin), true);
assert.equal(configIsPathExcluded(".obsidian/plugins/fast-note-sync/manifest.json", plugin), true);
assert.equal(
  configIsPathExcluded(".obsidian/plugins/fast-note-sync.backup-2.5.5-20260824-2235/manifest.json", plugin),
  true,
  "same-id backup copies must be excluded (ISSUE-026 resurrection path)"
);
assert.equal(configIsPathExcluded(".obsidian/plugins/calendar/main.js", plugin), true);
assert.equal(configIsPathExcluded(".obsidian/plugins", plugin), true);

// 2. Regular .obsidian settings outside the plugins tree keep syncing.
assert.equal(configIsPathExcluded(".obsidian/community-plugins.json", plugin), false);
assert.equal(configIsPathExcluded(".obsidian/appearance.json", plugin), false);
assert.equal(configIsPathExcluded(".obsidian/workspace.json", plugin), false);

// 3. The plugins-tree exclusion is a hard tier: an explicit whitelist entry
//    cannot re-enable plugin binary syncing.
const whitelistPlugin = {
  ...plugin,
  settings: {
    syncExcludeFolders: "",
    syncExcludeWhitelist: '[{"pattern":".obsidian/plugins/fast-note-sync/main.js","caseSensitive":false}]',
  },
};
assert.equal(
  configIsPathExcluded(".obsidian/plugins/fast-note-sync/main.js", whitelistPlugin),
  true,
  "hard exclusion must not be overridable by whitelist"
);
assert.equal(configIsPathExcluded(".obsidian/appearance.json", whitelistPlugin), false);

// 4. Plugin self-dir state files stay excluded (pre-existing contract).
assert.equal(configIsPathExcluded(".obsidian/plugins/fast-note-sync/data.json", plugin), true);

console.log("config-plugin-tree-exclusion: all scenarios passed");
