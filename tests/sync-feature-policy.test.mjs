import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_feature_policy.ts");
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
vm.runInNewContext(transpiled, { module, exports: module.exports, console }, { filename: sourcePath });
const policy = module.exports;

// Contract: a release build keeps experimental sync paths off even when an
// older device persisted their switches as true.
assert.equal(policy.EXPERIMENTAL_SYNC_FEATURES_ENABLED, false);
const legacy = {
  changeFeedEnabled: true,
  cloudPreviewEnabled: true,
  cloudPreviewAutoDeleteLocal: true,
  cloudPreviewDynamicAttachment: true,
  sidecarUrl: "http://127.0.0.1:9100",
};
const normalized = policy.applyStableSyncPolicy(legacy);
assert.deepEqual(JSON.parse(JSON.stringify(normalized.settings)), {
  changeFeedEnabled: false,
  cloudPreviewEnabled: false,
  cloudPreviewAutoDeleteLocal: false,
  cloudPreviewDynamicAttachment: false,
  sidecarUrl: "http://127.0.0.1:9100",
});
assert.deepEqual(JSON.parse(JSON.stringify(normalized.disabledFeatures)), [
  "change-feed",
  "cloud-preview",
  "cloud-preview-auto-delete",
  "cloud-preview-dynamic-attachment",
]);
assert.equal(legacy.changeFeedEnabled, true, "policy must not mutate the caller's settings");
assert.equal(policy.isChangeFeedRuntimeEnabled(legacy), false);
assert.equal(policy.isCloudPreviewRuntimeEnabled(legacy), false);

// Contract: already-stable settings are unchanged and produce no migration.
const stable = { changeFeedEnabled: false, cloudPreviewEnabled: false };
const stableResult = policy.applyStableSyncPolicy(stable);
assert.deepEqual(JSON.parse(JSON.stringify(stableResult.settings)), stable);
assert.deepEqual(JSON.parse(JSON.stringify(stableResult.disabledFeatures)), []);

console.log("sync-feature-policy.test.mjs: all scenarios passed");
