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

// Contract: the production change-feed path is available independently from
// cloud-preview experiments. A valid endpoint+token can be enabled without
// re-enabling auto-delete or dynamic attachment projection.
assert.equal(policy.EXPERIMENTAL_SYNC_FEATURES_ENABLED, false);
assert.equal(policy.CHANGE_FEED_RUNTIME_AVAILABLE, true);
const legacy = {
  changeFeedEnabled: true,
  cloudPreviewEnabled: true,
  cloudPreviewAutoDeleteLocal: true,
  cloudPreviewDynamicAttachment: true,
  sidecarUrl: "http://127.0.0.1:9100",
  sidecarToken: "configured-token",
};
const normalized = policy.applyStableSyncPolicy(legacy);
assert.deepEqual(JSON.parse(JSON.stringify(normalized.settings)), {
  changeFeedEnabled: true,
  cloudPreviewEnabled: false,
  cloudPreviewAutoDeleteLocal: false,
  cloudPreviewDynamicAttachment: false,
  sidecarUrl: "http://127.0.0.1:9100",
  sidecarToken: "configured-token",
});
assert.deepEqual(JSON.parse(JSON.stringify(normalized.disabledFeatures)), [
  "cloud-preview",
  "cloud-preview-auto-delete",
  "cloud-preview-dynamic-attachment",
]);
assert.equal(legacy.changeFeedEnabled, true, "policy must not mutate the caller's settings");
assert.equal(policy.isChangeFeedRuntimeEnabled(legacy), true);
assert.equal(policy.isChangeFeedRuntimeEnabled({ ...legacy, sidecarToken: "" }), false);
assert.equal(policy.isChangeFeedRuntimeEnabled({ ...legacy, sidecarUrl: "" }), false);
assert.equal(policy.isCloudPreviewRuntimeEnabled(legacy), false);

// Contract: an already-provisioned mobile device is automatically moved from
// the 2.5.12 safety-off state to the production change-feed exactly once.
const mobileRollout = policy.applyMobileChangeFeedRollout({
  changeFeedEnabled: false,
  sidecarUrl: "http://192.168.1.47:9100",
  sidecarToken: "configured-token",
}, true);
assert.equal(mobileRollout.settings.changeFeedEnabled, true);
assert.equal(mobileRollout.settings.changeFeedRolloutVersion, 1);
assert.equal(mobileRollout.enabled, true);
assert.equal(mobileRollout.migrated, true);

const deliberateOff = policy.applyMobileChangeFeedRollout({
  ...mobileRollout.settings,
  changeFeedEnabled: false,
}, true);
assert.equal(deliberateOff.settings.changeFeedEnabled, false, "one-time rollout must respect a later manual off");
assert.equal(deliberateOff.enabled, false);
assert.equal(deliberateOff.migrated, false);

const unprovisionedMobile = policy.applyMobileChangeFeedRollout({
  changeFeedEnabled: false,
  sidecarUrl: "",
  sidecarToken: "",
}, true);
assert.equal(unprovisionedMobile.settings.changeFeedEnabled, false);
assert.equal(unprovisionedMobile.settings.changeFeedRolloutVersion, undefined, "missing credentials must not consume the one-time rollout");
assert.equal(unprovisionedMobile.enabled, false);
assert.equal(unprovisionedMobile.migrated, false);

const desktopRollout = policy.applyMobileChangeFeedRollout({
  changeFeedEnabled: false,
  sidecarUrl: "http://127.0.0.1:9100",
  sidecarToken: "configured-token",
}, false);
assert.equal(desktopRollout.settings.changeFeedEnabled, false, "desktop remains on the current source-side path");
assert.equal(desktopRollout.enabled, false);
assert.equal(desktopRollout.migrated, false);

// Contract: the real settings loader applies the rollout using Obsidian's
// device classification before persisting migrations.
const mainSource = fs.readFileSync(path.join(root, "src", "main.ts"), "utf8");
assert.match(mainSource, /applyMobileChangeFeedRollout\(this\.settings, Platform\.isMobile\)/);
assert.match(mainSource, /if \(changeFeedRollout\.migrated\)/);

// Contract: already-stable settings are unchanged and produce no migration.
const stable = { changeFeedEnabled: false, cloudPreviewEnabled: false };
const stableResult = policy.applyStableSyncPolicy(stable);
assert.deepEqual(JSON.parse(JSON.stringify(stableResult.settings)), stable);
assert.deepEqual(JSON.parse(JSON.stringify(stableResult.disabledFeatures)), []);

console.log("sync-feature-policy.test.mjs: all scenarios passed");
