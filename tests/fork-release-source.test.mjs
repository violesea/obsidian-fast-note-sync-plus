import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const versionManager = fs.readFileSync(
  path.join(root, "src", "lib", "utils", "version_manager.ts"),
  "utf8",
);
const settings = fs.readFileSync(
  path.join(root, "src", "setting.tsx"),
  "utf8",
);

// Contract: a fork installation must download plugin upgrades from the fork's
// own Release assets. It must never silently reinstall upstream plugin code.
assert.match(versionManager, /PLUGIN_RELEASE_REPOSITORY\s*=\s*"violesea\/obsidian-fast-note-sync-plus"/);
assert.match(versionManager, /github\.com\/\$\{PLUGIN_RELEASE_REPOSITORY\}\/releases\/download/);
assert.doesNotMatch(versionManager, /github\.com\/haierkeys\/obsidian-fast-note-sync\/releases\/download/);
assert.doesNotMatch(versionManager, /cnb\.cool\/haierkeys\/obsidian-fast-note-sync/);
assert.match(settings, /PLUGIN_RELEASE_REPOSITORY/);
assert.match(settings, /github\.com\/\$\{PLUGIN_RELEASE_REPOSITORY\}\/releases\/download/);
assert.doesNotMatch(settings, /github\.com\/haierkeys\/obsidian-fast-note-sync\/releases\/download/);
assert.doesNotMatch(settings, /cnb\.cool\/haierkeys\/obsidian-fast-note-sync/);

console.log("fork-release-source.test.mjs: all scenarios passed");
