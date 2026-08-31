import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const policyPath = path.join(root, "src", "lib", "sync", "concurrency_limiter.ts");
const source = fs.readFileSync(policyPath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
  },
  fileName: policyPath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: (id) => {
    if (id === "obsidian") return { Platform: { isIosApp: true } };
    if (id === "../utils/helpers") return { dump: () => undefined };
    throw new Error(`Unexpected dependency: ${id}`);
  },
  module,
  exports: module.exports,
}, { filename: policyPath });

const {
  ConcurrencyLimiter,
  effectiveOperationConcurrency,
  shouldEnforceOperationLimiter,
} = module.exports;

// Contract: iOS never admits a parallel burst large enough for WKWebView
// jetsam, even if an old device setting retained a higher concurrency value.
assert.equal(effectiveOperationConcurrency(20, true), 1);
assert.equal(effectiveOperationConcurrency(1, true), 1);

// Contract: disabling the user-facing limiter cannot disable the mandatory
// iOS safety gate.
assert.equal(shouldEnforceOperationLimiter(false, true), true);
assert.equal(shouldEnforceOperationLimiter(false, false), false);

const limiter = new ConcurrencyLimiter({
  settings: {
    concurrencyControlEnabled: false,
    maxConcurrentUploads: 20,
  },
});
await limiter.waitForSlot("first");
let secondAdmitted = false;
const secondAdmission = limiter.waitForSlot("second").then(() => {
  secondAdmitted = true;
});
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(secondAdmitted, false);
limiter.releaseSlot("first");
await secondAdmission;
assert.equal(secondAdmitted, true);
limiter.releaseSlot("second");

// Contract: the emergency iOS safety policy does not silently change desktop
// or Android behavior.
assert.equal(effectiveOperationConcurrency(20, false), 20);

// Contract: the policy is wired into operation admission, download storage,
// and file hashing. A green helper test without these call sites would not
// protect the user-visible reload failure.
const limiterSource = source;
const downloadSource = fs.readFileSync(path.join(root, "src", "lib", "sync", "operator_file.ts"), "utf8");
const helpersSource = fs.readFileSync(path.join(root, "src", "lib", "utils", "helpers.ts"), "utf8");
assert.match(limiterSource, /shouldEnforceOperationLimiter\([\s\S]*?Platform\.isIosApp/);
assert.match(limiterSource, /effectiveOperationConcurrency\([\s\S]*?Platform\.isIosApp/);
assert.match(downloadSource, /Platform\.isMobile\s*&&\s*!Platform\.isIosApp\s*&&\s*size\s*<=\s*MAX_DOWNLOAD_BUFFER_BYTES/);
assert.match(helpersSource, /let iosHashTail:[\s\S]*?runWithIosHashAdmission[\s\S]*?await predecessor[\s\S]*?release\(\)/);
assert.match(helpersSource, /hashFileAsync[\s\S]*?runWithIosHashAdmission\(\(\) => hashFileWithoutAdmission/);

console.log("mobile-resource-policy.test.mjs: all scenarios passed");
