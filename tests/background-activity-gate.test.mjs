import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "background_activity_gate.ts");
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
vm.runInNewContext(transpiled, {
  module,
  exports: module.exports,
}, { filename: sourcePath });

const { BackgroundActivityGate } = module.exports;

// Contract: a background transition pauses a caller without polling or
// resolving it until the app becomes visible again.
{
  const gate = new BackgroundActivityGate();
  gate.markBackgrounded();
  let resolved = false;
  const waiting = gate.waitUntilForeground().then((open) => {
    resolved = true;
    return open;
  });
  await Promise.resolve();
  assert.equal(resolved, false);
  gate.markForegrounded();
  assert.equal(await waiting, true);
  assert.equal(resolved, true);
}

// Contract: unloading wakes every waiter and tells pending work to abandon
// itself, so flush loops cannot remain alive after plugin unload.
{
  const gate = new BackgroundActivityGate();
  gate.markBackgrounded();
  const waiting = gate.waitUntilForeground();
  gate.close();
  assert.equal(await waiting, false);
  assert.equal(await gate.waitUntilForeground(), false);
}

// Contract: duplicate background notifications do not create duplicate state
// transitions or duplicate wake-ups.
{
  const gate = new BackgroundActivityGate();
  gate.markBackgrounded();
  gate.markBackgrounded();
  let wakeCount = 0;
  const waiting = gate.waitUntilForeground().then(() => { wakeCount += 1; });
  gate.markForegrounded();
  gate.markForegrounded();
  await waiting;
  assert.equal(wakeCount, 1);
}

console.log("background-activity-gate.test.mjs: all scenarios passed");
