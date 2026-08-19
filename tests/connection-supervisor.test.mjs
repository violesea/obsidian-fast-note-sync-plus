import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "connection_supervisor.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, { module, exports: module.exports }, { filename: sourcePath });
const { ConnectionSupervisor } = module.exports;

let releaseRegister;
const registerGate = new Promise((resolve) => { releaseRegister = resolve; });
const calls = { register: 0, unregister: [], reconnect: 0, forceReconnect: 0 };
const transport = {
  register: async () => { calls.register += 1; await registerGate; },
  unRegister: (disabled) => { calls.unregister.push(disabled); },
  triggerReconnect: () => { calls.reconnect += 1; },
  forceReconnect: () => { calls.forceReconnect += 1; },
};
const supervisor = new ConnectionSupervisor(transport);

// Contract: concurrent register intents share the supervisor pump.
const first = supervisor.requestRegister();
const second = supervisor.requestRegister();
assert.equal(calls.register, 1);
releaseRegister();
await Promise.all([first, second]);
assert.equal(calls.register, 1);

// Contract: reconfigure invalidates the current transport before registering again.
await supervisor.requestReconfigure();
assert.deepEqual(calls.unregister, [false]);

// Contract: disabling registration blocks recovery intents until explicitly enabled.
await supervisor.requestUnregister(true);
supervisor.requestReconnect();
supervisor.requestForceReconnect();
assert.equal(calls.reconnect, 0);
assert.equal(calls.forceReconnect, 0);

console.log("connection-supervisor.test.mjs: all scenarios passed");
