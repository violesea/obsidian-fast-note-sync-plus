import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "resume_recovery.ts");
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
  console,
}, { filename: sourcePath });

const { ResumeRecoveryCoordinator } = module.exports;

function makeTransport(overrides = {}) {
  const calls = { probe: 0, triggerReconnect: 0, forceReconnect: 0, logs: [] };
  const transport = {
    isMobile: true,
    isRegistered: () => true,
    isReady: () => true,
    isOpen: () => true,
    probe: async () => {
      calls.probe++;
      return true;
    },
    triggerReconnect: () => { calls.triggerReconnect++; },
    forceReconnect: () => { calls.forceReconnect++; },
    log: (message) => calls.logs.push(message),
    ...overrides,
  };
  return { transport, calls };
}

// Contract: a healthy mobile resume probes the existing socket and does not
// create a replacement connection or a second logical sync round.
{
  const { transport, calls } = makeTransport();
  const coordinator = new ResumeRecoveryCoordinator(transport, { probeTimeoutMs: 25 });
  coordinator.markBackgrounded();
  await Promise.all([coordinator.recover("focus"), coordinator.recover("visibilitychange")]);
  await coordinator.recover("online");
  assert.equal(calls.probe, 1);
  assert.equal(calls.forceReconnect, 0);
  assert.equal(calls.triggerReconnect, 0);
}

// Contract: an unresponsive OPEN mobile socket is replaced exactly once.
{
  const { transport, calls } = makeTransport({ probe: async () => { calls.probe++; return false; } });
  const coordinator = new ResumeRecoveryCoordinator(transport);
  coordinator.markBackgrounded();
  await coordinator.recover("visibilitychange");
  await coordinator.recover("focus");
  assert.equal(calls.probe, 1);
  assert.equal(calls.forceReconnect, 1);

  // A later background -> foreground transition is a new lifecycle cycle and
  // is allowed to make one fresh recovery decision.
  coordinator.markBackgrounded();
  await coordinator.recover("visibilitychange");
  assert.equal(calls.probe, 2);
  assert.equal(calls.forceReconnect, 2);
}

// Contract: repeated initial focus notifications without a hidden transition
// are still one recovery decision.
{
  const { transport, calls } = makeTransport({ probe: async () => { calls.probe++; return false; } });
  const coordinator = new ResumeRecoveryCoordinator(transport);
  await coordinator.recover("focus");
  await coordinator.recover("visibilitychange");
  assert.equal(calls.probe, 1);
  assert.equal(calls.forceReconnect, 1);
}

// Contract: a closed mobile socket follows ordinary backoff reconnect and does
// not force-close a socket that is already gone.
{
  const { transport, calls } = makeTransport({
    isReady: () => false,
    isOpen: () => false,
  });
  const coordinator = new ResumeRecoveryCoordinator(transport);
  await coordinator.recover("resume");
  assert.equal(calls.probe, 0);
  assert.equal(calls.triggerReconnect, 1);
  assert.equal(calls.forceReconnect, 0);
}

// Contract: repeated online events within one network cycle do not reset the
// transport backoff by forcing another replacement.
{
  const { transport, calls } = makeTransport({ probe: async () => { calls.probe++; return false; } });
  const coordinator = new ResumeRecoveryCoordinator(transport);
  await coordinator.recover("online");
  await coordinator.recover("online");
  assert.equal(calls.probe, 1);
  assert.equal(calls.forceReconnect, 1);

  coordinator.markNetworkLost();
  await coordinator.recover("online");
  assert.equal(calls.probe, 2);
  assert.equal(calls.forceReconnect, 2);
}

// Contract: desktop resume keeps a healthy connection without an application
// probe, preserving the silent background-sync path.
{
  const { transport, calls } = makeTransport({ isMobile: false });
  const coordinator = new ResumeRecoveryCoordinator(transport);
  await coordinator.recover("visibilitychange");
  assert.equal(calls.probe, 0);
  assert.equal(calls.forceReconnect, 0);
  assert.equal(calls.triggerReconnect, 0);
}

console.log("resume-recovery.test.mjs: all scenarios passed");
