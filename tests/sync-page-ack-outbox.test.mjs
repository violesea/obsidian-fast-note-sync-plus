import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "sync_page_ack_outbox.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, { module, exports: module.exports }, { filename: sourcePath });
const { SyncPageAckOutbox } = module.exports;

const outbox = new SyncPageAckOutbox();
const sent = [];

// Contract: an ACK without context is queued and never sent as an empty-context packet.
outbox.enqueue("note", 2);
outbox.flush(null, (ack) => { sent.push(ack); return true; });
assert.equal(sent.length, 0);
assert.equal(outbox.size, 1);

// Contract: initial ACK precedes the highest page watermark after a context is available.
outbox.setActiveContext("ctx-1");
outbox.enqueue("note", -1);
outbox.enqueue("note", 1, "ctx-1");
outbox.enqueue("note", 4, "ctx-1");
outbox.enqueue("note", 3, "ctx-1");
outbox.flush("ctx-1", (ack) => { sent.push(ack); return true; });
assert.deepEqual(sent.map((ack) => [ack.pageIndex, ack.context]), [[-1, "ctx-1"], [4, "ctx-1"]]);
assert.equal(outbox.size, 0);

// Contract: a failed write remains in the outbox and is retried after reconnect.
outbox.enqueue("file", 7, "ctx-1");
outbox.flush("ctx-1", () => false);
assert.equal(outbox.size, 1);
outbox.flush("ctx-1", (ack) => { sent.push(ack); return true; });
assert.equal(sent.at(-1).pageIndex, 7);
assert.equal(outbox.size, 0);

// Contract: a new logical context drops all old-context ACKs.
outbox.enqueue("folder", 3, "ctx-1");
outbox.beginContext("ctx-2");
outbox.flush("ctx-2", (ack) => { sent.push(ack); return true; });
assert.equal(sent.some((ack) => ack.context === "ctx-1" && ack.type === "folder"), false);

console.log("sync-page-ack-outbox.test.mjs: all scenarios passed");
