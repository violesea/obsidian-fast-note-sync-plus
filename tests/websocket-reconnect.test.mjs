import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "websocket_client.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
  },
  fileName: sourcePath,
}).outputText;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = FakeWebSocket.CONNECTING;
    this.bufferedAmount = 0;
    this.closeCalls = [];
    FakeWebSocket.instances.push(this);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.(new Event("open"));
  }

  close(code, reason) {
    this.closeCalls.push({ code, reason });
    this.readyState = FakeWebSocket.CLOSED;
  }

  send() {}
}

const module = { exports: {} };
const windowStub = { setTimeout, clearTimeout };

vm.runInNewContext(
  transpiled,
  {
    require: (id) => {
      switch (id) {
        case "obsidian":
          return {
            moment: Object.assign(() => ({ format: () => "" }), { locale: () => "zh-cn" }),
          };
        case "../utils/helpers":
          return {
            dump: () => undefined,
            dumpError: () => undefined,
            isWsUrl: (value) => /^wss?:\/\//.test(value),
            showSyncNotice: () => undefined,
          };
        default:
          throw new Error(`Unexpected require: ${id}`);
      }
    },
    module,
    exports: module.exports,
    console,
    ArrayBuffer,
    Blob,
    Event,
    MessageEvent,
    TextDecoder,
    TextEncoder,
    WebSocket: FakeWebSocket,
    window: windowStub,
    setTimeout,
    clearTimeout,
  },
  { filename: sourcePath },
);

const { WebSocketClient } = module.exports;
const savedValues = new Map();
const plugin = {
  app: {
    vault: { getName: () => "test-vault" },
    loadLocalStorage: (key) => savedValues.get(key) ?? null,
    saveLocalStorage: (key, value) => savedValues.set(key, value),
  },
};

const client = new WebSocketClient(plugin, {
  getWsUrl: (count) => `ws://127.0.0.1:9000/sync?count=${count}`,
});

await client.register();
assert.equal(FakeWebSocket.instances.length, 1);
const firstSocket = FakeWebSocket.instances[0];
firstSocket.open();
assert.equal(client.isOpen, true);

// Contract: ordinary reconnect may keep a healthy socket, but mobile recovery
// must replace an OPEN object whose transport may have been suspended.
client.triggerReconnect();
assert.equal(FakeWebSocket.instances.length, 1);

client.forceReconnect();
assert.equal(FakeWebSocket.instances.length, 2);
assert.equal(firstSocket.closeCalls.length, 1);
assert.notEqual(client.ws, firstSocket);

// Contract: two focus/visibility notifications in one turn still converge to
// a live socket instead of leaving the second forced reconnect without a socket.
client.forceReconnect();
client.forceReconnect();
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(FakeWebSocket.instances.length, 3);
assert.equal(client.ws, FakeWebSocket.instances[2]);

// Contract: an explicit unregister followed immediately by register does not
// get stuck waiting on the invalidated health probe.
let releaseProbe;
const probePromise = new Promise((resolve) => {
  releaseProbe = resolve;
});
const probeClient = new WebSocketClient(plugin, {
  getWsUrl: (count) => `ws://127.0.0.1:9000/probe?count=${count}`,
  preConnectProbe: async () => probePromise,
});
const firstRegister = probeClient.register();
probeClient.unRegister();
const secondRegister = probeClient.register();
releaseProbe(true);
await Promise.all([firstRegister, secondRegister]);
assert.equal(FakeWebSocket.instances.length, 4);
assert.equal(probeClient.ws, FakeWebSocket.instances[3]);

console.log("websocket-reconnect.test.mjs: all scenarios passed");
