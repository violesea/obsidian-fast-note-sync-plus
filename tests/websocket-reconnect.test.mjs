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

  send() {
    if (this.sendShouldThrow) throw new Error("simulated socket send failure");
  }
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
client.isAuth = true;
client.markAuthenticated();

// Contract: an iOS WebView send failure is converted into a closed transport
// result instead of rejecting the upload task from inside the socket callback.
firstSocket.sendShouldThrow = true;
assert.equal(await client.SendBinary(new Uint8Array([1, 2, 3]), "fs"), "closed");
firstSocket.sendShouldThrow = false;

// Contract: an asynchronous binary handler rejection is contained by the
// transport boundary and never becomes an unhandled Promise rejection.
client.registerBinaryHandler("fs", () => Promise.reject(new Error("simulated handler failure")));
const unhandledRejections = [];
const onUnhandledRejection = (reason) => unhandledRejections.push(reason);
process.on("unhandledRejection", onUnhandledRejection);
firstSocket.onmessage({ data: new Uint8Array([102, 115, 1]).buffer });
await new Promise((resolve) => setTimeout(resolve, 0));
process.off("unhandledRejection", onUnhandledRejection);
assert.equal(unhandledRejections.length, 0);

// Contract: ordinary reconnect keeps a healthy socket.
client.triggerReconnect();
assert.equal(FakeWebSocket.instances.length, 1);

// Contract: if the client has already marked an OPEN object unusable, ordinary
// recovery replaces that stale object instead of getting stuck on register's
// OPEN guard.
client.isOpen = false;
client.triggerReconnect();
assert.equal(FakeWebSocket.instances.length, 2);
const recoveredSocket = FakeWebSocket.instances[1];
recoveredSocket.open();
client.isAuth = true;
client.markAuthenticated();
await new Promise((resolve) => setTimeout(resolve, 0));

// Contract: explicit force reconnect still replaces an OPEN socket when a
// caller deliberately requests a hard reset.
client.forceReconnect();
assert.equal(FakeWebSocket.instances.length, 3);
assert.equal(recoveredSocket.closeCalls.length, 1);
assert.notEqual(client.ws, recoveredSocket);

// Contract: two focus/visibility notifications while the first replacement is
// still in flight are coalesced into one socket replacement.
client.forceReconnect();
client.forceReconnect();
await new Promise((resolve) => setTimeout(resolve, 0));
assert.equal(FakeWebSocket.instances.length, 3);
assert.equal(client.ws, FakeWebSocket.instances[2]);

// Contract: the replacement guard is released only after the new socket is
// authenticated, so a later lifecycle cycle can deliberately replace it.
FakeWebSocket.instances[2].open();
client.isAuth = true;
client.markAuthenticated();
client.forceReconnect();
assert.equal(FakeWebSocket.instances.length, 4);

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

// Contract: unloading/disabling while a pre-connect probe is pending cannot
// create a late WebSocket after the old async callback resolves.
let releaseUnloadedProbe;
const unloadedProbe = new Promise((resolve) => {
  releaseUnloadedProbe = resolve;
});
const unloadedClient = new WebSocketClient(plugin, {
  getWsUrl: (count) => `ws://127.0.0.1:9000/unloaded?count=${count}`,
  preConnectProbe: async () => unloadedProbe,
});
const unloadedRegister = unloadedClient.register();
unloadedClient.unRegister(true);
releaseUnloadedProbe(true);
await unloadedRegister;
assert.equal(unloadedClient.ws, undefined);

const firstRegister = probeClient.register();
probeClient.unRegister();
const secondRegister = probeClient.register();
releaseProbe(true);
await Promise.all([firstRegister, secondRegister]);
assert.equal(FakeWebSocket.instances.length, 5);
assert.equal(probeClient.ws, FakeWebSocket.instances[4]);

console.log("websocket-reconnect.test.mjs: all scenarios passed");
