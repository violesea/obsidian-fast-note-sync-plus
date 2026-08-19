import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "websocket_manager.ts");
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
const requireStub = (id) => {
  switch (id) {
    case "obsidian":
      return {
        moment: Object.assign(() => ({ format: () => "" }), { locale: () => "zh-cn" }),
        Platform: {},
        normalizePath: (value) => value,
      };
    case "../utils/helpers":
      return {
        dump: () => undefined,
        addRandomParam: (value) => value,
        showSyncNotice: () => undefined,
        safeStringify: (value) => String(value),
        getPluginDir: () => ".obsidian/plugins/fast-note-sync",
        hashContent: () => "hash",
      };
    case "../../pb/protobuf_mapper":
      return { enSendDTOToProtobuf: () => new Uint8Array(), deReceivePacket: () => ({}) };
    case "./operator_file":
      return {
        handleFileChunkDownload: () => undefined,
        BINARY_PREFIX_FILE_SYNC: "fs",
        clearUploadQueue: () => undefined,
        receiveFileUploadSessionNotFound: () => undefined,
      };
    case "./operator":
      return {
        receiveOperators: {},
        handleSync: () => undefined,
        settleAllBatchSendSessionsOnClose: () => undefined,
      };
    case "./websocket_action":
      return new Proxy({}, { get: (_target, property) => String(property) });
    case "./sync_log_manager":
      return { SyncLogManager: { getInstance: () => ({ logReceivedMessage: () => undefined, logSentMessage: () => undefined }) } };
    case "./websocket_client":
      return { WebSocketClient: class {} };
    case "./resume_recovery":
      return { ResumeRecoveryCoordinator: class {} };
    case "./auth_sync_coordinator":
      return { AuthSyncCoordinator: class {} };
    case "./connection_supervisor":
      return { ConnectionSupervisor: class {} };
    case "./sync_trigger_policy":
      return { decideSyncAfterAuthentication: () => ({ kind: "none", reason: "manual-mode" }) };
    case "../utils/types":
      return { CLIENT_TYPE: "test" };
    case "../../i18n/lang":
      return { $: (key) => key };
    default:
      throw new Error(`Unexpected require: ${id}`);
  }
};

vm.runInNewContext(transpiled, {
  require: requireStub,
  module,
  exports: module.exports,
  console,
  WebSocket,
  TextDecoder,
  setTimeout,
  clearTimeout,
}, { filename: sourcePath });

const { formatAuthorizationError } = module.exports;

assert.equal(typeof formatAuthorizationError, "function");

const missingMessage = formatAuthorizationError({ code: 308 });
assert.match(missingMessage, /Code=308/);
assert.match(missingMessage, /Session expired or token has been revoked/);
assert.match(missingMessage, /Please re-import the API configuration/);
assert.doesNotMatch(missingMessage, /undefined/);

const rotated = formatAuthorizationError({ code: 308, details: ["Token has been rotated"] });
assert.match(rotated, /Details=Token has been rotated/);
assert.match(rotated, /Please re-import the API configuration/);

const scopeRestricted = formatAuthorizationError({ code: 315, message: undefined, details: "Permission denied: Handshake" });
assert.match(scopeRestricted, /Authorization token scope is restricted/);
assert.match(scopeRestricted, /Details=Permission denied: Handshake/);
assert.doesNotMatch(scopeRestricted, /Please re-import/);
assert.doesNotMatch(scopeRestricted, /undefined/);
