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
        resetFileDownloadSessions: () => undefined,
        receiveFileUploadSessionNotFound: () => undefined,
      };
    case "./operator":
      return {
        receiveOperators: {},
        handleSync: () => undefined,
        cancelSync: () => undefined,
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
    case "../utils/types":
      return { CLIENT_TYPE: "test" };
    case "../../i18n/lang":
      return { $: (key) => key };
    case "./background_activity_gate":
      return { waitForForeground: async () => true };
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

const { shouldUseProtobufTransport } = module.exports;

// Contract: an existing protobuf=true preference cannot re-enable the broken
// wire format on iOS after a reconnect or ClientInfo acknowledgement.
assert.equal(shouldUseProtobufTransport(true, true), false);
assert.equal(shouldUseProtobufTransport(false, true), false);

// Contract: the safety gate is scoped to iOS and preserves the user's choice
// on desktop and Android.
assert.equal(shouldUseProtobufTransport(true, false), true);
assert.equal(shouldUseProtobufTransport(false, false), false);
