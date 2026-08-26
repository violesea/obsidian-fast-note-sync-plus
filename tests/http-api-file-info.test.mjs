import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "api", "http_api_service.ts");
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
  require: (id) => {
    if (id === "obsidian") return { requestUrl: async () => ({ status: 200, json: {} }) };
    if (id === "../utils/helpers") {
      return {
        hashContent: () => "path-hash",
        addRandomParam: (url) => url,
        showSyncNotice: () => undefined,
        dump: () => undefined,
        dumpError: () => undefined,
        nativeFetch: async () => ({ ok: true, status: 200, url: "", clone: () => ({ json: async () => ({}) }) }),
        isAllowedRedirect: () => true,
      };
    }
    if (id === "../utils/types") return { CLIENT_TYPE: "ObsidianPlugin" };
    if (id === "../../i18n/lang") return { getLocale: () => "en" };
    if (id === "../../main") return {};
    throw new Error(`Unexpected require: ${id}`);
  },
  module,
  exports: module.exports,
  URLSearchParams,
  Promise,
}, { filename: sourcePath });

const { HttpApiService } = module.exports;
const plugin = { settings: { vault: "vault" } };
const service = new HttpApiService(plugin);

// Contract: the real HTTP-200 business envelope for a missing record maps to
// null so cloud-preview reconciliation can upload it.
service.request = async () => ({
  status: 200,
  json: { code: 0, status: false, message: "file info failed", data: null, details: "record not found" },
});
assert.equal(await service.getFileInfo("missing.bin"), null);

// Contract: a successful file-info envelope remains a typed response.
const fileInfo = { id: 7, path: "present.bin", pathHash: "h", size: 10, mtime: 20, contentHash: "c", isRecycle: false, updatedAt: "now" };
service.request = async () => ({ status: 200, json: { code: 200, data: fileInfo } });
assert.deepEqual(await service.getFileInfo("present.bin"), fileInfo);

// Contract: transport and other server errors are not converted to missing.
service.request = async () => { throw new Error("network down"); };
await assert.rejects(() => service.getFileInfo("unknown.bin"), /network down/);
service.request = async () => ({ status: 500, json: { code: 500, message: "server down", data: null } });
await assert.rejects(() => service.getFileInfo("unknown.bin"), /server down/);

console.log("http-api-file-info.test.mjs: all scenarios passed");
