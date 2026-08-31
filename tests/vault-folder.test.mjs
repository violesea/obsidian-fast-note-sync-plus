import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "vault_folder.ts");
const source = fs.readFileSync(sourcePath, "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
  },
  fileName: sourcePath,
}).outputText;

const module = { exports: {} };
vm.runInNewContext(transpiled, {
  require: () => ({}),
  module,
  exports: module.exports,
  console,
  Promise,
}, { filename: sourcePath });

const { createVaultFolderIdempotent } = module.exports;

// Contract: Obsidian's metadata cache may still miss a folder after another
// concurrent materializer created it. The adapter stat is authoritative for
// this race, so "Folder already exists" must resolve as an idempotent success.
{
  const vault = {
    getFolderByPath: () => null,
    adapter: { stat: async () => ({ type: "folder", ctime: 1, mtime: 1, size: 0 }) },
    createFolder: async () => { throw new Error("Folder already exists."); },
  };
  assert.equal(await createVaultFolderIdempotent(vault, "shared/parent"), "existing");
}

// Contract: a real create failure must not be swallowed merely because some
// path exists. Only an adapter stat whose type is folder satisfies the guard.
{
  const expected = new Error("permission denied");
  const vault = {
    getFolderByPath: () => null,
    adapter: { stat: async () => ({ type: "file", ctime: 1, mtime: 1, size: 1 }) },
    createFolder: async () => { throw expected; },
  };
  await assert.rejects(
    () => createVaultFolderIdempotent(vault, "blocked/parent"),
    (error) => error === expected,
  );
}

console.log("vault-folder.test.mjs: all scenarios passed");
