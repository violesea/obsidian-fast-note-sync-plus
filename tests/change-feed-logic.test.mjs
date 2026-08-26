// FNS v2 变更流纯逻辑契约测试（无 obsidian 依赖，VM 直跑 transpile 产物）。
// 保护的契约见 src/lib/sync/change_feed_logic.ts 头注释与施工方案-v2 4.2/3.4 节。
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "change_feed_logic.ts");
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
  Date,
  Math,
  crypto: undefined, // 强制走 generateUuidV4 的 rng 注入分支
}, { filename: sourcePath });
const L = module.exports;

// ---------------------------------------------------------------- plan ---
const base = { enabled: true, sidecarUrl: "http://127.0.0.1:9100", deviceId: "u", cursorRev: null, baselinesReady: true, syncEnabled: true, syncMode: "auto" };

// 契约：未启用/无URL/非auto轮 → off（v1 原路径，默认关闭即无行为变化）
assert.equal(L.planChangeFeedRound({ ...base, enabled: false }), "off");
assert.equal(L.planChangeFeedRound({ ...base, sidecarUrl: "  " }), "off");
assert.equal(L.planChangeFeedRound({ ...base, syncMode: "note" }), "off");
assert.equal(L.planChangeFeedRound({ ...base, syncEnabled: false }), "off");
assert.equal(L.planChangeFeedRound({ ...base, deviceId: null }), "off");

// 契约：启用但基线未就绪（首装）→ defer，先走 v1 全量，完成后再采纳
assert.equal(L.planChangeFeedRound({ ...base, baselinesReady: false }), "defer");

// 契约：基线就绪且无游标 → adopt；有正游标 → poll；游标为 0（失效残留）→ defer
assert.equal(L.planChangeFeedRound({ ...base, cursorRev: null }), "adopt");
assert.equal(L.planChangeFeedRound({ ...base, cursorRev: 457200 }), "poll");
assert.equal(L.planChangeFeedRound({ ...base, cursorRev: 0 }), "defer");

// Contract: a change-feed round does not resend a stale prepared snapshot
// after reconnect; disabled, empty-sidecar, and partial-sync rounds retain the
// legacy prepared-snapshot behavior.
assert.equal(L.shouldRestartFreshRoundOnResume({
  enabled: true,
  sidecarUrl: base.sidecarUrl,
  syncEnabled: true,
  syncMode: "auto",
}), true);
assert.equal(L.shouldRestartFreshRoundOnResume({
  enabled: true,
  sidecarUrl: "",
  syncEnabled: true,
  syncMode: "auto",
}), false);
assert.equal(L.shouldRestartFreshRoundOnResume({
  enabled: true,
  sidecarUrl: base.sidecarUrl,
  syncEnabled: true,
  syncMode: "note",
}), false);

// ------------------------------------------------------------ classify ---
const mk = (over) => ({ rev: 1, type: "note", action: "modify", path: "a/b.md", path_hash: "ph", content_hash: "ch", ...over });
const noExcl = () => false;

// folder/setting 派生类型跳过（D-106：文件夹不再独立协商）
assert.equal(L.classifyChange(mk({ type: "folder", action: "create" }), noExcl).kind, "skip");
assert.equal(L.classifyChange(mk({ type: "setting", action: "modify" }), noExcl).kind, "skip");
// 排除路径跳过
assert.equal(L.classifyChange(mk({}), (p) => p === "a/b.md").kind, "skip");
// 删除动作 → delete（含 soft_delete）
assert.equal(L.classifyChange(mk({ action: "soft_delete" }), noExcl).kind, "delete");
assert.equal(L.classifyChange(mk({ type: "file", action: "delete" }), noExcl).kind, "delete");
// mtime-only 修改跳过（免费省流）
assert.equal(L.classifyChange(mk({ changed_fields: "mtime" }), noExcl).kind, "skip");
assert.equal(L.classifyChange(mk({ changed_fields: "content,mtime" }), noExcl).kind, "fetch");
// rename 仅新路径一行 → 按 fetch upsert（旧路径清理由 manifest/digest 对账）
assert.equal(L.classifyChange(mk({ action: "rename", changed_fields: "path" }), noExcl).kind, "fetch");

// ------------------------------------------------- selectApplicable ---
const probe = {
  trackedHash: (p) => (p === "synced.md" ? "ch-server" : null),
  fileExists: (p) => p !== "missing.md" && p !== "gone.md",
  hasPendingEdit: (p) => p === "dirty-local.md",
  verifyLocalHash: async (change) => change.path === "synced.md" ? "ch-server" : "ch-local",
};
const batch = [
  mk({ path: "synced.md", content_hash: "ch-server" }),     // 已最新 → skip
  mk({ path: "changed.md", content_hash: "ch-new" }),        // 需拉取 → fetch
  mk({ path: "dirty-local.md", content_hash: "ch-x" }),      // 本地未同步编辑 → skip 保护
  mk({ path: "gone.md", action: "soft_delete" }),            // 本地已不存在 → skip
  mk({ path: "here.md", action: "delete" }),                 // 本地存在 → delete
];
const { actions, counts } = await L.selectApplicableChanges(batch, probe, noExcl);
assert.equal(counts.fetch, 1);
assert.equal(counts.delete, 1);
assert.equal(counts.skipped, 3);
assert.equal(actions.filter((a) => a.kind === "fetch")[0].change.path, "changed.md");
assert.equal(actions.filter((a) => a.kind === "delete")[0].change.path, "here.md");

// Contract: a persisted baseline match without a current filesystem
// verification must not suppress the remote fetch.
const unverifiable = await L.selectApplicableChanges(
  [mk({ path: "synced.md", content_hash: "ch-server" })],
  {
    trackedHash: () => "ch-server",
    fileExists: () => true,
    hasPendingEdit: () => false,
  },
  noExcl,
);
assert.equal(unverifiable.counts.fetch, 1);
assert.equal(unverifiable.actions[0].kind, "fetch");

// ------------------------------------------------------ computeAdoption ---
// 服务端记得游标则以服务端为准（幂等重装）；新设备回溯 ADOPTION_BACKTRACK_REVS
assert.equal(L.computeAdoptionRev(457263, 459900), 457263);
assert.equal(L.computeAdoptionRev(0, 459900), 459900 - 5000);
assert.equal(L.computeAdoptionRev(0, 100), 0);
assert.equal(L.computeRepairStartRev(464990), 459990);
assert.equal(L.computeRepairStartRev(3000), 0);

// Contract: schema 1 cursors are downgraded to a bounded replay window and
// remain marked until a successful catch-up clears the repair flag.
const migratedCursor = L.migrateChangeFeedCursorState({
  schema: 1,
  deviceId: "ipad-device",
  vault: "New-World",
  rev: 464990,
  noteWatermarkMs: 123,
  fileWatermarkMs: 456,
});
assert.equal(migratedCursor.migrated, true);
assert.equal(migratedCursor.state.schema, 2);
assert.equal(migratedCursor.state.rev, 459990);
assert.equal(migratedCursor.state.repairPending, true);
const v2Cursor = L.migrateChangeFeedCursorState({
  schema: 2,
  deviceId: "ipad-device",
  vault: "New-World",
  rev: 464990,
  repairPending: false,
});
assert.equal(v2Cursor.migrated, false);
assert.equal(v2Cursor.state.rev, 464990);
assert.equal(v2Cursor.state.repairPending, false);

// ------------------------------------------------------ advanceWatermark ---
// 无时区后缀按 UTC 解析；水位只进不退
const w = L.advanceWatermark(0, [mk({ created_at: "2026-08-22T08:00:00" })]);
assert.equal(w, Date.parse("2026-08-22T08:00:00Z"));
assert.equal(L.advanceWatermark(w, [mk({ created_at: "2026-08-21T00:00:00Z" })]), w);
// 带显式时区且绝对时间更晚（20:00+08:00 = 12:00Z）才推进
assert.equal(L.advanceWatermark(w, [mk({ created_at: "2026-08-22T20:00:00+08:00" })]), Date.parse("2026-08-22T20:00:00+08:00"));

// ---------------------------------------------------------- envelope ---
assert.deepEqual(L.unwrapEnvelope({ ok: true, data: { x: 1 } }, 200), { x: 1 });
const capture = (fn) => { try { fn(); return null; } catch (e) { return e; } };
const err = capture(() => L.unwrapEnvelope({ ok: false, error: "stale_cursor", detail: {} }, 409));
assert.ok(err instanceof L.SidecarProtocolError);
assert.equal(err.isStaleCursor, true);
const err401 = capture(() => L.unwrapEnvelope({ ok: false, error: "unauthorized" }, 401));
assert.ok(err401 instanceof L.SidecarProtocolError);
assert.equal(err401.isUnauthorized, true);
assert.ok(capture(() => L.unwrapEnvelope("not-an-object", 502)) instanceof L.SidecarProtocolError);

// ------------------------------------------------------------- uuid ---
assert.equal(L.isValidUuidV4("6db99ac7-09a3-4c8e-8b1f-2f2c6b5b9c11"), true);
assert.equal(L.isValidUuidV4("not-a-uuid"), false);
assert.equal(L.isValidUuidV4(null), false);
assert.equal(L.isValidUuidV4("6db99ac7-09a3-1c8e-8b1f-2f2c6b5b9c11"), false); // 非 v4
const rngId = L.generateUuidV4(() => 0.5);
assert.match(rngId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);

console.log("change-feed-logic.test.mjs: all scenarios passed");
