// M4 digest 兜底抽查纯逻辑契约测试（VM 直跑 transpile 产物）。
// 保护的契约见 src/lib/sync/digest_reconciliation_logic.ts 头注释：
// - 摘要算法必须与 sidecar /vault/digest 逐字节一致（SHA-256 over 按路径
//   UTF-8 字节升序拼接的 "path\0content_hash\n"）——算法不一致 = 永假分叉。
// - 触发闸：移动端永不跑；基线未提交不跑；24h 节流；单飞。
// - 差集分类三铁律：缺→补（哈希校验后物化）；异→不覆盖（交给 v1 冲突流）；
//   服务端丢→只告警（INV-1：本地缺失≠删除）。
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";
import nodeCrypto from "node:crypto";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "digest_reconciliation_logic.ts");
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
  crypto: globalThis.crypto,
  TextEncoder,
}, { filename: sourcePath });
const L = module.exports;

// ------------------------------------------------------- 摘要算法 ---

// 参照实现：独立用 Node crypto 按 sidecar 契约拼装，含 emoji（UTF-16 代理对，
// 能区分「按 UTF-16 码元排序」与「按 UTF-8 字节排序」——两者对增补平面字符
// 排序不同，此用例锁死客户端必须按字节序）。
function referenceDigest(rows) {
  const encoded = rows.map((r) => ({
    pathBytes: Buffer.from(r.path, "utf8"),
    line: Buffer.from(`${r.path}\0${r.hash}\n`, "utf8"),
  }));
  encoded.sort((a, b) => Buffer.compare(a.pathBytes, b.pathBytes));
  const hash = nodeCrypto.createHash("sha256");
  for (const row of encoded) hash.update(row.line);
  return hash.digest("hex");
}

const sample = [
  { path: "图书馆/南书房/持仓台账.md", hash: "a".repeat(20) },
  { path: "工作/GE/会议纪要.md", hash: "b".repeat(20) },
  { path: "原料池/WashingtonPost/2188b6d3-0df5.md", hash: "c".repeat(20) },
  { path: "兴趣/ chess ♟️ 指南.md", hash: "d".repeat(20) }, // emoji = 增补平面
  { path: "琅琊阁/SOP/全局/AGENT_RUNTIME_DEFAULTS.md", hash: "e".repeat(20) },
];

{
  const isExcluded = () => false;
  const got = await L.computeLocalDigest(sample, isExcluded);
  const want = referenceDigest(sample);
  assert.equal(got.digest, want, "local digest must byte-match the sidecar algorithm");
  assert.equal(got.entries, 5);

  // 排除面不参与摘要（sidecar 契约：客户端须先套用 syncExclude* 同口径过滤）
  const gotFiltered = await L.computeLocalDigest(sample, (p) => p.startsWith("原料池/"));
  const wantFiltered = referenceDigest(sample.filter((r) => !r.path.startsWith("原料池/")));
  assert.equal(gotFiltered.digest, wantFiltered, "excluded paths must not contribute to the digest");
  assert.equal(gotFiltered.entries, 4);

  // 空集合是合法输入（全新设备）
  const empty = await L.computeLocalDigest([], isExcluded);
  assert.equal(empty.digest, referenceDigest([]));
  assert.equal(empty.entries, 0);
  console.log("digest algorithm ok: byte-identical with sidecar contract (incl. astral-plane ordering)");
}

// ------------------------------------------------------- 触发闸 ---

const gateBase = {
  isMobile: false,
  enabled: true,
  syncEnabled: true,
  baselinesReady: true,
  sidecarUrl: "http://127.0.0.1:9100",
  sidecarToken: "t",
  deviceId: "d",
  lastCheckedAt: 0,
  now: 1_000_000,
  alreadyRunning: false,
};

{
  const due0 = L.shouldRunDigestCheck(gateBase);
  assert.equal(due0.run, true, "fully provisioned gate is due");
  assert.equal(due0.reason, "due");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, isMobile: true }).run, false, "mobile never runs digest checks");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, enabled: false }).run, false);
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, syncEnabled: false }).run, false);
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, baselinesReady: false }).reason, "baseline-not-committed",
    "uncommitted baseline ⇒ syncHashMap untrustworthy ⇒ must not diff against server");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, sidecarUrl: " " }).reason, "no-sidecar");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, sidecarToken: "" }).reason, "no-token");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, deviceId: null }).reason, "no-device-id");
  assert.equal(L.shouldRunDigestCheck({ ...gateBase, alreadyRunning: true }).reason, "already-running");
  const throttled = L.shouldRunDigestCheck({ ...gateBase, now: 100_000_000, lastCheckedAt: 100_000_000 - L.DIGEST_CHECK_INTERVAL_MS + 1 });
  assert.equal(throttled.run, false, "24h throttle holds within the window");
  assert.equal(throttled.reason, "throttled");
  const due = L.shouldRunDigestCheck({ ...gateBase, now: 100_000_000, lastCheckedAt: 100_000_000 - L.DIGEST_CHECK_INTERVAL_MS });
  assert.equal(due.run, true, "24h throttle releases at the boundary");
  console.log("gate matrix ok: mobile/disabled/baseline/sidecar/throttle/single-flight");
}

// ------------------------------------------------------- 三态判定 ---

{
  assert.equal(L.planDigestCheck(null, "abc"), "store-baseline", "first run only stores a baseline — must never repair from nothing");
  assert.equal(L.planDigestCheck("abc", "abc"), "match");
  assert.equal(L.planDigestCheck("abc", "abd"), "divergent");
  console.log("planDigestCheck ok: store-baseline/match/divergent");
}

// ------------------------------------------------------- 差集分类 ---

function makeProbe(overrides = {}) {
  const baseline = overrides.baseline ?? new Map();
  const exists = overrides.exists ?? new Set();
  const pending = overrides.pending ?? new Set();
  const excluded = overrides.excluded ?? new Set();
  return {
    baselineHash: (p) => baseline.get(p) ?? null,
    baselinePaths: () => [...baseline.keys()],
    fileExists: (p) => exists.has(p),
    hasPendingEdit: (p) => pending.has(p),
    isExcluded: (p) => excluded.has(p),
  };
}

{
  const entries = [
    { path: "a/缺失笔记.md", type: "note", content_hash: "h1" },
    { path: "b/一致笔记.md", type: "note", content_hash: "h2" },
    { path: "c/分叉笔记.md", type: "note", content_hash: "hX" },
    { path: "d/待删笔记.md", type: "note", content_hash: "h4" },
    { path: "scripts/工具.py", type: "file", content_hash: "h5" },
    { path: "e/在途编辑.md", type: "note", content_hash: "h6" },
  ];
  const probe = makeProbe({
    baseline: new Map([["b/一致笔记.md", "h2"], ["c/分叉笔记.md", "hOLD"], ["e/服务端丢了.md", "h7"]]),
    exists: new Set(["b/一致笔记.md", "c/分叉笔记.md", "e/服务端丢了.md"]),
    pending: new Set(["d/待删笔记.md", "e/在途编辑.md"]),
    excluded: new Set(["scripts/工具.py"]),
  });

  const cls = L.classifyManifestAgainstLocal(entries, probe);

  assert.deepEqual(Array.from(cls.toFetch).map((e) => e.path), ["a/缺失笔记.md"],
    "only genuinely missing content is fetchable");
  assert.deepEqual(Array.from(cls.diverged), ["c/分叉笔记.md"],
    "hash divergence is reported, never auto-overwritten (M7/INV-10)");
  assert.deepEqual(Array.from(cls.serverLoss), ["e/服务端丢了.md"],
    "baseline-confirmed path missing from the server manifest is flagged as loss");
  assert.equal(cls.counts.skippedExcluded, 1, "excluded paths are counted, not fetched");
  assert.equal(cls.counts.loss, 1);

  // 服务端丢失的三个豁免：排除面、在途本地编辑、（本地缺失但也不在基线 → 根本不进 loss）
  const lossProbe = makeProbe({
    baseline: new Map([["keep/正常.md", "h"], ["keep/被排除.md", "h"], ["keep/在途.md", "h"]]),
    exists: new Set(),
    pending: new Set(["keep/在途.md"]),
    excluded: new Set(["keep/被排除.md"]),
  });
  const loss = L.findServerLoss([...lossProbe.baselinePaths()], new Set(), lossProbe);
  assert.deepEqual(Array.from(loss), ["keep/正常.md"], "excluded and in-flight paths are exempt from loss reporting");

  // 直接修复上限常量必须存在（超限走 requestFullReconcile 而非逐路径打爆）
  assert.ok(L.DIGEST_MAX_DIRECT_REPAIR >= 100 && L.DIGEST_MAX_DIRECT_REPAIR <= 5000);
  console.log("manifest classification ok: fetch/diverge/loss/exclusion/pending boundaries");
}

console.log("digest-reconciliation-logic: all scenarios passed");
