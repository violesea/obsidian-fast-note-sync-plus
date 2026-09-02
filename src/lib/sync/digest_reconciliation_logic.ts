/**
 * FNS v2 · M4 分叉抽查（digest 兜底）· 纯逻辑层（无 obsidian 依赖）。
 *
 * 职责：
 * - shouldRunDigestCheck：触发闸（平台/开关/基线/节流）纯判定
 * - computeLocalDigest：与 sidecar /vault/digest 同算法的本地摘要
 *   （SHA-256( 按路径 UTF-8 字节升序拼接的 "path\0content_hash\n" )）
 * - planDigestCheck：首轮建基线 / 一致 / 分叉 三态判定
 * - classifyManifestAgainstLocal + findServerLoss：manifest 差集分类
 *
 * 对应施工方案-v2 第 5 节 digest 契约与产品架构 INV-1（本地缺失≠删除）、
 * 想法1 裁决「只推不校会永久分叉，分叉时客户端毫不知情——必须补 digest 抽查」。
 * 本模块不得 import obsidian 或插件单例，保证 node 测试可直接加载。
 */

// ---------------------------------------------------------------- 类型 ---

export interface DigestEntryProbe {
  /** 服务端确认基线哈希（syncHashMap 视图）；无则 null */
  baselineHash(path: string): string | null;
  /** 本地基线中已确认的全部路径（找服务端丢失面用） */
  baselinePaths(): Iterable<string>;
  /** 本地文件是否真实存在 */
  fileExists(path: string): boolean;
  /** 本地存在未同步编辑（pending 修改/删除）的路径：跳过，避免覆盖或在途操作抢跑 */
  hasPendingEdit(path: string): boolean;
  /** 排除规则（与 syncExclude* 同口径，白名单优先） */
  isExcluded(path: string): boolean;
}

export interface ManifestEntry {
  path: string;
  type: "note" | "file";
  content_hash: string;
  size?: number;
  mtime?: number;
}

/** sidecar GET /vault/digest 响应 data（施工方案-v2 第 5 节契约）。 */
export interface DigestResponse {
  prefix: string;
  entries: number;
  digest: string;
  children: Array<{ prefix: string; entries: number; digest: string }>;
}

// ---------------------------------------------------------------- 触发闸 ---

/** 兜底抽查默认周期：24h。它是兜底层，不是变更路径，跑太勤只会烧 manifest 流量。 */
export const DIGEST_CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000;

/** 直接逐路径修复的上限；超过则改走一次有界 v1 全量修复（repairRequested）。 */
export const DIGEST_MAX_DIRECT_REPAIR = 500;

export interface DigestCheckGateInput {
  isMobile: boolean;
  enabled: boolean;
  syncEnabled: boolean;
  /** 服务端基线已提交（canUseIncrementalSync）——基线未提交时 syncHashMap 不可信 */
  baselinesReady: boolean;
  sidecarUrl: string;
  sidecarToken: string;
  deviceId: string | null;
  lastCheckedAt: number;
  now: number;
  /** 上一轮抽查仍在跑 */
  alreadyRunning: boolean;
}

export function shouldRunDigestCheck(input: DigestCheckGateInput): { run: boolean; reason: string } {
  if (input.alreadyRunning) return { run: false, reason: "already-running" };
  if (input.isMobile) return { run: false, reason: "mobile" };
  if (!input.enabled) return { run: false, reason: "disabled" };
  if (!input.syncEnabled) return { run: false, reason: "sync-off" };
  if (!input.baselinesReady) return { run: false, reason: "baseline-not-committed" };
  if (!input.sidecarUrl || input.sidecarUrl.trim() === "") return { run: false, reason: "no-sidecar" };
  if (!input.sidecarToken || input.sidecarToken.trim() === "") return { run: false, reason: "no-token" };
  if (!input.deviceId) return { run: false, reason: "no-device-id" };
  if (input.lastCheckedAt > 0 && input.now - input.lastCheckedAt < DIGEST_CHECK_INTERVAL_MS) {
    return { run: false, reason: "throttled" };
  }
  return { run: true, reason: "due" };
}

// ---------------------------------------------------------------- 摘要 ---

/** SHA-256 十六进制（Web Crypto；Node 测试环境有等价 webcrypto）。 */
export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("Web Crypto subtle unavailable");
  const digest = await subtle.digest("SHA-256", bytes as BufferSource);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * 本地摘要：与 sidecar /vault/digest 完全同算法。
 * 输入只应来自服务端确认基线（syncHashMap）——本地未确认的写入不属于
 * 「客户端视角的服务端集合」，混入会让根摘要永久抖动、抽查退化为每轮全量 diff。
 */
export async function computeLocalDigest(
  entries: Iterable<{ path: string; hash: string }>,
  isExcluded: (path: string) => boolean,
): Promise<{ digest: string; entries: number }> {
  const encoder = new TextEncoder();
  const rows: Array<{ pathBytes: Uint8Array; line: Uint8Array }> = [];
  for (const entry of entries) {
    if (!entry.hash) continue;
    if (isExcluded(entry.path)) continue;
    rows.push({
      pathBytes: encoder.encode(entry.path),
      line: encoder.encode(`${entry.path}\0${entry.hash}\n`),
    });
  }
  // sidecar 按 UTF-8 字节序排序（Python bytes 比较），客户端必须同序。
  rows.sort((a, b) => {
    const ua = a.pathBytes;
    const ub = b.pathBytes;
    const len = Math.min(ua.length, ub.length);
    for (let i = 0; i < len; i++) {
      if (ua[i] !== ub[i]) return ua[i] - ub[i];
    }
    return ua.length - ub.length;
  });
  let total = 0;
  for (const row of rows) total += row.line.length;
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const row of rows) {
    bytes.set(row.line, offset);
    offset += row.line.length;
  }
  return { digest: await sha256Hex(bytes), entries: rows.length };
}

export type DigestCheckPlan = "store-baseline" | "match" | "divergent";

/**
 * 三态判定：首轮只存基线不动内容（避免全新设备把「尚未同步完」当成「服务端丢失」）；
 * 根一致直接通过；不一致才进 manifest diff。
 */
export function planDigestCheck(lastKnownRoot: string | null, serverRoot: string): DigestCheckPlan {
  if (!lastKnownRoot) return "store-baseline";
  return lastKnownRoot === serverRoot ? "match" : "divergent";
}

// ---------------------------------------------------------------- 差集 ---

export interface ManifestClassification {
  /** 服务端有、本地文件缺失或基线哈希不同且无在途本地编辑 → 可安全拉取（INV-1 的补集面） */
  toFetch: ManifestEntry[];
  /** 本地基线有、服务端 manifest 没有 → 服务端疑似丢失内容。只告警，绝不动本地（INV-1）。 */
  serverLoss: string[];
  /** 双方都有但哈希不同 → 交给 v1 上传/冲突流程，digest 不自动覆盖（绝不静默覆盖）。 */
  diverged: string[];
  counts: {
    serverEntries: number;
    fetchable: number;
    loss: number;
    diverged: number;
    skippedExcluded: number;
  };
}

export function classifyManifestAgainstLocal(
  serverEntries: Iterable<ManifestEntry>,
  probe: DigestEntryProbe,
): ManifestClassification {
  const toFetch: ManifestEntry[] = [];
  const diverged: string[] = [];
  const serverPaths = new Set<string>();
  let serverCount = 0;
  let skippedExcluded = 0;

  for (const entry of serverEntries) {
    if (entry.type !== "note" && entry.type !== "file") continue;
    serverCount++;
    serverPaths.add(entry.path);
    if (probe.isExcluded(entry.path)) {
      skippedExcluded++;
      continue;
    }
    const localFile = probe.fileExists(entry.path);
    const baseline = probe.baselineHash(entry.path);
    if (localFile) {
      if (probe.hasPendingEdit(entry.path)) continue;
      if (baseline !== null && baseline === entry.content_hash) continue;
      diverged.push(entry.path);
      continue;
    }
    if (probe.hasPendingEdit(entry.path)) continue;
    if (baseline === null || baseline !== entry.content_hash) toFetch.push(entry);
  }

  const serverLoss = findServerLoss(probe.baselinePaths(), serverPaths, probe);
  return {
    toFetch,
    serverLoss,
    diverged,
    counts: {
      serverEntries: serverCount,
      fetchable: toFetch.length,
      loss: serverLoss.length,
      diverged: diverged.length,
      skippedExcluded,
    },
  };
}

/**
 * 本地基线中的路径不在服务端 manifest → 服务端疑似丢失。
 * 排除面不计（本设备声明不持有）；在途本地编辑不计（上传/删除尚未确认属正常窗口）。
 */
export function findServerLoss(
  baselinePaths: Iterable<string>,
  serverPaths: Set<string>,
  probe: Pick<DigestEntryProbe, "isExcluded" | "hasPendingEdit">,
): string[] {
  const loss: string[] = [];
  for (const path of baselinePaths) {
    if (probe.isExcluded(path)) continue;
    if (probe.hasPendingEdit(path)) continue;
    if (!serverPaths.has(path)) loss.push(path);
  }
  return loss;
}
