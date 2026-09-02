/**
 * FNS v2 · M4 分叉抽查（digest 兜底）· 运行层。
 *
 * 目的（产品架构 v2 想法1 裁决原文）：「只推不校会永久分叉，且分叉时客户端
 * 毫不知情。」变更流与 v1 上传都依赖对方自觉；本模块是周期性第三方对账：
 *
 *   根摘要一致 → 通过（一次请求）
 *   根摘要分叉 → 本地摘要比对 → manifest 差集分类：
 *     - 服务端有、本地缺失（无在途本地编辑）→ 逐路径物化修复（复用变更流的
 *       applyRemoteNote/applyRemoteFile，哈希校验后写盘）
 *     - 超过直接修复上限 → requestFullReconcile 走一次有界 v1 修复
 *     - 双方哈希不同 → 只报告（绝不静默覆盖，交给 v1 上传/冲突流程）
 *     - 本地基线有、服务端没有 → 服务端疑似丢失：只告警（INV-1：本地缺失≠删除）
 *
 * 桌面端专用（gate 拦截移动端）：manifest 是一次性 NDJSON 全量流；
 * 移动端在 M1 working set 落地前不参与批量写入方角色，也不承担对账成本。
 * 默认每 24h 节流一次，在一轮成功同步完成后触发。
 */

import { Platform } from "obsidian";

import { dump, isPathExcluded, showSyncNotice, sleep } from "../utils/helpers";
import type FastSync from "../../main";
import { applyRemoteFile, applyRemoteNote, ChangeFeedClient } from "./change_feed";
import type { SidecarChange } from "./change_feed_logic";
import {
  DIGEST_MAX_DIRECT_REPAIR,
  classifyManifestAgainstLocal,
  computeLocalDigest,
  planDigestCheck,
  shouldRunDigestCheck,
} from "./digest_reconciliation_logic";
import type { DigestCheckPlan, DigestEntryProbe } from "./digest_reconciliation_logic";
import { requireForeground } from "./background_activity_gate";

const DIGEST_STATE_KEY = "digestCheckState";

interface DigestCheckState {
  /** 上次通过抽查时的服务端根摘要 */
  root: string | null;
  checkedAt: number;
}

export interface DigestRunSummary {
  trigger: string;
  plan: DigestCheckPlan;
  serverEntries: number;
  localEntries: number;
  fetched: number;
  fetchFailures: number;
  diverged: number;
  serverLoss: number;
  repairRequested: boolean;
  durationMs: number;
}

function loadDigestState(plugin: FastSync): DigestCheckState {
  try {
    const raw = plugin.localStorageManager.getMetadata(DIGEST_STATE_KEY) as string | null;
    if (!raw) return { root: null, checkedAt: 0 };
    const parsed = JSON.parse(raw) as Partial<DigestCheckState>;
    return {
      root: typeof parsed.root === "string" ? parsed.root : null,
      checkedAt: typeof parsed.checkedAt === "number" ? parsed.checkedAt : 0,
    };
  } catch (error) {
    dump("[Digest] failed to parse digest state; treating as first run:", error);
    return { root: null, checkedAt: 0 };
  }
}

function saveDigestState(plugin: FastSync, state: DigestCheckState): void {
  plugin.localStorageManager.setMetadata(DIGEST_STATE_KEY, JSON.stringify(state));
}

/** 单飞护栏：抽查跑半天也不会有第二份并发（跨触发源共用）。 */
let running = false;

/**
 * 触发入口（fire-and-forget）。在一轮成功同步完成后调用；闸门不过时静默跳过，
 * 只留 dump 痕迹——兜底层绝不能反过来制造噪音或阻塞同步主路径。
 */
export async function maybeRunDigestReconciliation(plugin: FastSync, trigger: string): Promise<DigestRunSummary | null> {
  const state = loadDigestState(plugin);
  const gate = shouldRunDigestCheck({
    isMobile: Platform.isMobile,
    enabled: plugin.settings.digestCheckEnabled !== false,
    syncEnabled: plugin.settings.syncEnabled !== false,
    baselinesReady:
      plugin.incrementalScanManager?.canUseIncrementalSync(plugin.localStorageManager.getMetadata("isInitSync")) === true,
    sidecarUrl: plugin.settings.sidecarUrl ?? "",
    sidecarToken: plugin.settings.sidecarToken ?? "",
    deviceId: plugin.changeFeedDeviceId,
    lastCheckedAt: state.checkedAt,
    now: Date.now(),
    alreadyRunning: running,
  });
  if (!gate.run) {
    dump(`[Digest] skip (${gate.reason}) trigger=${trigger}`);
    return null;
  }

  running = true;
  const startedAt = Date.now();
  try {
    const summary = await runDigestReconciliation(plugin, state, trigger, startedAt);
    dump(`[Digest] ${summary.plan} fetched=${summary.fetched}/${summary.fetchFailures} diverged=${summary.diverged} loss=${summary.serverLoss} repair=${summary.repairRequested} in ${summary.durationMs}ms`);
    return summary;
  } catch (error) {
    dump(`[Digest] run failed (will retry next window):`, error);
    return null;
  } finally {
    running = false;
  }
}

async function runDigestReconciliation(
  plugin: FastSync,
  state: DigestCheckState,
  trigger: string,
  startedAt: number,
): Promise<DigestRunSummary> {
  const vault = plugin.settings.vault;
  const client = new ChangeFeedClient({
    baseUrl: plugin.settings.sidecarUrl ?? "",
    token: plugin.settings.sidecarToken ?? "",
  });

  const server = await client.getDigest(vault, "", 0);
  const plan = planDigestCheck(state.root, server.digest);
  if (plan !== "divergent") {
    saveDigestState(plugin, { root: server.digest, checkedAt: Date.now() });
    return {
      trigger, plan, serverEntries: server.entries, localEntries: 0,
      fetched: 0, fetchFailures: 0, diverged: 0, serverLoss: 0,
      repairRequested: false, durationMs: Date.now() - startedAt,
    };
  }

  // 根摘要分叉：先比本地确认基线的摘要。一致 = 服务端只是新于上次记录（正常前进），存新基线即可。
  const hashManager = plugin.fileHashManager;
  if (!hashManager) throw new Error("hash manager unavailable");
  await requireForeground(plugin);
  const local = await computeLocalDigest(
    hashManager.getSyncEntries().map(([path, hash]) => ({ path, hash })),
    (p) => isPathExcluded(p, plugin),
  );
  if (local.digest === server.digest) {
    saveDigestState(plugin, { root: server.digest, checkedAt: Date.now() });
    return {
      trigger, plan: "match", serverEntries: server.entries, localEntries: local.entries,
      fetched: 0, fetchFailures: 0, diverged: 0, serverLoss: 0,
      repairRequested: false, durationMs: Date.now() - startedAt,
    };
  }

  // 真分叉：manifest 差集分类（一次性 NDJSON，桌面端专用）。
  const manifest = await client.getManifest(vault);
  const classification = classifyManifestAgainstLocal(manifest, buildDigestProbe(plugin));

  let repairRequested = false;
  let fetched = 0;
  let fetchFailures = 0;

  if (classification.toFetch.length > DIGEST_MAX_DIRECT_REPAIR) {
    // 缺口过大：逐路径物化不划算也不 RENDER 友好，交给一次有界 v1 全量修复。
    plugin.incrementalScanManager?.requestFullReconcile();
    repairRequested = true;
  } else {
    for (const entry of classification.toFetch) {
      // 新一轮同步开始时让路：剩余缺口留给下一次抽查（本次不存根摘要，下轮重新 diff）。
      if (plugin.syncState.isSyncing) break;
      try {
        if (entry.type === "note") {
          const note = await plugin.api.getNoteContent(entry.path);
          if (!note) throw new Error(`note fetch returned empty: ${entry.path}`);
          await applyRemoteNote(plugin, entry.path, note);
        } else {
          const change: SidecarChange = {
            rev: 0,
            type: "file",
            action: "modify",
            path: entry.path,
            content_hash: entry.content_hash,
            size: entry.size,
            mtime: entry.mtime,
            ctime: 0,
          };
          await applyRemoteFile(plugin, change);
        }
        fetched++;
      } catch (error) {
        fetchFailures++;
        dump(`[Digest] materialize failed (kept for next run): ${entry.path}`, error);
      }
      await sleep(0);
    }
    if (fetched + fetchFailures === classification.toFetch.length && fetchFailures === 0) {
      saveDigestState(plugin, { root: server.digest, checkedAt: Date.now() });
    }
  }

  // 服务端疑似丢失：只告警不行动（INV-1——本地缺失不构成删除依据，也绝不因
  // 服务端缺一行就反向推送覆盖真源裁决权）。「分叉时客户端毫不知情」是本层
  // 要消灭的原始缺陷，所以这里必须有用户可见的提示，不能只写 debug 日志。
  if (classification.serverLoss.length > 0) {
    dump(`[Digest] SERVER LOSS candidates (${classification.serverLoss.length}): ${classification.serverLoss.slice(0, 50).join(", ")}${classification.serverLoss.length > 50 ? " …" : ""}`);
    showSyncNotice(
      `⚠ digest 兜底抽查发现 ${classification.serverLoss.length} 条「本地已确认、服务端缺失」的路径（详见调试日志），未做任何自动处理`,
      10000,
    );
  }

  return {
    trigger,
    plan: "divergent",
    serverEntries: classification.counts.serverEntries,
    localEntries: local.entries,
    fetched,
    fetchFailures,
    diverged: classification.diverged.length,
    serverLoss: classification.serverLoss.length,
    repairRequested,
    durationMs: Date.now() - startedAt,
  };
}

function buildDigestProbe(plugin: FastSync): DigestEntryProbe {
  return {
    baselineHash: (path) => plugin.fileHashManager?.getPathHash(path) ?? null,
    baselinePaths: () => plugin.fileHashManager?.getAllPaths() ?? [],
    fileExists: (path) => plugin.app.vault.getAbstractFileByPath(path) != null,
    hasPendingEdit: (path) =>
      plugin.pendingNoteModifies.has(path)
      || plugin.syncState.conflictedPaths.has(path)
      || plugin.pendingDeleteNotePaths.has(path)
      || plugin.pendingDeleteFilePaths.has(path),
    isExcluded: (path) => isPathExcluded(path, plugin),
  };
}
