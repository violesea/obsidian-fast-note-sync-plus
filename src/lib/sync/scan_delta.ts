import { normalizePath } from "obsidian";
import { getPluginDir, dump } from "../utils/helpers";
import type FastSync from "../../main";

/**
 * 扫描断点续扫 / Scan delta checkpoint.
 *
 * 中途检查点原本只写内存（整图落盘会让移动端渲染卡死），iOS 在扫描完成前杀掉
 * WebView 时已算出的哈希全部丢失——下次重载从零重扫，形成"切后台→重载→全盘
 * 哈希"永动环（2026-08-28 iPad 实测）。本模块以追加式 JSONL 把每个检查点新算出
 * 的哈希增量持久化到插件目录；重载后的扫描先预载为内存缓存候选（使用时仍经
 * fingerprint 校验，过期即 miss），跳过已算部分；最终落盘成功后删除。
 *
 * Mid-scan checkpoints were memory-only, so a mid-scan process kill lost every
 * hash computed so far and each reload restarted the full walk from zero
 * (observed live on iPad 2026-08-28). Each checkpoint's newly computed hashes
 * are appended to a JSONL sidecar; the next scan preloads them as in-memory
 * cache candidates (still fingerprint-validated at use), and the file is
 * deleted after the final durable commit.
 */
export type ScanDeltaEntry = { hash: string; mtime: number; size: number; ctime?: number };
export type ScanProgressCheckpoint = {
  processedCount: number;
  totalFiles: number;
  anchorPath: string;
};
export type ScanDeltaRecovery = {
  loaded: number;
  progress: ScanProgressCheckpoint | null;
};

const scanDeltaPath = (plugin: FastSync): string => normalizePath(`${getPluginDir(plugin)}/scanDelta.jsonl`);

// Adapter.append is not guaranteed to be atomic across concurrent callers.
// Serialize note/file/progress records so a reload can never observe two
// interleaved JSON lines or a cursor that was written before its hash batch.
let appendChain: Promise<void> = Promise.resolve();

const appendLines = async (plugin: FastSync, lines: string[]): Promise<boolean> => {
  if (lines.length === 0) return true;
  const task = appendChain.then(async () => {
    await plugin.app.vault.adapter.append(scanDeltaPath(plugin), lines.join("\n") + "\n");
  });
  appendChain = task.catch(() => undefined);
  try {
    await task;
    return true;
  } catch (e) {
    // The caller must retain its in-memory batch when persistence fails.
    dump(`[ScanDelta] append failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`);
    return false;
  }
};

export const appendScanDelta = async (plugin: FastSync, kind: "note" | "file", entries: Map<string, ScanDeltaEntry>): Promise<boolean> => {
  if (entries.size === 0) return true;
  const lines: string[] = [];
  for (const [path, e] of entries) {
    lines.push(JSON.stringify({ k: kind, p: path, h: e.hash, m: e.mtime, s: e.size, c: e.ctime ?? 0 }));
  }
  return appendLines(plugin, lines);
};

export const appendScanProgress = async (plugin: FastSync, checkpoint: ScanProgressCheckpoint): Promise<boolean> => {
  if (!Number.isSafeInteger(checkpoint.processedCount)
    || !Number.isSafeInteger(checkpoint.totalFiles)
    || checkpoint.processedCount <= 0
    || checkpoint.processedCount > checkpoint.totalFiles
    || !checkpoint.anchorPath) return false;
  return appendLines(plugin, [JSON.stringify({
    k: "progress",
    v: 1,
    n: checkpoint.processedCount,
    t: checkpoint.totalFiles,
    a: checkpoint.anchorPath,
  })]);
};

const parseRecovery = (raw: string): {
  noteEntries: Map<string, ScanDeltaEntry>;
  fileEntries: Map<string, ScanDeltaEntry>;
  recovery: ScanDeltaRecovery;
} => {
  const noteEntries = new Map<string, ScanDeltaEntry>();
  const fileEntries = new Map<string, ScanDeltaEntry>();
  let loaded = 0;
  let progress: ScanProgressCheckpoint | null = null;
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    try {
      const d: unknown = JSON.parse(t);
      if (typeof d !== "object" || d === null) continue;
      const rec = d as { k?: unknown; p?: unknown; h?: unknown; m?: unknown; s?: unknown; c?: unknown; v?: unknown; n?: unknown; t?: unknown; a?: unknown };
      if (rec.k === "progress") {
        if (rec.v === 1
          && Number.isSafeInteger(rec.n)
          && Number.isSafeInteger(rec.t)
          && (rec.n as number) > 0
          && (rec.n as number) <= (rec.t as number)
          && typeof rec.a === "string"
          && rec.a.length > 0) {
          progress = { processedCount: rec.n as number, totalFiles: rec.t as number, anchorPath: rec.a };
        }
        continue;
      }
      if (typeof rec.p !== "string" || typeof rec.h !== "string") continue;
      const entry: ScanDeltaEntry = { hash: rec.h, mtime: Number(rec.m) || 0, size: Number(rec.s) || 0, ctime: Number(rec.c) || undefined };
      (rec.k === "file" ? fileEntries : noteEntries).set(rec.p, entry);
      loaded++;
    } catch { /* skip corrupt lines */ }
  }
  return { noteEntries, fileEntries, recovery: { loaded, progress } };
};

export const loadScanRecovery = async (plugin: FastSync): Promise<ScanDeltaRecovery> => {
  try {
    const file = scanDeltaPath(plugin);
    if (!(await plugin.app.vault.adapter.exists(file))) return { loaded: 0, progress: null };
    const raw = await plugin.app.vault.adapter.read(file);
    const { noteEntries, fileEntries, recovery } = parseRecovery(raw);
    // 预载目标：优先用调用方注入的 direct applier（冷建场景的调用者就是
    // FileHashManager 自身，plugin.fileHashManager 回调会指回 stub 而非被测对象）；
    // 无注入时回退到 plugin.fileHashManager（operator 扫描路径）。
    // Preload target: prefer a caller-injected direct applier (in the cold build
    // the caller IS the FileHashManager, so plugin.fileHashManager would point
    // elsewhere); fall back to plugin.fileHashManager for the operator scan.
    const applierContainer = plugin as FastSync & {
      __applyScanDelta?: (noteEntries: Map<string, ScanDeltaEntry>, fileEntries: Map<string, ScanDeltaEntry>) => void;
    };
    if (typeof applierContainer.__applyScanDelta === "function") {
      applierContainer.__applyScanDelta(noteEntries, fileEntries);
    } else if (plugin.fileHashManager) {
      if (noteEntries.size) plugin.fileHashManager.bulkSetFromScanned(noteEntries, false);
      if (fileEntries.size) plugin.fileHashManager.bulkSetFromScanned(fileEntries, false);
    }
    if (recovery.loaded > 0) {
      dump(`[ScanDelta] resumed ${recovery.loaded} hashed entr${recovery.loaded === 1 ? "y" : "ies"} from an interrupted scan`);
    }
    return recovery;
  } catch {
    return { loaded: 0, progress: null };
  }
};

export const loadScanDelta = async (plugin: FastSync): Promise<number> => (await loadScanRecovery(plugin)).loaded;

export const loadScanProgress = async (plugin: FastSync): Promise<ScanProgressCheckpoint | null> => {
  try {
    const file = scanDeltaPath(plugin);
    if (!(await plugin.app.vault.adapter.exists(file))) return null;
    return parseRecovery(await plugin.app.vault.adapter.read(file)).recovery.progress;
  } catch {
    return null;
  }
};

export const validateScanProgress = (
  checkpoint: ScanProgressCheckpoint | null,
  orderedPaths: readonly (string | { path: string })[],
): number => {
  const anchor = orderedPaths[checkpoint?.processedCount ? checkpoint.processedCount - 1 : -1];
  const anchorPath = typeof anchor === "string" ? anchor : anchor?.path;
  if (!checkpoint
    || checkpoint.totalFiles !== orderedPaths.length
    || checkpoint.processedCount <= 0
    || checkpoint.processedCount > orderedPaths.length
    || anchorPath !== checkpoint.anchorPath) return 0;
  return checkpoint.processedCount;
};

export const clearScanDelta = async (plugin: FastSync): Promise<void> => {
  try {
    const file = scanDeltaPath(plugin);
    if (await plugin.app.vault.adapter.exists(file)) {
      await plugin.app.vault.adapter.remove(file);
      dump("[ScanDelta] cleared after durable commit");
    }
  } catch { /* 清理失败无害：下次成功后重试 */ }
};
