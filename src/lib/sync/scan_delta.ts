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

const scanDeltaPath = (plugin: FastSync): string => normalizePath(`${getPluginDir(plugin)}/scanDelta.jsonl`);

export const appendScanDelta = async (plugin: FastSync, kind: "note" | "file", entries: Map<string, ScanDeltaEntry>): Promise<void> => {
  if (entries.size === 0) return;
  try {
    const lines: string[] = [];
    for (const [path, e] of entries) {
      lines.push(JSON.stringify({ k: kind, p: path, h: e.hash, m: e.mtime, s: e.size, c: e.ctime ?? 0 }));
    }
    await plugin.app.vault.adapter.append(scanDeltaPath(plugin), lines.join("\n") + "\n");
  } catch (e) {
    // 检查点 IO 失败绝不打断扫描——最坏情况退回旧行为（重启重扫）
    dump(`[ScanDelta] append failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`);
  }
};

export const loadScanDelta = async (plugin: FastSync): Promise<number> => {
  try {
    const file = scanDeltaPath(plugin);
    if (!(await plugin.app.vault.adapter.exists(file))) return 0;
    const raw = await plugin.app.vault.adapter.read(file);
    const noteEntries = new Map<string, ScanDeltaEntry>();
    const fileEntries = new Map<string, ScanDeltaEntry>();
    let loaded = 0;
    for (const line of raw.split("\n")) {
      const t = line.trim();
      if (!t) continue;
      try {
        const d: unknown = JSON.parse(t);
        if (typeof d !== "object" || d === null) continue;
        const rec = d as { k?: unknown; p?: unknown; h?: unknown; m?: unknown; s?: unknown; c?: unknown };
        if (typeof rec.p !== "string" || typeof rec.h !== "string") continue;
        const entry: ScanDeltaEntry = { hash: rec.h, mtime: Number(rec.m) || 0, size: Number(rec.s) || 0, ctime: Number(rec.c) || undefined };
        (rec.k === "file" ? fileEntries : noteEntries).set(rec.p, entry);
        loaded++;
      } catch { /* 跳过损坏行 */ }
    }
    // 仅作内存缓存候选：使用时仍经 fingerprint（mtime/size/ctime）校验，过期即 miss
    if (noteEntries.size) plugin.fileHashManager.bulkSetFromScanned(noteEntries, false);
    if (fileEntries.size) plugin.fileHashManager.bulkSetFromScanned(fileEntries, false);
    if (loaded > 0) dump(`[ScanDelta] resumed ${loaded} hashed entr${loaded === 1 ? "y" : "ies"} from an interrupted scan`);
    return loaded;
  } catch {
    return 0;
  }
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
