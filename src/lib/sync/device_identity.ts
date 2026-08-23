/**
 * FNS v2 设备身份（INV-6）。
 *
 * device_id 是安装时生成的 UUIDv4，持久化在 localStorage + 插件目录文件镜像
 * （deviceId.json，iOS WebView 清储兜底，与 incrementalScanState 同款模式）。
 * 与显示名解耦：显示名可随时改，device_id 永不变、永不由设备名推导。
 *
 * 2026-08-23（2.5.2）：display_name 一并落 deviceId.json。它被 LocalStateFileMirror
 * 的构造器自动排除出配置同步——修复 clientName 经 syncMetadata.json 被舰队级拉平的
 * 归因污染（实测 2026-08-23：本机改名"MacBook"后 iPad 同步跟着变成 "MacBook iPad"）。
 */

import { dump } from "../utils/helpers";
import { LocalStateFileMirror } from "../utils/helpers";
import { generateUuidV4, isValidUuidV4 } from "./change_feed_logic";
import type FastSync from "../../main";

const DEVICE_ID_STORAGE_KEY = "fns-deviceId";
const DEVICE_ID_MIRROR_FILE = "deviceId.json";

export interface DeviceIdentityMirror {
  schema: 1;
  device_id: string;
  created_at: number;
  /** 本机显示名（不参与配置同步；空 = 沿用 metadata 的 clientName） */
  display_name?: string;
}

interface LoadedIdentity {
  deviceId: string;
  displayName: string | null;
}

function parseMirror(raw: string): LoadedIdentity | null {
  try {
    const parsed = JSON.parse(raw) as Partial<DeviceIdentityMirror>;
    if (!isValidUuidV4(parsed.device_id)) return null;
    return {
      deviceId: parsed.device_id,
      displayName: typeof parsed.display_name === "string" && parsed.display_name.trim() !== ""
        ? parsed.display_name.trim()
        : null,
    };
  } catch {
    return null;
  }
}

/**
 * 读取或生成设备身份。优先级：localStorage → 文件镜像 → 生成新 UUID 并双写。
 * displayName 同时从镜像带回（getClientName 用；空则调用方回落 metadata）。
 */
export async function loadOrCreateDeviceId(plugin: FastSync): Promise<LoadedIdentity> {
  const mirror = new LocalStateFileMirror(plugin, DEVICE_ID_MIRROR_FILE);

  const fromLocal = plugin.app.loadLocalStorage(DEVICE_ID_STORAGE_KEY) as string | null;
  if (fromLocal && isValidUuidV4(fromLocal)) {
    const mirrored = await mirror.read();
    const parsed = mirrored ? parseMirror(mirrored) : null;
    // 本地 id 有效但镜像丢失（iOS 清储后 localStorage 侥幸存活等）：补写镜像
    if (!parsed || parsed.deviceId !== fromLocal) {
      mirror.scheduleWrite(JSON.stringify({
        schema: 1, device_id: fromLocal, created_at: Date.now(),
        ...(parsed?.displayName ? { display_name: parsed.displayName } : {}),
      } satisfies DeviceIdentityMirror));
      await mirror.flushAsync();
      return { deviceId: fromLocal, displayName: parsed?.displayName ?? null };
    }
    return parsed;
  }

  const mirrored = await mirror.read();
  if (mirrored) {
    const parsed = parseMirror(mirrored);
    if (parsed) {
      plugin.app.saveLocalStorage(DEVICE_ID_STORAGE_KEY, parsed.deviceId);
      dump(`[ChangeFeed] device_id restored from file mirror`);
      return parsed;
    }
  }

  const deviceId = generateUuidV4();
  plugin.app.saveLocalStorage(DEVICE_ID_STORAGE_KEY, deviceId);
  const payload: DeviceIdentityMirror = { schema: 1, device_id: deviceId, created_at: Date.now() };
  mirror.scheduleWrite(JSON.stringify(payload));
  await mirror.flushAsync();
  dump(`[ChangeFeed] device_id generated: ${deviceId}`);
  return { deviceId, displayName: null };
}

/** 更新本机显示名（写 deviceId.json；调用方负责刷新 plugin.deviceDisplayName）。 */
export async function saveDeviceDisplayName(plugin: FastSync, name: string): Promise<void> {
  const mirror = new LocalStateFileMirror(plugin, DEVICE_ID_MIRROR_FILE);
  const raw = await mirror.read();
  const parsed = raw ? parseMirror(raw) : null;
  const deviceId = parsed?.deviceId ?? plugin.changeFeedDeviceId;
  if (!deviceId) return;
  const trimmed = name.trim();
  const payload: DeviceIdentityMirror = {
    schema: 1,
    device_id: deviceId,
    created_at: Date.now(),
    ...(trimmed !== "" ? { display_name: trimmed } : {}),
  };
  mirror.scheduleWrite(JSON.stringify(payload));
  await mirror.flushAsync();
}
