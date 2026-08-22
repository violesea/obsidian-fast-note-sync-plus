/**
 * FNS v2 设备身份（INV-6）。
 *
 * device_id 是安装时生成的 UUIDv4，持久化在 localStorage + 插件目录文件镜像
 * （deviceId.json，iOS WebView 清储兜底，与 incrementalScanState 同款模式）。
 * 与显示名解耦：显示名可随时改，device_id 永不变、永不由设备名推导。
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
}

/**
 * 读取或生成 device_id。优先级：localStorage → 文件镜像 → 生成新 UUID 并双写。
 * 返回 null 表示尚未生成（不应发生——main.ts 初始化时已调用）。
 */
export async function loadOrCreateDeviceId(plugin: FastSync): Promise<string> {
  const fromLocal = plugin.app.loadLocalStorage(DEVICE_ID_STORAGE_KEY) as string | null;
  if (fromLocal && isValidUuidV4(fromLocal)) return fromLocal;

  const mirror = new LocalStateFileMirror(plugin, DEVICE_ID_MIRROR_FILE);
  const mirrored = await mirror.read();
  if (mirrored) {
    try {
      const parsed = JSON.parse(mirrored) as Partial<DeviceIdentityMirror>;
      if (isValidUuidV4(parsed.device_id)) {
        plugin.app.saveLocalStorage(DEVICE_ID_STORAGE_KEY, parsed.device_id);
        dump(`[ChangeFeed] device_id restored from file mirror`);
        return parsed.device_id;
      }
    } catch {
      // 镜像损坏则重新生成
    }
  }

  const deviceId = generateUuidV4();
  plugin.app.saveLocalStorage(DEVICE_ID_STORAGE_KEY, deviceId);
  const payload: DeviceIdentityMirror = { schema: 1, device_id: deviceId, created_at: Date.now() };
  mirror.scheduleWrite(JSON.stringify(payload));
  await mirror.flushAsync();
  dump(`[ChangeFeed] device_id generated: ${deviceId}`);
  return deviceId;
}
