/**
 * FNS v2 变更流 · 运行层（依赖 obsidian 与插件实例）。
 *
 * 三个部件：
 * - ChangeFeedClient：sidecar HTTP 客户端（独立 base URL + X-Sidecar-Token，
 *   走 requestUrl/nativeFetch 双通道，与 HttpApiService 同款纪律但不共享其 api 基址）
 * - ChangeFeedCursorStore：游标持久化（localStorage + 文件镜像，iOS 清储兜底）
 * - runChangeFeedCatchUp：轮次追平执行器（拉变更 → 分类 → 复用 v1 接收管线应用 → 推进游标）
 *
 * 正文获取走既有 WS RePush 链路（施工方案 4.2）。实测 2026-08-22：插件 apiToken
 * 对 GET /api/note 与 GET /api/file 均 315 scope restricted，HTTP 直读不可用，
 * v2 文档 3.3 节"读路径已全部具备"的判断在插件 token 口径下不成立，已回写项目档案。
 */

import { Platform, requestUrl } from "obsidian";

import { dump, hashContent, isPathExcluded, LocalStateFileMirror } from "../utils/helpers";
import { CLIENT_TYPE } from "../utils/types";
import { receiveNoteSyncDelete } from "./operator_note";
import { receiveFileSyncDelete } from "./operator_file";
import type FastSync from "../../main";
import {
  CHANGE_FEED_MAX_PAGES,
  CHANGE_FEED_PAGE_LIMIT,
  SidecarProtocolError,
  advanceWatermark,
  computeAdoptionRev,
  parseChangesResponse,
  parseRegisterResponse,
  planChangeFeedRound,
  selectApplicableChanges,
} from "./change_feed_logic";
import type { ChangesResponse, RegisterResponse } from "./change_feed_logic";
import * as WSAction from "./websocket_action";

function platformKind(): string {
  if (Platform.isIosApp) return Platform.isTablet ? "ipados" : "ios";
  if (Platform.isAndroidApp) return "android";
  if (Platform.isDesktopApp) return "desktop";
  return "web";
}

// ---------------------------------------------------------------- 客户端 ---

export interface ChangeFeedClientOptions {
  baseUrl: string;
  token?: string;
  timeoutMs?: number;
}

export class ChangeFeedClient {
  private readonly base: string;
  private readonly token: string;
  private readonly timeoutMs: number;

  constructor(opts: ChangeFeedClientOptions) {
    this.base = opts.baseUrl.replace(/\/+$/, "");
    this.token = opts.token ?? "";
    this.timeoutMs = opts.timeoutMs ?? 15_000;
  }

  private async request(path: string, method: "GET" | "POST", body?: unknown): Promise<{ status: number; json: unknown }> {
    const headers: Record<string, string> = { "x-client": CLIENT_TYPE };
    if (this.token) headers["X-Sidecar-Token"] = this.token;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const url = `${this.base}${path}`;
    // requestUrl 在移动端绕过 CORS 限制，与主服务 HttpApiService 同款选择；
    // 15s 超时用 Promise.race 兜底（requestUrl 自身无超时参数）
    const req = requestUrl({
      url,
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      throw: false,
    }).then((r) => ({ status: r.status, json: r.json as unknown }));
    const timeout = new Promise<never>((_, reject) => {
      window.setTimeout(() => reject(new SidecarProtocolError(`sidecar request timeout: ${url}`)), this.timeoutMs);
    });
    return await Promise.race([req, timeout]);
  }

  async register(deviceId: string, vault: string, displayName: string, platform: string, pluginVersion: string): Promise<RegisterResponse> {
    const { status, json } = await this.request("/device/register", "POST", {
      device_id: deviceId,
      vault,
      display_name: displayName,
      platform,
      plugin_version: pluginVersion,
    });
    return parseRegisterResponse(json, status);
  }

  async changes(vault: string, sinceRev: number, limit: number = CHANGE_FEED_PAGE_LIMIT): Promise<ChangesResponse> {
    const { status, json } = await this.request(
      `/vault/changes?vault=${encodeURIComponent(vault)}&since_rev=${sinceRev}&limit=${limit}&collapse=1`,
      "GET",
    );
    return parseChangesResponse(json, status);
  }

  async pushCursor(deviceId: string, vault: string, rev: number): Promise<void> {
    const { status, json } = await this.request("/device/cursor", "POST", {
      device_id: deviceId,
      vault,
      rev,
    });
    parseRegisterResponse(json, status); // 复用信封校验；仅确认 ok
  }
}

// ---------------------------------------------------------------- 游标存储 ---

export interface ChangeFeedCursorState {
  schema: 1;
  deviceId: string;
  vault: string;
  rev: number;
  /** 服务端事件时间水位（ms epoch），按类型落两个，供 announce lastTime 使用 */
  noteWatermarkMs: number;
  fileWatermarkMs: number;
  updatedAt: number;
}

const CURSOR_STORAGE_KEY = "fns-changeFeedCursor";
const CURSOR_MIRROR_FILE = "changeFeedCursor.json";

export class ChangeFeedCursorStore {
  private state: ChangeFeedCursorState | null = null;
  private readonly mirror: LocalStateFileMirror;

  constructor(private readonly plugin: FastSync) {
    this.mirror = new LocalStateFileMirror(plugin, CURSOR_MIRROR_FILE);
  }

  async initialize(): Promise<void> {
    const raw = this.plugin.app.loadLocalStorage(CURSOR_STORAGE_KEY) as string | null;
    if (raw && this.load(raw)) return;
    const mirrored = await this.mirror.read();
    if (mirrored && this.load(mirrored)) {
      this.persist();
      return;
    }
    this.state = null;
  }

  private load(raw: string): boolean {
    try {
      const parsed = JSON.parse(raw) as Partial<ChangeFeedCursorState>;
      if (parsed.schema !== 1 || !parsed.deviceId || typeof parsed.rev !== "number") return false;
      this.state = {
        schema: 1,
        deviceId: parsed.deviceId,
        vault: parsed.vault ?? "",
        rev: parsed.rev,
        noteWatermarkMs: parsed.noteWatermarkMs ?? 0,
        fileWatermarkMs: parsed.fileWatermarkMs ?? 0,
        updatedAt: parsed.updatedAt ?? 0,
      };
      return true;
    } catch {
      return false;
    }
  }

  get(): ChangeFeedCursorState | null {
    return this.state;
  }

  /** 只前进（INV-2 同类语义）：新 rev 不小于当前值才接受。 */
  setRev(rev: number, watermarks?: { noteMs?: number; fileMs?: number }): void {
    if (!this.state) return;
    if (rev >= this.state.rev) this.state.rev = rev;
    if (watermarks?.noteMs && watermarks.noteMs > this.state.noteWatermarkMs) this.state.noteWatermarkMs = watermarks.noteMs;
    if (watermarks?.fileMs && watermarks.fileMs > this.state.fileWatermarkMs) this.state.fileWatermarkMs = watermarks.fileMs;
    this.state.updatedAt = Date.now();
    this.persist();
  }

  adopt(deviceId: string, vault: string, rev: number): void {
    this.state = {
      schema: 1,
      deviceId,
      vault,
      rev,
      noteWatermarkMs: 0,
      fileWatermarkMs: 0,
      updatedAt: Date.now(),
    };
    this.persist();
  }

  /** 游标失效（409 stale_cursor）后清除，下一轮在 v1 对账完成后重新采纳。 */
  invalidate(): void {
    this.state = null;
    this.plugin.app.saveLocalStorage(CURSOR_STORAGE_KEY, "");
    this.mirror.scheduleWrite("");
    this.mirror.flush();
  }

  private persist(): void {
    if (!this.state) return;
    const raw = JSON.stringify(this.state);
    this.plugin.app.saveLocalStorage(CURSOR_STORAGE_KEY, raw);
    this.mirror.scheduleWrite(raw);
    this.mirror.flush();
  }
}

// ---------------------------------------------------------------- 追平执行器 ---

export interface CatchUpResult {
  ok: boolean;
  reason?: string;
  mode?: "adopt" | "poll";
  cursorFrom?: number;
  cursorTo?: number;
  fetched?: number;
  deleted?: number;
  skipped?: number;
}

export function buildChangeFeedClient(plugin: FastSync): ChangeFeedClient | null {
  const url = plugin.settings.sidecarUrl?.trim();
  if (!url) return null;
  return new ChangeFeedClient({ baseUrl: url, token: plugin.settings.sidecarToken ?? "" });
}

/**
 * 变更流追平：在 handleSync 连接就绪后、扫描/announce 之前运行。
 * 成功 → 调用方强制走增量路径（跳过全量枚举）；失败 → 原因写日志，本轮回落 v1。
 * 永不抛出：任何异常都折叠为 {ok:false, reason}。
 */
export async function runChangeFeedCatchUp(plugin: FastSync): Promise<CatchUpResult> {
  const client = buildChangeFeedClient(plugin);
  if (!client) return { ok: false, reason: "sidecar_url_empty" };
  const deviceId = plugin.changeFeedDeviceId;
  const cursorStore = plugin.changeFeedCursor;
  if (!deviceId || !cursorStore) return { ok: false, reason: "identity_not_ready" };
  if (!cursorStore.get()) await cursorStore.initialize();

  const vault = plugin.settings.vault;
  let cursor = cursorStore.get();
  let mode: "adopt" | "poll";

  try {
    if (!cursor || cursor.deviceId !== deviceId) {
      // 采纳：注册（服务端记得游标则以服务端为准），回溯起点，本轮先吃存量
      const reg = await client.register(
        deviceId,
        vault,
        plugin.getClientName(),
        platformKind(),
        plugin.manifest.version ?? "",
      );
      const startRev = computeAdoptionRev(reg.cursor_rev, reg.safe_rev);
      cursorStore.adopt(deviceId, vault, startRev);
      cursor = cursorStore.get();
      mode = "adopt";
      dump(`[ChangeFeed] adopted cursor rev=${startRev} (server_cursor=${reg.cursor_rev}, safe_rev=${reg.safe_rev})`);
    } else {
      mode = "poll";
    }
    if (!cursor) return { ok: false, reason: "cursor_missing" };

    let sinceRev = cursor.rev;
    const cursorFrom = sinceRev;
    let fetched = 0, deleted = 0, skipped = 0;
    let noteWatermark = cursor.noteWatermarkMs;
    let fileWatermark = cursor.fileWatermarkMs;

    for (let page = 0; page < CHANGE_FEED_MAX_PAGES; page++) {
      if (plugin.syncState.syncPhase === "idle") return { ok: false, reason: "round_cancelled" };
      const resp = await client.changes(vault, sinceRev);
      if (resp.changes.length === 0 && !resp.has_more) break;

      const probe = {
        trackedHash: (path: string) => plugin.fileHashManager.getPathHash(path),
        fileExists: (path: string) => plugin.app.vault.getAbstractFileByPath(path) != null,
        hasPendingEdit: (path: string) =>
          plugin.pendingNoteModifies.has(path) || plugin.syncState.conflictedPaths.has(path),
      };
      const { actions, counts } = selectApplicableChanges(resp.changes, probe, (p) => isPathExcluded(p, plugin));
      fetched += counts.fetch;
      deleted += counts.delete;
      skipped += counts.skipped;

      for (const action of actions) {
        if (action.kind === "fetch") {
          const payload = { vault, path: action.change.path, pathHash: action.change.path_hash ?? hashContent(action.change.path) };
          // 复用 v1 接收管线：RePush 触发服务端标准推送（冲突/基线/回声抑制全套现成）
          plugin.websocket.SendMessage(
            action.type === "note" ? WSAction.NoteReceiveRePush : WSAction.FileReceiveRePush,
            payload,
          );
        } else if (action.kind === "delete") {
          const payload = {
            vault,
            path: action.change.path,
            pathHash: action.change.path_hash ?? hashContent(action.change.path),
            action: action.change.action,
            content: "",
            contentHash: action.change.content_hash ?? "",
            ctime: action.change.ctime ?? 0,
            mtime: action.change.mtime ?? 0,
            lastTime: 0,
          };
          if (action.type === "note") {
            await receiveNoteSyncDelete(payload, plugin);
          } else {
            await receiveFileSyncDelete(payload, plugin);
          }
        }
      }

      // 水位与游标逐页推进（崩溃/断连时已消费区间不重放）
      noteWatermark = advanceWatermark(noteWatermark, resp.changes.filter((c) => c.type === "note"));
      fileWatermark = advanceWatermark(fileWatermark, resp.changes.filter((c) => c.type === "file"));
      cursorStore.setRev(resp.next_rev, { noteMs: noteWatermark, fileMs: fileWatermark });
      sinceRev = resp.next_rev;

      if (!resp.has_more) break;
    }

    // 水位反哺 announce lastTime：防止基线丢失设备（lastTime=0）触发服务端全量重推
    const cur = cursorStore.get();
    if (cur) {
      const lastNote = Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime")) || 0;
      if (cur.noteWatermarkMs > lastNote) plugin.localStorageManager.setMetadata("lastNoteSyncTime", cur.noteWatermarkMs);
      const lastFile = Number(plugin.localStorageManager.getMetadata("lastFileSyncTime")) || 0;
      if (cur.fileWatermarkMs > lastFile) plugin.localStorageManager.setMetadata("lastFileSyncTime", cur.fileWatermarkMs);
    }

    // 服务端游标观察点（失败不影响本地推进）
    void client.pushCursor(deviceId, vault, sinceRev).catch((e) => {
      dump(`[ChangeFeed] pushCursor failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`);
    });

    return { ok: true, mode, cursorFrom, cursorTo: sinceRev, fetched, deleted, skipped };
  } catch (e) {
    if (e instanceof SidecarProtocolError && e.isStaleCursor) {
      // INV-5：游标超出保留期 → 清游标本轮回落 v1（repair），禁止伪装增量成功
      cursorStore.invalidate();
      dump("[ChangeFeed] stale cursor invalidated; falling back to v1 repair this round");
      return { ok: false, reason: "stale_cursor" };
    }
    if (e instanceof SidecarProtocolError && e.isUnauthorized) {
      return { ok: false, reason: "unauthorized" };
    }
    return { ok: false, reason: e instanceof Error ? e.message : String(e) };
  }
}

/** 从插件状态构造判定输入（runChangeFeedCatchUp 的纯函数侧入口）。 */
export function changeFeedDecisionInput(plugin: FastSync, syncMode: string) {
  const cursor = plugin.changeFeedCursor?.get();
  const inc = plugin.incrementalScanManager;
  return {
    enabled: plugin.settings.changeFeedEnabled === true,
    sidecarUrl: plugin.settings.sidecarUrl ?? "",
    deviceId: plugin.changeFeedDeviceId,
    cursorRev: cursor ? cursor.rev : null,
    baselinesReady: inc?.canUseIncrementalSync(plugin.localStorageManager.getMetadata("isInitSync")) === true,
    syncEnabled: plugin.settings.syncEnabled !== false,
    syncMode,
  };
}

export { planChangeFeedRound };
