/**
 * FNS v2 变更流 · 运行层（依赖 obsidian 与插件实例）。
 *
 * 三个部件：
 * - ChangeFeedClient：sidecar HTTP 客户端（独立 base URL + X-Sidecar-Token，
 *   走 requestUrl/nativeFetch 双通道，与 HttpApiService 同款纪律但不共享其 api 基址）
 * - ChangeFeedCursorStore：游标持久化（localStorage + 文件镜像，iOS 清储兜底）
 * - runChangeFeedCatchUp：轮次追平执行器（拉变更 → 分类 → 复用 v1 接收管线应用 → 推进游标）
 *
 * 正文获取走插件 REST 直读（2.5.1 起）：笔记与附件分别通过既有 API 读取，
 * 避免依赖 WS RePush 的时序；请求头必须使用 ObsidianPlugin 客户端类型。
 */

import { Platform, TFile, TFolder, normalizePath, requestUrl } from "obsidian";

import {
  dump,
  hashArrayBuffer,
  hashContent,
  hashContentAsync,
  hashFileAsync,
  isPathExcluded,
  LocalStateFileMirror,
} from "../utils/helpers";
import { CLIENT_TYPE } from "../utils/types";
import { receiveNoteSyncDelete } from "./operator_note";
import { receiveFileSyncDelete } from "./operator_file";
import type FastSync from "../../main";
import {
  CHANGE_FEED_MAX_PAGES,
  CHANGE_FEED_PAGE_LIMIT,
  CHANGE_FEED_CURSOR_SCHEMA,
  SidecarProtocolError,
  advanceWatermark,
  computeAdoptionRev,
  migrateChangeFeedCursorState,
  parseChangesResponse,
  parseRegisterResponse,
  planChangeFeedRound,
  shouldRestartFreshRoundOnResume,
  selectApplicableChanges,
} from "./change_feed_logic";
import type { ChangesResponse, RegisterResponse, SidecarChange } from "./change_feed_logic";
import { requireForeground } from "./background_activity_gate";
import { isChangeFeedRuntimeEnabled, isCloudPreviewRuntimeEnabled } from "./sync_feature_policy";

function platformKind(): string {
  if (Platform.isIosApp) return Platform.isTablet ? "ipados" : "ios";
  if (Platform.isAndroidApp) return "android";
  if (Platform.isDesktopApp) return "desktop";
  return "web";
}

/** 附件是否属于本设备的同步面（与 operator activeTypes 同口径：云端预览全开时不落地附件） */
function filesInSyncScope(plugin: FastSync): boolean {
  return !isCloudPreviewRuntimeEnabled(plugin.settings) || plugin.settings.cloudPreviewTypeRestricted;
}

/**
 * Return the parent folders that did not exist before this materialization.
 * The list is used only for failure cleanup; pre-existing folders are never
 * eligible for deletion.
 */
function collectMissingParentFolders(plugin: FastSync, filePath: string): string[] {
  const folder = normalizePath(filePath).split("/").slice(0, -1).join("/");
  if (!folder) return [];

  const parts = folder.split("/");
  const missing: string[] = [];
  for (let index = 0; index < parts.length; index++) {
    const candidate = parts.slice(0, index + 1).join("/");
    if (plugin.app.vault.getFolderByPath(candidate) == null) missing.push(candidate);
  }
  return missing;
}

/** Create a parent folder and remember ownership for failure cleanup. */
async function ensureParentFolder(
  plugin: FastSync,
  filePath: string,
  createdFolders: string[],
): Promise<void> {
  const folder = normalizePath(filePath).split("/").slice(0, -1).join("/");
  const missing = collectMissingParentFolders(plugin, filePath);
  if (!folder || missing.length === 0) return;

  try {
    await requireForeground(plugin);
    await plugin.app.vault.createFolder(folder);
    // Obsidian creates the missing parent chain for createFolder(). Record
    // only paths that are now real, so cleanup never targets a guessed path.
    for (const candidate of missing) {
      if (plugin.app.vault.getFolderByPath(candidate) != null) createdFolders.push(candidate);
    }
  } catch (error) {
    // Concurrent materializers may win the same createFolder race. In that
    // case this invocation owns nothing and must not delete the winner's
    // empty folder if its later file write fails.
    await requireForeground(plugin);
    if (plugin.app.vault.getFolderByPath(folder) == null) throw error;
  }
}

/**
 * Remove only folders created by this materialization call, and only while
 * they are still empty. This prevents a failed note/file write from leaving
 * an apparently synced directory while protecting user-created directories.
 */
async function cleanupCreatedEmptyFolders(plugin: FastSync, createdFolders: string[]): Promise<void> {
  const ordered = [...new Set(createdFolders)].sort(
    (left, right) => right.split("/").length - left.split("/").length,
  );
  for (const folderPath of ordered) {
    try {
      await requireForeground(plugin);
      const folder = plugin.app.vault.getFolderByPath(folderPath);
      if (!(folder instanceof TFolder) || folder.children.length !== 0) continue;
      await plugin.app.fileManager.trashFile(folder);
      dump(`[ChangeFeed] removed empty folder after failed materialization: ${folderPath}`);
    } catch (error) {
      // Cleanup is best-effort. Preserve the original materialization error so
      // the cursor remains unacknowledged and the item is retried.
      dump(`[ChangeFeed] failed to remove empty folder ${folderPath}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

/**
 * 物化一篇服务端笔记到本地（HTTP 直取路径，2.5.1 起替代 RePush 通道）。
 * 写入模式复刻 receiveNoteSyncModify：ignored-file 回声抑制、服务端 mtime/ctime、
 * setFileHash 记同步基线、lastSyncMtime 拦截。失败抛出，由调用方计数。
 */
export async function applyRemoteNote(
  plugin: FastSync,
  path: string,
  note: { content: string; contentHash: string; mtime: number; ctime: number },
): Promise<void> {
  const normalized = normalizePath(path);
  const fetchedHash = await hashContentAsync(note.content, plugin);
  if (!note.contentHash || fetchedHash !== String(note.contentHash)) {
    throw new Error(`note content hash mismatch: ${path}`);
  }
  await requireForeground(plugin);
  plugin.addIgnoredFile(normalized);
  const createdFolders: string[] = [];
  try {
    const options = {
      ...((note.ctime ?? 0) > 0 ? { ctime: note.ctime } : {}),
      ...((note.mtime ?? 0) > 0 ? { mtime: note.mtime } : {}),
    };
    const existing = plugin.app.vault.getFileByPath(normalized);
    if (existing instanceof TFile) {
      await requireForeground(plugin);
      await plugin.app.vault.modify(existing, note.content, options);
    } else {
      await ensureParentFolder(plugin, normalized, createdFolders);
      await requireForeground(plugin);
      await plugin.app.vault.create(normalized, note.content, options);
    }
    await requireForeground(plugin);
    const written = plugin.app.vault.getFileByPath(normalized);
    if (!(written instanceof TFile)) {
      throw new Error(`materialized note is missing after write: ${normalized}`);
    }
    const writtenContent = await plugin.app.vault.read(written);
    const writtenHash = await hashContentAsync(writtenContent, plugin);
    if (writtenHash !== String(note.contentHash)) {
      throw new Error(`materialized note hash mismatch after write: ${normalized}`);
    }
    plugin.fileHashManager.setFileHash(normalized, note.contentHash, note.mtime, written instanceof TFile ? written.stat.size : 0);
    plugin.lastSyncMtime.set(normalized, note.mtime);
  } catch (error) {
    await cleanupCreatedEmptyFolders(plugin, createdFolders);
    throw error;
  } finally {
    window.setTimeout(() => plugin.removeIgnoredFile(normalized), 500);
  }
}

/**
 * 物化一个服务端附件到本地（HTTP 直取）。
 * 基线哈希用变更流下发的 content_hash；取回字节后必须完成尺寸（若有）与内容哈希校验，
 * 否则不得写盘或推进游标。服务端可能在我们取字节前又写了一版，失败后由下一轮变更流重试。
 */
export async function applyRemoteFile(
  plugin: FastSync,
  change: SidecarChange,
): Promise<void> {
  const normalized = normalizePath(change.path);
  const expectedHash = String(change.content_hash ?? "");
  if (!expectedHash) throw new Error(`binary change has no content hash: ${change.path}`);
  await requireForeground(plugin);
  const buf = await plugin.api.getFileBinary(change.path, change.path_hash);
  if (!buf) throw new Error(`binary fetch failed: ${change.path}`);
  if ((change.size ?? 0) > 0 && buf.byteLength !== change.size) {
    throw new Error(`binary size mismatch (server=${change.size}, got=${buf.byteLength}): ${change.path}`);
  }
  const fetchedHash = await hashArrayBuffer(buf);
  if (fetchedHash !== expectedHash) {
    throw new Error(`binary content hash mismatch: ${change.path}`);
  }
  plugin.addIgnoredFile(normalized);
  const createdFolders: string[] = [];
  try {
    const options = {
      ...((change.ctime ?? 0) > 0 ? { ctime: change.ctime } : {}),
      ...((change.mtime ?? 0) > 0 ? { mtime: change.mtime } : {}),
    };
    const existing = plugin.app.vault.getFileByPath(normalized);
    if (existing instanceof TFile) {
      await requireForeground(plugin);
      await plugin.app.vault.modifyBinary(existing, buf, options);
    } else {
      await ensureParentFolder(plugin, normalized, createdFolders);
      await requireForeground(plugin);
      await plugin.app.vault.createBinary(normalized, buf, options);
    }
    await requireForeground(plugin);
    plugin.fileHashManager.setFileHash(normalized, expectedHash, change.mtime ?? 0, buf.byteLength);
    plugin.lastSyncMtime.set(normalized, change.mtime ?? 0);
  } catch (error) {
    await cleanupCreatedEmptyFolders(plugin, createdFolders);
    throw error;
  } finally {
    window.setTimeout(() => plugin.removeIgnoredFile(normalized), 500);
  }
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
  schema: 2;
  deviceId: string;
  vault: string;
  rev: number;
  /** 服务端事件时间水位（ms epoch），按类型落两个，供 announce lastTime 使用 */
  noteWatermarkMs: number;
  fileWatermarkMs: number;
  updatedAt: number;
  /** 旧游标有一次有界回放待完成，期间每页成功仍可推进游标。 */
  repairPending: boolean;
}

const CURSOR_STORAGE_KEY = "fns-changeFeedCursor";
const CURSOR_MIRROR_FILE = "changeFeedCursor.json";

export class ChangeFeedCursorStore {
  private state: ChangeFeedCursorState | null = null;
  private initializePromise: Promise<void> | null = null;
  private readonly mirror: LocalStateFileMirror;

  constructor(private readonly plugin: FastSync) {
    this.mirror = new LocalStateFileMirror(plugin, CURSOR_MIRROR_FILE);
  }

  initialize(): Promise<void> {
    if (this.initializePromise) return this.initializePromise;
    const pending = this.initializeOnce();
    const guarded = pending.finally(() => {
      if (this.initializePromise === guarded) this.initializePromise = null;
    });
    this.initializePromise = guarded;
    return guarded;
  }

  private async initializeOnce(): Promise<void> {
    const candidates: { state: ChangeFeedCursorState; migrated: boolean }[] = [];
    const local = this.plugin.app.loadLocalStorage(CURSOR_STORAGE_KEY) as string | null;
    const localCandidate = local ? this.parse(local) : null;
    if (localCandidate) candidates.push(localCandidate);

    const mirrored = await this.mirror.read();
    const mirrorCandidate = mirrored ? this.parse(mirrored) : null;
    if (mirrorCandidate) candidates.push(mirrorCandidate);

    if (candidates.length === 0) {
      this.state = null;
      return;
    }

    // localStorage and the file mirror are crash-recovery replicas, not an
    // ordered fallback chain. Choose the newest compatible replica and merge
    // monotonic fields so an older replica can never roll the cursor back.
    const expectedDeviceId = this.plugin.changeFeedDeviceId;
    const expectedVault = this.plugin.settings.vault;
    const compatible = candidates.filter(({ state }) =>
      (!expectedDeviceId || state.deviceId === expectedDeviceId)
      && (!expectedVault || !state.vault || state.vault === expectedVault),
    );
    const pool = compatible.length > 0 ? compatible : candidates;
    const newest = pool.reduce((best, candidate) => {
      if (candidate.state.rev > best.state.rev) return candidate;
      if (candidate.state.rev === best.state.rev && candidate.state.updatedAt > best.state.updatedAt) return candidate;
      return best;
    });
    this.state = {
      ...newest.state,
      rev: Math.max(...pool.map(({ state }) => state.rev)),
      noteWatermarkMs: Math.max(...pool.map(({ state }) => state.noteWatermarkMs)),
      fileWatermarkMs: Math.max(...pool.map(({ state }) => state.fileWatermarkMs)),
      updatedAt: Math.max(...pool.map(({ state }) => state.updatedAt)),
      repairPending: pool.some(({ state }) => state.repairPending),
    };
    // Always rehydrate both replicas. This also persists schema-1 migration and
    // repairs a stale localStorage copy after iOS WebView eviction.
    this.persist();
  }

  private parse(raw: string): { state: ChangeFeedCursorState; migrated: boolean } | null {
    try {
      return migrateChangeFeedCursorState(JSON.parse(raw));
    } catch {
      return null;
    }
  }

  get(): ChangeFeedCursorState | null {
    return this.state;
  }

  /** 只前进（INV-2 同类语义）：新 rev 不小于当前值才接受。 */
  setRev(rev: number, watermarks?: { noteMs?: number; fileMs?: number }): void {
    if (!this.state) return;
    if (!Number.isFinite(rev) || rev < this.state.rev) return;
    this.state.rev = rev;
    if (watermarks?.noteMs && watermarks.noteMs > this.state.noteWatermarkMs) this.state.noteWatermarkMs = watermarks.noteMs;
    if (watermarks?.fileMs && watermarks.fileMs > this.state.fileWatermarkMs) this.state.fileWatermarkMs = watermarks.fileMs;
    this.state.updatedAt = Date.now();
    this.persist();
  }

  adopt(deviceId: string, vault: string, rev: number): void {
    const current = this.state;
    if (current && current.deviceId === deviceId && current.vault === vault && rev <= current.rev) {
      // A late register response is only an observation. It must not replace a
      // cursor that a newer catch-up page has already acknowledged.
      return;
    }
    this.state = {
      schema: CHANGE_FEED_CURSOR_SCHEMA,
      deviceId,
      vault,
      rev: Math.max(0, rev, current?.deviceId === deviceId && current.vault === vault ? current.rev : 0),
      noteWatermarkMs: current?.deviceId === deviceId && current.vault === vault ? current.noteWatermarkMs : 0,
      fileWatermarkMs: current?.deviceId === deviceId && current.vault === vault ? current.fileWatermarkMs : 0,
      updatedAt: Date.now(),
      repairPending: current?.deviceId === deviceId && current.vault === vault ? current.repairPending : false,
    };
    this.persist();
  }

  /** Mark the one-time bounded replay complete after a successful catch-up. */
  completeRepair(): void {
    if (!this.state?.repairPending) return;
    this.state.repairPending = false;
    this.state.updatedAt = Date.now();
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

type ChangeFeedInFlight = {
  context?: string;
  promise: Promise<CatchUpResult>;
};

const changeFeedInFlight = new WeakMap<object, ChangeFeedInFlight>();

export interface CatchUpResult {
  ok: boolean;
  reason?: string;
  /** Stable failure code for policy decisions; detail is diagnostic only. */
  detail?: string;
  mode?: "adopt" | "poll";
  cursorFrom?: number;
  cursorTo?: number;
  /** 变更流判定为需拉取的条数 */
  fetched?: number;
  /** HTTP 直取成功物化的条数（2.5.1 起） */
  applied?: number;
  /** 物化失败条数；失败页不确认，下一次有界重试从同一游标重放。 */
  failures?: number;
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
 * 成功 → 调用方强制走增量路径（跳过全量枚举）；失败 → 原因写日志，由调用方按策略重试、repair 或停在未完成态。
 * 永不抛出：任何异常都折叠为 {ok:false, reason}。
 * context 为本轮 sync context：追平中途若被 transport 重启或新轮次顶替则让位退出。
 */
export async function runChangeFeedCatchUp(plugin: FastSync, context?: string): Promise<CatchUpResult> {
  const existing = changeFeedInFlight.get(plugin);
  if (existing) {
    // Calls for the same logical round share one HTTP/register sequence. If a
    // new context supersedes the old one, wait for the old operation to yield;
    // the new owner then gets a chance to start from the durable cursor.
    if (!context || !existing.context || existing.context === context) return existing.promise;
    await existing.promise;
    if (plugin.syncState.activeSyncContext !== context) return { ok: false, reason: "round_superseded" };
    const replacement = changeFeedInFlight.get(plugin);
    if (replacement) return replacement.promise;
  }

  const operation = runChangeFeedCatchUpOnce(plugin, context);
  const promise = operation.finally(() => {
    const current = changeFeedInFlight.get(plugin);
    if (current?.promise === promise) changeFeedInFlight.delete(plugin);
  });
  changeFeedInFlight.set(plugin, { context, promise });
  return promise;
}

async function runChangeFeedCatchUpOnce(plugin: FastSync, context?: string): Promise<CatchUpResult> {
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
    if (!cursor || cursor.deviceId !== deviceId || cursor.vault !== vault) {
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
    let fetched = 0, deleted = 0, skipped = 0, applied = 0, failures = 0;
    let noteWatermark = cursor.noteWatermarkMs;
    let fileWatermark = cursor.fileWatermarkMs;
    let lastResponseHasMore = false;

    for (let page = 0; page < CHANGE_FEED_MAX_PAGES; page++) {
      // 让位判定：transport 重启或本轮 context 被新轮次顶替。（syncPhase 回到 idle
      // 不代表取消——上一轮正常收尾也会置 idle，不能作为取消信号）
      if (plugin.syncState.transportResetPending) return { ok: false, reason: "transport_reset" };
      if (context && plugin.syncState.activeSyncContext && plugin.syncState.activeSyncContext !== context) {
        return { ok: false, reason: "round_superseded" };
      }
      const resp = await client.changes(vault, sinceRev);
      lastResponseHasMore = resp.has_more;
      if (resp.changes.length === 0 && !resp.has_more) {
        // An empty response still carries the server's next watermark. This
        // matters after schema migration: do not leave the device at the
        // replay start when there is nothing left to materialize.
        if (resp.next_rev > sinceRev) {
          cursorStore.setRev(resp.next_rev, { noteMs: noteWatermark, fileMs: fileWatermark });
          sinceRev = resp.next_rev;
        }
        break;
      }

      if (resp.next_rev <= sinceRev) {
        // A page that does not advance next_rev would be replayed forever.
        // Leave the cursor untouched and let the bounded retry policy surface
        // the protocol stall instead of scanning the whole vault.
        return {
          ok: false,
          reason: "cursor_stalled",
          mode,
          cursorFrom,
          cursorTo: sinceRev,
          fetched,
          deleted,
          skipped,
          applied,
          failures,
        };
      }

      await requireForeground(plugin);

      const probe = {
        trackedHash: (path: string) => plugin.fileHashManager.getPathHash(path),
        fileExists: (path: string) => plugin.app.vault.getAbstractFileByPath(path) != null,
        hasPendingEdit: (path: string) =>
          plugin.pendingNoteModifies.has(path) || plugin.syncState.conflictedPaths.has(path),
        verifyLocalHash: async (change: SidecarChange): Promise<string | null> => {
          const file = plugin.app.vault.getFileByPath(change.path);
          if (!(file instanceof TFile)) return null;
          await requireForeground(plugin);
          if (change.type === "note") {
            const content = await plugin.app.vault.read(file);
            await requireForeground(plugin);
            return await hashContentAsync(content, plugin);
          }
          return await hashFileAsync(plugin.app, change.path, plugin);
        },
      };
      const { actions, counts } = await selectApplicableChanges(resp.changes, probe, (p) => isPathExcluded(p, plugin));
      fetched += counts.fetch;
      deleted += counts.delete;
      skipped += counts.skipped;

      let pageFailures = 0;
      for (const action of actions) {
        await requireForeground(plugin);
        if (action.kind === "fetch") {
          // HTTP 直取物化（2.5.1）：同步、可验证、可复现，绕开 RePush 通道（ISSUE-023 疑云路径）。
          // 单条失败不影响其它条目，但本页不确认，失败项必须在下一轮重试。
          try {
            if (action.type === "note") {
              const note = await plugin.api.getNoteContent(action.change.path);
              if (!note) throw new Error(`note fetch returned null: ${action.change.path}`);
              await applyRemoteNote(plugin, action.change.path, note);
              applied++;
            } else if (filesInSyncScope(plugin)) {
              await applyRemoteFile(plugin, action.change);
              applied++;
            } else {
              // 云端预览全开设备不落地附件（与 operator activeTypes 同口径）
              skipped++;
            }
          } catch (itemErr) {
            failures++;
            pageFailures++;
            dump(`[ChangeFeed] http-fetch apply failed (${failures}): ${action.change.path} — ${itemErr instanceof Error ? itemErr.message : String(itemErr)}`);
          }
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

      // Commit the page cursor only after every materialization in the page
      // succeeded. Failed items must remain replayable after iOS termination
      // or a transient API failure; the old code acknowledged the whole page
      // and permanently skipped those items.
      if (pageFailures > 0) {
        dump(`[ChangeFeed] page not acknowledged: failures=${pageFailures} since_rev=${sinceRev}`);
        return { ok: false, reason: "materialization_failed", mode, cursorFrom, cursorTo: sinceRev, fetched, deleted, skipped, applied, failures };
      }

      // 水位与游标逐页推进（崩溃/断连时已消费区间不重放）
      noteWatermark = advanceWatermark(noteWatermark, resp.changes.filter((c) => c.type === "note"));
      fileWatermark = advanceWatermark(fileWatermark, resp.changes.filter((c) => c.type === "file"));
      cursorStore.setRev(resp.next_rev, { noteMs: noteWatermark, fileMs: fileWatermark });
      sinceRev = resp.next_rev;

      if (!resp.has_more) break;
    }

    if (lastResponseHasMore) {
      // Reaching the page budget is an incomplete catch-up, never success.
      // The cursor already contains the successfully materialized prefix; the
      // next bounded retry resumes from that prefix without a vault scan.
      return {
        ok: false,
        reason: "page_limit",
        mode,
        cursorFrom,
        cursorTo: sinceRev,
        fetched,
        deleted,
        skipped,
        applied,
        failures,
      };
    }

    // 水位反哺 announce lastTime：防止基线丢失设备（lastTime=0）触发服务端全量重推
    const cur = cursorStore.get();
    if (cur) {
      const lastNote = Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime")) || 0;
      if (cur.noteWatermarkMs > lastNote) plugin.localStorageManager.setMetadata("lastNoteSyncTime", cur.noteWatermarkMs);
      const lastFile = Number(plugin.localStorageManager.getMetadata("lastFileSyncTime")) || 0;
      if (cur.fileWatermarkMs > lastFile) plugin.localStorageManager.setMetadata("lastFileSyncTime", cur.fileWatermarkMs);
    }

    cursorStore.completeRepair();

    // 服务端游标观察点（失败不影响本地推进）
    void client.pushCursor(deviceId, vault, sinceRev).catch((e) => {
      dump(`[ChangeFeed] pushCursor failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`);
    });

    return { ok: true, mode, cursorFrom, cursorTo: sinceRev, fetched, deleted, skipped, applied, failures };
  } catch (e) {
    if (e instanceof SidecarProtocolError && e.isStaleCursor) {
      // INV-5：游标超出保留期 → 清游标本轮回落 v1（repair），禁止伪装增量成功
      cursorStore.invalidate();
      dump("[ChangeFeed] stale cursor invalidated; falling back to v1 repair this round");
      return { ok: false, reason: "stale_cursor" };
    }
    if (e instanceof SidecarProtocolError && e.isUnauthorized) {
      return { ok: false, reason: "unauthorized", detail: e.message };
    }
    if (e instanceof SidecarProtocolError) {
      return {
        ok: false,
        reason: e.code && e.code !== "unknown" ? e.code : "sidecar_error",
        detail: e.message,
      };
    }
    return {
      ok: false,
      reason: "network_error",
      detail: e instanceof Error ? e.message : String(e),
    };
  }
}

/** 从插件状态构造判定输入（runChangeFeedCatchUp 的纯函数侧入口）。 */
export function changeFeedDecisionInput(plugin: FastSync, syncMode: string) {
  const cursor = plugin.changeFeedCursor?.get();
  const inc = plugin.incrementalScanManager;
  return {
    enabled: isChangeFeedRuntimeEnabled(plugin.settings),
    sidecarUrl: plugin.settings.sidecarUrl ?? "",
    deviceId: plugin.changeFeedDeviceId,
    cursorRev: cursor ? cursor.rev : null,
    baselinesReady: inc?.canUseIncrementalSync(plugin.localStorageManager.getMetadata("isInitSync")) === true,
    syncEnabled: plugin.settings.syncEnabled !== false,
    syncMode,
  };
}

export { planChangeFeedRound, shouldRestartFreshRoundOnResume };
