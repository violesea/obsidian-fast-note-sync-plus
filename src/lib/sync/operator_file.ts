import { TFile, TAbstractFile, normalizePath, Platform } from "obsidian";

import { ReceiveFileSyncUpdateMessage, FileUploadMessage, FileSyncChunkDownloadMessage, FileDownloadSession, ReceiveMtimeMessage, ReceivePathMessage, SyncEndData } from "../utils/types";
import { hashContent, hashArrayBuffer, getPluginDir, dump, dumpError, sleep, isPathExcluded, getSafeCtime, isLargeBinarySyncRisk, describeBinarySyncLimit, showSyncNotice, checkAndNotifyCaseConflict, logMemorySnapshot, hashFileAsync, vaultDelete } from "../utils/helpers";
import { FileCloudPreview } from "../storage/file_cloud_preview";
import { SyncLogManager } from "./sync_log_manager";
import { HttpApiService } from "../api/http_api_service";
import type FastSync from "../../main";
import { waitForForeground } from "./background_activity_gate";
import {
  advanceCloudPreviewCheckState,
  completeCloudPreviewCheckState,
  parseCloudPreviewCheckState,
  serializeCloudPreviewCheckState,
} from "./cloud_preview_reconciliation";
import { captureStableSnapshot, stableCaptureCoordinator } from "./stable_capture";
import { isCloudPreviewRuntimeEnabled } from "./sync_feature_policy";

const waitForFileActivity = async (plugin: FastSync): Promise<boolean> => waitForForeground(plugin);

const readStableStat = async (plugin: FastSync, path: string) => {
  const stat = await plugin.app.vault.adapter.stat(path);
  if (!stat) return null;
  return { size: stat.size, mtime: stat.mtime, ctime: stat.ctime };
};

const stableCaptureKey = (plugin: FastSync, path: string): string => (
  `${plugin.settings.vault}:${path}`
);


// 下载内存缓冲控制 (20MB 阈值防止 OOM)
let currentDownloadBufferBytes = 0
const MAX_DOWNLOAD_BUFFER_BYTES = 20 * 1024 * 1024

// 上传中的文件追踪，用于删除时取消上传
// Active uploads are also tagged with a transport generation. A reconnect must
// invalidate every task that was admitted by the old socket, including tasks
// released from ConcurrencyLimiter.clear().
type ActiveUpload = {
  cancelled: boolean;
  queueGeneration: number;
  slotAcquired: boolean;
  slotReleased: boolean;
  taskRecorded: boolean;
  sessionId: string;
  descriptor: UploadCheckDescriptor;
  message: FileUploadMessage;
  pageIndex?: number;
  chunkSize: number;
  retryCount: number;
  retryTimer: number | null;
  retrying: boolean;
  retryResponseReceived: boolean;
  runActive: boolean;
  chunksSent: boolean;
  actualTotalChunks: number;
  totalCountSettled: boolean;
}

type UploadCheckDescriptor = {
  vault: string;
  path: string;
  pathHash: string;
  contentHash: string;
  size: number;
  ctime: number;
  mtime: number;
};

const activeUploadsMap = new Map<string, ActiveUpload>()
let uploadQueueGeneration = 0

const CLOUD_PREVIEW_CHECK_STATE = "cloudPreviewAttachmentCheckState" as const;
let cloudPreviewCheckPromise: Promise<void> | null = null;

// 全局中止信号，用于插件卸载时
export let isPluginUnloading = false;

// Session identity is mapped to the exact upload state, not just a path. This
// prevents a late 463 for an old session from cancelling a replacement upload
// for the same path.
const sessionIdToUploadMap = new Map<string, ActiveUpload>()
const retryingUploadsMap = new Map<string, ActiveUpload>()
const MAX_UPLOAD_SESSION_RETRIES = 3;
const UPLOAD_SESSION_RETRY_BASE_DELAY_MS = 500;
const UPLOAD_SESSION_RESPONSE_TIMEOUT_MS = 5000;

const uploadPathKey = (path: string): string => normalizePath(path);

const uploadMessageForState = (state: ActiveUpload): FileUploadMessage => ({
  ...state.message,
  path: state.descriptor.path,
  pathHash: state.descriptor.pathHash,
  ctime: state.descriptor.ctime,
  mtime: state.descriptor.mtime,
  sessionId: state.sessionId,
  chunkSize: state.chunkSize,
  pageIndex: state.pageIndex,
});

const clearUploadRetryTimer = (state: ActiveUpload): void => {
  if (state.retryTimer !== null) {
    window.clearTimeout(state.retryTimer);
    state.retryTimer = null;
  }
};

const removeUploadStateIfCurrent = (path: string, state: ActiveUpload): void => {
  const key = uploadPathKey(path);
  if (activeUploadsMap.get(key) === state) activeUploadsMap.delete(key);
  if (retryingUploadsMap.get(key) === state) retryingUploadsMap.delete(key);
  if (sessionIdToUploadMap.get(state.sessionId) === state) {
    sessionIdToUploadMap.delete(state.sessionId);
  }
};

/** Number of upload tasks that can still perform work on the current queue. */
export const getActiveUploadCount = (): number => {
  let count = 0;
  for (const state of activeUploadsMap.values()) {
    // `cancelled` also marks the old transport session after a 463. The
    // logical upload remains active in retryingUploadsMap until its replacement
    // is ACKed or the retry budget is exhausted, so only taskRecorded is a
    // terminal signal here.
    if (!state.taskRecorded) count++;
  }
  return count;
};

const releaseUploadSlot = (plugin: FastSync, path: string, state: ActiveUpload): void => {
  if (state.slotReleased || !state.slotAcquired) return;
  state.slotReleased = true;
  state.slotAcquired = false;
  plugin.concurrencyLimiter.releaseSlot(path);
};

const recordUploadTask = (plugin: FastSync, data: FileUploadMessage, state: ActiveUpload, failed = false): void => {
  if (state.taskRecorded) return;
  state.taskRecorded = true;
  if (failed) plugin.fileSyncTasks.failed++;
  plugin.recordSyncCompleted('file', data.pageIndex);
};

const settleUploadTotal = (plugin: FastSync, state: ActiveUpload): void => {
  if (state.totalCountSettled) return;
  state.totalCountSettled = true;
  plugin.totalChunksToUpload = Math.max(0, plugin.totalChunksToUpload - state.actualTotalChunks);
};

const clearUploadCheckpoint = (plugin: FastSync, state: ActiveUpload): void => {
  const vaultName = plugin.app.vault.getName();
  try {
    plugin.app.saveLocalStorage(`fns-${vaultName}-uploadSession-${state.descriptor.pathHash}`, null);
  } catch {
    // Ignore storage cleanup errors; the next retry validates the session ID.
  }
};

const clearPendingUploadHashIfCurrent = (plugin: FastSync, state: ActiveUpload): void => {
  const path = state.descriptor.path;
  const pendingHash = plugin.pendingUploadHashes.get(path);
  if (pendingHash === undefined || state.descriptor.contentHash === "" || pendingHash === state.descriptor.contentHash) {
    plugin.pendingUploadHashes.delete(path);
    plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes);
  }
};

const removeUploadTaskState = (plugin: FastSync, state: ActiveUpload, settleTotal = false): void => {
  const path = state.descriptor.path;
  state.cancelled = true;
  state.retrying = false;
  state.retryResponseReceived = false;
  clearUploadRetryTimer(state);
  removeUploadStateIfCurrent(path, state);
  plugin.syncState.pendingFileUploadAcks.delete(uploadPathKey(path));
  plugin.syncState.pendingFilePushPageIndex.delete(path);
  if (settleTotal) settleUploadTotal(plugin, state);
  releaseUploadSlot(plugin, path, state);
};

const getRetryDescriptor = (plugin: FastSync, state: ActiveUpload): UploadCheckDescriptor => {
  const file = plugin.app.vault.getFileByPath(normalizePath(state.descriptor.path));
  const pendingHash = plugin.pendingUploadHashes.get(state.descriptor.path);
  return {
    ...state.descriptor,
    ...(pendingHash ? { contentHash: pendingHash } : {}),
    ...(file ? {
      size: file.stat.size,
      ctime: file.stat.ctime ?? state.descriptor.ctime,
      mtime: file.stat.mtime,
    } : {}),
  };
};

const finishUploadSessionFailure = (plugin: FastSync, state: ActiveUpload): void => {
  const data = uploadMessageForState(state);
  removeUploadTaskState(plugin, state, true);
  clearUploadCheckpoint(plugin, state);
  // Keep the durable dirty-journal entry. Clear only the pending hash so the
  // next incremental round can discover the local content and try again.
  clearPendingUploadHashIfCurrent(plugin, state);
  recordUploadTask(plugin, data, state, true);
};

const scheduleUploadSessionCheck = (plugin: FastSync, state: ActiveUpload): void => {
  if (state.retryTimer !== null || isPluginUnloading) return;
  if (state.retryCount >= MAX_UPLOAD_SESSION_RETRIES) {
    finishUploadSessionFailure(plugin, state);
    return;
  }

  const generation = uploadQueueGeneration;
  const key = uploadPathKey(state.descriptor.path);
  const delay = UPLOAD_SESSION_RETRY_BASE_DELAY_MS * (2 ** state.retryCount);
  state.retryCount++;
  state.retryTimer = window.setTimeout(() => {
    state.retryTimer = null;
    if (generation !== uploadQueueGeneration || isPluginUnloading || retryingUploadsMap.get(key) !== state) {
      return;
    }
    const currentFile = plugin.app.vault.getFileByPath(normalizePath(state.descriptor.path));
    if (!currentFile) {
      finishUploadSessionFailure(plugin, state);
      return;
    }

    const descriptor = getRetryDescriptor(plugin, state);
    state.descriptor = descriptor;
    dump(`[FileUploadSession] retrying FileUploadCheck ${state.descriptor.path} attempt=${state.retryCount}/${MAX_UPLOAD_SESSION_RETRIES}`);
    void plugin.websocket.SendMessage("FileUploadCheck", descriptor).then((result) => {
      if (generation !== uploadQueueGeneration || retryingUploadsMap.get(key) !== state) return;
      if (result === "sent") {
        // A sent check still needs a bounded response wait. Without this timer,
        // a dropped server response leaves the logical upload active forever.
        state.retryTimer = window.setTimeout(() => {
          state.retryTimer = null;
          if (generation === uploadQueueGeneration && retryingUploadsMap.get(key) === state) {
            scheduleUploadSessionCheck(plugin, state);
          }
        }, UPLOAD_SESSION_RESPONSE_TIMEOUT_MS);
        return;
      }
      // A transient send failure is retried within the same logical attempt;
      // a transport close will invalidate this timer through clearUploadQueue.
      scheduleUploadSessionCheck(plugin, state);
    }).catch((error) => {
      if (generation !== uploadQueueGeneration) return;
      dumpError(`[FileUploadSession] retry check failed: ${state.descriptor.path}`, error);
      scheduleUploadSessionCheck(plugin, state);
    });
  }, delay);
};

const cancelUploadRetry = (plugin: FastSync, path: string): boolean => {
  const key = uploadPathKey(path);
  const state = retryingUploadsMap.get(key);
  if (!state) return false;
  removeUploadTaskState(plugin, state, true);
  clearPendingUploadHashIfCurrent(plugin, state);
  return true;
};

const cancelUploadForPath = (plugin: FastSync, path: string): boolean => {
  const key = uploadPathKey(path);
  if (retryingUploadsMap.has(key)) return cancelUploadRetry(plugin, path);
  const state = activeUploadsMap.get(key);
  if (!state) return false;
  removeUploadTaskState(plugin, state, true);
  clearPendingUploadHashIfCurrent(plugin, state);
  return true;
};

// 大文件跳过同步通知去重：本会话内已提示过的 "path|size"，避免同一文件每轮同步重复弹 toast
const sessionLargeFileNotified = new Set<string>()

/**
 * 大文件跳过同步的通知去重：同一 (path, size) 组合本会话或历史设置中已提示过则只 dump，不再弹 toast
 * Dedup notice for large-file-skip: only dump (no toast) if this (path, size) pair was already notified
 * either in this session or recorded in settings history
 */
function notifyLargeFileSkipped(plugin: FastSync, path: string, size: number, message: string): void {
  const key = `${path}|${size}`
  const shown = plugin.settings.largeFileNoticeShown ?? []
  if (sessionLargeFileNotified.has(key) || shown.includes(key)) {
    dump(`Large file skip notice suppressed (already shown): ${key}`)
    return
  }
  showSyncNotice(message, 5000)
  sessionLargeFileNotified.add(key)
  // 重新赋值而非 push：settings 可能与 DEFAULT_SETTINGS 浅拷贝共享同一数组引用，原地 push 会污染默认值
  const next = [...shown, key]
  if (next.length > 200) next.shift()
  plugin.settings.largeFileNoticeShown = next
  void plugin.saveSettings()
}

/**
 * 获取临时分片目录路径
 */
export const getTempChunksDir = (plugin: FastSync, sessionId?: string) => {
  const base = normalizePath(`${getPluginDir(plugin)}/temp-chunks`)
  return sessionId ? normalizePath(`${base}/${sessionId}`) : base
}

/**
 * 清理指定会话的临时目录
 */
const clearTempDirectory = async (plugin: FastSync, path: string): Promise<void> => {
  if (!(await waitForFileActivity(plugin))) return
  try {
    if (await plugin.app.vault.adapter.exists(path)) {
      if (!(await waitForFileActivity(plugin))) return
      await plugin.app.vault.adapter.rmdir(path, true)
    }
  } catch (error) {
    dump(`Failed to clear temporary sync directory: ${path}`, error)
  }
};

export const clearTempChunksDir = async (plugin: FastSync, sessionId: string) => {
  await clearTempDirectory(plugin, getTempChunksDir(plugin, sessionId))
}

const shouldUseMemoryDownload = (size: number) => Platform.isMobile && size <= MAX_DOWNLOAD_BUFFER_BYTES

const createDownloadStorage = (plugin: FastSync, sessionId: string, size: number): Pick<FileDownloadSession, "chunks" | "downloadedChunks" | "tempDir"> => {
  if (shouldUseMemoryDownload(size)) {
    return { chunks: new Map<number, ArrayBuffer>() }
  }
  return {
    downloadedChunks: new Set<number>(),
    tempDir: getTempChunksDir(plugin, sessionId),
  }
}

const getCompletedDownloadChunks = (session: FileDownloadSession) => {
  return session.tempDir ? session.downloadedChunks?.size || 0 : session.chunks?.size || 0
}

const getSessionMemoryBytes = (session: FileDownloadSession) => {
  if (session.tempDir) return 0
  return Array.from(session.chunks?.values() || []).reduce((sum, c) => sum + c.byteLength, 0)
}

const releaseSessionMemory = (session: FileDownloadSession) => {
  const sessionSize = getSessionMemoryBytes(session)
  if (sessionSize > 0) {
    currentDownloadBufferBytes = Math.max(0, currentDownloadBufferBytes - sessionSize)
  }
}

const formatDownloadError = (e: unknown) => {
  return e instanceof Error ? e.message : String(e)
}

// failed=true 表示本次会话是因下载/写盘失败而清理，需计入 fileSyncTasks.failed，
// 与 completed（驱动完成判定/翻页 ACK，语义不变）区分开，避免失败被状态栏当作成功展示
// failed=true means this session is being cleaned up due to a download/write failure and
// should count toward fileSyncTasks.failed, kept separate from completed (which still drives
// completion detection / page ACK unchanged), so failures aren't surfaced as success in the status bar
const cleanupFileDownloadSession = async (plugin: FastSync, session: FileDownloadSession, failed = false) => {
  releaseSessionMemory(session)
  plugin.fileDownloadSessions.delete(session.sessionId)
  if (session.tempDir) await clearTempChunksDir(plugin, session.sessionId)
  if (failed) plugin.fileSyncTasks.failed++
  plugin.recordSyncCompleted('file', session.pageIndex)
}

/** 下载失败路径的冷却时长：冷却期内该路径的下载任务直接按失败记账跳过，到期自动恢复重试 */
const DOWNLOAD_COOLDOWN_MS = 10 * 60 * 1000

const markDownloadCooldown = (plugin: FastSync, path: string): void => {
  if (!path) return
  // 有界性：名单超长时先过期清扫，防止长会话内存增长
  const cooldowns = plugin.downloadCooldownPaths
  if (cooldowns.size > 2000) {
    const now = Date.now()
    for (const [p, expiry] of cooldowns) {
      if (expiry <= now) cooldowns.delete(p)
    }
  }
  cooldowns.set(path, Date.now() + DOWNLOAD_COOLDOWN_MS)
}

export const isDownloadCoolingDown = (plugin: FastSync, path: string): boolean => {
  const expiry = plugin.downloadCooldownPaths.get(path)
  if (!expiry) return false
  if (expiry <= Date.now()) {
    plugin.downloadCooldownPaths.delete(path)
    return false
  }
  return true
}

/**
 * 收割孤儿下载会话 / Reap orphaned download sessions.
 * 服务端条目销毁或分片永不到达时，会话会永远挂起并卡死 allDownloadsComplete
 * 完成判定（2026-08-27 实测：.19 曾积累 189 个空 temp-chunks 会话目录；iPad
 * 的"同一批文件无限循环"同源）。超过 maxAgeMs 无任何活动（通告/分片）的会话
 * 按失败清算并进入冷却名单。
 */
export const reapStaleFileDownloadSessions = async (plugin: FastSync, maxAgeMs = 120000): Promise<number> => {
  if (plugin.fileDownloadSessions.size === 0) return 0
  const now = Date.now()
  const stale: FileDownloadSession[] = []
  for (const session of plugin.fileDownloadSessions.values()) {
    // 无 lastActivityAt 的旧会话（本补丁前创建）按 0 处理，立即收割——它们正是孤儿
    const lastActivity = session.lastActivityAt ?? 0
    if (now - lastActivity > maxAgeMs) {
      stale.push(session)
    }
  }
  for (const session of stale) {
    await failFileDownloadSession(plugin, session, `session reaped: no chunk activity for ${Math.round((now - (session.lastActivityAt ?? 0)) / 1000)}s`)
  }
  return stale.length
}

const failFileDownloadSession = async (plugin: FastSync, session: FileDownloadSession, message: string, releaseSlot = true) => {
  dumpError(`File download failed: ${session.path} (${session.sessionId}) - ${message}`)
  markDownloadCooldown(plugin, session.path)
  const completedCount = getCompletedDownloadChunks(session)
  SyncLogManager.getInstance().addOrUpdateLog({
    id: session.sessionId,
    type: 'receive',
    action: 'FileDownload',
    path: session.path,
    status: 'error',
    progress: session.totalChunks === 0 ? 0 : Math.floor((completedCount / session.totalChunks) * 100),
    message
  });
  await cleanupFileDownloadSession(plugin, session, true)
  if (releaseSlot) {
    plugin.concurrencyLimiter.releaseSlot(`download_${session.path}`)
  }
}

const storeMemoryChunk = (session: FileDownloadSession, chunkIndex: number, chunkData: ArrayBuffer) => {
  if (!session.chunks) session.chunks = new Map<number, ArrayBuffer>()
  const existingChunk = session.chunks.get(chunkIndex)
  session.chunks.set(chunkIndex, chunkData)
  if (existingChunk) {
    currentDownloadBufferBytes = Math.max(0, currentDownloadBufferBytes - existingChunk.byteLength + chunkData.byteLength)
    return false
  }
  currentDownloadBufferBytes += chunkData.byteLength
  return true
}

const fallbackFileDownloadSessionToMemory = async (plugin: FastSync, session: FileDownloadSession, chunkIndex: number, chunkData: ArrayBuffer) => {
  if (!(await waitForFileActivity(plugin))) return false
  const oldTempDir = session.tempDir
  const downloadedChunks = Array.from(session.downloadedChunks || [])
  session.chunks = new Map<number, ArrayBuffer>()

  if (oldTempDir) {
    for (const index of downloadedChunks) {
      const chunkPath = normalizePath(`${oldTempDir}/${index}.bin`)
      if (!(await waitForFileActivity(plugin))) return false
      if (await plugin.app.vault.adapter.exists(chunkPath)) {
        if (!(await waitForFileActivity(plugin))) return false
        const chunk = await plugin.app.vault.adapter.readBinary(chunkPath)
        storeMemoryChunk(session, index, chunk)
      }
    }
  }

  const wasNewChunk = storeMemoryChunk(session, chunkIndex, chunkData)
  session.tempDir = undefined
  session.downloadedChunks = undefined
  if (oldTempDir) await clearTempChunksDir(plugin, session.sessionId)
  return wasNewChunk
}

/**
 * 清理所有残留的临时目录
 */
export const clearAllTempChunks = async (plugin: FastSync) => {
  if (!(await waitForFileActivity(plugin))) return
  const path = getTempChunksDir(plugin)
  if (await plugin.app.vault.adapter.exists(path)) {
    dump(`Cleaning all temp chunks: ${path}`)
    if (!(await waitForFileActivity(plugin))) return
    await plugin.app.vault.adapter.rmdir(path, true)
  }
  // 确保基础目录在清理后立即存在 (Ensure base dir exists immediately after cleanup)
  if (!(await waitForFileActivity(plugin))) return
  if (!(await plugin.app.vault.adapter.exists(path))) {
    if (!(await waitForFileActivity(plugin))) return
    await plugin.app.vault.adapter.mkdir(path)
  }
}

/**
 * Drop download sessions owned by a dead WebSocket without counting them as
 * completed work. The prepared sync snapshot will request fresh sessions on
 * the replacement connection.
 */
export const resetFileDownloadSessions = (plugin: FastSync): void => {
  const sessions = Array.from(plugin.fileDownloadSessions.values());
  plugin.fileDownloadSessions.clear();

  for (const session of sessions) {
    releaseSessionMemory(session);
    plugin.concurrencyLimiter.releaseSlot(session.initialSlotKey || `download_${session.path}`);
    // An initialization directory has no stable server session ID yet. Leave
    // it for the next FileSyncChunkDownload handler, which clears it before
    // adopting the path. A real session directory is safe to remove now.
    if (session.tempDir && session.sessionId) {
      void clearTempDirectory(plugin, session.tempDir);
    }
  }
}

export const clearUploadQueue = (plugin?: FastSync) => {
  uploadQueueGeneration++
  let cancelledCount = 0
  const uploads = new Set([...activeUploadsMap.values(), ...retryingUploadsMap.values()]);
  for (const upload of uploads) {
    if (!upload.cancelled) cancelledCount++
    upload.cancelled = true
    upload.retrying = false
    upload.retryResponseReceived = false
    clearUploadRetryTimer(upload)
    if (plugin) {
      settleUploadTotal(plugin, upload)
      releaseUploadSlot(plugin, upload.descriptor.path, upload)
      plugin.syncState.pendingFilePushPageIndex.delete(upload.descriptor.path)
      plugin.syncState.pendingFileUploadAcks.delete(uploadPathKey(upload.descriptor.path))
    }
  }
  activeUploadsMap.clear()
  retryingUploadsMap.clear()
  // Session IDs belong to the old WebSocket. Late 463 responses must not be
  // allowed to settle a replacement upload task.
  sessionIdToUploadMap.clear()
  // A replacement transport must not accept ACKs belonging to the dead socket.
  plugin?.syncState.pendingFileUploadAcks.clear()
  if (cancelledCount > 0) {
    dump(`Upload queue cancelled for transport generation ${uploadQueueGeneration}: ${cancelledCount} task(s)`)
  }
}

/**
 * 中止所有进行中的文件操作 (插件卸载时调用)
 */
export const abortAllFileOperations = (plugin?: FastSync) => {
  isPluginUnloading = true;
  clearUploadQueue(plugin);
  dump("All file operations aborted.");
}

/**
 * 重置文件操作状态 (插件加载时调用)
 */
export const resetFileOperations = () => {
  isPluginUnloading = false;
  // A plugin reload must not allow a Promise from the previous instance to
  // resume against the new WebSocket and vault state.
  uploadQueueGeneration++;
  for (const upload of activeUploadsMap.values()) {
    upload.cancelled = true;
    upload.retrying = false;
    upload.retryResponseReceived = false;
    clearUploadRetryTimer(upload);
  }
  for (const upload of retryingUploadsMap.values()) {
    upload.cancelled = true;
    upload.retrying = false;
    upload.retryResponseReceived = false;
    clearUploadRetryTimer(upload);
  }
  activeUploadsMap.clear();
  retryingUploadsMap.clear();
  sessionIdToUploadMap.clear();
}

export const BINARY_PREFIX_FILE_SYNC = "00"

/**
 * 文件（非笔记）修改事件处理
 */
export const fileModify = async function (file: TAbstractFile, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!(file instanceof TFile)) return
  if (file.path.endsWith(".md")) return
  if (eventEnter && plugin.isIgnoredFile(file.path)) return
  if (isPathExcluded(file.path, plugin)) return

  if (isLargeBinarySyncRisk(file.stat.size, plugin)) {
    dump(`Skip file modify for large attachment (${describeBinarySyncLimit()} limit): ${file.path}`, file.stat.size)
    notifyLargeFileSkipped(plugin, file.path, file.stat.size, `Fast Note Sync skipped large file: ${file.path}`)
    return
  }

  const initialBaseHash = plugin.fileHashManager.getPathHash(file.path)
  const initialCachedHash = plugin.fileHashManager.getValidHash(file.path, file.stat.mtime, file.stat.size, file.stat.ctime)
  const initialLastSyncMtime = plugin.lastSyncMtime.get(file.path)
  if (initialCachedHash !== null
    && ((initialCachedHash === initialBaseHash && initialLastSyncMtime !== undefined && initialLastSyncMtime === file.stat.mtime)
      || plugin.pendingUploadHashes.get(file.path) === initialCachedHash)) {
    dump(`File modify intercepted (stable cache match): ${file.path}`)
    plugin.incrementalScanManager?.markSent("file", file.path)
    plugin.incrementalScanManager?.acknowledge("file", file.path)
    return
  }

  const capture = await stableCaptureCoordinator.capture(
    stableCaptureKey(plugin, file.path),
    () => captureStableSnapshot({
      stat: () => readStableStat(plugin, file.path),
      hash: async () => hashFileAsync(plugin.app, file.path, plugin),
    }),
  );
  if (!capture) {
    dump(`[StableCapture] File changed during quiet window; discarded: ${file.path}`)
    return;
  }

  await plugin.lockManager.withLock(file.path, async () => {
    plugin.addIgnoredFile(file.path)
    try {
      const baseHash = plugin.fileHashManager.getPathHash(file.path)
      const lastSyncMtime = plugin.lastSyncMtime.get(file.path)
      const contentHash = capture.hash;
      const stableStat = capture.stat;

      if (plugin.pendingUploadHashes.get(file.path) === contentHash) {
        dump(`File modify intercepted (pending hash match): ${file.path}`)
        plugin.incrementalScanManager?.markSent("file", file.path)
        plugin.incrementalScanManager?.acknowledge("file", file.path)
        return
      }

      if (contentHash === baseHash && lastSyncMtime !== undefined && lastSyncMtime === stableStat.mtime) {
        dump(`File modify intercepted (stable capture matches baseline): ${file.path}`)
        plugin.incrementalScanManager?.markSent("file", file.path)
        plugin.incrementalScanManager?.acknowledge("file", file.path)
        return
      }

      const data = {
        vault: plugin.settings.vault,
        path: file.path,
        pathHash: hashContent(file.path),
        contentHash: contentHash,
        mtime: stableStat.mtime,
        ctime: stableStat.ctime ?? getSafeCtime(file.stat),
        size: stableStat.size,
        // 始终传递 baseHash 信息，如果不可用则标记 baseHashMissing
        ...(baseHash !== null ? { baseHash } : { baseHashMissing: true }),
      }
      // 将 hash 暂存到 pending map，等待服务端 FileUploadAck 后再写入 hashManager
      // Temporarily store hash in pending map, update hashManager only after server FileUploadAck
      // 新建操作覆盖删除意图，清除 pending 防止晚到的 Ack 错误删除新文件 hash
      // New upload supersedes delete intent; clear pending to prevent stale Ack from removing new hash
      plugin.pendingFileDeleteAcks.delete(file.path)
      plugin.pendingUploadHashes.set(file.path, contentHash)
      plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes)
      await plugin.concurrencyLimiter.waitForSlot(file.path)
      plugin.incrementalScanManager?.markSent("file", file.path)
      void plugin.websocket.SendMessage("FileUploadCheck", data, undefined, () => {
        plugin.syncState.pendingFileUploadAcks.add(normalizePath(file.path))
      })
      dump(`File modify check sent`, data.path, data.contentHash)
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 5, retryInterval: 50 });
}

/**
 * 文件删除事件处理
 */
export const fileDelete = async function (file: TAbstractFile, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!(file instanceof TFile)) return
  if (file.path.endsWith(".md")) return
  if (eventEnter && plugin.isIgnoredFile(file.path)) return
  if (isPathExcluded(file.path, plugin)) return


  // --- 新增：删除拦截 ---
  if (plugin.lastSyncPathDeleted.has(file.path)) {
    dump(`File delete intercepted: ${file.path}`)
    return
  }

  await plugin.lockManager.withLock(file.path, async () => {
    // 如果该文件正在上传或在队列中，则标记为取消，且不再发送服务端删除消息
    if (cancelUploadForPath(plugin, file.path)) {
      dump(`Upload cancelled due to file deletion: ${file.path}`);
      // 仅清理本地状态
      plugin.fileHashManager.removeFileHash(file.path)
      return
    }

    // 清理可能存在的待确认上传记录，避免 pending map 内存泄漏
    // Clean up any pending upload record to avoid pending map memory leak
    plugin.pendingUploadHashes.delete(file.path)
    plugin.syncState.pendingFileUploadAcks.delete(normalizePath(file.path))
    plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes)

    plugin.addIgnoredFile(file.path)
    try {
      const data = {
        vault: plugin.settings.vault,
        path: file.path,
        pathHash: hashContent(file.path),
      }
      await plugin.concurrencyLimiter.waitForSlot(file.path)
      void plugin.websocket.SendMessage("FileDelete", data, undefined, () => {
        // 消息真正写入 TCP 缓冲区后加入 pending set，等待 FileDeleteAck 再删 hash
        // Add to pending set only after message is actually buffered; remove hash only on FileDeleteAck
        plugin.incrementalScanManager?.markSent("file", file.path)
        plugin.pendingFileDeleteAcks.add(file.path)
      })
      dump(`File delete send`, file.path)
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 3, retryInterval: 50 });
}

/**
 * 按路径字符串发送文件删除消息（用于无法获取 TFile 对象的场景，如 rename 后旧路径已不存在）
 * Send file delete message by path string (for scenarios where TFile object is unavailable, e.g., old path after rename)
 */
export const fileDeleteByPath = async function (filePath: string, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (filePath.endsWith(".md")) return
  if (isPathExcluded(filePath, plugin)) return

  if (plugin.lastSyncPathDeleted.has(filePath)) return

  await plugin.lockManager.withLock(filePath, async () => {
    // 如果该文件正在上传或在队列中，则标记为取消，且不再发送服务端删除消息
    // If the file is being uploaded or in the queue, cancel and skip server delete
    if (cancelUploadForPath(plugin, filePath)) {
      plugin.fileHashManager.removeFileHash(filePath)
      return
    }

    // 清理可能存在的待确认上传记录，避免 pending map 内存泄漏
    // Clean up any pending upload record to avoid pending map memory leak
    plugin.pendingUploadHashes.delete(filePath)
    plugin.syncState.pendingFileUploadAcks.delete(normalizePath(filePath))
    plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes)

    plugin.addIgnoredFile(filePath)
    try {
      await plugin.concurrencyLimiter.waitForSlot(filePath)
      void plugin.websocket.SendMessage("FileDelete", {
        vault: plugin.settings.vault,
        path: filePath,
        pathHash: hashContent(filePath),
      }, undefined, () => {
        // 消息真正写入 TCP 缓冲区后加入 pending set，等待 FileDeleteAck 再删 hash
        // Add to pending set only after message is actually buffered; remove hash only on FileDeleteAck
        plugin.incrementalScanManager?.markSent("file", filePath)
        plugin.pendingFileDeleteAcks.add(filePath)
      })
      dump(`File delete by path send`, filePath)
    } finally {
      plugin.removeIgnoredFile(filePath)
    }
  }, { maxRetries: 3, retryInterval: 50 });
}

/**
 * 文件重命名事件处理
 */
export const fileRename = async function (file: TAbstractFile, oldfile: string, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (file.path.endsWith(".md")) return
  if (plugin.isIgnoredFile(file.path) && eventEnter) return
  const newExcluded = isPathExcluded(file.path, plugin)
  const oldExcluded = isPathExcluded(oldfile, plugin)

  // Cross-exclusion-boundary rename handling
  // 跨排除边界重命名处理
  if (newExcluded && !oldExcluded) {
    // Moving from normal folder to excluded folder: delete old path on server
    // 从正常文件夹移至排除文件夹：删除服务端旧路径
    void fileDeleteByPath(oldfile, plugin)
    return
  }
  if (!newExcluded && oldExcluded) {
    // Moving from excluded folder to normal folder: create new file on server
    // 从排除文件夹移至正常文件夹：在服务端创建新文件
    void fileModify(file, plugin, true)
    return
  }
  if (newExcluded && oldExcluded) {
    // Both paths excluded: do nothing
    // 两个路径均被排除：不处理
    return
  }

  if (!(file instanceof TFile)) return

  // --- 新增：重命名拦截 ---
  if (plugin.lastSyncPathRenamed.has(file.path)) {
    dump(`File rename intercepted: ${file.path}`)
    return
  }

  await plugin.lockManager.withLock(file.path, async () => {
    plugin.addIgnoredFile(file.path)
    try {
      dump(`File rename`, oldfile, file.path)

      // 如果旧文件正在上传，则取消上传且不发送删除消息
      if (cancelUploadForPath(plugin, oldfile)) {
        // 重新上传
        void fileModify(file, plugin)
        dump(`Upload cancelled due to file rename: ${oldfile}`);
      } else {

        let contentHash = plugin.fileHashManager.getPathHash(oldfile)
        if (contentHash == null) {
          // 尝试新路径哈希缓存 (Try new path cache)
          contentHash = plugin.fileHashManager.getValidHash(file.path, file.stat.mtime, file.stat.size, file.stat.ctime);
          if (contentHash == null) {
            if (isLargeBinarySyncRisk(file.stat.size, plugin)) {
              dump(`Skip rename hash for large attachment (${describeBinarySyncLimit()} limit): ${file.path}`, file.stat.size)
              return
            }
            contentHash = await hashFileAsync(plugin.app, file.path, plugin)
          }
        }

        const data = {
          vault: plugin.settings.vault,
          oldPath: oldfile,
          oldPathHash: hashContent(oldfile),
          path: file.path,
          pathHash: hashContent(file.path),
        }
        // 将重命名推入待确认队列，等待服务端 FileRenameAck 后再更新 hashManager
        // Push rename to pending queue; hashManager will be updated after server FileRenameAck
        plugin.pendingFileRenames.push({ oldPath: oldfile, newPath: file.path, contentHash })
        await plugin.concurrencyLimiter.waitForSlot(file.path, true)
        plugin.incrementalScanManager?.markSent("file", oldfile)
        plugin.incrementalScanManager?.markSent("file", file.path)
        void plugin.websocket.SendMessage("FileRename", data)
      }
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 5, retryInterval: 50 });
}


const createUploadDescriptor = (plugin: FastSync, data: FileUploadMessage, file: TFile): UploadCheckDescriptor => ({
  vault: plugin.settings.vault,
  path: data.path,
  pathHash: data.pathHash,
  contentHash: plugin.pendingUploadHashes.get(data.path) || "",
  size: file.stat.size,
  ctime: file.stat.ctime ?? data.ctime ?? 0,
  mtime: file.stat.mtime ?? data.mtime ?? 0,
});

const updateUploadSession = (plugin: FastSync, state: ActiveUpload, data: FileUploadMessage, file: TFile): void => {
  const key = uploadPathKey(data.path);
  if (sessionIdToUploadMap.get(state.sessionId) === state) {
    sessionIdToUploadMap.delete(state.sessionId);
  }
  state.sessionId = data.sessionId;
  state.message = {
    ...state.message,
    ...data,
    pageIndex: data.pageIndex ?? state.pageIndex,
  };
  state.pageIndex = data.pageIndex ?? state.pageIndex;
  state.chunkSize = data.chunkSize || state.chunkSize || 1024 * 1024;
  state.descriptor = {
    ...state.descriptor,
    ...createUploadDescriptor(plugin, data, file),
    pathHash: data.pathHash || state.descriptor.pathHash,
  };

  const nextTotalChunks = file.stat.size === 0 ? 1 : Math.ceil(file.stat.size / state.chunkSize);
  if (nextTotalChunks !== state.actualTotalChunks && !state.totalCountSettled) {
    plugin.totalChunksToUpload += nextTotalChunks - state.actualTotalChunks;
    state.actualTotalChunks = nextTotalChunks;
  }
  sessionIdToUploadMap.set(state.sessionId, state);
  if (state.pageIndex !== undefined) {
    plugin.syncState.pendingFilePushPageIndex.set(data.path, state.pageIndex);
  }
  if (activeUploadsMap.get(key) !== state) {
    activeUploadsMap.set(key, state);
  }
};

const startUploadRun = (plugin: FastSync, state: ActiveUpload): void => {
  if (state.runActive || state.cancelled || state.retrying || isPluginUnloading) return;
  void runUpload(plugin, state);
};

/**
 * Execute one logical upload session. A 463 transitions the same state into
 * retrying mode; it never creates a second logical task for the same path.
 */
async function runUpload(plugin: FastSync, state: ActiveUpload): Promise<void> {
  if (state.runActive) return;
  const path = state.descriptor.path;
  const key = uploadPathKey(path);
  const generation = state.queueGeneration;
  state.runActive = true;
  let checkpointKey = "";

  try {
    state.slotAcquired = false;
    state.slotReleased = false;
    await plugin.concurrencyLimiter.waitForSlot(path, false, 10);
    state.slotAcquired = true;

    if (isPluginUnloading || state.cancelled || state.retrying
      || state.queueGeneration !== uploadQueueGeneration || activeUploadsMap.get(key) !== state) {
      dump("Upload dropped before read: " + path);
      if (state.retrying) {
        releaseUploadSlot(plugin, path, state);
      } else if (state.queueGeneration === uploadQueueGeneration && !isPluginUnloading) {
        removeUploadTaskState(plugin, state, true);
        recordUploadTask(plugin, uploadMessageForState(state), state);
      } else {
        removeUploadTaskState(plugin, state, true);
      }
      return;
    }

    const file = plugin.app.vault.getFileByPath(normalizePath(path));
    if (!file) {
      dump("File not found for upload: " + path);
      removeUploadTaskState(plugin, state, true);
      recordUploadTask(plugin, uploadMessageForState(state), state);
      return;
    }
    if (isLargeBinarySyncRisk(file.stat.size, plugin)) {
      dump("Skip file upload for large attachment (" + describeBinarySyncLimit() + " limit): " + path, file.stat.size);
      notifyLargeFileSkipped(plugin, path, file.stat.size, "Fast Note Sync skipped large file upload: " + path);
      removeUploadTaskState(plugin, state, true);
      recordUploadTask(plugin, uploadMessageForState(state), state);
      return;
    }

    checkpointKey = "fns-" + plugin.app.vault.getName() + "-uploadSession-" + state.descriptor.pathHash;

    let content: ArrayBuffer | null = null;
    try {
      logMemorySnapshot("before upload read " + path);
      content = await plugin.app.vault.readBinary(file);
    } catch (error) {
      dump("Failed to read file for upload: " + path, error);
    }
    if (!content) {
      removeUploadTaskState(plugin, state, true);
      recordUploadTask(plugin, uploadMessageForState(state), state);
      return;
    }

    const contentHash = await hashFileAsync(plugin.app, file.path, plugin);
    logMemorySnapshot("after upload hash " + path);
    state.descriptor.contentHash = contentHash;
    plugin.pendingUploadHashes.set(path, contentHash);
    plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes);
    plugin.fileHashManager.setLocalFileHash(path, contentHash, file.stat.mtime, file.stat.size);
    plugin.syncState.pendingFileUploadAcks.add(key);

    const contentTotalChunks = content.byteLength === 0 ? 1 : Math.ceil(content.byteLength / state.chunkSize);
    if (contentTotalChunks !== state.actualTotalChunks && !state.totalCountSettled) {
      plugin.totalChunksToUpload += contentTotalChunks - state.actualTotalChunks;
      state.actualTotalChunks = contentTotalChunks;
    }
    const actualTotalChunks = state.actualTotalChunks;
    const sessionId = state.sessionId;
    const chunkSize = state.chunkSize;

    let startChunkIndex = 0;
    try {
      const cpRaw = plugin.app.loadLocalStorage(checkpointKey) as string | undefined;
      if (cpRaw) {
        const cp = JSON.parse(cpRaw) as { sessionId?: string; lastChunkIndex?: number; contentHash?: string };
        if (cp.sessionId === sessionId && cp.contentHash === contentHash
          && typeof cp.lastChunkIndex === 'number'
          && cp.lastChunkIndex >= 0 && cp.lastChunkIndex < actualTotalChunks - 1) {
          startChunkIndex = cp.lastChunkIndex + 1;
          dump("Resume upload from chunk " + startChunkIndex + "/" + actualTotalChunks + ": " + path);
        }
      }
    } catch (error) {
      dump("Failed to read upload checkpoint for " + path, error);
    }
    if (startChunkIndex > 0) plugin.uploadedChunksCount += startChunkIndex;

    dump([{
      操作: "文件上传",
      路径: path,
      文件大小: (content.byteLength / 1024 / 1024).toFixed(2) + " MB",
      分片大小: (chunkSize / 1024).toFixed(0) + " KB",
      分片数量: actualTotalChunks,
      SessionID: sessionId.substring(0, 8) + "...",
    }]);

    const sleepTime = Platform.isMobile ? 10 : 2;
    for (let i = startChunkIndex; i < actualTotalChunks; i++) {
      const chunk = new Uint8Array(content, i * chunkSize, Math.min(chunkSize, content.byteLength - i * chunkSize));
      const sessionIdBytes = new TextEncoder().encode(sessionId);
      const chunkIndexBytes = new Uint8Array(4);
      new DataView(chunkIndexBytes.buffer).setUint32(0, i, false);
      const frame = new Uint8Array(36 + 4 + chunk.byteLength);
      frame.set(sessionIdBytes, 0);
      frame.set(chunkIndexBytes, 36);
      frame.set(chunk, 40);

      const sendResult = await plugin.websocket.SendBinary(
        frame,
        BINARY_PREFIX_FILE_SYNC,
        () => {
          if (isPluginUnloading || state.cancelled || state.retrying
            || state.sessionId !== sessionId || activeUploadsMap.get(key) !== state) {
            dump("Upload aborted for " + path + " (state changed before send)");
            return true;
          }
          return false;
        },
        () => {
          if (state.cancelled || state.sessionId !== sessionId || state.retrying
            || state.queueGeneration !== uploadQueueGeneration || activeUploadsMap.get(key) !== state) return;
          plugin.uploadedChunksCount++;
          const currentProgress = Math.floor(((i + 1) / actualTotalChunks) * 100);
          const isLastChunk = (i + 1) === actualTotalChunks;
          if (!isLastChunk) {
            try {
              plugin.app.saveLocalStorage(checkpointKey, JSON.stringify({
                sessionId,
                lastChunkIndex: i,
                contentHash,
                timestamp: Date.now(),
              }));
            } catch (error) {
              dump("Failed to save upload checkpoint for " + path, error);
            }
          }
          SyncLogManager.getInstance().addOrUpdateLog({
            id: sessionId,
            type: 'send',
            action: 'FileUpload',
            path,
            status: isLastChunk ? 'success' : 'pending',
            progress: currentProgress,
          });
        },
      );

      if (state.retrying) {
        releaseUploadSlot(plugin, path, state);
        return;
      }
      if (sendResult === 'closed') {
        dump("Upload interrupted for " + path + " at chunk " + i + "/" + actualTotalChunks + " (connection closed), will resume after reconnect");
        SyncLogManager.getInstance().addOrUpdateLog({
          id: sessionId,
          type: 'send',
          action: 'FileUpload',
          path,
          status: 'pending',
          message: '连接已断开，等待重连后续传',
        });
        removeUploadTaskState(plugin, state, true);
        return;
      }
      if (sendResult === 'cancelled' || isPluginUnloading || state.cancelled) {
        if (state.retrying) {
          releaseUploadSlot(plugin, path, state);
          return;
        }
        try { plugin.app.saveLocalStorage(checkpointKey, null); } catch { /* ignore */ }
        removeUploadTaskState(plugin, state, true);
        recordUploadTask(plugin, uploadMessageForState(state), state);
        return;
      }
      await sleep(sleepTime);
    }

    content = null;
    if (state.retrying || state.cancelled || state.sessionId !== sessionId) {
      if (state.retrying) releaseUploadSlot(plugin, path, state);
      return;
    }

    // Keep the state and the limiter slot until FileUploadAck. This preserves
    // the session identity long enough to recover a late 463 and prevents
    // SyncEnd from declaring the upload complete before the server commits it.
    state.chunksSent = true;

    // 上传完成后，如果开启了附件云预览 - 上传后删除，则删除本地附件
    if (isCloudPreviewRuntimeEnabled(plugin.settings) && plugin.settings.cloudPreviewAutoDeleteLocal) {
      const ext = file.path.substring(file.path.lastIndexOf(".")).toLowerCase();
      const isRestricted = FileCloudPreview.isRestrictedType(ext);
      if (!(plugin.settings.cloudPreviewTypeRestricted && !isRestricted)) {
        void (async () => {
          await sleep(2000);
          if (isPluginUnloading) return;
          try {
            const apiService = new HttpApiService(plugin);
            const serverInfo = await apiService.getFileInfo(file.path);
            if (serverInfo && serverInfo.path === file.path
              && serverInfo.size === file.stat.size && serverInfo.mtime === file.stat.mtime) {
              dump("Cloud Preview: Auto delete verified file: " + file.path);
              plugin.addIgnoredFile(file.path);
              try {
                await vaultDelete(plugin.app.vault, file);
                plugin.fileHashManager.removeFileHash(file.path);
              } finally {
                plugin.removeIgnoredFile(file.path);
              }
            }
          } catch (error) {
            dump("Cloud Preview: Auto delete failed to fetch info for " + file.path, error);
          }
        })();
      }
    }
  } catch (error) {
    if (state.retrying) {
      releaseUploadSlot(plugin, path, state);
      return;
    }
    dump("Upload process error for " + path, error);
    if (checkpointKey) {
      try { plugin.app.saveLocalStorage(checkpointKey, null); } catch { /* ignore */ }
    }
    removeUploadTaskState(plugin, state, true);
    recordUploadTask(plugin, uploadMessageForState(state), state, true);
  } finally {
    state.runActive = false;
    if (state.retryResponseReceived && retryingUploadsMap.get(key) === state
      && !isPluginUnloading && generation === uploadQueueGeneration) {
      state.retryResponseReceived = false;
      state.retrying = false;
      state.cancelled = false;
      state.chunksSent = false;
      state.queueGeneration = uploadQueueGeneration;
      retryingUploadsMap.delete(key);
      sessionIdToUploadMap.set(state.sessionId, state);
      startUploadRun(plugin, state);
    } else if (!state.retrying && !state.chunksSent && activeUploadsMap.get(key) === state && state.cancelled) {
      removeUploadStateIfCurrent(path, state);
    }
  }
}

/**
 * 接收服务端文件上传指令 (FileUpload)
 */
export const receiveFileUpload = async function (data: FileUploadMessage, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

  if (plugin.settings.readonlySyncEnabled) {
    dump("Read-only mode: Intercepted file upload request for " + data.path);
    plugin.recordSyncCompleted('file', data.pageIndex);
    return;
  }
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('file', data.pageIndex);
    return;
  }

  const key = uploadPathKey(data.path);
  const file = plugin.app.vault.getFileByPath(normalizePath(data.path));
  if (!file) {
    dump("File not found for upload: " + data.path);
    plugin.recordSyncCompleted('file', data.pageIndex);
    return;
  }
  if (isLargeBinarySyncRisk(file.stat.size, plugin)) {
    dump("Skip file upload for large attachment (" + describeBinarySyncLimit() + " limit): " + data.path, file.stat.size);
    notifyLargeFileSkipped(plugin, data.path, file.stat.size, "Fast Note Sync skipped large file upload: " + data.path);
    plugin.recordSyncCompleted('file', data.pageIndex);
    return;
  }

  const existing = activeUploadsMap.get(key);
  if (existing) {
    if (existing.sessionId === data.sessionId && !existing.retrying) {
      dump("Duplicate FileUpload ignored: " + data.path + " (" + data.sessionId + ")");
      return;
    }
    if (!existing.retrying) {
      dump("Stale FileUpload ignored while another session is active: " + data.path + " (" + data.sessionId + ")");
      return;
    }

    // This is the response to our FileUploadCheck retry. Keep one logical task
    // and let the old run unwind before starting the new session.
    clearUploadRetryTimer(existing);
    updateUploadSession(plugin, existing, data, file);
    existing.chunksSent = false;
    existing.retryResponseReceived = existing.runActive;
    if (existing.runActive) return;
    existing.retrying = false;
    existing.cancelled = false;
    retryingUploadsMap.delete(key);
    startUploadRun(plugin, existing);
    return;
  }

  const chunkSize = data.chunkSize || 1024 * 1024;
  const actualTotalChunks = file.stat.size === 0 ? 1 : Math.ceil(file.stat.size / chunkSize);
  const state: ActiveUpload = {
    cancelled: false,
    queueGeneration: uploadQueueGeneration,
    slotAcquired: false,
    slotReleased: false,
    taskRecorded: false,
    sessionId: data.sessionId,
    descriptor: createUploadDescriptor(plugin, data, file),
    message: { ...data },
    pageIndex: data.pageIndex,
    chunkSize,
    retryCount: 0,
    retryTimer: null,
    retrying: false,
    retryResponseReceived: false,
    runActive: false,
    chunksSent: false,
    actualTotalChunks,
    totalCountSettled: false,
  };
  activeUploadsMap.set(key, state);
  sessionIdToUploadMap.set(state.sessionId, state);
  if (state.pageIndex !== undefined) {
    plugin.syncState.pendingFilePushPageIndex.set(data.path, state.pageIndex);
  }
  plugin.syncState.pendingFileUploadAcks.add(key);
  plugin.totalChunksToUpload += actualTotalChunks;
  dump("Receive file need upload (queued): " + data.path + " " + data.sessionId);
  startUploadRun(plugin, state);
};

/**
 * 接收服务端文件更新通知 (FileSyncUpdate)
 */
export const receiveFileSyncUpdate = async function (data: ReceiveFileSyncUpdateMessage, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

  // 服务端推送说明该路径已有新内容，清除可能残留的 deleteAck pending 防止 Ack 删除新 hash
  // Server push means path has new content; clear stale deleteAck pending to protect newly-written hash
  plugin.pendingFileDeleteAcks.delete(data.path)
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }
  if (isLargeBinarySyncRisk(data.size, plugin)) {
    dump(`Skip file download for large attachment (${describeBinarySyncLimit()} limit): ${data.path}`, data.size)
    notifyLargeFileSkipped(plugin, data.path, data.size, `Fast Note Sync skipped large file download: ${data.path}`)
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }

  // 如果开启了云预览，且是初始化同步阶段，由于云预览可以按需加载，跳过所有附件下载
  if (plugin.localStorageManager.getMetadata("isInitSync") && isCloudPreviewRuntimeEnabled(plugin.settings)) {
    if (plugin.settings.cloudPreviewTypeRestricted) {
      // 开启了类型限制：仅跳过受限类型 (图片、视频、音频、PDF)
      const ext = data.path.substring(data.path.lastIndexOf(".")).toLowerCase();
      if (FileCloudPreview.isRestrictedType(ext)) {
        dump(`Cloud Preview: Skipping restricted file download: ${data.path}`);
        plugin.recordSyncCompleted('file', data.pageIndex);
        return;
      }
    } else {
      // 未开启类型限制：由于启用了云预览，跳过所有附件下载
      dump(`Cloud Preview: Skipping all file downloads: ${data.path}`);
      plugin.recordSyncCompleted('file', data.pageIndex);
      return;
    }
  }

  // 冷却跳过：该路径本轮运行期间刚下载失败过（孤儿会话/分片丢失）。服务器每轮都会
  // 重新下发缺失路径，立即重试会以同样方式失败，制造"同一批文件无限循环同步"。冷却
  // 期内按失败记账跳过（不产生 I/O 与会话），到期自动恢复重试。
  // Cooldown skip: this path failed to download recently in this app run (orphaned
  // session / lost chunks). The server re-offers missing paths every round; an
  // immediate retry would fail identically and loop the same batch forever.
  // Skip with failure accounting while cooling down; retry resumes on expiry.
  if (isDownloadCoolingDown(plugin, data.path)) {
    dump(`File download skipped (cooldown after recent failure): ${data.path}`);
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }

  // 等待并发槽位，防止大量并发下载导致内存耗尽
  const slotKey = `download_${data.path}`
  await plugin.concurrencyLimiter.waitForSlot(slotKey, false, -10) // 优先级设为 -10，延后处理下载

  try {
    // 下载内存缓冲控制：如果当前内存中待写盘的分块过多，由于下载是异步触发的，此处等待
    while (currentDownloadBufferBytes > MAX_DOWNLOAD_BUFFER_BYTES) {
      await sleep(200);
    }

    dump(`Receive file sync update(download): `, data.path)
    const tempKey = `temp_${data.path}`
    const tempSession: FileDownloadSession = {
      path: data.path,
      contentHash: data.contentHash,
      ctime: data.ctime,
      mtime: data.mtime,
      lastTime: data.lastTime,
      sessionId: "",
      totalChunks: 0,
      size: data.size,
      pageIndex: data.pageIndex,
      initialSlotKey: slotKey,
      lastActivityAt: Date.now(),
      ...createDownloadStorage(plugin, `init_${data.pathHash}`, data.size),
    }
    plugin.fileDownloadSessions.set(tempKey, tempSession)

    const requestData = {
      vault: plugin.settings.vault,
      path: data.path,
      pathHash: data.pathHash,
    }
    void plugin.websocket.SendMessage("FileChunkDownload", requestData)
    plugin.totalFilesToDownload++

    // 更新同步时间
    // Update sync time
    if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
      plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
    }
  } catch (e) {
    plugin.concurrencyLimiter.releaseSlot(slotKey)
    throw e;
  }
}

/**
 * 接收服务端文件删除通知
 */
export const receiveFileSyncDelete = async function (data: ReceivePathMessage, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }

  if (plugin.localStorageManager.getMetadata("isInitSync") && isCloudPreviewRuntimeEnabled(plugin.settings)) {
    if (plugin.settings.cloudPreviewTypeRestricted) {
      const ext = data.path.substring(data.path.lastIndexOf(".")).toLowerCase();
      if (FileCloudPreview.isRestrictedType(ext)) {
        dump(`Cloud Preview: Skipping restricted file delete: ${data.path}`);
        plugin.recordSyncCompleted('file', data.pageIndex);
        return;
      }
    } else {
      dump(`Cloud Preview: Skipping all file deletes: ${data.path}`);
      plugin.recordSyncCompleted('file', data.pageIndex);
      return;
    }
  }

  dump(`Receive file delete: `, data.path)
  const normalizedPath = normalizePath(data.path)

  await plugin.lockManager.withLock(normalizedPath, async () => {
    const file = plugin.app.vault.getFileByPath(normalizedPath)
    if (file instanceof TFile) {
      plugin.addIgnoredFile(normalizedPath)
      // 记录待删除路径
      plugin.lastSyncPathDeleted.add(normalizedPath)
      try {
        if (!(await waitForFileActivity(plugin))) return
        await vaultDelete(plugin.app.vault, file)
        // 服务端推送删除,从哈希表中移除
        plugin.fileHashManager.removeFileHash(normalizedPath)
        plugin.lastSyncMtime.delete(normalizedPath)
        // 更新同步时间
        if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
          plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
        }
      } finally {
        // 延时 500ms 清理
        window.setTimeout(() => {
          plugin.removeIgnoredFile(normalizedPath)
          plugin.lastSyncPathDeleted.delete(normalizedPath)
        }, 500);
      }
    }
  }, { maxRetries: 5, retryInterval: 100 }).catch(e => {
    dumpError(`[FastSync] Failed to receiveFileSyncDelete: ${normalizedPath}`, e);
    SyncLogManager.getInstance().addLog('receive', 'FileDelete', e instanceof Error ? e.message : String(e), 'error', data.path);
    plugin.fileSyncTasks.failed++
  });

  plugin.recordSyncCompleted('file', data.pageIndex)
}

/**
 * 接收服务端文件元数据(mtime)更新通知
 */
export const receiveFileSyncMtime = async function (data: ReceiveMtimeMessage, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }

  if (plugin.localStorageManager.getMetadata("isInitSync") && isCloudPreviewRuntimeEnabled(plugin.settings)) {
    if (plugin.settings.cloudPreviewTypeRestricted) {
      const ext = data.path.substring(data.path.lastIndexOf(".")).toLowerCase();
      if (FileCloudPreview.isRestrictedType(ext)) {
        dump(`Cloud Preview: Skipping restricted file mtime update: ${data.path}`);
        plugin.recordSyncCompleted('file', data.pageIndex);
        return;
      }
    } else {
      dump(`Cloud Preview: Skipping all file mtime updates: ${data.path}`);
      plugin.recordSyncCompleted('file', data.pageIndex);
      return;
    }
  }

  dump(`Receive file sync mtime: `, data.path, data.mtime)
  const normalizedPath = normalizePath(data.path)

  await plugin.lockManager.withLock(normalizedPath, async () => {
    const file = plugin.app.vault.getFileByPath(normalizedPath)
    if (file) {
      if (isLargeBinarySyncRisk(file.stat.size, plugin)) {
        dump(`Skip binary mtime rewrite for large attachment (${describeBinarySyncLimit()} limit): ${normalizedPath}`, file.stat.size)
        plugin.lastSyncMtime.set(data.path, data.mtime)
        if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
          plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
        }
        return
      }
      if (!(await waitForFileActivity(plugin))) return
      const content = await plugin.app.vault.readBinary(file)
      plugin.addIgnoredFile(normalizedPath)
      try {
        if (!(await waitForFileActivity(plugin))) return
        await plugin.app.vault.modifyBinary(file, content, { ...(data.ctime > 0 && { ctime: data.ctime }), ...(data.mtime > 0 && { mtime: data.mtime }) })
        // 记录 mtime
        plugin.lastSyncMtime.set(data.path, data.mtime)
        // 更新同步时间
        if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
          plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
        }
      } finally {
        window.setTimeout(() => {
          plugin.removeIgnoredFile(normalizedPath)
        }, 500);
      }
    }
  }, { maxRetries: 5, retryInterval: 100 }).catch(e => {
    dumpError(`[FastSync] Failed to receiveFileSyncMtime: ${normalizedPath}`, e);
    if (!checkAndNotifyCaseConflict(e, data.path, plugin, 'FileMtime')) {
      SyncLogManager.getInstance().addLog('receive', 'FileMtime', e instanceof Error ? e.message : String(e), 'error', data.path);
    }
    plugin.fileSyncTasks.failed++
  });

  // FileSyncMtime 表示文件已在服务端存在（无需上传），释放 fileModify 中获取的并发槽位
  // FileSyncMtime indicates file already exists on server (no upload needed), release slot acquired by fileModify
  if (data.path) plugin.concurrencyLimiter.releaseSlot(data.path)
  plugin.recordSyncCompleted('file', data.pageIndex)
}

/**
 * 接收服务端分片下载响应 (FileSyncChunkDownload)
 */
export const receiveFileSyncChunkDownload = async function (data: FileSyncChunkDownloadMessage, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

  dump(`Receive file chunk download: `, data.path, data.sessionId, `totalChunks: ${data.totalChunks}`)

  // 打印下载信息 (Print download info)
  dump(
    "文件下载",
    {
      路径: data.path,
      文件大小: `${(data.size / 1024 / 1024).toFixed(2)} MB`,
      分片大小: `${(data.chunkSize / 1024).toFixed(0)} KB`,
      分片数量: data.totalChunks,
      SessionID: data.sessionId.substring(0, 8) + "...",
    }
  )

  const tempKey = `temp_${data.path}`
  const tempSession = plugin.fileDownloadSessions.get(tempKey)
  let session: FileDownloadSession

  if (tempSession) {
    // The initialization directory is keyed by pathHash because the server
    // session ID is not known yet. Clear it before a replacement session can
    // write new chunks, so stale chunks are never mixed into the new download.
    if (tempSession.tempDir) {
      await clearTempDirectory(plugin, tempSession.tempDir)
    }
    session = {
      path: data.path,
      contentHash: data.contentHash,
      ctime: data.ctime,
      mtime: data.mtime,
      lastTime: tempSession.lastTime,
      sessionId: data.sessionId,
      totalChunks: data.totalChunks,
      size: data.size,
      pageIndex: tempSession.pageIndex,
      initialSlotKey: tempSession.initialSlotKey,
      lastActivityAt: Date.now(),
      ...createDownloadStorage(plugin, data.sessionId, data.size),
    }
    plugin.fileDownloadSessions.set(data.sessionId, session)
    plugin.fileDownloadSessions.delete(tempKey)
  } else {
    session = {
      path: data.path,
      contentHash: data.contentHash,
      ctime: data.ctime,
      mtime: data.mtime,
      lastTime: 0,
      sessionId: data.sessionId,
      totalChunks: data.totalChunks,
      size: data.size,
      initialSlotKey: `download_${data.path}`,
      lastActivityAt: Date.now(),
      ...createDownloadStorage(plugin, data.sessionId, data.size),
    }
    plugin.fileDownloadSessions.set(data.sessionId, session)
  }

  // Replay any binary chunks that were buffered while this registration was
  // still in flight (see handleFileChunkDownload). Registration is complete
  // now, so each stored frame resolves its session and writes normally.
  const earlyChunks = plugin.pendingFileChunks.get(data.sessionId)
  if (earlyChunks && earlyChunks.length > 0) {
    plugin.pendingFileChunks.delete(data.sessionId)
    dump(`Replaying ${earlyChunks.length} early chunk(s) for session ${data.sessionId.slice(0, 8)}... (${data.path})`)
    for (const frame of earlyChunks) {
      void handleFileChunkDownload(frame, plugin)
    }
  }

  // 确保临时目录存在 (Ensure temp directory exists)
  // 并发下多个下载会话可能同时 mkdir 同一目录而抛错：catch 后复验是否已存在（并发创建属正常，吞掉）；
  // 真正未创建成功则不在此处兜底，交给首个分片写入时的既有失败路径处理（handleFileChunkDownload 的内存 fallback / failFileDownloadSession）
  if (data.totalChunks > 0 && session.tempDir) {
    const baseDir = getTempChunksDir(plugin)
    try {
      if (!(await waitForFileActivity(plugin))) return
      if (!(await plugin.app.vault.adapter.exists(baseDir))) {
        if (!(await waitForFileActivity(plugin))) return
        await plugin.app.vault.adapter.mkdir(baseDir)
      }
      if (!(await waitForFileActivity(plugin))) return
      if (!(await plugin.app.vault.adapter.exists(session.tempDir))) {
        if (!(await waitForFileActivity(plugin))) return
        await plugin.app.vault.adapter.mkdir(session.tempDir)
      }
    } catch (e) {
      if (!(await waitForFileActivity(plugin))) return
      if (!(await plugin.app.vault.adapter.exists(session.tempDir))) {
        dumpError(`Temp dir creation failed for session ${session.sessionId}, will retry on first chunk`, e)
      }
    }
  }

  // 仅在非同步期间(实时监听时)手动增加分片计数。同步期间由 SyncEnd 包装器统一预估
  if (!plugin.isSyncing) {
    plugin.totalChunksToDownload += data.totalChunks
  }

  // 创建初始日志记录
  const isTotalChunksZero = data.totalChunks === 0
  SyncLogManager.getInstance().addOrUpdateLog({
    id: data.sessionId,
    type: 'receive',
    action: 'FileDownload',
    path: data.path,
    status: isTotalChunksZero ? 'success' : 'pending',
    progress: isTotalChunksZero ? 100 : 0
  });

  // 如果分片数为 0（空文件），立即触发完成逻辑1
  if (data.totalChunks === 0) {
    const finalSession = plugin.fileDownloadSessions.get(data.sessionId);
    if (finalSession) {
      await handleFileChunkDownloadComplete(finalSession, plugin);
    }
  }
}



/**
 * 接收文件同步结束通知
 */
export const receiveFileSyncEnd = async function (data: unknown, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  dump(`Receive file sync end:`, data)

  const syncData = data as SyncEndData
  // 更新任务统计信息，用于进度条计算 (Update task stats for progress bar)
  plugin.fileSyncTasks.needUpload = syncData.needUploadCount || 0
  plugin.fileSyncTasks.needModify = syncData.needModifyCount || 0
  plugin.fileSyncTasks.needSyncMtime = syncData.needSyncMtimeCount || 0
  plugin.fileSyncTasks.needDelete = syncData.needDeleteCount || 0

  // 无条件更新 lastFileSyncTime，确保包含服务端本轮同步后的所有异步操作（如 SyncResourceFID）
  // Unconditionally update lastFileSyncTime to cover all async server-side ops after this sync round (e.g., SyncResourceFID)
  plugin.localStorageManager.setMetadata("lastFileSyncTime", syncData.lastTime)
  plugin.syncTypeCompleteCount++
}

/**
 * Reconcile attachments once after cloud preview is enabled.
 *
 * Normal attachment create/modify events already go through fileModify. This
 * pass is only a bounded repair for attachments that predate cloud preview.
 * Its cursor is persisted after every successful check so an iOS process
 * termination resumes from the last confirmed path instead of restarting the
 * whole vault scan. A process-local promise prevents overlapping repair passes
 * when several sync rounds finish close together.
 */
export const checkAndUploadAttachments = async function (plugin: FastSync) {
  if (!isCloudPreviewRuntimeEnabled(plugin.settings) || plugin.settings.readonlySyncEnabled) return;
  if (cloudPreviewCheckPromise) return cloudPreviewCheckPromise;

  const run = async () => {
    if (!(await waitForFileActivity(plugin))) return;

    const mode = plugin.settings.cloudPreviewTypeRestricted ? "restricted" : "all";
    let state = parseCloudPreviewCheckState(
      plugin.localStorageManager.getMetadata(CLOUD_PREVIEW_CHECK_STATE),
      mode,
    );
    if (state.complete) return;

    const files = plugin.app.vault.getFiles()
      .filter((file) => {
        if (file.extension === "md" || isPathExcluded(file.path, plugin)) return false;
        return mode === "all" || FileCloudPreview.isRestrictedType(`.${file.extension}`);
      })
      .sort((left, right) => left.path.localeCompare(right.path));
    const remainingFiles = files.filter((file) => file.path > state.nextPath);

    dump(`Cloud Preview: Resuming attachment reconciliation at ${state.nextPath || "<start>"}; ${remainingFiles.length} candidates remain`);

    const apiService = new HttpApiService(plugin);
    let checkedCount = 0;
    let uploadCount = 0;

    for (const file of remainingFiles) {
      if (!(await waitForFileActivity(plugin))) return;

      try {
        const res = await apiService.getFileInfo(file.path);
        if (!res) {
          dump(`Cloud Preview: File missing on server, starting upload: ${file.path}`);
          await fileModify(file, plugin, false);
          uploadCount++;
        }
      } catch (error) {
        dump(`Cloud Preview: Pausing attachment reconciliation at ${file.path}`, error);
        return;
      }

      state = advanceCloudPreviewCheckState(state, file.path);
      plugin.localStorageManager.setMetadata(CLOUD_PREVIEW_CHECK_STATE, serializeCloudPreviewCheckState(state));
      checkedCount++;

      if (checkedCount % 10 === 0) await sleep(50);
    }

    state = completeCloudPreviewCheckState(state);
    plugin.localStorageManager.setMetadata(CLOUD_PREVIEW_CHECK_STATE, serializeCloudPreviewCheckState(state));
    dump(`Cloud Preview: Attachment reconciliation complete. Checked: ${checkedCount}, Uploaded: ${uploadCount}`);
  };

  cloudPreviewCheckPromise = run();
  try {
    await cloudPreviewCheckPromise;
  } finally {
    cloudPreviewCheckPromise = null;
  }
}

/**
 * 处理接收到的二进制文件分片
 */
export const handleFileChunkDownload = async function (buf: ArrayBuffer | Blob, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || isPluginUnloading) return

  const binaryData = buf instanceof Blob ? await buf.arrayBuffer() : buf
  if (binaryData.byteLength < 40 || isPluginUnloading) {
    dump(`File chunk download dropped: invalid payload length ${binaryData.byteLength}`)
    return
  }

  const sessionIdBytes = new Uint8Array(binaryData, 0, 36)
  const sessionId = new TextDecoder().decode(sessionIdBytes)
  const chunkIndexBytes = new Uint8Array(binaryData, 36, 4)
  const view = new DataView(chunkIndexBytes.buffer, chunkIndexBytes.byteOffset, 4)
  const chunkIndex = view.getUint32(0, false)
  const chunkData = binaryData.slice(40)

  const session = plugin.fileDownloadSessions.get(sessionId)
  if (!session) {
    // The announcement handler (receiveFileSyncChunkDownload) awaits temp-dir
    // cleanup before it inserts this id into fileDownloadSessions, so the first
    // binary chunk can arrive first and lose the lookup race. The server sends
    // each chunk exactly once; dropping it here starves the download forever.
    // Buffer the raw frame and replay it once the session registers.
    const pending = plugin.pendingFileChunks.get(sessionId)
    if (pending && pending.length >= 64) {
      const message = `File download chunk dropped: pending buffer full for unregistered session (${sessionId}), chunk ${chunkIndex}`
      dumpError(message)
      SyncLogManager.getInstance().addOrUpdateLog({
        id: sessionId,
        type: 'receive',
        action: 'FileDownload',
        status: 'error',
        progress: 0,
        message
      });
      return
    }
    if (!pending) dump(`File download chunk buffered early (session not yet registered): ${sessionId.slice(0, 8)}..., chunk ${chunkIndex}`)
    const buf = pending ?? []
    buf.push(binaryData)
    plugin.pendingFileChunks.set(sessionId, buf)
    return
  }
  session.lastActivityAt = Date.now()

  try {
    if (chunkIndex >= session.totalChunks) {
      await failFileDownloadSession(plugin, session, `Invalid chunk index ${chunkIndex}, total chunks ${session.totalChunks}`)
      return
    }

    let isNewChunk = false

    if (session.tempDir) {
      try {
        if (!(await waitForFileActivity(plugin))) return
        if (!(await plugin.app.vault.adapter.exists(session.tempDir))) {
          const baseDir = getTempChunksDir(plugin)
          try {
            if (!(await waitForFileActivity(plugin))) return
            if (!(await plugin.app.vault.adapter.exists(baseDir))) {
              if (!(await waitForFileActivity(plugin))) return
              await plugin.app.vault.adapter.mkdir(baseDir)
            }
            if (!(await waitForFileActivity(plugin))) return
            await plugin.app.vault.adapter.mkdir(session.tempDir)
          } catch (mkdirErr) {
            // 并发下多个分片会话可能同时 mkdir 同一目录：复验已存在则吞掉；
            // 仍不存在说明是真实创建失败，交给外层 catch 走既有失败路径（内存 fallback / failFileDownloadSession）
            if (!(await waitForFileActivity(plugin))) return
            if (!(await plugin.app.vault.adapter.exists(session.tempDir))) {
              throw mkdirErr
            }
          }
        }
        const chunkPath = normalizePath(`${session.tempDir}/${chunkIndex}.bin`)
        isNewChunk = !session.downloadedChunks?.has(chunkIndex)
        if (!(await waitForFileActivity(plugin))) return
        await plugin.app.vault.adapter.writeBinary(chunkPath, chunkData)
        session.downloadedChunks?.add(chunkIndex)
      } catch (e) {
        if (session.size <= MAX_DOWNLOAD_BUFFER_BYTES) {
          dumpError(`File chunk temp write failed, fallback to memory: ${session.path} chunk ${chunkIndex}`, e)
          isNewChunk = await fallbackFileDownloadSessionToMemory(plugin, session, chunkIndex, chunkData)
        } else {
          await failFileDownloadSession(plugin, session, `Failed to write temp chunk ${chunkIndex}: ${formatDownloadError(e)}`)
          return
        }
      }
    } else {
      isNewChunk = storeMemoryChunk(session, chunkIndex, chunkData)
    }

    if (isNewChunk) {
      plugin.downloadedChunksCount++
    }

    // 更新日志进度
    const completedCount = getCompletedDownloadChunks(session)
    SyncLogManager.getInstance().addOrUpdateLog({
      id: sessionId,
      type: 'receive',
      action: 'FileDownload',
      path: session.path,
      status: completedCount === session.totalChunks ? 'success' : 'pending',
      progress: session.totalChunks === 0 ? 100 : Math.floor((completedCount / session.totalChunks) * 100)
    });


    if (completedCount === session.totalChunks) {
      await handleFileChunkDownloadComplete(session, plugin)
    }
  } catch (e) {
    await failFileDownloadSession(plugin, session, `Failed to receive chunk ${chunkIndex}: ${formatDownloadError(e)}`)
  }
}

/**
 * 接收服务端文件重命名通知
 */
export const receiveFileSyncRename = async function (data: { oldPath: string; path: string; mtime?: number; ctime?: number; contentHash?: string; lastTime?: number; size?: number; pathHash?: string; pageIndex?: number }, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return

    if (isPathExcluded(data.path, plugin) || isPathExcluded(data.oldPath, plugin)) {
    plugin.recordSyncCompleted('file', data.pageIndex);
    return
  }

  dump(`Receive file rename:`, data.oldPath, "->", data.path)

  const normalizedOldPath = normalizePath(data.oldPath)
  const normalizedNewPath = normalizePath(data.path)

  // Check if there is an active file download session for this old path
  // 检查本地是否存在该旧路径的活跃下载会话
  let downloadingSession: FileDownloadSession | undefined
  for (const sess of plugin.fileDownloadSessions.values()) {
    if (normalizePath(sess.path) === normalizedOldPath) {
      downloadingSession = sess
      break
    }
  }

  if (downloadingSession) {
    dump(`Redirecting active file download session: ${downloadingSession.path} -> ${data.path}`)
    downloadingSession.path = data.path
    if (data.contentHash) {
      downloadingSession.contentHash = data.contentHash
    }
    // Session redirected, bypass the rename execution and skip RePush
    // 会话已成功重定向，直接跳过重命名执行和 RePush
    plugin.recordSyncCompleted('file', data.pageIndex)
    return
  }

  await plugin.lockManager.withLock(normalizedNewPath, async () => {
    const file = plugin.app.vault.getFileByPath(normalizedOldPath)
    if (file instanceof TFile) {
      plugin.addIgnoredFile(normalizedNewPath)
      plugin.addIgnoredFile(normalizedOldPath)

      // 记录新路径
      plugin.lastSyncPathRenamed.add(normalizedNewPath)

      try {
        const targetFile = plugin.app.vault.getFileByPath(normalizedNewPath)
        if (targetFile) {
          if (!(await waitForFileActivity(plugin))) return
          await vaultDelete(plugin.app.vault, targetFile)
        }

        if (!(await waitForFileActivity(plugin))) return
        await plugin.app.vault.rename(file, normalizedNewPath)

        if (data.mtime) {
          const renamedFile = plugin.app.vault.getFileByPath(normalizedNewPath)
          if (renamedFile instanceof TFile) {
            if (isLargeBinarySyncRisk(renamedFile.stat.size, plugin)) {
              dump(`Skip renamed binary mtime rewrite for large attachment (${describeBinarySyncLimit()} limit): ${normalizedNewPath}`, renamedFile.stat.size)
            } else {
              if (!(await waitForFileActivity(plugin))) return
              const content = await plugin.app.vault.readBinary(renamedFile)
              if (!(await waitForFileActivity(plugin))) return
              await plugin.app.vault.modifyBinary(renamedFile, content, { ...((data.ctime ?? 0) > 0 && { ctime: data.ctime }), ...((data.mtime ?? 0) > 0 && { mtime: data.mtime }) })
            }
          }
        }

        plugin.fileHashManager.removeFileHash(data.oldPath)
        const renamedFile = plugin.app.vault.getFileByPath(normalizedNewPath)
        plugin.fileHashManager.setFileHash(data.path, data.contentHash || "", renamedFile instanceof TFile ? renamedFile.stat.mtime : 0, renamedFile instanceof TFile ? renamedFile.stat.size : 0)

        // 更新同步时间
        if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
          plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
        }
      } finally {
        window.setTimeout(() => {
          plugin.removeIgnoredFile(normalizedNewPath)
          plugin.removeIgnoredFile(normalizedOldPath)
          plugin.lastSyncPathRenamed.delete(normalizedNewPath)
        }, 500);
      }
    } else {
      // 找不到旧文件...
      const targetFile = plugin.app.vault.getFileByPath(normalizedNewPath)
      if (targetFile instanceof TFile) {
        const sizeMatch = data.size === undefined || targetFile.stat.size === data.size
        if (sizeMatch) {
          if (isLargeBinarySyncRisk(targetFile.stat.size, plugin)) {
            dump(`Skip rename target hash for large attachment (${describeBinarySyncLimit()} limit): ${data.path}`, targetFile.stat.size)
            plugin.recordSyncCompleted('file', data.pageIndex)
            return
          }
          const localContentHash = await hashFileAsync(plugin.app, targetFile.path, plugin)
          if (localContentHash === data.contentHash) {
            dump(`Target attachment already exists and matches hash, skipping rename: ${data.path}`)
            plugin.fileHashManager.setFileHash(data.path, data.contentHash, targetFile.stat.mtime, targetFile.stat.size)
            plugin.recordSyncCompleted('file', data.pageIndex)
            return
          }
        }
      }

      dump(`Local attachment not found for rename, requesting RePush: ${data.oldPath} -> ${data.path}`)
      const rePushData = {
        vault: plugin.settings.vault,
        path: data.path,
        pathHash: data.pathHash,
      }
      void plugin.websocket.SendMessage("FileRePush", rePushData)
      if (data.contentHash) {
        const targetFile = plugin.app.vault.getFileByPath(normalizePath(data.path))
        plugin.fileHashManager.setFileHash(data.path, data.contentHash, targetFile instanceof TFile ? targetFile.stat.mtime : 0, targetFile instanceof TFile ? targetFile.stat.size : 0)
      }
    }
  }, { maxRetries: 10, retryInterval: 100 }).catch(e => {
    dumpError(`[FastSync] Failed to receiveFileSyncRename: ${normalizedOldPath} -> ${normalizedNewPath}`, e);
    if (!checkAndNotifyCaseConflict(e, data.path, plugin, 'FileRename')) {
      SyncLogManager.getInstance().addLog('receive', 'FileRename', e instanceof Error ? e.message : String(e), 'error', data.path);
    }
    plugin.fileSyncTasks.failed++
  });

  plugin.recordSyncCompleted('file', data.pageIndex)
}

/**
 * 完成文件下载
 */
const handleFileChunkDownloadComplete = async function (session: FileDownloadSession, plugin: FastSync) {
  if (!(await waitForFileActivity(plugin))) return
  const slotKey = session.initialSlotKey || `download_${session.path}`;
  try {
    if (isLargeBinarySyncRisk(session.size, plugin)) {
      dump(`Skip assembling large downloaded attachment (${describeBinarySyncLimit()} limit): ${session.path}`, session.size)
      await cleanupFileDownloadSession(plugin, session)
      return
    }
    // 逐片读入后立即写入目标缓冲区并释放分片引用，避免 chunks 数组与整份 buffer 同时驻留内存 (2x 峰值)
    // Write each chunk into the target buffer as it is read, then drop the reference immediately,
    // instead of accumulating a chunks array alongside the full assembled buffer (avoids the 2x peak).
    const completeFile = new Uint8Array(session.size)
    let offset = 0
    for (let i = 0; i < session.totalChunks; i++) {
      let chunk: ArrayBuffer | undefined;
      if (session.tempDir) {
        const chunkPath = normalizePath(`${session.tempDir}/${i}.bin`)
        if (!(await waitForFileActivity(plugin))) return
        if (await plugin.app.vault.adapter.exists(chunkPath)) {
          if (!(await waitForFileActivity(plugin))) return
          chunk = await plugin.app.vault.adapter.readBinary(chunkPath)
        }
      } else {
        chunk = session.chunks?.get(i)
      }

      if (!chunk) {
        await failFileDownloadSession(plugin, session, `Missing downloaded chunk ${i}`, false)
        return
      }
      if (offset + chunk.byteLength > completeFile.byteLength) {
        await failFileDownloadSession(plugin, session, `Downloaded file size mismatch: exceeds expected ${session.size}`, false)
        return
      }
      completeFile.set(new Uint8Array(chunk), offset)
      offset += chunk.byteLength
    }

    if (offset !== session.size) {
      await failFileDownloadSession(plugin, session, `Downloaded file size mismatch: got ${offset}, expected ${session.size}`, false)
      return
    }

    const normalizedPath = normalizePath(session.path)
    await plugin.lockManager.withLock(normalizedPath, async () => {
      plugin.addIgnoredFile(normalizedPath)
      try {
        const file = plugin.app.vault.getFileByPath(normalizedPath)
        if (file) {
          if (!(await waitForFileActivity(plugin))) return
          await plugin.app.vault.modifyBinary(file, completeFile.buffer, { ...(session.ctime > 0 && { ctime: session.ctime }), ...(session.mtime > 0 && { mtime: session.mtime }) })
        } else {
          const folder = normalizedPath.split("/").slice(0, -1).join("/")
          if (folder != "") {
            const dirExists = plugin.app.vault.getFolderByPath(folder)
            if (dirExists == null) {
              try {
                if (!(await waitForFileActivity(plugin))) return
                await plugin.app.vault.createFolder(folder)
              } catch (e) {
                // 并发竞争时只有一个调用成功，另一方忽略"已存在"错误
                // In concurrent race only one call succeeds; ignore "already exists" error
                if (!(await waitForFileActivity(plugin))) return
                if (!plugin.app.vault.getFolderByPath(folder)) throw e
              }
            }
          }
          if (!(await waitForFileActivity(plugin))) return
          await plugin.app.vault.createBinary(normalizedPath, completeFile.buffer, { ...(session.ctime > 0 && { ctime: session.ctime }), ...(session.mtime > 0 && { mtime: session.mtime }) })
        }
      } finally {
        window.setTimeout(() => {
          plugin.removeIgnoredFile(normalizedPath)
        }, 500);
      }

      if (Number(plugin.localStorageManager.getMetadata("lastFileSyncTime")) < session.lastTime) {
        plugin.localStorageManager.setMetadata("lastFileSyncTime", session.lastTime)
      }

      // 下载完成后自动计算哈希并更新缓存 (如果服务器传了内容哈希就直接使用，否则重新计算以兼容旧版本)
      let contentHash = session.contentHash
      if (!contentHash) {
        contentHash = await hashArrayBuffer(completeFile.buffer)
        dump(`Download complete: server missing hash, local calculated: ${session.path}`, contentHash)
      } else {
        dump(`Download complete: using server provided hash: ${session.path}`, contentHash)
      }
      plugin.fileHashManager.setFileHash(session.path, contentHash, session.mtime, session.size)
      // 记录同步后的 mtime
      plugin.lastSyncMtime.set(session.path, session.mtime)
      dump(`Download complete and hash updated for: ${session.path}`, contentHash)
    }, { maxRetries: 5, retryInterval: 100 });

    // 释放内存计数
    releaseSessionMemory(session)

    plugin.fileDownloadSessions.delete(session.sessionId)
    if (session.tempDir) await clearTempChunksDir(plugin, session.sessionId)
    plugin.downloadedFilesCount++
    plugin.progressTracker.recordDownloadComplete('file');
    plugin.recordSyncCompleted('file', session.pageIndex)
  } catch (e) {
    dumpError(`Error completing file download for ${session.path}`, e)
    if (!checkAndNotifyCaseConflict(e, session.path, plugin, 'FileDownload')) {
      SyncLogManager.getInstance().addOrUpdateLog({
        id: session.sessionId,
        type: 'receive',
        action: 'FileDownload',
        path: session.path,
        status: 'error',
        progress: session.totalChunks === 0 ? 0 : Math.floor((getCompletedDownloadChunks(session) / session.totalChunks) * 100),
        message: `Failed to complete download: ${formatDownloadError(e)}`
      });
    }
    await cleanupFileDownloadSession(plugin, session, true)
  } finally {
    plugin.concurrencyLimiter.releaseSlot(slotKey)
  }
}

// 收到 FileRenameAck，服务端确认后更新 hashManager（FIFO 出队）并更新 lastFileSyncTime
// Receive FileRenameAck, update hashManager after server confirmation (FIFO dequeue) and update lastFileSyncTime
export const receiveFileRenameAck = function (data: { lastTime?: number }, plugin: FastSync) {
  // 服务端确认重命名成功，FIFO 出队并更新 hashManager
  // Server confirmed rename success, dequeue FIFO and update hashManager
  const pending = plugin.pendingFileRenames.shift()
  if (pending) {
    const oldResult = plugin.incrementalScanManager?.acknowledge("file", pending.oldPath)
    const newResult = plugin.incrementalScanManager?.acknowledge("file", pending.newPath)
    if (oldResult !== "stale" && newResult !== "stale") {
      const file = plugin.app.vault.getFileByPath(normalizePath(pending.newPath))
      plugin.fileHashManager.setFileHash(pending.newPath, pending.contentHash, file?.stat.mtime || 0, file?.stat.size || 0)
      plugin.fileHashManager.removeFileHash(pending.oldPath)
    } else {
      dump(`FileRenameAck ignored as stale for ${pending.newPath}`)
    }
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
    dump(`FileRenameAck: lastFileSyncTime updated to`, data.lastTime)
  }
  plugin.concurrencyLimiter.releaseFifoSlot()
}

// 收到 FileUploadAck，将 pending hash 转移到正式 hashManager 并更新 lastFileSyncTime
// Receive FileUploadAck, move pending hash to formal hashManager and update lastFileSyncTime
export const receiveFileUploadAck = function (data: { lastTime?: number; path?: string; pathHash?: string }, plugin: FastSync) {
  const active = data.path ? activeUploadsMap.get(uploadPathKey(data.path)) : undefined;
  if (active && data.pathHash && active.descriptor.pathHash && data.pathHash !== active.descriptor.pathHash) {
    dump(`FileUploadAck ignored for stale path hash: ${data.path} (${data.pathHash})`);
    return;
  }
  if (data.path) {
    const ackKey = uploadPathKey(data.path)
    if (!plugin.syncState.pendingFileUploadAcks.delete(ackKey)) {
      dump(`FileUploadAck ignored as duplicate or stale: ${data.path}`)
      return
    }
  }
  if (active) {
    active.cancelled = true;
    active.retrying = false;
    active.retryResponseReceived = false;
    clearUploadRetryTimer(active);
    removeUploadStateIfCurrent(data.path!, active);
  }
  // 服务端确认上传成功，将 pending hash 转移到正式 hashManager
  // Server confirmed upload success, move pending hash to formal hashManager
  if (data.path) {
    const journalResult = plugin.incrementalScanManager?.acknowledge("file", data.path)
    const contentHash = plugin.pendingUploadHashes.get(data.path)
      ?? plugin.pendingUploadHashes.get(uploadPathKey(data.path))
    if (contentHash !== undefined && journalResult !== "stale") {
      const file = plugin.app.vault.getFileByPath(normalizePath(data.path))
      plugin.fileHashManager.setFileHash(data.path, contentHash, file?.stat.mtime || 0, file?.stat.size || 0)
      plugin.pendingUploadHashes.delete(data.path)
      plugin.localStorageManager.savePending('pendingUploadHashes', plugin.pendingUploadHashes)
    }
  }
  // 上传完成，清除断点续传 checkpoint
  // Upload complete, clear resume checkpoint
  const checkpointPathHash = data.pathHash || active?.descriptor.pathHash;
  if (checkpointPathHash) {
    const vaultName = plugin.app.vault.getName()
    try { plugin.app.saveLocalStorage(`fns-${vaultName}-uploadSession-${checkpointPathHash}`, null) } catch { /* ignore */ }
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
    dump(`FileUploadAck: lastFileSyncTime updated to`, data.lastTime)
  }
  if (data.path) {
    if (active) releaseUploadSlot(plugin, data.path, active)
    else plugin.concurrencyLimiter.releaseSlot(data.path)
  }
  // 查表归账所属下载页（NeedPush=FileUpload 驱动）；查不到说明是本地用户自发编辑触发的上传 Ack，走旧路径
  // Look up the owning download page (NeedPush=FileUpload-driven); a miss means a local
  // user-initiated edit triggered this upload Ack, falls back to the legacy path
  const pushPageIndex = data.path
    ? plugin.syncState.pendingFilePushPageIndex.get(data.path) ?? active?.pageIndex
    : undefined;
  if (data.path) plugin.syncState.pendingFilePushPageIndex.delete(data.path)
  if (active) {
    // The upload may receive its ACK before runUpload gets past waitForSlot.
    // Mark it recorded now so that the late run cannot count the same page a
    // second time when it observes the cancelled state.
    active.pageIndex = pushPageIndex ?? active.pageIndex;
    recordUploadTask(plugin, uploadMessageForState(active), active);
  } else {
    plugin.recordSyncCompleted('file', pushPageIndex)
  }
}

// 收到 FileDeleteAck，仅当路径仍在 pending set 中时才从 hashManager 移除
// Receive FileDeleteAck; only remove from hashManager if path is still pending
export const receiveFileDeleteAck = function (data: { lastTime?: number; path?: string }, plugin: FastSync) {
  if (data.path && plugin.pendingFileDeleteAcks.has(data.path)) {
    const journalResult = plugin.incrementalScanManager?.acknowledge("file", data.path)
    if (journalResult !== "stale") plugin.fileHashManager.removeFileHash(data.path)
    plugin.pendingFileDeleteAcks.delete(data.path)
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastFileSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastFileSyncTime", data.lastTime)
  }
  if (data.path) {
    plugin.concurrencyLimiter.releaseSlot(data.path)
  }
}

/**
 * 收到服务端 463 错误（上传附件会话不存在），清理该文件的活跃上传状态并增加完成计数
 */
export const receiveFileUploadSessionNotFound = function (sessionId: string, plugin: FastSync) {
  const active = sessionIdToUploadMap.get(sessionId)
  if (!active) {
    dump(`FileUploadSessionNotFound: Ignored stale session: ${sessionId}`)
    return
  }
  const path = active.descriptor.path
  const key = uploadPathKey(path)
  if (active.sessionId !== sessionId || activeUploadsMap.get(key) !== active) {
    sessionIdToUploadMap.delete(sessionId)
    dump(`FileUploadSessionNotFound: Ignored stale upload state for path: ${path} (${sessionId})`)
    return
  }
  if (active.retrying) {
    // The first 463 already moved this state to the retry map. Repeated 463s
    // for the consumed session must not release or complete it a second time.
    sessionIdToUploadMap.delete(sessionId)
    dump(`FileUploadSessionNotFound: Duplicate 463 ignored for path: ${path} (${sessionId})`)
    return
  }

  sessionIdToUploadMap.delete(sessionId)
  active.cancelled = true;
  active.retrying = true;
  active.retryResponseReceived = false;
  active.chunksSent = false;
  retryingUploadsMap.set(key, active);
  // Keep both the dirty hash and page ACK marker alive until the replacement
  // session is acknowledged or the bounded retry budget is exhausted.
  plugin.syncState.pendingFileUploadAcks.add(key);
  releaseUploadSlot(plugin, path, active)
  scheduleUploadSessionCheck(plugin, active)
  dump(`FileUploadSessionNotFound: scheduling FileUploadCheck retry for path: ${path} (${sessionId})`)
}
