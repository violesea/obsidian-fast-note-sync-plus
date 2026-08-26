import { TFile, TAbstractFile, normalizePath } from "obsidian";

import { ReceiveMessage, ReceiveMtimeMessage, ReceivePathMessage, SyncEndData } from "../utils/types";
import { hashContent, hashContentAsync, dump, dumpError, isPathExcluded, getSafeCtime, vaultDelete, checkAndNotifyCaseConflict, getPluginDir, showSyncNotice } from "../utils/helpers";
import { SyncLogManager } from "./sync_log_manager";
import type FastSync from "../../main";
import { waitForForeground } from "./background_activity_gate";
import { captureStableSnapshot, stableCaptureCoordinator } from "./stable_capture";
import { HttpApiService } from "../api/http_api_service";
import { decideWrite, shouldCheckPrecondition } from "./write_precondition";

const waitForNoteActivity = async (plugin: FastSync): Promise<boolean> => waitForForeground(plugin);

const readStableStat = async (plugin: FastSync, path: string) => {
  const stat = await plugin.app.vault.adapter.stat(path);
  if (!stat) return null;
  return { size: stat.size, mtime: stat.mtime, ctime: stat.ctime };
};

const stableCaptureKey = (plugin: FastSync, path: string): string => (
  `${plugin.settings.vault}:${path}`
);

/**
 * Park both sides of a diverged path instead of overwriting one of them.
 *
 * Reuses the same conflict-notes layout the receive path already writes, so a
 * conflict raised by the upload guard and a conflict raised by an incoming
 * push land in one place for the existing conflict UI to resolve.
 */
const parkUploadConflict = async function (
  plugin: FastSync,
  notePath: string,
  localContent: string,
  serverContent: string,
): Promise<void> {
  const adapter = plugin.app.vault.adapter;
  const conflictDir = `${getPluginDir(plugin)}/conflict-notes`;
  const safeName = notePath.replace(/\.md$/, "").replace(/[/\\]/g, "_");
  const pathHash = hashContent(notePath);

  // 与接收方向的冲突落盘同款纪律：每一次 Vault I/O 之前都过前台闸，
  // 避免在 iOS 后台切换时留下半写的备份。
  if (!(await waitForNoteActivity(plugin))) return;
  if (!(await adapter.exists(conflictDir))) await adapter.mkdir(conflictDir);
  if (!(await waitForNoteActivity(plugin))) return;
  await adapter.write(`${conflictDir}/${safeName}_${pathHash}.remote.md`, serverContent);
  const baseBackupPath = `${conflictDir}/${safeName}_${pathHash}.base.md`;
  if (!(await waitForNoteActivity(plugin))) return;
  if (!(await adapter.exists(baseBackupPath))) await adapter.write(baseBackupPath, localContent);

  plugin.syncState.conflictedPaths.add(notePath);
};

/**
 * Read the server's current hash for one path.
 *
 * Returns null for every failure mode on purpose. A null makes decideWrite
 * fail open, so an unreachable server degrades to the previous behaviour
 * rather than stalling the upload queue.
 */
const readServerNoteState = async function (
  plugin: FastSync,
  notePath: string,
): Promise<{ hash: string; content: string } | null> {
  try {
    const remote = await new HttpApiService(plugin).getNoteContent(notePath);
    if (!remote || typeof remote.contentHash !== "string" || remote.contentHash === "") return null;
    return { hash: String(remote.contentHash), content: remote.content };
  } catch (error) {
    dumpError(`[WritePrecondition] server state unreadable: ${notePath}`, error);
    return null;
  }
};

const EMPTY_NOTE_HASH = hashContent("");
const MAX_EMPTY_NOTE_REPAIRS_PER_ROUND = 20;

type NoteReceiveOptions = {
  recordCompletion?: boolean;
};

const resolveIncomingNoteContent = async (data: ReceiveMessage, plugin: FastSync): Promise<string> => {
  if (typeof data.content !== "string") {
    throw new Error(`Invalid note content payload: ${data.path}`);
  }

  const expectedHash = String(data.contentHash ?? "");
  // A missing hash is an old/invalid server payload that cannot be verified;
  // preserve the legacy behavior. When a hash is present, accept the payload
  // only if its content actually matches. This covers empty, truncated, and
  // otherwise incomplete WebSocket payloads through the existing HTTP path.
  if (expectedHash === "") {
    return data.content;
  }

  const payloadHash = await hashContentAsync(data.content, plugin);
  if (payloadHash === expectedHash) {
    return data.content;
  }

  dump(`[FastSync] Note payload hash mismatch (${payloadHash} != ${expectedHash}); fetching canonical content: ${data.path}`);
  const note = await plugin.api.getNoteContent(data.path);
  if (!note || typeof note.content !== "string") {
    throw new Error(`Canonical note fetch failed: ${data.path}`);
  }

  const fetchedHash = await hashContentAsync(note.content, plugin);
  if (fetchedHash !== expectedHash || String(note.contentHash ?? "") !== expectedHash) {
    throw new Error(`Canonical note hash mismatch: ${data.path}`);
  }

  dump(`[FastSync] Canonical note content recovered: ${data.path}`);
  return note.content;
};


/**
 * 笔记修改事件处理
 */
export const noteModify = async function (file: TAbstractFile, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!(file instanceof TFile)) return
  if (!file.path.endsWith(".md")) return
  if (eventEnter && plugin.isIgnoredFile(file.path)) return
  if (isPathExcluded(file.path, plugin)) return

  const initialBaseHash = plugin.fileHashManager.getPathHash(file.path)
  const initialCachedHash = plugin.fileHashManager.getValidHash(file.path, file.stat.mtime, file.stat.size, file.stat.ctime)
  const initialLastSyncMtime = plugin.lastSyncMtime.get(file.path)
  if (initialCachedHash !== null
    && ((initialCachedHash === initialBaseHash && initialLastSyncMtime !== undefined && initialLastSyncMtime === file.stat.mtime)
      || plugin.pendingNoteModifies.get(file.path) === initialCachedHash)) {
    dump(`Note modify intercepted (stable cache match): ${file.path}`)
    plugin.incrementalScanManager?.markSent("note", file.path)
    plugin.incrementalScanManager?.acknowledge("note", file.path)
    return
  }

  const capture = await stableCaptureCoordinator.capture(
    stableCaptureKey(plugin, file.path),
    () => captureStableSnapshot({
      stat: () => readStableStat(plugin, file.path),
      read: () => plugin.app.vault.read(file),
      hash: (content) => hashContentAsync(content ?? "", plugin),
    }),
  );
  if (!capture || typeof capture.value !== "string") {
    dump(`[StableCapture] Note changed during quiet window; discarded: ${file.path}`)
    return;
  }
  // Narrow once here: the guard above does not survive the closure boundary,
  // so the captured text must be bound to a string before the lock body.
  const capturedContent: string = capture.value;

  await plugin.lockManager.withLock(file.path, async () => {
    plugin.addIgnoredFile(file.path)

    try {
      const baseHash = plugin.fileHashManager.getPathHash(file.path)
      const lastSyncMtime = plugin.lastSyncMtime.get(file.path)
      const content = capturedContent;
      const contentHash = capture.hash;
      const stableStat = capture.stat;

      if (plugin.pendingNoteModifies.get(file.path) === contentHash) {
        dump(`Note modify intercepted (pending hash match): ${file.path}`)
        plugin.incrementalScanManager?.markSent("note", file.path)
        plugin.incrementalScanManager?.acknowledge("note", file.path)
        return
      }

      if (contentHash === baseHash && lastSyncMtime !== undefined && lastSyncMtime === stableStat.mtime) {
        dump(`Note modify intercepted (stable capture matches baseline): ${file.path}`)
        plugin.incrementalScanManager?.markSent("note", file.path)
        plugin.incrementalScanManager?.acknowledge("note", file.path)
        return
      }

      // M7 写入乐观锁：服务端自本设备上次 ACK 之后动过，且本地内容与服务端不同，
      // 说明两侧都改了。这种情况绝不覆盖，两份都留住交给冲突 UI。
      // 服务端读不到时 decideWrite 会 fail open，回到本次修复之前的行为。
      if (shouldCheckPrecondition({
        enabled: plugin.settings.writePreconditionEnabled !== false,
        baseHash,
        localHash: contentHash,
      })) {
        const remote = await readServerNoteState(plugin, file.path);
        const decision = decideWrite({ localHash: contentHash, baseHash, serverHash: remote?.hash ?? null });
        if (decision.kind === "conflict" && remote) {
          dump(`[WritePrecondition] conflict, upload withheld: ${file.path} base=${baseHash} server=${remote.hash} local=${contentHash}`)
          await parkUploadConflict(plugin, file.path, content, remote.content)
          showSyncNotice(`同步冲突，未覆盖服务端：${file.path}`, 10000)
          plugin.incrementalScanManager?.markSent("note", file.path)
          plugin.incrementalScanManager?.acknowledge("note", file.path)
          return
        }
        if (decision.kind === "skip" && remote) {
          // 服务端当前就持有这份内容，是直接读回来的服务端状态，不是本地自证，
          // 因此按 ACK 同款方式推进基线（INV-2）。
          dump(`[WritePrecondition] server already holds this content: ${file.path}`)
          plugin.fileHashManager.setFileHash(file.path, contentHash, stableStat.mtime, stableStat.size)
          plugin.incrementalScanManager?.markSent("note", file.path)
          plugin.incrementalScanManager?.acknowledge("note", file.path)
          return
        }
        if (decision.reason === "precondition-unavailable") {
          dump(`[WritePrecondition] server state unavailable, proceeding without guard: ${file.path}`)
        }
      }

      const data = {
        vault: plugin.settings.vault,
        ctime: stableStat.ctime ?? getSafeCtime(file.stat),
        mtime: stableStat.mtime,
        path: file.path,
        pathHash: hashContent(file.path),
        content: content,
        contentHash: contentHash,
        // 始终传递 baseHash 信息，如果不可用则标记 baseHashMissing
        ...(baseHash !== null ? { baseHash } : { baseHashMissing: true }),
      }
      // 将 hash 暂存到 pending map，等待服务端 NoteModifyAck 后再写入 hashManager
      // Temporarily store hash in pending map, update hashManager only after server NoteModifyAck
      if (contentHash != baseHash) {
        // 新建操作覆盖删除意图，清除 pending 防止晚到的 Ack 错误删除新文件 hash
        // New create supersedes delete intent; clear pending to prevent stale Ack from removing new hash
        plugin.pendingNoteDeleteAcks.delete(file.path)
        plugin.pendingNoteModifies.set(file.path, contentHash)
        plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
      }
      await plugin.concurrencyLimiter.waitForSlot(file.path)
      plugin.incrementalScanManager?.markSent("note", file.path)
      void plugin.websocket.SendMessage("NoteModify", data).then((result) => {
        if (result !== "sent") {
          plugin.concurrencyLimiter.releaseSlot(file.path)
          dump(`[FastSync] NoteModify was not sent; retaining pending hash for retry: ${file.path}`)
        }
      }).catch((error) => {
        plugin.concurrencyLimiter.releaseSlot(file.path)
        dumpError(`[FastSync] NoteModify send failed; retaining pending hash: ${file.path}`, error)
      })
      dump(`Note modify send`, data.path, data.contentHash, data.mtime, data.pathHash)
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 5, retryInterval: 50 });
}

/**
 * 笔记删除事件处理
 */
export const noteDelete = async function (file: TAbstractFile, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!(file instanceof TFile)) return
  if (!file.path.endsWith(".md")) return
  if (eventEnter && plugin.isIgnoredFile(file.path)) return
  if (isPathExcluded(file.path, plugin)) return

  // --- 新增：删除拦截 ---
  if (plugin.lastSyncPathDeleted.has(file.path)) {
    dump(`Note delete intercepted: ${file.path}`)
    return
  }

  await plugin.lockManager.withLock(file.path, async () => {
    // 清理可能存在的待确认上传记录，避免 pending map 内存泄漏
    // Clean up any pending note modify record to avoid memory leak
    plugin.pendingNoteModifies.delete(file.path)
    plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
    plugin.addIgnoredFile(file.path)
    try {
      const data = {
        vault: plugin.settings.vault,
        path: file.path,
        pathHash: hashContent(file.path),
      }
      await plugin.concurrencyLimiter.waitForSlot(file.path)
      void plugin.websocket.SendMessage("NoteDelete", data, undefined, () => {
        // 消息真正写入 TCP 缓冲区后加入 pending set，等待 NoteDeleteAck 再删 hash
        // Add to pending set only after message is actually buffered; remove hash only on NoteDeleteAck
        plugin.incrementalScanManager?.markSent("note", file.path)
        plugin.pendingNoteDeleteAcks.add(file.path)
      }).then((result) => {
        if (result !== "sent") plugin.concurrencyLimiter.releaseSlot(file.path)
      }).catch((error) => {
        plugin.concurrencyLimiter.releaseSlot(file.path)
        dumpError(`[FastSync] NoteDelete send failed: ${file.path}`, error)
      })

      dump(`Note delete send`, file.path)
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 3, retryInterval: 50 });
}

/**
 * 按路径字符串发送笔记删除消息（用于无法获取 TFile 对象的场景，如 rename 后旧路径已不存在）
 * Send note delete message by path string (for scenarios where TFile object is unavailable, e.g., old path after rename)
 */
export const noteDeleteByPath = async function (filePath: string, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!filePath.endsWith(".md")) return
  if (isPathExcluded(filePath, plugin)) return
  if (plugin.lastSyncPathDeleted.has(filePath)) return

  await plugin.lockManager.withLock(filePath, async () => {
    // 清理可能存在的待确认上传记录，避免 pending map 内存泄漏
    // Clean up any pending note modify record to avoid memory leak
    plugin.pendingNoteModifies.delete(filePath)
    plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
    plugin.addIgnoredFile(filePath)
    try {
      await plugin.concurrencyLimiter.waitForSlot(filePath)
      void plugin.websocket.SendMessage("NoteDelete", {
        vault: plugin.settings.vault,
        path: filePath,
        pathHash: hashContent(filePath),
      }, undefined, () => {
        // 消息真正写入 TCP 缓冲区后加入 pending set，等待 NoteDeleteAck 再删 hash
        // Add to pending set only after message is actually buffered; remove hash only on NoteDeleteAck
        plugin.incrementalScanManager?.markSent("note", filePath)
        plugin.pendingNoteDeleteAcks.add(filePath)
      }).then((result) => {
        if (result !== "sent") plugin.concurrencyLimiter.releaseSlot(filePath)
      }).catch((error) => {
        plugin.concurrencyLimiter.releaseSlot(filePath)
        dumpError(`[FastSync] NoteDelete send failed: ${filePath}`, error)
      })
      dump(`Note delete by path send`, filePath)
    } finally {
      plugin.removeIgnoredFile(filePath)
    }
  }, { maxRetries: 3, retryInterval: 50 });
}

/**
 * 笔记重命名事件处理
 */
export const noteRename = async function (file: TAbstractFile, oldfile: string, plugin: FastSync, eventEnter: boolean = false) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false || plugin.settings.readonlySyncEnabled) return
  if (!(file instanceof TFile)) return
  if (!file.path.endsWith(".md")) return
  if (eventEnter && plugin.isIgnoredFile(file.path)) return
  const newExcluded = isPathExcluded(file.path, plugin)
  const oldExcluded = isPathExcluded(oldfile, plugin)

  // Cross-exclusion-boundary rename handling
  // 跨排除边界重命名处理
  if (newExcluded && !oldExcluded) {
    // Moving from normal folder to excluded folder: delete old path on server
    // 从正常文件夹移至排除文件夹：删除服务端旧路径
    void noteDeleteByPath(oldfile, plugin)
    return
  }
  if (!newExcluded && oldExcluded) {
    // Moving from excluded folder to normal folder: create new note on server
    // 从排除文件夹移至正常文件夹：在服务端创建新笔记
    void noteModify(file, plugin, true)
    return
  }
  if (newExcluded && oldExcluded) {
    // Both paths excluded: do nothing
    // 两个路径均被排除：不处理
    return
  }

  // --- 新增：重命名拦截 ---
  if (plugin.lastSyncPathRenamed.has(file.path)) {
    dump(`Note rename intercepted: ${file.path}`)
    return
  }

  // 重命名涉及两个路径，我们锁定新路径，旧路径由调用方或原子性保证
  await plugin.lockManager.withLock(file.path, async () => {
    plugin.addIgnoredFile(file.path)
    try {
      let contentHash = plugin.fileHashManager.getPathHash(oldfile)
      if (contentHash == null) {
        const content: string = await plugin.app.vault.read(file)
        contentHash = await hashContentAsync(content, plugin)
      }

      const data = {
        vault: plugin.settings.vault,
        path: file.path,
        pathHash: hashContent(file.path),
        oldPath: oldfile,
        oldPathHash: hashContent(oldfile),
      }

      // 将重命名信息存入 Map（key 为 newPath），等待服务端 NoteRenameAck 按 path 精确匹配后再更新 hashManager
      // Store rename info in Map (keyed by newPath), update hashManager only after server NoteRenameAck matches by path
      plugin.pendingNoteRenames.set(file.path, { oldPath: oldfile, newPath: file.path, contentHash })
      await plugin.concurrencyLimiter.waitForSlot(file.path, true)
      plugin.incrementalScanManager?.markSent("note", oldfile)
      plugin.incrementalScanManager?.markSent("note", file.path)
      void plugin.websocket.SendMessage("NoteRename", data).then((result) => {
        if (result !== "sent") plugin.concurrencyLimiter.releaseFifoSlot()
      }).catch((error) => {
        plugin.concurrencyLimiter.releaseFifoSlot()
        dumpError(`[FastSync] NoteRename send failed: ${file.path}`, error)
      })
      dump(`Note rename send`, data.path, data.pathHash)
    } finally {
      plugin.removeIgnoredFile(file.path)
    }
  }, { maxRetries: 5, retryInterval: 50 });
}

/**
 * 接收服务端笔记修改通知
 */
export const receiveNoteSyncModify = async function (data: ReceiveMessage, plugin: FastSync, options: NoteReceiveOptions = {}) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  dump(`Receive note modify:`, data.path, data.contentHash, data.mtime, data.pathHash)

  const normalizedPath = normalizePath(data.path)
  let resolvedContent = data.content
  let processed = false

  try {
    resolvedContent = await resolveIncomingNoteContent(data, plugin)
    await plugin.lockManager.withLock(normalizedPath, async () => {
      const file = plugin.app.vault.getFileByPath(normalizedPath)
      plugin.addIgnoredFile(normalizedPath)
      try {
        if (file) {
          // Fail-safe：写盘前检查本地是否有未推送的编辑，避免服务端推送盲覆盖用户刚做的改动
          // Fail-safe: before overwriting, check for unsynced local edits so a server push
          // doesn't blindly clobber changes the user just made
          const hasPendingLocalEdit = plugin.pendingNoteModifies.has(data.path)
          let hasDivergedSinceLastSync = false
          if (!hasPendingLocalEdit) {
            const knownSyncMtime = plugin.lastSyncMtime.get(data.path)
            const localMtimeIsNewer = knownSyncMtime !== undefined && file.stat.mtime > knownSyncMtime
            if (localMtimeIsNewer) {
              // mtime 已比上次同步记录的新，进一步用内容哈希确认本地内容是否真的偏离了已知基准
              // mtime is newer than what we last synced; confirm with content hash whether
              // local content actually diverged from the known baseline
              const knownBaseHash = plugin.fileHashManager.getPathHash(normalizedPath)
              let localContentHash = plugin.fileHashManager.getValidHash(normalizedPath, file.stat.mtime, file.stat.size, file.stat.ctime)
              if (localContentHash === null) {
                const localContent = await plugin.app.vault.read(file)
                localContentHash = await hashContentAsync(localContent, plugin)
              }
              hasDivergedSinceLastSync = localContentHash !== knownBaseHash
            }
          }

          if (hasPendingLocalEdit || hasDivergedSinceLastSync || plugin.syncState.conflictedPaths.has(normalizedPath)) {
            dump(`[FastSync] Skip overwrite, local unsynced edit detected: ${normalizedPath}`)

            // 如果存在未同步的本地修改或已在冲突列表中，将服务端最新推送的内容写入/更新到远端备份文件 xxx.remote.md
            // If local unsynced edit exists or path is in conflict state, write/update the server's latest content into xxx.remote.md
            try {
              const adapter = plugin.app.vault.adapter;
              const conflictDir = `${getPluginDir(plugin)}/conflict-notes`;
              const safeName = normalizedPath.replace(/\.md$/, "").replace(/[/\\]/g, "_");
              const pathHash = hashContent(normalizedPath);
              const baseBackupPath = `${conflictDir}/${safeName}_${pathHash}.base.md`;
              const remoteBackupPath = `${conflictDir}/${safeName}_${pathHash}.remote.md`;

              if (!(await waitForNoteActivity(plugin))) return
              if (!(await adapter.exists(conflictDir))) {
                if (!(await waitForNoteActivity(plugin))) return
                await adapter.mkdir(conflictDir);
              }

              // 写入服务端最新推送的内容至 remote 备份文件
              if (!(await waitForNoteActivity(plugin))) return
              await adapter.write(remoteBackupPath, resolvedContent);

              // 若 base 备份不存在，使用当前本地内容建立初始 base 备份
              if (!(await waitForNoteActivity(plugin))) return
              if (!(await adapter.exists(baseBackupPath))) {
                if (!(await waitForNoteActivity(plugin))) return
                const localContent = await plugin.app.vault.read(file);
                if (!(await waitForNoteActivity(plugin))) return
                await adapter.write(baseBackupPath, localContent);
              }

              dump(`[FastSync] Updated remote backup file for conflict: ${remoteBackupPath}`);
            } catch (err) {
              dumpError(`[FastSync] Failed to update remote backup file: ${normalizedPath}`, err);
            }

            // 保持在冲突列表中并刷新状态栏
            plugin.syncState.conflictedPaths.add(normalizedPath);
            plugin.syncState.newConflictedPathsThisRound.add(normalizedPath);
            plugin.localStorageManager.setConflictedPaths(plugin.syncState.conflictedPaths);
            plugin.statusBarManager.updateConflictBadge();

            SyncLogManager.getInstance().addLog('receive', 'NoteModifyConflict', `本地存在未同步的改动，跳过服务端覆盖，等待下一轮同步处理冲突: ${normalizedPath}`, 'cancelled', data.path)
            processed = true
            return
          }

          if (!(await waitForNoteActivity(plugin))) return
          await plugin.app.vault.modify(file, resolvedContent, { ...(data.ctime > 0 && { ctime: data.ctime }), ...(data.mtime > 0 && { mtime: data.mtime }) })
        } else {
          const folder = normalizedPath.split("/").slice(0, -1).join("/")
          if (folder != "") {
            const dirExists = plugin.app.vault.getFolderByPath(folder)
            if (dirExists == null) {
              try {
                if (!(await waitForNoteActivity(plugin))) return
                await plugin.app.vault.createFolder(folder)
              } catch (e) {
                // 并发竞争时只有一个调用成功，另一方忽略"已存在"错误
                // In concurrent race only one call succeeds; ignore "already exists" error
                if (!(await waitForNoteActivity(plugin))) return
                if (!plugin.app.vault.getFolderByPath(folder)) throw e
              }
            }
          }
          if (!(await waitForNoteActivity(plugin))) return
          await plugin.app.vault.create(normalizedPath, resolvedContent, { ...(data.ctime > 0 && { ctime: data.ctime }), ...(data.mtime > 0 && { mtime: data.mtime }) })
        }
        if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
          plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
        }

        // Read back the materialized file before advancing the local hash or
        // page ACK. A process kill or adapter failure after the write must
        // remain retryable instead of becoming a false synced baseline.
        const updatedFile = plugin.app.vault.getFileByPath(normalizedPath);
        if (!(await waitForNoteActivity(plugin))) return
        if (!(updatedFile instanceof TFile)) {
          throw new Error(`Materialized note is missing after write: ${normalizedPath}`)
        }
        const writtenContent = await plugin.app.vault.read(updatedFile)
        const writtenHash = await hashContentAsync(writtenContent, plugin)
        if (String(data.contentHash ?? "") !== "" && writtenHash !== String(data.contentHash)) {
          throw new Error(`Materialized note hash mismatch: ${normalizedPath}`)
        }
        plugin.fileHashManager.setFileHash(data.path, data.contentHash, data.mtime, updatedFile.stat.size)
        // 记录同步后的 mtime 用于拦截
        plugin.lastSyncMtime.set(data.path, data.mtime)
        // 服务端版本已覆盖本地，清理 pending 避免增量过滤器旁路导致该笔记无限重传
        // Server version overrides local; clear pending to avoid incremental filter bypass loop
        plugin.pendingNoteModifies.delete(data.path)
        plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
        // 服务端推送新内容说明该路径已被创建/更新，清理可能残留 de deleteAck pending
        // Server push means path was created/updated; clear any stale deleteAck pending
        plugin.pendingNoteDeleteAcks.delete(data.path)
        processed = true
      } finally {
        window.setTimeout(() => {
          plugin.removeIgnoredFile(normalizedPath)
        }, 500);
      }
    }, { maxRetries: 5, retryInterval: 100 });
  } catch (e) {
    dumpError(`[FastSync] Failed to receiveNoteSyncModify: ${normalizedPath}`, e);
    if (!checkAndNotifyCaseConflict(e, data.path, plugin, 'NoteModify')) {
      SyncLogManager.getInstance().addLog('receive', 'NoteModify', e instanceof Error ? e.message : String(e), 'error', data.path);
    }
    plugin.noteSyncTasks.failed++
  } finally {
    if (options.recordCompletion !== false && processed) {
      plugin.recordSyncCompleted('note', data.pageIndex)
    }
  }
}

/**
 * Repair the narrow corruption signature produced by an interrupted remote
 * note write: a zero-byte local file whose cached local and server hashes are
 * both non-empty and identical. This scans the in-memory hash index only.
 */
export const repairSuspiciousEmptyNotes = async function (plugin: FastSync): Promise<number> {
  if (!plugin.api?.getNoteContent || !plugin.fileHashManager?.getZeroLengthNoteHashEntries) return 0

  let repaired = 0
  const candidates = plugin.fileHashManager.getZeroLengthNoteHashEntries()
  for (const candidate of candidates) {
    if (repaired >= MAX_EMPTY_NOTE_REPAIRS_PER_ROUND) break
    if (candidate.hash === EMPTY_NOTE_HASH || !candidate.path.endsWith(".md")) continue

    const file = plugin.app.vault.getFileByPath(candidate.path)
    if (!(file instanceof TFile) || file.stat.size !== 0 || file.stat.mtime !== candidate.mtime) continue
    if (plugin.fileHashManager.getPathHash(candidate.path) !== candidate.hash) continue
    if (plugin.pendingNoteModifies.has(candidate.path) || plugin.syncState.conflictedPaths.has(candidate.path)) continue

    try {
      const note = await plugin.api.getNoteContent(candidate.path)
      if (!note || typeof note.content !== "string" || String(note.contentHash ?? "") !== candidate.hash) {
        throw new Error(`Suspicious empty note recovery hash mismatch: ${candidate.path}`)
      }

      await receiveNoteSyncModify({
        vault: plugin.settings.vault,
        path: candidate.path,
        pathHash: note.pathHash || hashContent(candidate.path),
        action: "modify",
        content: note.content,
        contentHash: candidate.hash,
        ctime: note.ctime,
        mtime: note.mtime,
        lastTime: note.lastTime,
      }, plugin, { recordCompletion: false })

      const restored = plugin.app.vault.getFileByPath(candidate.path)
      if (!(restored instanceof TFile)) throw new Error(`Recovered note is missing: ${candidate.path}`)
      const restoredHash = await hashContentAsync(await plugin.app.vault.read(restored), plugin)
      if (restoredHash !== candidate.hash) throw new Error(`Recovered note verification failed: ${candidate.path}`)
      repaired++
      dump(`[FastSync] Repaired suspicious empty note: ${candidate.path}`)
    } catch (error) {
      dumpError(`[FastSync] Suspicious empty note repair deferred: ${candidate.path}`, error)
    }
  }
  return repaired
}

/**
 * 接收服务端请求上传笔记
 */
export const receiveNoteUpload = async function (data: ReceivePathMessage, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  if (plugin.settings.readonlySyncEnabled) {
    dump(`Read-only mode: Intercepted note upload request for ${data.path}`)
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  dump(`Receive note need push:`, data.path)
  if (!data.path.endsWith(".md")) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  const file = plugin.app.vault.getFileByPath(normalizePath(data.path))
  if (!file) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }

  // NeedPush 驱动的上传-回执往返：Ack（receiveNoteModifyAck）本身不带 pageIndex，
  // 在此按 path 记下所属页，供 Ack 到达时查表归账（见 sync_state.ts pendingNotePushPageIndex 注释）
  // NeedPush-driven upload/ack round trip: the Ack (receiveNoteModifyAck) carries no pageIndex;
  // record the owning page by path here so the Ack can look it up on arrival (see sync_state.ts
  // pendingNotePushPageIndex comment)
  if (data.pageIndex !== undefined) {
    plugin.syncState.pendingNotePushPageIndex.set(file.path, data.pageIndex)
  }

  plugin.addIgnoredFile(file.path)

  const baseHash = plugin.fileHashManager.getPathHash(file.path)
  // 尝试从缓存获取 (Try to get from cache)
  let contentHash = plugin.fileHashManager.getValidHash(file.path, file.stat.mtime, file.stat.size, file.stat.ctime);
  const content = await plugin.app.vault.read(file);
  if (contentHash === null) contentHash = await hashContentAsync(content, plugin);

  if (content.length === 0) {
    dump(`Empty note upload: ${data.path}`);
  }

  const sendData = {
    vault: plugin.settings.vault,
    ctime: getSafeCtime(file.stat),
    mtime: file.stat.mtime,
    path: file.path,
    pathHash: hashContent(file.path),
    content: content,
    contentHash: contentHash,
    // 始终传递 baseHash 信息，如果不可用则标记 baseHashMissing
    ...(baseHash !== null ? { baseHash } : { baseHashMissing: true }),
  }
  // 将 hash 写入 pending map，等待 NoteModifyAck 确认后再写 hashManager
  // 若此路径已有旧 pending（来自中断的 noteModify），覆盖为最新 hash
  // Store hash in pending map; hashManager is written only after NoteModifyAck arrives.
  // Overwrites any stale pending entry left by a previously interrupted noteModify.
  plugin.pendingNoteModifies.set(file.path, contentHash)
  plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
  await plugin.concurrencyLimiter.waitForSlot(file.path)
  const sendResult = await plugin.websocket.SendMessage("NoteModify", sendData, undefined, () => {
    plugin.removeIgnoredFile(file.path)
  }, (data as ReceivePathMessage & { context?: string }).context)
  if (sendResult !== "sent") {
    plugin.removeIgnoredFile(file.path)
    plugin.syncState.pendingNotePushPageIndex.delete(file.path)
    plugin.concurrencyLimiter.releaseSlot(file.path)
    dump(`[FastSync] NeedPush NoteModify was not sent; retaining pending hash for retry: ${file.path}`)
    return
  }
  dump(`Note modify send`, sendData.path, sendData.contentHash, sendData.mtime, sendData.pathHash)
}

/**
 * 接收服务端笔记元数据(mtime)更新通知
 */
export const receiveNoteSyncMtime = async function (data: ReceiveMtimeMessage, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  dump(`Receive note sync mtime:`, data.path, data.mtime)

  const normalizedPath = normalizePath(data.path)

  try {
    await plugin.lockManager.withLock(normalizedPath, async () => {
      const file = plugin.app.vault.getFileByPath(normalizedPath)
      if (file) {
        if (!(await waitForNoteActivity(plugin))) return
        const content: string = await plugin.app.vault.read(file)
        plugin.addIgnoredFile(normalizedPath)
        try {
          if (!(await waitForNoteActivity(plugin))) return
          await plugin.app.vault.modify(file, content, { ...(data.ctime > 0 && { ctime: data.ctime }), ...(data.mtime > 0 && { mtime: data.mtime }) })
          // 记录 mtime
          plugin.lastSyncMtime.set(data.path, data.mtime)
          // 服务端走 UpdateMtime 说明内容 hash 与客户端发送的一致，提交 pending hash 到 hashManager
          // Server UpdateMtime means content hash matches what client sent; commit pending hash to hashManager
          const pendingHash = plugin.pendingNoteModifies.get(data.path)
          if (pendingHash !== undefined) {
            plugin.fileHashManager.setFileHash(data.path, pendingHash, data.mtime, file.stat.size)
            plugin.pendingNoteModifies.delete(data.path)
            plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
          }
          // 更新同步时间
          if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
            plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
          }
        } finally {
          plugin.removeIgnoredFile(normalizedPath)
        }
      }
    }, { maxRetries: 5, retryInterval: 100 });
  } catch (e) {
    dumpError(`[FastSync] Failed to receiveNoteSyncMtime: ${normalizedPath}`, e);
    if (!checkAndNotifyCaseConflict(e, data.path, plugin, 'NoteMtime')) {
      SyncLogManager.getInstance().addLog('receive', 'NoteMtime', e instanceof Error ? e.message : String(e), 'error', data.path);
    }
    plugin.noteSyncTasks.failed++
  } finally {
    plugin.recordSyncCompleted('note', data.pageIndex)
  }
}

/**
 * 接收服务端笔记删除通知
 */
export const receiveNoteSyncDelete = async function (data: ReceiveMessage, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  if (isPathExcluded(data.path, plugin)) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }
  dump(`Receive note delete:`, data.path, data.mtime, data.pathHash)
  const normalizedPath = normalizePath(data.path)

  try {
    await plugin.lockManager.withLock(normalizedPath, async () => {
      const file = plugin.app.vault.getFileByPath(normalizedPath)
      if (file instanceof TFile) {
        plugin.addIgnoredFile(normalizedPath)
        // 记录待删除路径，用于拦截本地删除事件
        plugin.lastSyncPathDeleted.add(normalizedPath)
        try {
          if (!(await waitForNoteActivity(plugin))) return
          await vaultDelete(plugin.app.vault, file)
          // 服务端推送删除,从哈希表中移除
          plugin.fileHashManager.removeFileHash(normalizedPath)
          plugin.lastSyncMtime.delete(normalizedPath)
          // 清理 pending，避免已删除路径的 pending 条目泄漏
          // Clean up pending to prevent memory leak for deleted path
          plugin.pendingNoteModifies.delete(normalizedPath)
          plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
          // 更新同步时间
          if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
            plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
          }
        } finally {
          // 延时 500ms 清理拦截集合，确保本地事件已被处理
          window.setTimeout(() => {
            plugin.removeIgnoredFile(normalizedPath)
            plugin.lastSyncPathDeleted.delete(normalizedPath)
          }, 500);
        }
      }
    }, { maxRetries: 5, retryInterval: 100 });
  } catch (e) {
    dumpError(`[FastSync] Failed to receiveNoteSyncDelete: ${normalizedPath}`, e);
    SyncLogManager.getInstance().addLog('receive', 'NoteDelete', e instanceof Error ? e.message : String(e), 'error', data.path);
    plugin.noteSyncTasks.failed++
  } finally {
    plugin.recordSyncCompleted('note', data.pageIndex)
  }
}

/**
 * 接收笔记同步结束通知
 */
export const receiveNoteSyncEnd = async function (data: unknown, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  dump(`Receive note end:`, data)

  const syncData = data as SyncEndData
  // 更新任务统计信息，用于进度条计算 (Update task stats for progress bar)
  plugin.noteSyncTasks.needUpload = syncData.needUploadCount || 0
  plugin.noteSyncTasks.needModify = syncData.needModifyCount || 0
  plugin.noteSyncTasks.needSyncMtime = syncData.needSyncMtimeCount || 0
  plugin.noteSyncTasks.needDelete = syncData.needDeleteCount || 0

  // 无条件更新 lastNoteSyncTime，确保包含服务端本轮同步后的所有异步操作（如 SyncResourceFID）
  // Unconditionally update lastNoteSyncTime to cover all async server-side ops after this sync round (e.g., SyncResourceFID)
  plugin.localStorageManager.setMetadata("lastNoteSyncTime", syncData.lastTime)
  plugin.syncTypeCompleteCount++
}

/**
 * 接收服务端笔记重命名通知
 */
export const receiveNoteSyncRename = async function (data: { path: string, oldPath: string, contentHash: string, mtime?: number, ctime?: number, lastTime?: number, pathHash?: string, pageIndex?: number }, plugin: FastSync) {
  if (!(await waitForNoteActivity(plugin))) return
  if (plugin.settings.syncEnabled == false) return
  if (isPathExcluded(data.path, plugin) || isPathExcluded(data.oldPath, plugin)) {
    plugin.recordSyncCompleted('note', data.pageIndex)
    return
  }

  dump(`Receive note rename:`, data.oldPath, "->", data.path)

  const normalizedOldPath = normalizePath(data.oldPath)
  const normalizedNewPath = normalizePath(data.path)

  try {
    // 对于重命名，我们需要确新路径不被占用。旧路径通常正在被移动，所以锁定新路径。
    await plugin.lockManager.withLock(normalizedNewPath, async () => {
      const file = plugin.app.vault.getFileByPath(normalizedOldPath)
      if (file instanceof TFile) {
        plugin.addIgnoredFile(normalizedNewPath)
        plugin.addIgnoredFile(normalizedOldPath)

        // 记录重命名后的新路径，用于拦截本地事件
        plugin.lastSyncPathRenamed.add(normalizedNewPath)

        try {
          // 如果目标路径已存在文件，先尝试删除
          const targetFile = plugin.app.vault.getFileByPath(normalizedNewPath)
          if (targetFile) {
            if (!(await waitForNoteActivity(plugin))) return
            await vaultDelete(plugin.app.vault, targetFile)
          }

          if (!(await waitForNoteActivity(plugin))) return
          await plugin.app.vault.rename(file, normalizedNewPath)

          // 更新元数据
          if (data.mtime) {
            const renamedFile = plugin.app.vault.getFileByPath(normalizedNewPath)
            if (renamedFile instanceof TFile) {
              if (!(await waitForNoteActivity(plugin))) return
              const content = await plugin.app.vault.read(renamedFile)
              const options: { ctime?: number; mtime?: number } = {};
              if (data.ctime && data.ctime > 0) options.ctime = data.ctime;
              if (data.mtime && data.mtime > 0) options.mtime = data.mtime;
              if (!(await waitForNoteActivity(plugin))) return
              await plugin.app.vault.modify(renamedFile, content, options);
            }
          }

          // 更新哈希表：移除旧路径，添加新路径
          plugin.fileHashManager.removeFileHash(data.oldPath)
          const renamedFile = plugin.app.vault.getFileByPath(normalizedNewPath)
          plugin.fileHashManager.setFileHash(data.path, data.contentHash, data.mtime || (renamedFile instanceof TFile ? renamedFile.stat.mtime : 0), renamedFile instanceof TFile ? renamedFile.stat.size : 0)

          // 更新同步时间
          if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
            plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
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
          if (!(await waitForNoteActivity(plugin))) return
          const content = await plugin.app.vault.read(targetFile)
          const localContentHash = await hashContentAsync(content, plugin)
          if (localContentHash === data.contentHash) {
            dump(`Target file already exists and matches hash, skipping rename: ${data.path}`)
            plugin.fileHashManager.setFileHash(data.path, data.contentHash)
            return
          }
        }

        dump(`Local file not found for rename, requesting RePush: ${data.oldPath} -> ${data.path}`)
        const rePushData = {
          vault: plugin.settings.vault,
          path: data.path,
          pathHash: data.pathHash,
        }
        void plugin.websocket.SendMessage("NoteRePush", rePushData)
        if (targetFile instanceof TFile) {
          plugin.fileHashManager.setFileHash(data.path, data.contentHash, targetFile.stat.mtime, targetFile.stat.size)
        } else {
          plugin.fileHashManager.setFileHash(data.path, data.contentHash)
        }
      }
    }, { maxRetries: 10, retryInterval: 100 });
  } catch (e) {
    dumpError(`[FastSync] Failed to receiveNoteSyncRename: ${normalizedOldPath} -> ${normalizedNewPath}`, e);
    if (!checkAndNotifyCaseConflict(e, data.path, plugin, 'NoteRename')) {
      SyncLogManager.getInstance().addLog('receive', 'NoteRename', e instanceof Error ? e.message : String(e), 'error', data.path);
    }
    plugin.noteSyncTasks.failed++
  } finally {
    plugin.recordSyncCompleted('note', data.pageIndex)
  }
}

// 收到 NoteModifyAck，将 pending hash 转移到正式 hashManager 并更新 lastNoteSyncTime
// Receive NoteModifyAck, move pending hash to formal hashManager and update lastNoteSyncTime
export const receiveNoteModifyAck = function (data: { lastTime?: number; path?: string }, plugin: FastSync) {
  const journalResult = data.path
    ? plugin.incrementalScanManager?.acknowledge("note", data.path)
    : "untracked"
  // A stale ACK must not consume the pending hash belonging to a newer local
  // edit. Keep the journal and pending map for the next retry in that case.
  if (journalResult === "stale") {
    dump(`NoteModifyAck ignored as stale for ${data.path}`)
  }
  // 服务端确认笔记写入成功，将 pending hash 转移到正式 hashManager
  // Server confirmed note write success, move pending hash to formal hashManager
  if (data.path && journalResult !== "stale") {
    const contentHash = plugin.pendingNoteModifies.get(data.path)
    if (contentHash !== undefined) {
      // 尝试获取本地文件信息以存入缓存
      const file = plugin.app.vault.getFileByPath(normalizePath(data.path))
      plugin.fileHashManager.setFileHash(data.path, contentHash, file?.stat.mtime || 0, file?.stat.size || 0)
      plugin.pendingNoteModifies.delete(data.path)
      plugin.localStorageManager.savePending('pendingNoteModifies', plugin.pendingNoteModifies)
    } else {
      dump(`NoteModifyAck received for non-pending path: ${data.path}`)
    }
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
  }
  if (data.path) {
    plugin.concurrencyLimiter.releaseSlot(data.path)
  }
  // 该 Ack 若对应服务端 NeedPush 驱动的上传，查表取回其所属下载页归账（供 ack 水位线推进）；
  // 查不到说明是本地用户自发编辑触发的 Ack，不属于任何页，走旧路径（现状语义不变）
  // If this Ack corresponds to a server NeedPush-driven upload, look up its owning download page
  // for correct accounting (drives the ack watermark); a miss means a local user-initiated edit
  // triggered it — doesn't belong to any page, falls back to the legacy path (unchanged semantics)
  const pushPageIndex = data.path ? plugin.syncState.pendingNotePushPageIndex.get(data.path) : undefined;
  if (data.path) plugin.syncState.pendingNotePushPageIndex.delete(data.path)
  plugin.recordSyncCompleted('note', pushPageIndex)
}

// 收到 NoteRenameAck，按 data.path 精确匹配待确认条目并更新 hashManager；老服务端不下发 path 时回退 FIFO
// Receive NoteRenameAck, match pending entry precisely by data.path and update hashManager; falls back to FIFO when legacy server omits path
export const receiveNoteRenameAck = function (data: { lastTime?: number; path?: string }, plugin: FastSync) {
  let pending: { oldPath: string; newPath: string; contentHash: string } | undefined
  if (data.path) {
    pending = plugin.pendingNoteRenames.get(data.path)
    if (pending) plugin.pendingNoteRenames.delete(data.path)
  } else {
    // 老服务端未下发 path，回退 FIFO（Map 插入顺序取首个）
    // Legacy server omits path, fall back to FIFO (first inserted entry in Map order)
    const firstKey = Array.from(plugin.pendingNoteRenames.keys())[0];
    if (firstKey !== undefined) {
      pending = plugin.pendingNoteRenames.get(firstKey)
      plugin.pendingNoteRenames.delete(firstKey)
    }
  }
  if (pending) {
    const oldResult = plugin.incrementalScanManager?.acknowledge("note", pending.oldPath)
    const newResult = plugin.incrementalScanManager?.acknowledge("note", pending.newPath)
    if (oldResult === "stale" || newResult === "stale") {
      dump(`NoteRenameAck ignored as stale for ${pending.newPath}`)
    } else {
      plugin.fileHashManager.removeFileHash(pending.oldPath)
      // 重命名 Ack 时，内容哈希未变，尝试获取新路径的文件信息
      const file = plugin.app.vault.getFileByPath(normalizePath(pending.newPath))
      plugin.fileHashManager.setFileHash(pending.newPath, pending.contentHash, file?.stat.mtime || 0, file?.stat.size || 0, file?.stat.ctime)
    }
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
  }
  plugin.concurrencyLimiter.releaseFifoSlot()
}

// 收到 NoteDeleteAck，仅当路径仍在 pending set 中时才从 hashManager 移除
// Receive NoteDeleteAck; only remove from hashManager if path is still pending
export const receiveNoteDeleteAck = function (data: { lastTime?: number; path?: string }, plugin: FastSync) {
  if (data.path && plugin.pendingNoteDeleteAcks.has(data.path)) {
    const journalResult = plugin.incrementalScanManager?.acknowledge("note", data.path)
    if (journalResult !== "stale") plugin.fileHashManager.removeFileHash(data.path)
    plugin.pendingNoteDeleteAcks.delete(data.path)
  }
  // 释放并发槽位：与 FileDeleteAck/ConfigDeleteAck 保持一致，仅检查 data.path 是否存在
  // Release concurrency slot: consistent with FileDeleteAck/ConfigDeleteAck, only check data.path
  if (data.path) {
    plugin.concurrencyLimiter.releaseSlot(data.path)
  }
  if (data.lastTime && data.lastTime > Number(plugin.localStorageManager.getMetadata("lastNoteSyncTime"))) {
    plugin.localStorageManager.setMetadata("lastNoteSyncTime", data.lastTime)
  }
}
