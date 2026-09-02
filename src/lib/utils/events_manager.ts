import { Platform, TAbstractFile, TFile, TFolder, Menu, MenuItem, normalizePath } from "obsidian";

import { noteModify, noteDelete, noteRename, noteDeleteByPath } from "../sync/operator_note";
import { fileModify, fileDelete, fileRename, fileDeleteByPath } from "../sync/operator_file";
import { folderModify, folderDelete, folderRename } from "../sync/operator_folder";
import { NoteHistoryModal } from "../../views/note-history/history-modal";
import { dump, isPathInConfigSyncDirs, isPathExcluded, configIsPathExcluded } from "./helpers";

/** 点文件/隐藏路径（.obsidian、.DS_Store、流水线 .gate_pass 等）不进正文桥接 */
const isDotOrHiddenPath = (path: string): boolean => /(^|\/)\./.test(path)
import { ShareModal } from "../../views/share-modal";
import type FastSync from "../../main";
import { $ } from "../../i18n/lang";


export class EventManager {
  private static readonly RESUME_DEBOUNCE_MS = 1000
  /**
   * 外部写入桥新文件的重试退避（毫秒）。Obsidian 把外部写入索引进 vault
   * 存在秒级异步延迟，防抖到期时一次未命中就丢弃会让新文件掉出增量视野
   * （2026-09-01 实证：新写入 2 分钟零上行）。脏账已预写，重试只负责触发上传。
   */
  private static readonly BRIDGE_RETRY_DELAYS_MS = [2000, 5000, 15000, 30000, 60000] as const
  private plugin: FastSync
  private rawEventTimers: Map<string, number> = new Map()
  private bridgeRetryTimers: Map<string, number> = new Map()
  private resumeTimer: number | null = null
  private lifecycleEventsRegistered = false
  private vaultEventsRegistered = false
  private cleanupRegistered = false
  //保存待处理的重命名文件的路径，用于跳过同时触发的 modify 事件
  private pendingRenamePaths: Set<string> = new Set()

  constructor(plugin: FastSync) {
    this.plugin = plugin
  }

  public registerEvents() {
    const { app } = this.plugin

    // --- Window Events ---
    if (!this.lifecycleEventsRegistered) {
      window.addEventListener("focus", this.onWindowFocus)
      window.addEventListener("visibilitychange", this.onVisibilityChange)
      window.addEventListener("online", this.onOnline)
      window.addEventListener("offline", this.onOffline)
      this.lifecycleEventsRegistered = true
    }

    // Add vault/workspace events as soon as the plugin managers exist. The old
    // gate ("wait until hash state is ready") turned a slow or failed hash
    // initialization into permanent silent death of every vault listener —
    // including the external-write bridge — with no error anywhere. Registering
    // before hash state loads is safe: handlers journal durable dirty entries
    // first, and the debounced upload tasks tolerate an unloaded cache.
    // Vault/workspace 事件在插件管理器就绪后立即注册。旧实现等哈希状态加载完，
    // 一旦哈希初始化慢或失败，全部 vault 监听器（含外部写入桥）会永久静默死亡；
    // 处理器先写脏账再走防抖上传，提前注册是安全的。
    if (!this.vaultEventsRegistered) {
      const hashReady = !!this.plugin.fileHashManager?.isReady()
      if (!hashReady) {
        dump("EventManager: registering vault listeners before hash state is ready (journal-first capture)")
      }
      this.plugin.registerEvent(app.vault.on("create", this.watchModify))
      this.plugin.registerEvent(app.vault.on("modify", this.watchModify))
      this.plugin.registerEvent(app.vault.on("delete", this.watchDelete))
      this.plugin.registerEvent(app.vault.on("rename", this.watchRename))
      //@ts-ignore Internal RAW API
      this.plugin.registerEvent(app.vault.on("raw", this.watchRaw))
      this.plugin.registerEvent(app.workspace.on("file-menu", this.watchFileMenu))
      this.vaultEventsRegistered = true
    }

    // 注册插件卸载时的清理逻辑
    if (!this.cleanupRegistered) {
      this.plugin.register(() => {
        dump("EventManager: cleaning up")
        this.stop() // 清除所有待处理任务定时器 (Clear all pending task timers)
        window.removeEventListener("focus", this.onWindowFocus)
        window.removeEventListener("visibilitychange", this.onVisibilityChange)
        window.removeEventListener("online", this.onOnline)
        window.removeEventListener("offline", this.onOffline)
        if (Platform.isMobile) this.plugin.backgroundActivityGate?.close()
      })
      this.cleanupRegistered = true
    }
  }

  /**
   * 停止所有定时器并清除任务状态
   * Stop all timers and clear task status
   */
  public stop() {
    this.rawEventTimers.forEach((timer) => window.clearTimeout(timer))
    this.rawEventTimers.clear()
    this.bridgeRetryTimers.forEach((timer) => window.clearTimeout(timer))
    this.bridgeRetryTimers.clear()
    if (this.resumeTimer !== null) {
      window.clearTimeout(this.resumeTimer)
      this.resumeTimer = null
    }
    this.pendingRenamePaths.clear()
  }

  private recordOfflineModify(file: TAbstractFile): boolean {
    if (file.path === "/" || isPathExcluded(file.path, this.plugin)) return true
    if (this.plugin.isIgnoredFile?.(file.path)) return false

    if (file instanceof TFolder) {
      this.plugin.incrementalScanManager?.markModified("folder", file.path)
    } else if (file instanceof TFile) {
      this.plugin.incrementalScanManager?.markModified(file.path.endsWith(".md") ? "note" : "file", file.path)
    }
    const offline = !this.plugin.websocket?.isAuth || this.isBrowserOffline()
    dump(`${offline ? "Offline" : "Online"} vault change journaled: ${file.path}`)
    return offline
  }

  private recordOfflineDelete(file: TAbstractFile): boolean {
    if (file.path === "/" || isPathExcluded(file.path, this.plugin)) return true
    if (this.plugin.isIgnoredFile?.(file.path)) return false

    if (file instanceof TFolder) {
      this.plugin.incrementalScanManager?.markDeleted("folder", file.path)
    } else if (file instanceof TFile) {
      this.plugin.incrementalScanManager?.markDeleted(file.path.endsWith(".md") ? "note" : "file", file.path)
    }
    const offline = !this.plugin.websocket?.isAuth || this.isBrowserOffline()
    dump(`${offline ? "Offline" : "Online"} vault deletion journaled: ${file.path}`)
    return offline
  }

  private recordOfflineRename(file: TAbstractFile, oldPath: string): boolean {
    const oldExcluded = isPathExcluded(oldPath, this.plugin)
    const newExcluded = isPathExcluded(file.path, this.plugin)
    if (this.plugin.isIgnoredFile?.(file.path) || this.plugin.isIgnoredFile?.(oldPath)) return false
    if (!oldExcluded) {
      const oldKind = file instanceof TFolder ? "folder" : oldPath.endsWith(".md") ? "note" : "file"
      this.plugin.incrementalScanManager?.markDeleted(oldKind, oldPath)
    }
    if (!newExcluded) {
      const newKind = file instanceof TFolder ? "folder" : file.path.endsWith(".md") ? "note" : "file"
      this.plugin.incrementalScanManager?.markModified(newKind, file.path)
    }
    const offline = !this.plugin.websocket?.isAuth || this.isBrowserOffline()
    dump(`${offline ? "Offline" : "Online"} vault rename journaled: ${oldPath} -> ${file.path}`)
    return offline
  }

  private onOnline = () => {
    dump(`Network restored (Event).`)
    this.scheduleResume("online")
  }

  private onOffline = () => {
    dump(`Network lost (Event).`)
    // Do not unregister here. The WebView may emit offline while the process
    // is still alive; closing the socket would also clear its retry schedule.
    // Local vault events are queued while navigator.onLine is false.
    this.plugin.websocket?.noteNetworkLost()
  }

  private onWindowFocus = () => {
    // Foregrounding is a recovery opportunity, not a prerequisite for sync.
    // Coalesce focus and visibility notifications because mobile WebViews often
    // emit both during one resume transition.
    dump("Obsidian window focus; background sync remains enabled")
    this.scheduleResume("focus")
  }

  private onVisibilityChange = () => {
    if (activeDocument.visibilityState === "hidden") {
      // Do not unregister here. Desktop WebViews can remain alive while minimized,
      // and mobile platforms may suspend the process independently of the plugin.
      if (Platform.isMobile) this.plugin.backgroundActivityGate?.markBackgrounded()
      this.plugin.websocket?.noteBackgrounded()
      dump(`Obsidian backgrounded; keeping sync connection alive${Platform.isMobile ? " and deferring vault I/O" : ""}`)
    } else {
      // Foregrounding drains any durable changes queued while the process was
      // offline or suspended. Do not tear down a healthy sync session here.
      if (Platform.isMobile) {
        this.plugin.backgroundActivityGate?.markForegrounded()
        void Promise.all([
          this.plugin.fileHashManager?.flushAsync(),
          this.plugin.configHashManager?.flushAsync(),
          this.plugin.localStorageManager?.flushAsync(),
          this.plugin.incrementalScanManager?.flushAsync(),
        ])
      }
      this.scheduleResume("visibilitychange")
      void this.plugin.shareIndicatorManager?.syncWithServer()
    }
  }

  private scheduleResume = (source: string) => {
    if (this.resumeTimer !== null) {
      window.clearTimeout(this.resumeTimer)
    }

    this.resumeTimer = window.setTimeout(() => {
      this.resumeTimer = null
      const websocket = this.plugin.websocket
      if (!websocket) return

      // Recovery is delegated to the connection coordinator. It probes a
      // mobile OPEN socket before replacing it, so a healthy resume does not
      // create a new WebSocket or a new logical sync round.
      void websocket.recoverAfterResume(source)
    }, EventManager.RESUME_DEBOUNCE_MS)
  }

  private isBrowserOffline(): boolean {
    return typeof navigator !== "undefined" && navigator.onLine === false
  }

  private watchModify = (file: TAbstractFile, ctx?: unknown) => {
    if (this.plugin.settings.manualSyncEnabled || this.plugin.settings.readonlySyncEnabled) return

    // 重命名会同时触发 rename 和 modify 事件，但只需要发送 rename 消息即可完成处理，因此跳过 modify 事件
    if (this.pendingRenamePaths.has(file.path)) {
      dump(`Modify skipped due to pending rename: ${file.path}`)
      return
    }

    if (this.recordOfflineModify(file)) return

    this.runWithDelay(file.path, () => {
      if (file instanceof TFile) {
        if (file.path.endsWith(".md")) {
          void noteModify(file, this.plugin, true)
        } else {
          void fileModify(file, this.plugin, true)
        }
      } else if (file instanceof TFolder) {
        void folderModify(file, this.plugin, true)
      }
    })
  }

  private watchDelete = (file: TAbstractFile, ctx?: unknown) => {
    if (this.plugin.settings.manualSyncEnabled || this.plugin.settings.readonlySyncEnabled) return

    if (this.recordOfflineDelete(file)) return

    this.runWithDelay(file.path, () => {
      if (file instanceof TFile) {
        if (file.path.endsWith(".md")) {
          void noteDelete(file, this.plugin, true)
        } else {
          void fileDelete(file, this.plugin, true)
        }
      } else if (file instanceof TFolder) {
        void folderDelete(file, this.plugin, true)
      }
    })
  }

  private watchRename = (file: TAbstractFile, oldFile: string, ctx?: unknown) => {
    if (this.plugin.settings.manualSyncEnabled || this.plugin.settings.readonlySyncEnabled) return

    if (this.recordOfflineRename(file, oldFile)) return

    // 清除旧路径上可能存在的 modify/delete 定时器
    // 因为旧路径已经被重命名，这些操作已无意义
    this.clearTimer(oldFile)

    // 将新路径加入待处理的重命名任务，用于跳过同时触发的 modify 事件
    this.pendingRenamePaths.add(file.path)

    // 直接使用延迟执行，不走 runWithDelay 的加锁逻辑
    // 因为 noteRename/fileRename 内部已经有锁机制，避免嵌套锁导致死锁
    let delay = this.plugin.settings.syncUpdateDelay || 0

    const executeRename = async () => {
      try {
        if (file instanceof TFile) {
          //对比新文件名和旧文件名后缀是否一致，如果不一致，则认为是文件类型变更，需要发送文件删除和文件创建消息
          const oldExt = oldFile.match(/\.([^.]+)$/)?.[1] ?? ""
          let isDiffFileType = file.extension !== oldExt
          if (isDiffFileType) {
            // 修复：rename 完成后旧路径的 TFile 已不存在，直接使用路径字符串避免空指针崩溃
            // Fix: TFile of old path no longer exists after rename completes, use path string to avoid null pointer crash
            this.runWithDelay(
              oldFile,
              () => {
                if (oldFile.endsWith(".md")) void noteDeleteByPath(oldFile, this.plugin)
                else void fileDeleteByPath(oldFile, this.plugin)
              },
              0,
            )

            this.runWithDelay(
              file.path,
              () => {
                //如果新文件是markdown文件，则发送笔记创建消息，否则发送文件创建消息
                if (file.path.endsWith(".md")) void noteModify(file, this.plugin, true)
                else void fileModify(file, this.plugin, true)
              },
              0,
            )
          } else {
            if (file.path.endsWith(".md")) {
              await noteRename(file, oldFile, this.plugin, true)
            } else {
              await fileRename(file, oldFile, this.plugin, true)
            }
          }
        } else if (file instanceof TFolder) {
          await folderRename(file, oldFile, this.plugin, true)
        }
      } finally {
        // 重命名任务完成后，移除待处理标志
        this.pendingRenamePaths.delete(file.path)
      }
    }

    if (delay <= 0) {
      void executeRename()
    } else {
      const timer = window.setTimeout(() => {
        this.rawEventTimers.delete(file.path)
        void executeRename()
      }, delay)
      this.rawEventTimers.set(file.path, timer)
    }
  }

  private watchRaw = (path: string, ctx?: unknown) => {
    if (!path) return

    if (this.plugin.settings.manualSyncEnabled || this.plugin.settings.readonlySyncEnabled) return

    // 正文通道桥接：外部（shell/agent/cron）写入的 vault 内容不经过 Obsidian 的
    // modify 事件，增量模式因此永远看不见它们——服务端长期欠账（2026-08-28 实证：
    // dasikou/wx 流水线整夜产出零上传）。raw 是 Obsidian 桌面端的文件系统级事件流，
    // 外部写入同样触发；把非 config 路径喂进 note/file 的正常变更路径。
    // Vault-content bridge: writes made outside Obsidian never fire vault 'modify'
    // events, so incremental sync never sees them and the server accumulates a
    // content debt (verified 2026-08-28: overnight pipeline output, zero uploads).
    // 'raw' is the desktop filesystem-level event stream and fires for external
    // writes too; feed non-config paths into the regular note/file change path.
    if (!isPathInConfigSyncDirs(path, this.plugin)) {
      this.bridgeExternalVaultWrite(normalizePath(path))
      return
    }

    if (!this.plugin.settings.configSyncEnabled || configIsPathExcluded(path, this.plugin)) return
    const normalizedPath = normalizePath(path)
    if (this.plugin.ignoredConfigFiles?.has(normalizedPath)) return
    this.plugin.incrementalScanManager?.markModified("config", normalizedPath)
    if (!this.plugin.websocket?.isAuth) {
      dump(`Offline config change journaled: ${path}`)
      return
    }

    this.runWithDelay(
      path,
      () => {
        if (this.plugin.configManager) {
          void this.plugin.configManager.handleRawEvent(normalizePath(path), true)
        }
      },
      300,
    )
  }

  /**
   * 外部写入桥接（正文通道）/ Bridge one externally-written vault path into sync.
   *
   * 安全边界：
   * - 回声抑制：插件自身物化下载的写入在 ignoredFiles 中，跳过（否则下载→raw→重传
   *   循环）；延迟到期后二次复查。
   * - 点文件/隐藏路径过滤：流水线点标记、.DS_Store 等在事件时即被丢弃。
   * - 排除规则：isPathExcluded（含白名单）在事件时过滤。
   * - 预写脏账：事件时即按扩展名分类记账（note/file），不依赖 Obsidian 已完成
   *   索引——外部写入的索引是异步的，等索引再记账会让新文件在慢索引/重载窗掉出
   *   视野。markModified 幂等；删除永不在此记账，瞬时路径最多产生一条空跑扫描项。
   * - 上传触发：500ms 防抖让写入方写完；到期未索引则按退避表重试
   *   （2s/5s/15s/30s/60s），重试耗尽后脏账仍在，交给下轮对账兜底。
   * - 重命名窗口跳过：与 watchModify 相同的 pendingRenamePaths 语义。
   *
   * Safety: echo-suppressed via ignoredFiles (re-checked at flush); dot/hidden
   * paths and excluded rules filtered at event time; the dirty journal is
   * written ahead of the debounce window classified by extension (Obsidian
   * indexes external writes asynchronously, so waiting for a TFile loses new
   * files); deletions are never journaled here; the upload trigger retries
   * with backoff when the path is not indexed yet instead of being dropped.
   */
  private bridgeExternalVaultWrite = (normalizedPath: string) => {
    if (this.plugin.ignoredFiles.has(normalizedPath)) return
    if (isPathExcluded(normalizedPath, this.plugin)) return
    if (isDotOrHiddenPath(normalizedPath)) return

    this.plugin.incrementalScanManager?.markModified(
      normalizedPath.endsWith(".md") ? "note" : "file",
      normalizedPath,
    )

    this.runWithDelay(
      normalizedPath,
      () => {
        this.flushExternalVaultWrite(normalizedPath, 0)
      },
      500,
    )
  }

  /**
   * 防抖到期后的上传触发；路径未索引时按退避表重试而不是丢弃。
   * Upload trigger after the debounce window; retries with backoff while the
   * path is still not indexed instead of dropping the event.
   */
  private flushExternalVaultWrite = (normalizedPath: string, attempt: number) => {
    const pendingTimer = this.bridgeRetryTimers.get(normalizedPath)
    if (pendingTimer !== undefined) {
      window.clearTimeout(pendingTimer)
      this.bridgeRetryTimers.delete(normalizedPath)
    }
    if (this.plugin.ignoredFiles.has(normalizedPath)) return
    if (this.pendingRenamePaths.has(normalizedPath)) return
    const file = this.plugin.app.vault.getAbstractFileByPath(normalizedPath)
    if (!(file instanceof TFile)) {
      if (attempt < EventManager.BRIDGE_RETRY_DELAYS_MS.length) {
        const delay = EventManager.BRIDGE_RETRY_DELAYS_MS[attempt]
        this.bridgeRetryTimers.set(normalizedPath, window.setTimeout(() => {
          this.bridgeRetryTimers.delete(normalizedPath)
          this.flushExternalVaultWrite(normalizedPath, attempt + 1)
        }, delay))
        dump(`[ExternalWriteBridge] path not indexed yet, retry ${attempt + 1}/${EventManager.BRIDGE_RETRY_DELAYS_MS.length} in ${delay}ms: ${normalizedPath}`)
        return
      }
      dump(`[ExternalWriteBridge] path still unindexed after retries; dirty journal entry remains for reconciliation: ${normalizedPath}`)
      return
    }
    dump(`[ExternalWriteBridge] bridging external write into sync: ${normalizedPath}`)
    if (normalizedPath.endsWith(".md")) {
      void noteModify(file, this.plugin, true)
    } else {
      void fileModify(file, this.plugin, true)
    }
  }

  /**
   * 清除指定 key 的定时器
   * @param key 定时器的 key（文件路径）
   */
  private clearTimer(key: string) {
    if (this.rawEventTimers.has(key)) {
      window.clearTimeout(this.rawEventTimers.get(key))
      this.rawEventTimers.delete(key)
    }
  }

  /**
   * 延迟执行同步任务，引入 Atomics 保证原子性
   * @param key 任务唯一标识（通常是文件路径）
   * @param task 待执行的任务（支持 async）
   * @param delayset 额外延迟时间
   */
  private runWithDelay(key: string, task: () => void | Promise<void>, delayset: number = 0) {
    // 如果已有相同 key 的定时器，先清除
    if (this.rawEventTimers.has(key)) {
      window.clearTimeout(this.rawEventTimers.get(key))
      this.rawEventTimers.delete(key)
    }

    let delay = this.plugin.settings.syncUpdateDelay || 0
    delay = delay + delayset

    if (delay <= 0) {
      // 立即执行也需要加锁，以防与其他异步任务冲突
      // 如果获取锁失败，尝试重试 3 次，每次 50ms
      void this.plugin.lockManager.withLock(
        key,
        async () => {
          await task()
        },
        { maxRetries: 3, retryInterval: 50 },
      )
      return
    }

    const timer = window.setTimeout(() => {
      void (async () => {
        this.rawEventTimers.delete(key)

        // 执行任务时加锁，并带重试逻辑
        // 这里的重试是为了应对可能正好有远程同步在写该文件的情况
        await this.plugin.lockManager.withLock(
          key,
          async () => {
            await task()
          },
          { maxRetries: 5, retryInterval: 100 },
        )
      })()
    }, delay)

    this.rawEventTimers.set(key, timer)
  }

  private watchFileMenu = (menu: Menu, file: TAbstractFile) => {
    if (!(file instanceof TFile) || !file.path.endsWith(".md")) return

    menu.addItem((item: MenuItem) => {
      item
        .setTitle($("ui.history.title"))
        .setIcon("history")
        .onClick(() => {
          new NoteHistoryModal(this.plugin.app, this.plugin, file.path).open()
        })
    })

    menu.addItem((item: MenuItem) => {
      item
        .setTitle($("ui.share.title"))
        .setIcon("share-2")
        .onClick(() => {
          new ShareModal(this.plugin.app, this.plugin, file.path).open()
        })
    })

  }
}
