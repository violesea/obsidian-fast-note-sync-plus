import { TAbstractFile, TFile, TFolder, Menu, MenuItem, Platform, normalizePath } from "obsidian";

import { noteModify, noteDelete, noteRename, noteDeleteByPath } from "../sync/operator_note";
import { fileModify, fileDelete, fileRename, fileDeleteByPath } from "../sync/operator_file";
import { folderModify, folderDelete, folderRename } from "../sync/operator_folder";
import { NoteHistoryModal } from "../../views/note-history/history-modal";
import { dump, isPathInConfigSyncDirs, isPathExcluded, configIsPathExcluded } from "./helpers";
import { ShareModal } from "../../views/share-modal";
import type FastSync from "../../main";
import { $ } from "../../i18n/lang";


export class EventManager {
  private static readonly RESUME_DEBOUNCE_MS = 1000
  private plugin: FastSync
  private rawEventTimers: Map<string, number> = new Map()
  private resumeTimer: number | null = null
  //保存待处理的重命名文件的路径，用于跳过同时触发的 modify 事件
  private pendingRenamePaths: Set<string> = new Set()

  constructor(plugin: FastSync) {
    this.plugin = plugin
  }

  public registerEvents() {
    // 添加哈希表就绪检查
    if (!this.plugin.fileHashManager || !this.plugin.fileHashManager.isReady()) {
      dump("EventManager: 文件哈希管理器未就绪,跳过事件注册")
      return
    }

    const { app } = this.plugin

    // --- Vault Events ---
    this.plugin.registerEvent(app.vault.on("create", this.watchModify))
    this.plugin.registerEvent(app.vault.on("modify", this.watchModify))
    this.plugin.registerEvent(app.vault.on("delete", this.watchDelete))
    this.plugin.registerEvent(app.vault.on("rename", this.watchRename))
    //@ts-ignore Internal RAW API
    this.plugin.registerEvent(app.vault.on("raw", this.watchRaw))

    // --- Workspace Events ---
    this.plugin.registerEvent(app.workspace.on("file-menu", this.watchFileMenu))

    // --- Window Events ---
    window.addEventListener("focus", this.onWindowFocus)
    window.addEventListener("visibilitychange", this.onVisibilityChange)
    window.addEventListener("online", this.onOnline)
    window.addEventListener("offline", this.onOffline)

    // 注册插件卸载时的清理逻辑
    this.plugin.register(() => {
      dump("EventManager: cleaning up")
      this.stop() // 清除所有待处理任务定时器 (Clear all pending task timers)
      window.removeEventListener("focus", this.onWindowFocus)
      window.removeEventListener("visibilitychange", this.onVisibilityChange)
      window.removeEventListener("online", this.onOnline)
      window.removeEventListener("offline", this.onOffline)
    })
  }

  /**
   * 停止所有定时器并清除任务状态
   * Stop all timers and clear task status
   */
  public stop() {
    this.rawEventTimers.forEach((timer) => window.clearTimeout(timer))
    this.rawEventTimers.clear()
    if (this.resumeTimer !== null) {
      window.clearTimeout(this.resumeTimer)
      this.resumeTimer = null
    }
    this.pendingRenamePaths.clear()
  }

  private recordOfflineModify(file: TAbstractFile): boolean {
    if (this.plugin.websocket?.isAuth && !this.isBrowserOffline()) return false
    if (file.path === "/" || isPathExcluded(file.path, this.plugin)) return true

    if (file instanceof TFolder) {
      this.plugin.incrementalScanManager?.markModified("folder", file.path)
    } else if (file instanceof TFile) {
      this.plugin.incrementalScanManager?.markModified(file.path.endsWith(".md") ? "note" : "file", file.path)
    }
    dump(`Offline vault change queued: ${file.path}`)
    return true
  }

  private recordOfflineDelete(file: TAbstractFile): boolean {
    if (this.plugin.websocket?.isAuth && !this.isBrowserOffline()) return false
    if (file.path === "/" || isPathExcluded(file.path, this.plugin)) return true

    if (file instanceof TFolder) {
      this.plugin.incrementalScanManager?.markDeleted("folder", file.path)
    } else if (file instanceof TFile) {
      this.plugin.incrementalScanManager?.markDeleted(file.path.endsWith(".md") ? "note" : "file", file.path)
    }
    dump(`Offline vault deletion queued: ${file.path}`)
    return true
  }

  private recordOfflineRename(file: TAbstractFile, oldPath: string): boolean {
    if (this.plugin.websocket?.isAuth && !this.isBrowserOffline()) return false

    const oldExcluded = isPathExcluded(oldPath, this.plugin)
    const newExcluded = isPathExcluded(file.path, this.plugin)
    if (!oldExcluded) {
      const oldKind = file instanceof TFolder ? "folder" : oldPath.endsWith(".md") ? "note" : "file"
      this.plugin.incrementalScanManager?.markDeleted(oldKind, oldPath)
    }
    if (!newExcluded) {
      const newKind = file instanceof TFolder ? "folder" : file.path.endsWith(".md") ? "note" : "file"
      this.plugin.incrementalScanManager?.markModified(newKind, file.path)
    }
    dump(`Offline vault rename queued: ${oldPath} -> ${file.path}`)
    return true
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
      dump("Obsidian backgrounded; keeping sync connection alive")
    } else {
      // Foregrounding drains any durable changes queued while the process was
      // offline or suspended. Do not tear down a healthy sync session here.
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

      // iOS may suspend the WebView while leaving its JavaScript WebSocket
      // object in an OPEN state. A mobile resume must therefore rebuild the
      // transport even when the cached client state still says "open".
      // Desktop minimized windows can keep a healthy socket alive, so retain
      // the non-destructive path there.
      if (Platform.isMobile) {
        dump(`Resume event (${source}); forcing a fresh mobile sync connection`)
        websocket.forceReconnect()
        return
      }

      // A healthy socket is already the silent background-sync path. Rebuilding
      // it here would interrupt in-flight page ACKs and can strand a sync round.
      if (websocket.isOpen) {
        dump(`Resume event (${source}); sync connection is healthy, no reconnect needed`)
        return
      }

      dump(`Resume event (${source}); reconnecting the disconnected sync connection`)
      websocket.triggerReconnect()
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
    // 路径安全性校验
    if (!isPathInConfigSyncDirs(path, this.plugin)) return

    if (!this.plugin.settings.configSyncEnabled || configIsPathExcluded(path, this.plugin)) return
    if (!this.plugin.websocket?.isAuth) {
      this.plugin.incrementalScanManager?.markModified("config", normalizePath(path))
      dump(`Offline config change queued: ${path}`)
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
