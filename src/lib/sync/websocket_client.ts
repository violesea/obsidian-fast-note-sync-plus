import { moment } from "obsidian";
import { dump, dumpError, isWsUrl, showSyncNotice } from "../utils/helpers";

const safeMoment = moment as unknown as (inp?: unknown) => { format(format: string): string };

// WebSocket 连接常量
const RECONNECT_BASE_DELAY = 1000; // 重连基础延迟 (毫秒)
const AUTH_TIMEOUT_MS = 10000;
const NON_RECONNECT_REASONS = new Set([
  "AuthorizationFaild",
  "ClientClose",
  "kicked by admin",
  "TokenRotatedOrRevoked",
  "broadcast failed"
]);

export interface AppStoragePlugin {
  app: {
    vault: {
      getName: () => string;
    };
    loadLocalStorage: (key: string) => unknown;
    saveLocalStorage: (key: string, value: string | null) => void;
  };
  settings?: {
    protobufEnabled?: boolean;
  };
}

function getWsCountStorageKey(plugin: AppStoragePlugin): string {
  const vaultName = plugin.app.vault.getName();
  return `fns-${vaultName}-wsCount`;
}

export interface WebSocketClientOptions {
  getWsUrl: (count: number) => string;
  preConnectProbe?: () => Promise<boolean>;
  onBackoffReconnect?: () => void;
  
  onOpen?: (client: WebSocketClient) => void;
  onClose?: (client: WebSocketClient, code: number, reason: string) => void;
  onMessage?: (client: WebSocketClient, action: string, data: unknown) => void;
  onActivity?: () => void;

  serializeMessage?: (action: string, payload: unknown) => Uint8Array;
  deserializeMessage?: (data: Uint8Array) => { action: string; [key: string]: unknown };
}

/** Result of attempting to write a text/protobuf message to the socket. */
export type TextSendResult = "sent" | "cancelled" | "closed";

export class WebSocketClient {
  public ws: WebSocket;
  private plugin: AppStoragePlugin;
  private options: WebSocketClientOptions;

  public isOpen = false;
  public isAuth = false;
  public useProtobuf = false;
  public checkConnection: number;
  public checkReConnectTimeout: number;
  public timeConnect = 0;
  // 是否已经在本轮重连失败序列中提示过用户（首次达到原上限第 16 次时提示一次，重连成功后重置）
  private hasNotifiedReconnectFailure = false;
  public count = 0;
  /** Monotonic identifier for the current physical WebSocket connection. */
  public connectionId = 0;
  private registerPromise: Promise<void> | null = null;
  private connectionGeneration = 0;
  private forceReconnectInFlight = false;
  private authTimeout: number | null = null;
  public isRegister = true;
  
  private statusListeners: Set<(status: boolean) => void> = new Set();
  private activityListeners: Set<() => void> = new Set();
  private binaryHandlers = new Map<string, (data: ArrayBuffer | Blob) => void | Promise<void>>();

  constructor(plugin: AppStoragePlugin, options: WebSocketClientOptions) {
    this.plugin = plugin;
    this.options = options;

    const storageKey = getWsCountStorageKey(this.plugin);
    let storedCount = this.plugin.app.loadLocalStorage(storageKey) as string | null;

    // 迁移逻辑：如果新键无值，尝试按顺序读取旧键
    if (storedCount === null) {
      const vaultName = this.plugin.app.vault.getName();
      // 1. 尝试上一个格式: fast-note-sync-[Vault]-wsCount
      const prevKey1 = `fast-note-sync-${vaultName}-wsCount`;
      let oldValue = this.plugin.app.loadLocalStorage(prevKey1) as string | null;

      // 2. 尝试更早的格式: fast-note-sync-[Vault]-ws-count
      if (oldValue === null) {
        const prevKey2 = `fast-note-sync-${vaultName}-ws-count`;
        oldValue = this.plugin.app.loadLocalStorage(prevKey2) as string | null;
      }

      // 3. 尝试最初始格式: fast-note-sync-ws-count
      if (oldValue === null) {
        const oldKey = "fast-note-sync-ws-count";
        oldValue = this.plugin.app.loadLocalStorage(oldKey) as string | null;
      }

      if (oldValue !== null) {
        storedCount = oldValue;
        this.plugin.app.saveLocalStorage(storageKey, storedCount);
      }
    }

    this.count = storedCount ? parseInt(storedCount) : 0;
  }

  public registerBinaryHandler(prefix: string, handler: (data: ArrayBuffer | Blob) => void | Promise<void>) {
    if (prefix.length !== 2) {
      dumpError("Binary handler prefix must be exactly 2 characters");
      return;
    }
    this.binaryHandlers.set(prefix, handler);
  }

  public addStatusListener(listener: (status: boolean) => void) {
    this.statusListeners.add(listener);
    if (this.isRegister) {
      listener(this.isOpen);
    }
  }

  public removeStatusListener(listener: (status: boolean) => void) {
    this.statusListeners.delete(listener);
  }

  public notifyStatusChange(status: boolean) {
    this.statusListeners.forEach(listener => listener(status));
  }

  public addActivityListener(listener: () => void) {
    this.activityListeners.add(listener);
  }

  public notifyActivity() {
    this.activityListeners.forEach(fn => fn());
    this.options.onActivity?.();
  }

  public isConnected(): boolean {
    return this.isOpen;
  }

  /**
   * Check both the physical connection generation and its authenticated state.
   * A stale StartHandle callback must not use the shared client flags alone:
   * those flags may already describe a newer socket.
   */
  public isCurrentConnection(connectionId: number): boolean {
    return connectionId > 0
      && this.connectionId === connectionId
      && this.isOpen
      && this.isAuth
      && this.ws?.readyState === WebSocket.OPEN;
  }

  public async register(force = false) {
    const generation = this.connectionGeneration;

    if (!force && this.ws && (this.ws.readyState === WebSocket.CONNECTING || this.ws.readyState === WebSocket.OPEN)) {
      dump("WebSocket already connecting or open, skipping register");
      return;
    }

    if (this.registerPromise) {
      const pendingRegister = this.registerPromise;
      await pendingRegister;
      // A foreground event can arrive while a health probe or socket creation
      // is still in flight. The first attempt may have been invalidated by a
      // later forceReconnect(), so the latest request must get its own socket.
      if (force && generation === this.connectionGeneration && this.isRegister) {
        await this.register(true);
      } else if (!force && this.isRegister && !this.ws) {
        // unRegister() intentionally invalidates an in-flight probe. A caller
        // that immediately registers again must not inherit the cancelled task.
        await this.register();
      }
      return;
    }

    const registerTask = this._doRegister(generation);
    let trackedTask: Promise<void>;
    trackedTask = registerTask.finally(() => {
      if (this.registerPromise === trackedTask) {
        this.registerPromise = null;
      }
    });
    this.registerPromise = trackedTask;
    await trackedTask;
  }

  private async _doRegister(generation: number) {
    if (this.ws) {
      this.clearAuthTimeout();
      const previousWs = this.ws;
      this.cleanupWebSocket(previousWs);
      if (this.ws === previousWs) {
        this.ws = null as unknown as WebSocket;
      }
    }

    this.isRegister = true;

    if (this.options.preConnectProbe) {
      const isHealthy = await this.options.preConnectProbe();
      if (generation !== this.connectionGeneration) {
        return;
      }
      if (!isHealthy) {
        dump("Health check failed before ws connect, scheduling reconnect...");
        this.isOpen = false;
        this.notifyStatusChange(false);
        this.checkReconnect();
        return;
      }
    }

    if (generation !== this.connectionGeneration || !this.isRegister) {
      return;
    }

    const wsUrl = this.options.getWsUrl(this.count);
    if (isWsUrl(wsUrl)) {
      this.connectionId++;
      const ws = new WebSocket(wsUrl);
      this.ws = ws;
      ws.binaryType = "arraybuffer";
      this.count++;
      this.plugin.app.saveLocalStorage(getWsCountStorageKey(this.plugin), this.count.toString());

      ws.onerror = (error: Event) => {
        if (this.ws !== ws) return;
        dump("WebSocket error:", {
          timestamp: safeMoment().format("YYYY-MM-DD HH:mm:ss.SSS"),
          url: wsUrl,
          readyState: ws.readyState,
          error: error
        });
        this.notifyStatusChange(false);
      };

      ws.onopen = (e: Event): void => {
        if (this.ws !== ws) return;
        this.timeConnect = 0;
        this.hasNotifiedReconnectFailure = false;
        this.isAuth = false;
        this.useProtobuf = false;
        this.isOpen = true;
        dump("Service connected", {
          timestamp: safeMoment().format("YYYY-MM-DD HH:mm:ss.SSS"),
          url: wsUrl
        });
        this.options.onOpen?.(this);
        this.authTimeout = window.setTimeout(() => {
          if (this.ws !== ws || !this.isOpen || this.isAuth) return;

          dump(`WebSocket authorization timed out after ${AUTH_TIMEOUT_MS}ms`);
          this.forceReconnectInFlight = false;
          this.isOpen = false;
          this.isAuth = false;
          this.useProtobuf = false;
          this.notifyTransportReplacement(ws);
          this.closeCurrentWebSocket();
          this.notifyStatusChange(false);
          if (this.isRegister) this.checkReconnect();
        }, AUTH_TIMEOUT_MS);
      };

      ws.onclose = (e: CloseEvent) => {
        if (this.ws !== ws) return;
        this.clearAuthTimeout();
        // A replacement attempt that reaches a terminal close before auth is
        // no longer in flight. The normal backoff loop may now own recovery.
        this.forceReconnectInFlight = false;
        this.isAuth = false;
        this.useProtobuf = false;
        this.isOpen = false;
        this.notifyStatusChange(false);

        dump("Service close details:", {
          timestamp: safeMoment().format("YYYY-MM-DD HH:mm:ss.SSS"),
          code: e.code,
          reason: e.reason,
          wasClean: e.wasClean,
          timeConnect: this.timeConnect,
          isRegister: this.isRegister
        });

        if (NON_RECONNECT_REASONS.has(e.reason)) {
          this.isRegister = false;
        }

        this.options.onClose?.(this, e.code, e.reason);

        if (this.isRegister && !NON_RECONNECT_REASONS.has(e.reason)) {
          this.checkReconnect();
        }
        dump("Service close");
      };

      ws.onmessage = (event: MessageEvent) => {
        if (this.ws !== ws) return;
        if (event.data instanceof ArrayBuffer || event.data instanceof Blob) {
          void (async () => {
            let buf: ArrayBuffer;
            if (event.data instanceof Blob) {
              buf = await event.data.arrayBuffer();
            } else {
              buf = event.data as ArrayBuffer;
            }
            // A late Blob conversion from a socket that has already been
            // replaced must not feed stale chunks into the new sync session.
            if (this.ws !== ws || buf.byteLength < 2) return;

            const prefixBytes = new Uint8Array(buf.slice(0, 2));
            const prefixStr = new TextDecoder().decode(prefixBytes);

            const handler = this.binaryHandlers.get(prefixStr);
            if (handler) {
              const rest = buf.slice(2);
              await handler(rest);
              this.notifyActivity();
            } else if (prefixStr === "pb") {
              try {
                const rest = buf.slice(2);
                const view = new Uint8Array(rest);
                if (this.options.deserializeMessage) {
                  const result = this.options.deserializeMessage(view);
                  
                  // Only upgrade to Protobuf if the setting is enabled locally
                  // 仅在本地设置启用时才升级为 Protobuf
                  if (result.action === "ClientInfo" && this.plugin.settings?.protobufEnabled !== false) {
                    this.useProtobuf = true;
                    dump("WS Client upgraded to Protobuf successfully");
                  }
                  
                  this.options.onMessage?.(this, result.action, result);
                }
              } catch (err) {
                dumpError("Failed to decode incoming Protobuf message:", err);
              }
            } else {
              dump("No handler for binary prefix:", prefixStr);
            }
          })().catch((err) => {
            dumpError("Failed to process incoming binary message:", err);
          });

          return;
        }

        const fullMsg = event.data as string;
        let msgData: string = fullMsg;
        let msgAction: string = "";
        const index = fullMsg.indexOf("|");
        if (index !== -1) {
          msgData = fullMsg.slice(index + 1);
          msgAction = fullMsg.slice(0, index);
        }
        try {
          const data: unknown = JSON.parse(msgData);
          this.options.onMessage?.(this, msgAction, data);
        } catch (err) {
          dumpError("Failed to parse incoming JSON message:", err);
        }
      };
    }
  }

  private cleanupWebSocket(ws: WebSocket) {
    if (!ws) return;

    ws.onopen = null;
    ws.onmessage = null;
    ws.onerror = null;
    ws.onclose = null;

    try {
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        ws.close(1000, "Cleanup");
      }
    } catch (e) {
      dumpError("Error closing WebSocket:", e);
    }
  }

  private closeCurrentWebSocket() {
    const ws = this.ws;
    if (!ws) return;

    this.clearAuthTimeout();

    this.cleanupWebSocket(ws);
    if (this.ws === ws) {
      this.ws = null as unknown as WebSocket;
    }
  }

  private clearAuthTimeout(): void {
    if (this.authTimeout !== null) {
      window.clearTimeout(this.authTimeout);
      this.authTimeout = null;
    }
  }

  /**
   * Notify the owner before deliberately replacing a socket. cleanupWebSocket
   * detaches onclose first, so without this explicit lifecycle signal a
   * prepared logical sync would remain in `monitoring` and never resume.
   */
  private notifyTransportReplacement(ws: WebSocket): void {
    if (this.ws !== ws) return;
    this.options.onClose?.(this, 1000, "Cleanup");
  }

  public unRegister(setUnregistered = false) {
    this.connectionGeneration++;
    this.forceReconnectInFlight = false;
    this.clearAuthTimeout();
    window.clearTimeout(this.checkReConnectTimeout);
    this.timeConnect = 0;
    this.hasNotifiedReconnectFailure = false;
    this.isOpen = false;
    this.isAuth = false;
    this.useProtobuf = false;
    if (setUnregistered) {
      this.isRegister = false;
    }

    this.closeCurrentWebSocket();

    this.notifyStatusChange(false);
    dump("Service unregister");
  }

  /**
   * Complete the replacement guard after the new physical socket has been
   * authenticated. Creating a WebSocket is not enough: iOS may open a socket
   * that is still waiting for auth while another lifecycle callback arrives.
   */
  public completeForcedReconnect(): void {
    this.clearAuthTimeout();
    if (this.forceReconnectInFlight) {
      dump("Forced WebSocket reconnect authenticated");
      this.forceReconnectInFlight = false;
    }
  }

  /** Mark the current physical connection authenticated and release guards. */
  public markAuthenticated(): void {
    this.completeForcedReconnect();
  }

  public checkReconnect() {
    window.clearTimeout(this.checkReConnectTimeout);
    // 不再设硬上限：超过原上限（15 次）后仍持续重试，退避延迟封顶 30 分钟；
    // 首次达到原上限时提示用户一次，之后静默在后台继续重试
    if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
      this.timeConnect++;

      if (this.timeConnect === 16 && !this.hasNotifiedReconnectFailure) {
        this.hasNotifiedReconnectFailure = true;
        showSyncNotice("同步连接持续失败，将继续在后台重试");
      }

      // Delay backoff: first 3 times 1s, then exponential growth up to 30 min
      const delay = this.timeConnect <= 3
        ? RECONNECT_BASE_DELAY
        : Math.min(RECONNECT_BASE_DELAY * Math.pow(2, this.timeConnect - 3), 1800000);

      dump(`Service waiting reconnect: ${this.timeConnect}, delay: ${delay}ms`);

      this.checkReConnectTimeout = window.setTimeout(() => {
        if (this.options.onBackoffReconnect) {
          this.options.onBackoffReconnect();
        } else {
          void this.register();
        }
      }, delay);
    }
  }

  public triggerReconnect() {
    if (!this.isRegister) {
      dump("Trigger reconnect skipped because WebSocket registration is disabled");
      return;
    }

    const currentWs = this.ws;
    if (currentWs && currentWs.readyState === WebSocket.CONNECTING) {
      dump("Trigger reconnect skipped because WebSocket is still connecting");
      return;
    }

    if (currentWs && currentWs.readyState === WebSocket.OPEN && this.isOpen) {
      dump("Trigger reconnect skipped because WebSocket is already healthy");
      return;
    }

    dump("Triggering manual reconnect due to network change");
    this.timeConnect = 0;
    this.hasNotifiedReconnectFailure = false;
    window.clearTimeout(this.checkReConnectTimeout);

    // An error can leave the browser WebSocket object in OPEN while the
    // transport is unusable. The normal register() guard would mistake that
    // object for a healthy connection, so discard it before registering again.
    if (currentWs && (currentWs.readyState === WebSocket.OPEN || currentWs.readyState === WebSocket.CLOSING)) {
      this.isOpen = false;
      this.isAuth = false;
      this.useProtobuf = false;
      this.notifyTransportReplacement(currentWs);
      this.closeCurrentWebSocket();
      this.notifyStatusChange(false);
    }

    void this.register();
  }

  /**
   * Rebuild the socket after a mobile foreground/network transition.
   * A WebSocket can remain OPEN in the WebView while its underlying transport
   * is already dead, so the normal register() guard is insufficient here.
   */
  public forceReconnect() {
    if (!this.isRegister) {
      dump("Force reconnect skipped because WebSocket registration is disabled");
      return;
    }

    // Focus/visibility events can arrive in a burst, and iOS may deliver a
    // second one while the probe for the first replacement is still pending.
    // One logical recovery must produce one socket replacement.
    if (this.forceReconnectInFlight) {
      dump("Force reconnect skipped because a replacement is already in flight");
      return;
    }

    const currentWs = this.ws;
    if (currentWs && currentWs.readyState === WebSocket.CONNECTING) {
      dump("Force reconnect skipped because the replacement socket is still connecting");
      return;
    }
    dump("Forcing WebSocket reconnect after foreground/network transition");
    this.forceReconnectInFlight = true;
    this.connectionGeneration++;
    window.clearTimeout(this.checkReConnectTimeout);
    this.timeConnect = 0;
    this.hasNotifiedReconnectFailure = false;
    this.isOpen = false;
    this.isAuth = false;
    this.useProtobuf = false;
    if (currentWs) {
      this.notifyTransportReplacement(currentWs);
    }
    this.closeCurrentWebSocket();
    this.notifyStatusChange(false);
    const replacementGeneration = this.connectionGeneration;
    void this.register(true).then(() => {
      // register() resolves as soon as the physical socket is created. Keep
      // the guard until auth succeeds; release it here only when no socket was
      // created or the attempt was invalidated.
      if (this.connectionGeneration !== replacementGeneration || !this.ws || this.ws.readyState === WebSocket.CLOSED) {
        this.forceReconnectInFlight = false;
      }
    }, () => {
      this.forceReconnectInFlight = false;
    });
  }

  private async waitForBufferDrain(ws: WebSocket, maxBufferSize = 5 * 1024 * 1024): Promise<void> {
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }

    while (ws.readyState === WebSocket.OPEN && ws.bufferedAmount > maxBufferSize) {
      await new Promise(resolve => window.setTimeout(resolve, 50));
    }
  }

  public async SendMessage(action: string, data: unknown, before?: () => boolean, after?: () => void): Promise<TextSendResult> {
    if (before && before()) {
      return "cancelled";
    }

    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return "closed";
    }
    await this.waitForBufferDrain(ws);
    if (this.ws !== ws || ws.readyState !== WebSocket.OPEN) {
      return "closed";
    }

    const sent = this.Send(action, data, () => {
      after?.();
      this.notifyActivity();
    });
    return sent ? "sent" : "closed";
  }

  public Send(action: string, data: unknown, after?: () => void): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      dump(`Service not connected, message dropped (will rely on next sync cycle): ${action}`);
      return false;
    }

    try {
      if (this.useProtobuf && this.options.serializeMessage) {
        let payloadObj: unknown = data;
        if (typeof data === "string") {
          try {
            payloadObj = JSON.parse(data) as unknown;
          } catch {
            payloadObj = data;
          }
        }
        const bytes = this.options.serializeMessage(action, payloadObj);
        const prefixBytes = new TextEncoder().encode("pb");
        const bytesWithPrefix = new Uint8Array(prefixBytes.length + bytes.length);
        bytesWithPrefix.set(prefixBytes);
        bytesWithPrefix.set(bytes, prefixBytes.length);
        this.ws.send(bytesWithPrefix);
      } else {
        this.sendTextFallback(action, data);
      }
    } catch (err) {
      dumpError(`Failed to send WebSocket message for action: ${action}`, err);
      return false;
    }

    after?.();
    return true;
  }

  private sendTextFallback(action: string, data: unknown) {
    if (typeof data === "string") {
      this.ws.send(action + "|" + data);
    } else {
      this.ws.send(action + "|" + JSON.stringify(data));
    }
  }

  /**
   * 发送二进制分片。返回值细化为三态，避免"连接已断开未发送"和"发送成功"
   * 都返回 false 而无法区分（分片假成功问题）：
   * - 'sent': 已实际写入 WebSocket
   * - 'cancelled': 被调用方 before() 钩子主动取消
   * - 'closed': 连接不可用，未发送
   */
  public async SendBinary(data: ArrayBuffer | Uint8Array, prefix: string, before?: () => boolean, after?: () => void): Promise<'sent' | 'cancelled' | 'closed'> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return 'closed';
    }

    if (!prefix || prefix.length !== 2) {
      return 'closed';
    }

    if (before && before()) {
      return 'cancelled';
    }

    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return "closed";
    }

    await this.waitForBufferDrain(ws);

    // 等待缓冲区排空期间连接可能已断开，发送前再次确认，避免对已关闭的 socket 调用 send()
    // Connection may have dropped while waiting for the buffer to drain; re-check before sending
    if (this.ws !== ws || ws.readyState !== WebSocket.OPEN) {
      return 'closed';
    }

    const prefixBytes = new TextEncoder().encode(prefix);
    let dataToSend: Uint8Array;

    if (data instanceof Uint8Array) {
      dataToSend = new Uint8Array(prefixBytes.length + data.length);
      dataToSend.set(prefixBytes);
      dataToSend.set(data, prefixBytes.length);
    } else {
      const dataView = new Uint8Array(data);
      dataToSend = new Uint8Array(prefixBytes.length + dataView.length);
      dataToSend.set(prefixBytes);
      dataToSend.set(dataView, prefixBytes.length);
    }

    try {
      ws.send(dataToSend);
    } catch (err) {
      dumpError("Failed to send WebSocket binary message:", err);
      return 'closed';
    }
    after?.();
    this.notifyActivity();
    return 'sent';
  }
}
