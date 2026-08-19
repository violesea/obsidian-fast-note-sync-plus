/**
 * Coordinates application-resume recovery without confusing transport repair
 * with a new logical sync round.
 *
 * A mobile WebView can keep a JavaScript WebSocket object in OPEN state after
 * its native transport was suspended.  The coordinator therefore probes an
 * authenticated mobile connection before replacing it.  A successful probe
 * leaves the socket and the current sync snapshot untouched.
 */

export interface ResumeRecoveryTransport {
  readonly isMobile: boolean;
  isRegistered(): boolean;
  isReady(): boolean;
  isOpen(): boolean;
  probe(timeoutMs: number): Promise<boolean>;
  triggerReconnect(): void;
  forceReconnect(): void;
  log(message: string): void;
}

export interface ResumeRecoveryOptions {
  probeTimeoutMs?: number;
  now?: () => number;
}

export class ResumeRecoveryCoordinator {
  private static readonly DEFAULT_PROBE_TIMEOUT_MS = 2500;
  private readonly probeTimeoutMs: number;
  private readonly now: () => number;
  private recoveryInFlight: Promise<void> | null = null;
  private lifecycleCycle = 0;
  private recoveredLifecycleCycle = 0;
  private lifecycleState: "foreground" | "background" = "foreground";
  private networkCycle = 0;
  private recoveredNetworkCycle = -1;
  private backgroundedAt: number | null = null;

  constructor(
    private readonly transport: ResumeRecoveryTransport,
    options: ResumeRecoveryOptions = {},
  ) {
    this.probeTimeoutMs = options.probeTimeoutMs ?? ResumeRecoveryCoordinator.DEFAULT_PROBE_TIMEOUT_MS;
    this.now = options.now ?? (() => Date.now());
  }

  /** Record the beginning of a background interval once per transition. */
  public markBackgrounded(): void {
    if (this.lifecycleState === "background") return;
    this.lifecycleState = "background";
    this.lifecycleCycle += 1;
    this.backgroundedAt = this.now();
    this.transport.log(`Resume recovery armed for lifecycle cycle ${this.lifecycleCycle}`);
  }

  /** Record a browser offline -> online boundary separately from app resume. */
  public markNetworkLost(): void {
    this.networkCycle += 1;
    this.recoveredNetworkCycle = -1;
    this.transport.log(`Resume recovery armed for network cycle ${this.networkCycle}`);
  }

  /**
   * Recover the transport at most once for a burst of lifecycle events.
   * Concurrent focus/visibility/online events share the same promise.
   */
  public recover(source: string): Promise<void> {
    const isLifecycleResume = source === "focus" || source === "visibilitychange" || source === "resume";
    const isNetworkResume = source === "online";
    let backgroundedAt: number | null = null;

    if (isLifecycleResume) {
      // A focus + visibilitychange pair is one foreground transition. Consume
      // that transition before doing async work, so a later event in the same
      // visible period cannot replace the socket a second time.
      if (this.lifecycleCycle === 0) {
        // Initial focus can happen without a preceding hidden event.
        this.lifecycleCycle = 1;
      }
      if (this.lifecycleState === "foreground" && this.recoveredLifecycleCycle === this.lifecycleCycle) {
        this.transport.log(`Resume recovery skipped (${source}): lifecycle cycle ${this.lifecycleCycle} already handled`);
        return Promise.resolve();
      }
      backgroundedAt = this.backgroundedAt;
      this.backgroundedAt = null;
      this.lifecycleState = "foreground";
      this.recoveredLifecycleCycle = this.lifecycleCycle;
      // A lifecycle recovery also covers online notifications that belong to
      // the same foreground transition. An explicit offline boundary resets
      // this marker through markNetworkLost().
      this.recoveredNetworkCycle = this.networkCycle;
    }

    if (isNetworkResume) {
      // Browsers may emit more than one online notification. One online
      // transition gets one recovery decision; the socket's own retry loop
      // handles failures until the next offline boundary.
      if (this.recoveredNetworkCycle === this.networkCycle) {
        this.transport.log(`Resume recovery skipped (${source}): network cycle ${this.networkCycle} already handled`);
        return Promise.resolve();
      }
      this.recoveredNetworkCycle = this.networkCycle;
      if (this.lifecycleState === "foreground") {
        // If the app was already in the foreground, online is the recovery
        // event for the current visible period as well.
        if (this.lifecycleCycle === 0) this.lifecycleCycle = 1;
        this.recoveredLifecycleCycle = this.lifecycleCycle;
      }
    }

    if (this.recoveryInFlight) {
      this.transport.log(`Resume recovery coalesced (${source})`);
      return this.recoveryInFlight;
    }

    const task = this.recoverInternal(source, backgroundedAt);
    const tracked = task.finally(() => {
      if (this.recoveryInFlight === tracked) {
        this.recoveryInFlight = null;
      }
    });
    this.recoveryInFlight = tracked;
    return tracked;
  }

  private async recoverInternal(source: string, backgroundedAt: number | null): Promise<void> {
    if (!this.transport.isRegistered()) {
      this.transport.log(`Resume recovery skipped (${source}): registration disabled`);
      return;
    }

    const elapsed = backgroundedAt === null ? null : Math.max(0, this.now() - backgroundedAt);

    if (!this.transport.isMobile) {
      if (this.transport.isReady()) {
        this.transport.log(`Resume recovery skipped (${source}): desktop connection is healthy`);
      } else {
        this.transport.log(`Resume recovery reconnecting (${source}): desktop connection is unavailable`);
        this.transport.triggerReconnect();
      }
      return;
    }

    if (!this.transport.isReady()) {
      // An OPEN-but-unauthenticated/stale socket cannot be repaired by the
      // ordinary register guard.  Replace it; a CLOSED socket uses backoff.
      if (this.transport.isOpen()) {
        this.transport.log(`Resume recovery replacing stale mobile connection (${source})`);
        this.transport.forceReconnect();
      } else {
        this.transport.log(`Resume recovery reconnecting mobile connection (${source})`);
        this.transport.triggerReconnect();
      }
      return;
    }

    this.transport.log(
      `Resume recovery probing mobile connection (${source}, backgroundMs=${elapsed ?? "unknown"})`,
    );

    let healthy = false;
    try {
      healthy = await this.transport.probe(this.probeTimeoutMs);
    } catch (error) {
      this.transport.log(`Resume recovery probe failed (${source}): ${String(error)}`);
    }

    if (healthy) {
      this.transport.log(`Resume recovery kept existing mobile connection (${source})`);
      return;
    }

    if (!this.transport.isRegistered()) return;
    if (this.transport.isOpen()) {
      this.transport.log(`Resume recovery replacing unresponsive mobile connection (${source})`);
      this.transport.forceReconnect();
    } else {
      this.transport.log(`Resume recovery reconnecting after failed probe (${source})`);
      this.transport.triggerReconnect();
    }
  }
}
