import {
  decideSyncAfterAuthentication,
  type AuthSyncDecision,
  type PendingSyncRequest,
} from "./sync_trigger_policy";

export interface AuthSyncCoordinatorHooks {
  /** True only for the physical connection that is still authenticated. */
  isCurrentConnection: (connectionId: number) => boolean;
  /** Wait for plugin-side prerequisites without losing the connection guard. */
  waitUntilReady: (isCurrent: () => boolean) => Promise<boolean>;
  hasActiveSync: () => boolean;
  canUseIncremental: () => boolean;
  manualSyncEnabled: () => boolean;
  getPendingRequest: () => PendingSyncRequest | null;
  consumePendingRequest: () => void;
  resumeActiveSync: () => void;
  startSync: (decision: Extract<AuthSyncDecision, { kind: "start" }>) => void;
  log: (message: string) => void;
}

/**
 * Owns the hand-off from a physical WebSocket authentication to a logical
 * sync round.
 *
 * Authentication is a transport event, not a sync request. A mobile app can
 * authenticate several sockets while one scan is still waiting on the hash
 * manager. This coordinator makes the hand-off generation-aware and ensures a
 * stale authentication callback can neither start a second scan nor consume a
 * pending explicit request.
 */
export class AuthSyncCoordinator {
  private lastHandledConnectionId = 0;
  private dispatchGeneration = 0;

  constructor(private readonly hooks: AuthSyncCoordinatorHooks) {}

  /** Invalidate callbacks belonging to the socket that just disappeared. */
  public invalidate(): void {
    this.dispatchGeneration += 1;
  }

  /** Dispatch one authenticated physical connection. */
  public dispatch(connectionId: number): Promise<void> {
    if (connectionId <= 0 || connectionId <= this.lastHandledConnectionId) {
      this.hooks.log(`Authentication dispatch ignored for stale connection ${connectionId}`);
      return Promise.resolve();
    }

    this.lastHandledConnectionId = connectionId;
    const generation = ++this.dispatchGeneration;
    const isCurrent = () =>
      generation === this.dispatchGeneration && this.hooks.isCurrentConnection(connectionId);

    return this.runDispatch(connectionId, isCurrent);
  }

  private async runDispatch(connectionId: number, isCurrent: () => boolean): Promise<void> {
    if (!isCurrent()) return;

    if (!await this.hooks.waitUntilReady(isCurrent) || !isCurrent()) {
      this.hooks.log(`Authentication dispatch cancelled for connection ${connectionId}`);
      return;
    }

    const pendingRequest = this.hooks.getPendingRequest();
    const decision = decideSyncAfterAuthentication({
      hasActiveSync: this.hooks.hasActiveSync(),
      canUseIncremental: this.hooks.canUseIncremental(),
      manualSyncEnabled: this.hooks.manualSyncEnabled(),
      pendingRequest,
    });

    if (!isCurrent()) return;

    if (decision.kind === "resume-active") {
      this.hooks.log(`Authenticated connection ${connectionId} resumes the active sync session`);
      this.hooks.resumeActiveSync();
      return;
    }

    if (decision.kind === "none") {
      this.hooks.log("Automatic sync skipped because Full Manual Sync Mode is enabled");
      return;
    }

    this.hooks.consumePendingRequest();
    this.hooks.log(
      `Authenticated connection ${connectionId} dispatches ${decision.reason}, ` +
      `localScan=${decision.isLoadLastTime ? "incremental" : "full"}`,
    );
    this.hooks.startSync(decision);
  }
}
