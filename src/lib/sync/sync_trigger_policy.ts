export type SyncRequestType = "incremental" | "full";
export type SyncRequestMode = "auto" | "note" | "config";

export interface PendingSyncRequest {
  type: SyncRequestType;
  mode: SyncRequestMode;
}

export interface AuthSyncDecisionInput {
  hasActiveSync: boolean;
  canUseIncremental: boolean;
  manualSyncEnabled: boolean;
  pendingRequest: PendingSyncRequest | null;
}

export type AuthSyncDecision =
  | { kind: "resume-active" }
  | {
      kind: "start";
      isLoadLastTime: boolean;
      syncMode: SyncRequestMode;
      reason: "explicit-request" | "initial-full" | "reconnect-incremental";
    }
  | { kind: "none"; reason: "manual-mode" };

/**
 * Decide what an authenticated transport should do.
 *
 * Authentication establishes a transport only. It may resume an active logical
 * sync, consume one explicit request, or run the normal catch-up round. It must
 * never confuse a reconnect with a new full reconciliation.
 */
export function decideSyncAfterAuthentication(input: AuthSyncDecisionInput): AuthSyncDecision {
  if (input.hasActiveSync) {
    return { kind: "resume-active" };
  }

  if (input.pendingRequest) {
    return {
      kind: "start",
      isLoadLastTime: input.pendingRequest.type === "incremental",
      syncMode: input.pendingRequest.mode,
      reason: "explicit-request",
    };
  }

  if (input.manualSyncEnabled) {
    return { kind: "none", reason: "manual-mode" };
  }

  return {
    kind: "start",
    // A fork installation needs one calibration reconciliation. Once the
    // baseline exists, an empty local list is still useful: the server can
    // return changes made by other devices since lastTime.
    isLoadLastTime: input.canUseIncremental,
    syncMode: "auto",
    reason: input.canUseIncremental ? "reconnect-incremental" : "initial-full",
  };
}
