/**
 * Optimistic write precondition (M7).
 *
 * The upload path historically had no precondition: a client decided from its
 * own hashes whether to send, and the server accepted every write. When two
 * devices edited the same path between one device's last server ACK and its
 * next upload, the later upload silently replaced the other device's content
 * and no side was told.
 *
 * The check is the same shape CloudKit uses for `ifServerRecordUnchanged`:
 * compare the server's current state against the baseline this device last had
 * confirmed. Equal means the server has not moved since our ACK and overwriting
 * is safe. Different means both sides moved, which is a conflict that must not
 * be resolved by overwriting.
 *
 * The module is pure so the decision table is testable without a vault, a
 * transport or a server.
 */

export type WriteDecisionKind = "upload" | "skip" | "conflict";

export type WriteDecisionReason =
  /** No confirmed baseline exists yet (first sync, cleared state). Nothing to protect. */
  | "no-baseline"
  /** The server state could not be read. Fail open so a probe failure never stalls sync. */
  | "precondition-unavailable"
  /** Server still holds exactly what this device last had ACKed. Safe to overwrite. */
  | "server-matches-baseline"
  /** Server already holds our current content. Nothing to send. */
  | "local-matches-server"
  /** Server moved since our baseline and local content differs from it. */
  | "server-moved-and-local-diverged";

export interface WriteDecision {
  kind: WriteDecisionKind;
  reason: WriteDecisionReason;
}

export interface WritePreconditionInput {
  /** Hash of the content this device is about to upload. */
  localHash: string;
  /** Last server-ACKed hash for this path, or null when no baseline exists. */
  baseHash: string | null;
  /** Server's current hash for this path. null means unknown or unreachable. */
  serverHash: string | null;
}

/**
 * Decide whether an upload may proceed.
 *
 * Order matters. The two fail-open branches come first so that a missing
 * baseline or an unreadable server can never be reported as a conflict.
 */
export const decideWrite = (input: WritePreconditionInput): WriteDecision => {
  if (input.baseHash === null) return { kind: "upload", reason: "no-baseline" };
  if (input.serverHash === null) return { kind: "upload", reason: "precondition-unavailable" };
  if (input.serverHash === input.baseHash) return { kind: "upload", reason: "server-matches-baseline" };
  if (input.serverHash === input.localHash) return { kind: "skip", reason: "local-matches-server" };
  return { kind: "conflict", reason: "server-moved-and-local-diverged" };
};

/**
 * Whether this upload is worth one extra round trip.
 *
 * The probe is only meaningful when a baseline exists to compare against and
 * the local content has actually diverged from it. Cost therefore tracks the
 * number of changed notes, not the size of the vault.
 */
export const shouldCheckPrecondition = (params: {
  enabled: boolean;
  baseHash: string | null;
  localHash: string;
}): boolean => (
  params.enabled
  && params.baseHash !== null
  && params.localHash !== params.baseHash
);

export interface WritePreconditionCounters {
  checked: number;
  conflicts: number;
  unavailable: number;
  skipped: number;
}

export const createWritePreconditionCounters = (): WritePreconditionCounters => ({
  checked: 0,
  conflicts: 0,
  unavailable: 0,
  skipped: 0,
});

export const countWriteDecision = (
  counters: WritePreconditionCounters,
  decision: WriteDecision,
): WritePreconditionCounters => {
  const next = { ...counters, checked: counters.checked + 1 };
  if (decision.kind === "conflict") next.conflicts += 1;
  else if (decision.reason === "precondition-unavailable") next.unavailable += 1;
  else if (decision.kind === "skip") next.skipped += 1;
  return next;
};
