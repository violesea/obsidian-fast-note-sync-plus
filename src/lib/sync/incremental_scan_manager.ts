import type FastSync from "../../main";
import { LocalStateFileMirror, dump } from "../utils/helpers";
import { waitForForeground } from "./background_activity_gate";

export type DirtyKind = "note" | "file" | "folder" | "config";
export type DirtyOperation = "modify" | "delete";
export type DirtyAckResult = "acked" | "stale" | "untracked";

export interface DirtyEntry {
  kind: DirtyKind;
  operation: DirtyOperation;
  path: string;
  version: number;
  /** Reconciliation candidates may reuse a valid local hash. */
  forceHash?: boolean;
  sentVersion?: number;
}

export interface DirtySnapshot {
  entries: DirtyEntry[];
}

export const incrementalEntryKey = (kind: DirtyKind, path: string): string => {
  return `${kind}:${path.replace(/\\/g, "/")}`;
};

export const mergeDirtyEntries = (...groups: DirtyEntry[][]): DirtyEntry[] => {
  const entries = new Map<string, DirtyEntry>();
  for (const group of groups) {
    for (const entry of group) {
      const key = incrementalEntryKey(entry.kind, entry.path);
      const previous = entries.get(key);
      if (!previous || entry.version >= previous.version) entries.set(key, { ...entry });
    }
  }
  return Array.from(entries.values());
};

interface PersistedState {
  schema: 2;
  nextVersion: number;
  /** Monotonic proof of the last whole-round baseline commit. */
  committedBaselineEpoch: number;
  /** A requested repair is in progress; it does not revoke the last commit. */
  repairRequested: boolean;
  /** Compatibility fields kept for probes and older state readers. */
  completedInitialSync: boolean;
  localBaselineReady: boolean;
  serverBaselineReady: boolean;
  needsFullReconcile: boolean;
  entries: Record<string, DirtyEntry>;
}

const EMPTY_STATE = (): PersistedState => ({
  schema: 2,
  nextVersion: 1,
  committedBaselineEpoch: 0,
  repairRequested: false,
  completedInitialSync: false,
  localBaselineReady: false,
  serverBaselineReady: false,
  needsFullReconcile: false,
  entries: {},
});

/**
 * Durable path queue for changes observed while the WebSocket is unavailable.
 * A successful full sync establishes the baseline; incremental syncs then read
 * only the queued paths and retain entries that were not processed.
 */
export class IncrementalScanManager {
  private plugin: FastSync;
  private readonly storageKey: string;
  private readonly legacyStorageKey: string;
  private readonly mirror: LocalStateFileMirror;
  private state: PersistedState = EMPTY_STATE();
  private initialized = false;
  private activeSnapshot: DirtySnapshot | null = null;
  private activeProcessedKeys = new Set<string>();
  private activeFullReconcile = false;
  private activeStartVersion = 1;

  constructor(plugin: FastSync) {
    this.plugin = plugin;
    this.storageKey = "fns-incrementalScanState";
    this.legacyStorageKey = `fns-${this.plugin.app.vault.getName()}-incrementalScanState`;
    this.mirror = new LocalStateFileMirror(plugin, "incrementalScanState.json");
  }

  async initialize(): Promise<void> {
    const localKeys = [this.storageKey, this.legacyStorageKey];
    for (const key of localKeys) {
      const local = this.plugin.app.loadLocalStorage(key) as string | null;
      if (local && this.load(local)) {
        // Always persist the normalized schema. This also upgrades 2.5.22's
        // split baseline flags into the explicit committed epoch model.
        this.save();
        this.initialized = true;
        return;
      }
    }

    const mirrored = await this.mirror.read();
    if (mirrored && this.load(mirrored)) {
      this.save();
      this.initialized = true;
      dump("IncrementalScanManager: restored state from file mirror");
      return;
    }

    this.initialized = true;
    this.save();
  }

  isReady(): boolean {
    return this.initialized;
  }

  canUseIncrementalSync(localStorageInitialSync: unknown): boolean {
    if (!this.initialized) return false;
    this.adoptLegacyCommittedBaseline(localStorageInitialSync);
    if (this.state.repairRequested) return false;
    if (this.state.localBaselineReady && this.state.committedBaselineEpoch > 0) return true;
    // A state-less installation of the fork must perform one calibration scan.
    // The official plugin's isInitSync flag proves that it completed a sync at
    // some point, but it cannot prove that offline edits were observed because
    // the official event path did not persist a dirty queue.
    void localStorageInitialSync;
    return false;
  }

  /**
   * A local hash/metadata index is enough to avoid recomputing content hashes,
   * but it is not enough to skip the first server reconciliation. This state is
   * intentionally separate from completedInitialSync.
   */
  canUseMetadataReconciliation(): boolean {
    return this.initialized
      && !this.state.repairRequested
      && this.state.localBaselineReady
      && this.state.committedBaselineEpoch === 0;
  }

  markLocalBaselineReady(): void {
    if (this.state.localBaselineReady) return;
    this.state.localBaselineReady = true;
    this.save();
  }

  invalidateLocalBaseline(): void {
    this.state.localBaselineReady = false;
    this.state.repairRequested = true;
    this.save();
  }

  getPendingCount(): number {
    return Object.keys(this.state.entries).length;
  }

  /**
   * Return incremental snapshot entries that were neither processed nor
   * acknowledged during the active logical round.
   *
   * A full reconciliation deliberately returns zero here: its vault-wide
   * scan is the coverage proof for the captured snapshot. For an event-only
   * round, however, an unprocessed entry means the client must not report a
   * successful sync and discard the session context.
   */
  getActiveUnprocessedCount(): number {
    if (!this.activeSnapshot || this.activeFullReconcile) return 0;

    let count = 0;
    for (const entry of this.activeSnapshot.entries) {
      const key = incrementalEntryKey(entry.kind, entry.path);
      const current = this.state.entries[key];

      // An ACK may have removed the entry without the scanner marking it
      // processed. That is still a safe terminal state.
      if (!current) continue;

      // A newer event must survive this round even if the older snapshot
      // entry was already processed.
      if (current.version > entry.version || !this.activeProcessedKeys.has(key)) {
        count++;
      }
    }
    return count;
  }

  markInitialSyncComplete(): void {
    this.state.committedBaselineEpoch = Math.max(1, this.state.committedBaselineEpoch);
    this.state.localBaselineReady = true;
    this.state.repairRequested = false;
    this.save();
  }

  requestFullReconcile(): void {
    // Two-phase baseline protocol: requesting/starting a repair cannot erase
    // the last committed epoch. The repair flag alone blocks incremental use
    // until a whole zero-failure round commits a newer epoch.
    this.state.repairRequested = true;
    this.save();
  }

  markModified(kind: DirtyKind, path: string): number {
    return this.upsert(kind, "modify", path);
  }

  markDeleted(kind: DirtyKind, path: string): number {
    return this.upsert(kind, "delete", path);
  }

  markRenamed(kind: DirtyKind, oldPath: string, newPath: string): void {
    this.markDeleted(kind, oldPath);
    this.markModified(kind, newPath);
  }

  /** Mark the exact current journal version as sent on the wire. */
  markSent(kind: DirtyKind, path: string): number | null {
    const normalizedPath = path.replace(/\\/g, "/");
    const entry = this.state.entries[this.key(kind, normalizedPath)];
    if (!entry) return null;
    entry.sentVersion = entry.version;
    this.save();
    return entry.version;
  }

  /**
   * Remove a journal entry only when an ACK can be associated with the version
   * that was sent. A newer local event intentionally survives an old ACK.
   */
  acknowledge(kind: DirtyKind, path: string, sentVersion?: number): DirtyAckResult {
    const normalizedPath = path.replace(/\\/g, "/");
    const key = this.key(kind, normalizedPath);
    const entry = this.state.entries[key];
    if (!entry) return "untracked";
    const expectedVersion = sentVersion ?? entry.sentVersion;
    if (expectedVersion === undefined || entry.version > expectedVersion) return "stale";
    delete this.state.entries[key];
    this.save();
    return "acked";
  }

  beginSync(fullReconcile = false): DirtySnapshot {
    // Beginning work is not a commit operation. In particular, a full repair
    // must leave the previous committed baseline intact across an iOS reload.
    const entries = Object.values(this.state.entries).map((entry) => ({ ...entry }));
    this.activeSnapshot = { entries };
    this.activeProcessedKeys.clear();
    this.activeFullReconcile = fullReconcile;
    this.activeStartVersion = this.state.nextVersion;
    return { entries };
  }

  markProcessed(processedKeys: Iterable<string>): void {
    if (!this.activeSnapshot) return;
    for (const key of processedKeys) this.activeProcessedKeys.add(key);
  }

  completeSync(fullSync = this.activeFullReconcile, processedKeys: Iterable<string> = this.activeProcessedKeys): void {
    if (fullSync) {
      // A full scan covers the queue captured at its start. Keep events that
      // arrived after that point so a scan racing with a local edit cannot
      // silently lose the edit.
      this.state.entries = Object.fromEntries(
        Object.entries(this.state.entries).filter(([, entry]) => entry.version >= this.activeStartVersion),
      );
      this.state.committedBaselineEpoch = Math.max(1, this.state.committedBaselineEpoch + 1);
      this.state.repairRequested = false;
      this.state.localBaselineReady = true;
    } else if (this.activeSnapshot) {
      const processed = new Set(processedKeys);
      for (const entry of this.activeSnapshot.entries) {
        const key = incrementalEntryKey(entry.kind, entry.path);
        if (!processed.has(key)) continue;
        const current = this.state.entries[key];
        if (current && current.version <= entry.version) delete this.state.entries[key];
      }
    }
    this.activeSnapshot = null;
    this.activeProcessedKeys.clear();
    this.activeFullReconcile = false;
    this.activeStartVersion = 1;
    this.save();
  }

  abortSync(): void {
    this.activeSnapshot = null;
    this.activeProcessedKeys.clear();
    this.activeFullReconcile = false;
  }

  /** End an unsuccessful round without advancing any committed state. */
  failSync(): void {
    this.abortSync();
    // Persist normalized compatibility fields even when this was the first
    // operation after a legacy schema load. Existing repairRequested remains.
    this.save();
  }

  flush(): void {
    if (this.plugin.backgroundActivityGate?.isBackgrounded || this.plugin.backgroundActivityGate?.isClosed) return;
    this.mirror.flush();
  }

  async flushAsync(): Promise<void> {
    if (!(await waitForForeground(this.plugin))) return;
    await this.mirror.flushAsync();
  }

  private upsert(kind: DirtyKind, operation: DirtyOperation, path: string): number {
    if (!path) return 0;
    const normalizedPath = path.replace(/\\/g, "/");
    const version = this.state.nextVersion++;
    this.state.entries[this.key(kind, normalizedPath)] = {
      kind,
      operation,
      path: normalizedPath,
      version,
    };
    this.save();
    return version;
  }

  private key(kind: DirtyKind, path: string): string {
    return incrementalEntryKey(kind, path);
  }

  private load(raw: string): boolean {
    try {
      const parsed = JSON.parse(raw) as Partial<Omit<PersistedState, "schema">> & { schema?: 1 | 2 };
      if ((parsed.schema !== 1 && parsed.schema !== 2) || typeof parsed.entries !== "object" || parsed.entries === null) return false;
      if (!Number.isSafeInteger(parsed.nextVersion) || (parsed.nextVersion as number) < 1) return false;

      const entries: Record<string, DirtyEntry> = {};
      for (const value of Object.values(parsed.entries as Record<string, unknown>)) {
        if (!value || typeof value !== "object") return false;
        const entry = value as Partial<DirtyEntry>;
        if (!entry.path || typeof entry.path !== "string") return false;
        if (entry.kind !== "note" && entry.kind !== "file" && entry.kind !== "folder" && entry.kind !== "config") return false;
        if (entry.operation !== "modify" && entry.operation !== "delete") return false;
        if (typeof entry.version !== "number" || !Number.isSafeInteger(entry.version) || entry.version < 1) return false;
        const normalizedPath = entry.path.replace(/\\/g, "/");
        entries[incrementalEntryKey(entry.kind, normalizedPath)] = {
          kind: entry.kind,
          operation: entry.operation,
          path: normalizedPath,
          version: entry.version,
          ...(typeof entry.sentVersion === "number" && Number.isSafeInteger(entry.sentVersion) && entry.sentVersion >= 1
            ? { sentVersion: entry.sentVersion }
            : {}),
        };
      }

      const legacyCommitted = parsed.completedInitialSync === true || parsed.serverBaselineReady === true;
      const parsedEpoch = Number.isSafeInteger(parsed.committedBaselineEpoch)
        && (parsed.committedBaselineEpoch as number) >= 0
        ? parsed.committedBaselineEpoch as number
        : legacyCommitted ? 1 : 0;
      const repairRequested = parsed.schema === 2
        ? parsed.repairRequested === true
        : parsed.needsFullReconcile === true;

      this.state = {
        schema: 2,
        nextVersion: parsed.nextVersion as number,
        committedBaselineEpoch: parsedEpoch,
        repairRequested,
        completedInitialSync: parsedEpoch > 0,
        localBaselineReady: parsed.localBaselineReady === true || parsed.completedInitialSync === true,
        serverBaselineReady: parsedEpoch > 0,
        needsFullReconcile: repairRequested,
        entries,
      };
      return true;
    } catch (error) {
      dump("IncrementalScanManager: state parse failed", error);
      return false;
    }
  }

  private save(): void {
    this.refreshCompatibilityFields();
    const raw = JSON.stringify(this.state);
    try {
      this.plugin.app.saveLocalStorage(this.storageKey, raw);
    } catch (error) {
      dump("IncrementalScanManager: localStorage write failed", error);
    }
    this.mirror.scheduleWrite(raw);
  }

  private refreshCompatibilityFields(): void {
    const committed = this.state.committedBaselineEpoch > 0;
    this.state.completedInitialSync = committed;
    this.state.serverBaselineReady = committed;
    this.state.needsFullReconcile = this.state.repairRequested;
  }

  /**
   * Repair the exact 2.5.22 split-ledger state. isInitSync alone is not enough:
   * only the fork-specific zero-failure timestamp proves that this manager had
   * previously committed a clean round before beginSync revoked its flags.
   */
  private adoptLegacyCommittedBaseline(localStorageInitialSync: unknown): void {
    if (this.state.committedBaselineEpoch > 0
      || this.state.repairRequested
      || !this.state.localBaselineReady
      || localStorageInitialSync !== true) return;
    const lastSuccess = Number(this.plugin.localStorageManager?.getMetadata("lastSyncSuccessTime"));
    if (!Number.isFinite(lastSuccess) || lastSuccess <= 0) return;
    this.state.committedBaselineEpoch = 1;
    this.save();
    dump("IncrementalScanManager: recovered 2.5.22 split baseline from proven clean round");
  }
}
