import type FastSync from "../../main";
import { LocalStateFileMirror, dump } from "../utils/helpers";

export type DirtyKind = "note" | "file" | "folder" | "config";
export type DirtyOperation = "modify" | "delete";

export interface DirtyEntry {
  kind: DirtyKind;
  operation: DirtyOperation;
  path: string;
  version: number;
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
  schema: 1;
  nextVersion: number;
  completedInitialSync: boolean;
  needsFullReconcile: boolean;
  entries: Record<string, DirtyEntry>;
}

const EMPTY_STATE = (): PersistedState => ({
  schema: 1,
  nextVersion: 1,
  completedInitialSync: false,
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
        if (key !== this.storageKey) this.save();
        else this.mirror.scheduleWrite(local);
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
    if (!this.initialized || this.state.needsFullReconcile) return false;
    if (this.state.completedInitialSync) return true;
    // A state-less installation of the fork must perform one calibration scan.
    // The official plugin's isInitSync flag proves that it completed a sync at
    // some point, but it cannot prove that offline edits were observed because
    // the official event path did not persist a dirty queue.
    void localStorageInitialSync;
    return false;
  }

  markInitialSyncComplete(): void {
    this.state.completedInitialSync = true;
    this.state.needsFullReconcile = false;
    this.save();
  }

  requestFullReconcile(): void {
    this.state.needsFullReconcile = true;
    this.save();
  }

  markModified(kind: DirtyKind, path: string): void {
    this.upsert(kind, "modify", path);
  }

  markDeleted(kind: DirtyKind, path: string): void {
    this.upsert(kind, "delete", path);
  }

  markRenamed(kind: DirtyKind, oldPath: string, newPath: string): void {
    this.markDeleted(kind, oldPath);
    this.markModified(kind, newPath);
  }

  beginSync(fullReconcile = false): DirtySnapshot {
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
      this.state.needsFullReconcile = false;
      this.state.completedInitialSync = true;
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

  flush(): void {
    this.mirror.flush();
  }

  private upsert(kind: DirtyKind, operation: DirtyOperation, path: string): void {
    if (!path) return;
    const normalizedPath = path.replace(/\\/g, "/");
    const version = this.state.nextVersion++;
    this.state.entries[this.key(kind, normalizedPath)] = {
      kind,
      operation,
      path: normalizedPath,
      version,
    };
    this.save();
  }

  private key(kind: DirtyKind, path: string): string {
    return incrementalEntryKey(kind, path);
  }

  private load(raw: string): boolean {
    try {
      const parsed = JSON.parse(raw) as Partial<PersistedState>;
      if (parsed.schema !== 1 || typeof parsed.entries !== "object" || parsed.entries === null) return false;
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
        };
      }

      this.state = {
        schema: 1,
        nextVersion: parsed.nextVersion as number,
        completedInitialSync: parsed.completedInitialSync === true,
        needsFullReconcile: parsed.needsFullReconcile === true,
        entries,
      };
      return true;
    } catch (error) {
      dump("IncrementalScanManager: state parse failed", error);
      return false;
    }
  }

  private save(): void {
    const raw = JSON.stringify(this.state);
    try {
      this.plugin.app.saveLocalStorage(this.storageKey, raw);
    } catch (error) {
      dump("IncrementalScanManager: localStorage write failed", error);
    }
    this.mirror.scheduleWrite(raw);
  }
}
