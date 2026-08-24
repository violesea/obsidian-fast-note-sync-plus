import type { DirtyEntry } from "./incremental_scan_manager";

export interface ReconciliationLocalEntry {
  path: string;
  kind: "note" | "file" | "folder";
  mtime?: number;
  size?: number;
  ctime?: number;
}

export interface ReconciliationIndex {
  getValidHash: (path: string, mtime: number, size: number, ctime?: number) => string | null;
  getPathHash: (path: string) => string | null;
  getAllPaths: () => string[];
  getFolderMtime: (path: string) => number | null;
  getAllFolderPaths: () => string[];
}

export interface ReconciliationOptions {
  dirtyKeys: ReadonlySet<string>;
  isPathExcluded: (path: string) => boolean;
  isFolderPathExcluded: (path: string) => boolean;
  isIgnoredPath?: (path: string) => boolean;
}

const entryKey = (kind: DirtyEntry["kind"], path: string): string => {
  return `${kind}:${path.replace(/\\/g, "/")}`;
};

const isTrackedLocalEntry = (
  entry: ReconciliationLocalEntry,
  index: ReconciliationIndex,
): boolean => {
  if (entry.kind === "folder") return index.getFolderMtime(entry.path) !== null;

  const localHash = index.getValidHash(entry.path, entry.mtime ?? 0, entry.size ?? 0, entry.ctime);
  const baseHash = index.getPathHash(entry.path);

  // A path is safe to omit only when both local metadata/hash state and the
  // server-confirmed baseline are present and equal. This is metadata-only
  // work; content hashing happens later only for returned candidates.
  return localHash !== null && baseHash !== null && localHash === baseHash;
};

/**
 * Find local paths that an event-only sync could miss without hashing the
 * whole vault. The caller enumerates metadata, then hashes only the returned
 * candidates. A candidate remains discoverable until an ACK updates the
 * server-confirmed baseline.
 */
export const collectIncrementalReconciliationEntries = (
  localEntries: ReconciliationLocalEntry[],
  index: ReconciliationIndex,
  options: ReconciliationOptions,
): DirtyEntry[] => {
  const candidates: DirtyEntry[] = [];
  const localFilePaths = new Set<string>();
  const localFolderPaths = new Set<string>();

  for (const entry of localEntries) {
    if (entry.kind === "folder") {
      localFolderPaths.add(entry.path);
      if (entry.path === "/"
        || options.isFolderPathExcluded(entry.path)
        || options.isIgnoredPath?.(entry.path)) continue;

      const key = entryKey("folder", entry.path);
      if (options.dirtyKeys.has(key)) continue;
      if (!isTrackedLocalEntry(entry, index)) {
        candidates.push({ kind: "folder", operation: "modify", path: entry.path, version: 0 });
      }
      continue;
    }

    localFilePaths.add(entry.path);
    if (options.isPathExcluded(entry.path) || options.isIgnoredPath?.(entry.path)) continue;

    const key = entryKey(entry.kind, entry.path);
    if (options.dirtyKeys.has(key)) continue;

    if (!isTrackedLocalEntry(entry, index)) {
      // Reconciliation may reuse a valid local hash. Event-driven entries
      // still force a fresh hash in scanIncrementalVaultEntries.
      candidates.push({ kind: entry.kind, operation: "modify", path: entry.path, version: 0, forceHash: false });
    }
  }

  // The local file still existing is the easy case. Compare the server
  // baseline paths too, so a missed delete/rename cannot leave a stale remote
  // note forever. addDeletedPath decides delete vs missing according to the
  // user's offline-delete setting.
  for (const path of index.getAllPaths()) {
    if (localFilePaths.has(path) || options.isPathExcluded(path)) continue;
    const kind = path.endsWith(".md") ? "note" : "file";
    if (!options.dirtyKeys.has(entryKey(kind, path))) {
      candidates.push({ kind, operation: "delete", path, version: 0 });
    }
  }

  for (const path of index.getAllFolderPaths()) {
    if (localFolderPaths.has(path)
      || path === "/"
      || options.isFolderPathExcluded(path)
      || options.dirtyKeys.has(entryKey("folder", path))) continue;
    candidates.push({ kind: "folder", operation: "delete", path, version: 0 });
  }

  return candidates;
};
