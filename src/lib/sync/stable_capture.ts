/**
 * Stable local-file capture for event-driven synchronization.
 *
 * A capture is accepted only when the file's mtime and size are unchanged
 * across the quiet window, and the content hash agrees across two reads. The
 * caller may omit read() for binary files whose hash function already reads
 * the file through a bounded/sampled reader.
 */

export const DEFAULT_STABILITY_WINDOW_MS = 15_000;

export interface StableCaptureStat {
  size: number;
  mtime: number;
  ctime?: number;
}

export interface StableCapture<T> {
  value: T | undefined;
  hash: string;
  stat: StableCaptureStat;
}

export interface StableCaptureOptions<T> {
  stat: () => Promise<StableCaptureStat | null>;
  /** Optional value reader. Binary callers can omit this and hash in hash(). */
  read?: () => Promise<T>;
  hash: (value: T | undefined) => Promise<string>;
  stabilityWindowMs?: number;
  wait?: (milliseconds: number) => Promise<void>;
}

const defaultWait = (milliseconds: number): Promise<void> => new Promise((resolve) => {
  window.setTimeout(resolve, milliseconds);
});

export const sameStableStat = (left: StableCaptureStat, right: StableCaptureStat): boolean => (
  left.mtime === right.mtime && left.size === right.size
);

const readSample = async <T>(options: StableCaptureOptions<T>): Promise<T | undefined> => (
  options.read ? await options.read() : undefined
);

/**
 * Capture one settled version. A null result is an expected unstable-file
 * outcome; I/O and hashing errors remain exceptions for the caller to handle.
 */
export async function captureStableSnapshot<T>(
  options: StableCaptureOptions<T>,
): Promise<StableCapture<T> | null> {
  const initialStat = await options.stat();
  if (!initialStat) return null;

  const firstValue = await readSample(options);
  const firstHash = await options.hash(firstValue);
  const firstAfterReadStat = await options.stat();
  if (!firstAfterReadStat || !sameStableStat(initialStat, firstAfterReadStat)) return null;

  await (options.wait ?? defaultWait)(options.stabilityWindowMs ?? DEFAULT_STABILITY_WINDOW_MS);

  const secondBeforeReadStat = await options.stat();
  if (!secondBeforeReadStat || !sameStableStat(firstAfterReadStat, secondBeforeReadStat)) return null;

  const secondValue = await readSample(options);
  const secondHash = await options.hash(secondValue);
  const finalStat = await options.stat();
  if (!finalStat || !sameStableStat(secondBeforeReadStat, finalStat)) return null;
  if (firstHash !== secondHash) return null;

  return { value: secondValue, hash: secondHash, stat: finalStat };
}

/** Coalesce concurrent modify events for the same vault/path. */
export class StableCaptureCoordinator {
  private readonly inFlight = new Map<string, {
    promise: Promise<StableCapture<unknown> | null>;
    retryRequested: boolean;
  }>();

  capture<T>(key: string, task: () => Promise<StableCapture<T> | null>): Promise<StableCapture<T> | null> {
    const existing = this.inFlight.get(key);
    if (existing) {
      // A later event during an unstable capture must get another quiet
      // window; otherwise the event that observed the write is lost with the
      // first null result. A successful first capture still remains shared.
      existing.retryRequested = true;
      return existing.promise as Promise<StableCapture<T> | null>;
    }

    const entry: {
      promise: Promise<StableCapture<unknown> | null>;
      retryRequested: boolean;
    } = {
      promise: Promise.resolve(null),
      retryRequested: false,
    };
    const run = async (): Promise<StableCapture<T> | null> => {
      let result = await task();
      while (result === null && entry.retryRequested) {
        entry.retryRequested = false;
        result = await task();
      }
      return result;
    };
    entry.promise = Promise.resolve().then(run);
    this.inFlight.set(key, entry);
    const clear = () => {
      if (this.inFlight.get(key) === entry) this.inFlight.delete(key);
    };
    void entry.promise.then(clear, clear);
    return entry.promise as Promise<StableCapture<T> | null>;
  }
}

export const stableCaptureCoordinator = new StableCaptureCoordinator();
