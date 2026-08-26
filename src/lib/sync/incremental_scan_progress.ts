export interface IncrementalScanProgress {
  processed: number;
  total: number;
  percent: number;
}

/**
 * Keep incremental scan progress accounting independent from the scanner's
 * note/file/config branches so every processed entry advances one shared total.
 */
export const createIncrementalScanProgress = (
  total: number,
  onProgress: (progress: IncrementalScanProgress) => void,
): { step: () => void; completeEmpty: () => void } => {
  const normalizedTotal = Math.max(0, total);
  let processed = 0;

  const publish = (): void => {
    const percent = normalizedTotal > 0
      ? Math.min(100, Math.floor((processed / normalizedTotal) * 100))
      : 100;
    onProgress({ processed, total: normalizedTotal, percent });
  };

  return {
    step: () => {
      processed = Math.min(normalizedTotal, processed + 1);
      publish();
    },
    completeEmpty: () => {
      if (normalizedTotal === 0) publish();
    },
  };
};
