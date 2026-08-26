/**
 * Inputs used by the final sync completion gate.
 *
 * Completion is a protocol claim, not a UI estimate.  The caller supplies
 * the current progress snapshot so this pure function can be tested without
 * constructing an Obsidian runtime.
 */
export interface SyncCompletionGateInput {
  allSyncDone: boolean;
  allDownloadsComplete: boolean;
  bufferCleared: boolean;
  isSyncRequesting: boolean;
  syncPhase: string;
  activeUnprocessedCount: number;
  pendingNoteModifies: number;
  pendingUploadHashes: number;
  pendingConfigModifies: number;
  pendingFileUploadAcks: number;
  pendingNoteDeleteAcks: number;
  pendingFileDeleteAcks: number;
  pendingConfigDeleteAcks: number;
  pendingNoteRenames: number;
  pendingFileRenames: number;
  pendingDeleteNotePaths: number;
  pendingDeleteFilePaths: number;
  pendingDeleteFolderPaths: number;
  pendingDeleteConfigPaths: number;
  syncPageAckOutbox: number;
  activeUploads: number;
}

/**
 * Return true only when the logical round has drained every reliable queue.
 *
 * A SyncEnd frame and a 100% progress estimate are insufficient: an upload
 * ACK, page ACK, rename, delete, or dirty-journal entry may still be in
 * flight.  Keeping this rule pure makes it difficult for a future caller to
 * accidentally omit one of those state sources.
 */
export const canCompleteSync = (input: SyncCompletionGateInput): boolean => {
  return input.allSyncDone
    && input.allDownloadsComplete
    && input.bufferCleared
    && !input.isSyncRequesting
    && input.syncPhase === "monitoring"
    && input.activeUnprocessedCount === 0
    && input.pendingNoteModifies === 0
    && input.pendingUploadHashes === 0
    && input.pendingConfigModifies === 0
    && input.pendingFileUploadAcks === 0
    && input.pendingNoteDeleteAcks === 0
    && input.pendingFileDeleteAcks === 0
    && input.pendingConfigDeleteAcks === 0
    && input.pendingNoteRenames === 0
    && input.pendingFileRenames === 0
    && input.pendingDeleteNotePaths === 0
    && input.pendingDeleteFilePaths === 0
    && input.pendingDeleteFolderPaths === 0
    && input.pendingDeleteConfigPaths === 0
    && input.syncPageAckOutbox === 0
    && input.activeUploads === 0;
};
