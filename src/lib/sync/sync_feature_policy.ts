/**
 * Release policy for sync features that are not part of the stable v1
 * WebSocket path.
 *
 * The experimental change-feed and cloud-preview paths remain in the source
 * tree for later redesign, but a released client must not let either path
 * change the meaning of a normal note/file sync. Re-enable them only after a
 * separate end-to-end validation proves content completeness and recovery.
 */

export const EXPERIMENTAL_SYNC_FEATURES_ENABLED = false;

export type ExperimentalSyncSettings = {
  changeFeedEnabled?: boolean;
  cloudPreviewEnabled?: boolean;
  cloudPreviewAutoDeleteLocal?: boolean;
  cloudPreviewDynamicAttachment?: boolean;
};

export type DisabledSyncFeature =
  | "change-feed"
  | "cloud-preview"
  | "cloud-preview-auto-delete"
  | "cloud-preview-dynamic-attachment";

export interface StableSyncPolicyResult<T extends ExperimentalSyncSettings> {
  settings: T;
  disabledFeatures: DisabledSyncFeature[];
}

export function isChangeFeedRuntimeEnabled(settings: ExperimentalSyncSettings): boolean {
  return EXPERIMENTAL_SYNC_FEATURES_ENABLED && settings.changeFeedEnabled === true;
}

export function isCloudPreviewRuntimeEnabled(settings: ExperimentalSyncSettings): boolean {
  return EXPERIMENTAL_SYNC_FEATURES_ENABLED && settings.cloudPreviewEnabled === true;
}

/**
 * Normalize persisted settings at every load/save boundary. Do not clear
 * endpoint/token fields: they are user configuration and may be reused when
 * the experimental path is redesigned. Only behavior switches are disabled.
 */
export function applyStableSyncPolicy<T extends ExperimentalSyncSettings>(settings: T): StableSyncPolicyResult<T> {
  const next = { ...settings };
  const disabledFeatures: DisabledSyncFeature[] = [];

  if (!EXPERIMENTAL_SYNC_FEATURES_ENABLED && next.changeFeedEnabled) {
    next.changeFeedEnabled = false;
    disabledFeatures.push("change-feed");
  }
  if (!EXPERIMENTAL_SYNC_FEATURES_ENABLED && next.cloudPreviewEnabled) {
    next.cloudPreviewEnabled = false;
    disabledFeatures.push("cloud-preview");
  }
  if (!EXPERIMENTAL_SYNC_FEATURES_ENABLED && next.cloudPreviewAutoDeleteLocal) {
    next.cloudPreviewAutoDeleteLocal = false;
    disabledFeatures.push("cloud-preview-auto-delete");
  }
  if (!EXPERIMENTAL_SYNC_FEATURES_ENABLED && next.cloudPreviewDynamicAttachment) {
    next.cloudPreviewDynamicAttachment = false;
    disabledFeatures.push("cloud-preview-dynamic-attachment");
  }

  return { settings: next, disabledFeatures };
}
