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

/**
 * The read-only revision feed has its own production gate.  It must not be
 * coupled to cloud preview: enabling the feed only changes how remote changes
 * are discovered, while cloud preview can delete or defer local content.
 */
export const CHANGE_FEED_RUNTIME_AVAILABLE = true;
export const CHANGE_FEED_ROLLOUT_VERSION = 1;

export type ExperimentalSyncSettings = {
  changeFeedEnabled?: boolean;
  changeFeedRolloutVersion?: number;
  sidecarUrl?: string;
  sidecarToken?: string;
  cloudPreviewEnabled?: boolean;
  cloudPreviewAutoDeleteLocal?: boolean;
  cloudPreviewDynamicAttachment?: boolean;
};

export type DisabledSyncFeature =
  | "cloud-preview"
  | "cloud-preview-auto-delete"
  | "cloud-preview-dynamic-attachment";

export interface StableSyncPolicyResult<T extends ExperimentalSyncSettings> {
  settings: T;
  disabledFeatures: DisabledSyncFeature[];
}

export function isChangeFeedRuntimeEnabled(settings: ExperimentalSyncSettings): boolean {
  return CHANGE_FEED_RUNTIME_AVAILABLE
    && settings.changeFeedEnabled === true
    && typeof settings.sidecarUrl === "string"
    && settings.sidecarUrl.trim() !== ""
    && typeof settings.sidecarToken === "string"
    && settings.sidecarToken.trim() !== "";
}

export function isCloudPreviewRuntimeEnabled(settings: ExperimentalSyncSettings): boolean {
  return EXPERIMENTAL_SYNC_FEATURES_ENABLED && settings.cloudPreviewEnabled === true;
}

/**
 * Normalize persisted settings at every load/save boundary. Change-feed is a
 * stable discovery path now; only cloud projection behavior remains behind
 * the experimental gate.
 */
export function applyStableSyncPolicy<T extends ExperimentalSyncSettings>(settings: T): StableSyncPolicyResult<T> {
  const next = { ...settings };
  const disabledFeatures: DisabledSyncFeature[] = [];

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

export interface MobileChangeFeedRolloutResult<T extends ExperimentalSyncSettings> {
  settings: T;
  enabled: boolean;
  migrated: boolean;
}

/**
 * Move already-provisioned mobile devices off the 2.5.12 safety-off policy.
 * The marker is written exactly once so a later user choice to disable the
 * feed is preserved.  Devices without an endpoint/token are left untouched;
 * the settings UI can provision them without inventing or distributing a
 * credential in release code.
 */
export function applyMobileChangeFeedRollout<T extends ExperimentalSyncSettings>(
  settings: T,
  isMobile: boolean,
): MobileChangeFeedRolloutResult<T> {
  const next = { ...settings };
  const alreadyMigrated = (next.changeFeedRolloutVersion ?? 0) >= CHANGE_FEED_ROLLOUT_VERSION;
  const provisioned = typeof next.sidecarUrl === "string"
    && next.sidecarUrl.trim() !== ""
    && typeof next.sidecarToken === "string"
    && next.sidecarToken.trim() !== "";

  if (!isMobile || alreadyMigrated || !provisioned) {
    return {
      settings: next,
      enabled: isChangeFeedRuntimeEnabled(next),
      migrated: false,
    };
  }

  next.changeFeedEnabled = true;
  next.changeFeedRolloutVersion = CHANGE_FEED_ROLLOUT_VERSION;
  return {
    settings: next,
    enabled: isChangeFeedRuntimeEnabled(next),
    migrated: true,
  };
}
