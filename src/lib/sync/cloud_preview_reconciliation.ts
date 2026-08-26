export type CloudPreviewCheckMode = "all" | "restricted";

export interface CloudPreviewAttachmentCheckState {
  schema: 1;
  mode: CloudPreviewCheckMode;
  nextPath: string;
  complete: boolean;
  updatedAt: number;
}

export const createCloudPreviewCheckState = (mode: CloudPreviewCheckMode): CloudPreviewAttachmentCheckState => ({
  schema: 1,
  mode,
  nextPath: "",
  complete: false,
  updatedAt: Date.now(),
});

export const parseCloudPreviewCheckState = (
  raw: unknown,
  mode: CloudPreviewCheckMode,
): CloudPreviewAttachmentCheckState => {
  if (typeof raw !== "string" || raw.length === 0) return createCloudPreviewCheckState(mode);

  try {
    const parsed = JSON.parse(raw) as Partial<CloudPreviewAttachmentCheckState>;
    if (
      parsed.schema !== 1
      || parsed.mode !== mode
      || typeof parsed.nextPath !== "string"
      || typeof parsed.complete !== "boolean"
    ) {
      return createCloudPreviewCheckState(mode);
    }

    return {
      schema: 1,
      mode,
      nextPath: parsed.nextPath,
      complete: parsed.complete,
      updatedAt: typeof parsed.updatedAt === "number" ? parsed.updatedAt : Date.now(),
    };
  } catch {
    return createCloudPreviewCheckState(mode);
  }
};

export const advanceCloudPreviewCheckState = (
  state: CloudPreviewAttachmentCheckState,
  path: string,
): CloudPreviewAttachmentCheckState => ({
  ...state,
  nextPath: path,
  updatedAt: Date.now(),
});

export const completeCloudPreviewCheckState = (
  state: CloudPreviewAttachmentCheckState,
): CloudPreviewAttachmentCheckState => ({
  ...state,
  complete: true,
  updatedAt: Date.now(),
});

export const serializeCloudPreviewCheckState = (state: CloudPreviewAttachmentCheckState): string => JSON.stringify(state);
