export const CHANGE_FEED_HEALTH_SCHEMA = 1;
export const CHANGE_FEED_FALLBACK_ALERT_THRESHOLD = 3;

export interface ChangeFeedHealthState {
  schema: 1;
  consecutiveFallbacks: number;
  totalFallbacks: number;
  lastReason: string;
  lastFallbackAt: number;
  lastSuccessAt: number;
  alerted: boolean;
}

export const createChangeFeedHealthState = (): ChangeFeedHealthState => ({
  schema: CHANGE_FEED_HEALTH_SCHEMA,
  consecutiveFallbacks: 0,
  totalFallbacks: 0,
  lastReason: "",
  lastFallbackAt: 0,
  lastSuccessAt: 0,
  alerted: false,
});

export const parseChangeFeedHealthState = (raw: unknown): ChangeFeedHealthState => {
  if (typeof raw !== "string" || raw.length === 0) return createChangeFeedHealthState();

  try {
    const parsed = JSON.parse(raw) as Partial<ChangeFeedHealthState>;
    if (parsed.schema !== CHANGE_FEED_HEALTH_SCHEMA) return createChangeFeedHealthState();
    return {
      schema: CHANGE_FEED_HEALTH_SCHEMA,
      consecutiveFallbacks: Number.isFinite(parsed.consecutiveFallbacks) ? Math.max(0, Number(parsed.consecutiveFallbacks)) : 0,
      totalFallbacks: Number.isFinite(parsed.totalFallbacks) ? Math.max(0, Number(parsed.totalFallbacks)) : 0,
      lastReason: typeof parsed.lastReason === "string" ? parsed.lastReason : "",
      lastFallbackAt: Number.isFinite(parsed.lastFallbackAt) ? Number(parsed.lastFallbackAt) : 0,
      lastSuccessAt: Number.isFinite(parsed.lastSuccessAt) ? Number(parsed.lastSuccessAt) : 0,
      alerted: parsed.alerted === true,
    };
  } catch {
    return createChangeFeedHealthState();
  }
};

export const recordChangeFeedFallback = (
  state: ChangeFeedHealthState,
  reason: string,
  now = Date.now(),
): ChangeFeedHealthState => ({
  ...state,
  consecutiveFallbacks: state.consecutiveFallbacks + 1,
  totalFallbacks: state.totalFallbacks + 1,
  lastReason: reason,
  lastFallbackAt: now,
});

export const recordChangeFeedSuccess = (
  state: ChangeFeedHealthState,
  now = Date.now(),
): ChangeFeedHealthState => ({
  ...state,
  consecutiveFallbacks: 0,
  lastSuccessAt: now,
  alerted: false,
});

export const shouldAlertChangeFeedFallback = (state: ChangeFeedHealthState): boolean => (
  state.consecutiveFallbacks >= CHANGE_FEED_FALLBACK_ALERT_THRESHOLD && !state.alerted
);

export const markChangeFeedFallbackAlerted = (state: ChangeFeedHealthState): ChangeFeedHealthState => ({
  ...state,
  alerted: true,
});

export const serializeChangeFeedHealthState = (state: ChangeFeedHealthState): string => JSON.stringify(state);
