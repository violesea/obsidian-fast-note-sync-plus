/**
 * FNS v2 变更流 · 纯逻辑层（无 obsidian 依赖，可在 node 测试 VM 中直接运行）。
 *
 * 职责：
 * - 变更流轮次的模式判定（planChangeFeedRound）
 * - sidecar 变更条目的分类与本地动作选择（selectApplicableChanges）
 * - 采纳游标的回溯计算（computeAdoptionRev）
 * - sidecar 响应信封解析（parseChangesResponse / parseRegisterResponse）
 *
 * 对应施工方案-v2 第 4.2 节与架构 INV-2/INV-5/INV-6。
 * 本模块不得 import obsidian 或任何插件单例，保证 tests/change-feed-logic.test.mjs 可加载。
 */

// ---------------------------------------------------------------- 类型 ---

export interface SidecarChange {
  rev: number;
  type: "note" | "file" | "folder" | "setting";
  action: string;
  changed_fields?: string;
  path: string;
  path_hash?: string;
  content_hash?: string;
  size?: number;
  mtime?: number;
  ctime?: number;
  created_at?: string;
}

export interface ChangesResponse {
  changes: SidecarChange[];
  next_rev: number;
  has_more: boolean;
  safe_rev: number;
  min_available_rev: number;
  collapsed: boolean;
  scanned: number;
}

export interface RegisterResponse {
  device_id: string;
  vault: string;
  cursor_rev: number;
  safe_rev: number;
  min_available_rev: number;
}

/** 判定输入：由调用方从 plugin 状态抽取，保持本函数纯 */
export interface ChangeFeedDecisionInput {
  enabled: boolean;
  sidecarUrl: string;
  deviceId: string | null;
  cursorRev: number | null;
  /** 本地与服务端基线均已就绪（IncrementalScanManager 双基线） */
  baselinesReady: boolean;
  syncEnabled: boolean;
  /** 手动部分同步（note/config）不进变更流 */
  syncMode: string;
}

export type ChangeFeedPlan = "off" | "defer" | "adopt" | "poll";

/** 采纳游标的安全回溯窗口（条）。覆盖设备离线期间的变更缺口；
 * 重复应用是幂等的（哈希命中即跳过），更早的缺口由 M3 digest 兜底。 */
export const ADOPTION_BACKTRACK_REVS = 5000;

export const CHANGE_FEED_PAGE_LIMIT = 500;
/** 单轮最多翻 40 页，防止一次轮次吞掉超长积压导致移动端卡死 */
export const CHANGE_FEED_MAX_PAGES = 40;

// ---------------------------------------------------------------- 判定 ---

/**
 * 轮次模式判定。返回值语义：
 * - "off"   功能未启用/未配置/非 auto 全量轮次 → 走 v1 原路径
 * - "defer" 已启用但基线未就绪（如首次安装）→ 本轮走 v1，完成后再自然进入 adopt
 * - "adopt" 启用且基线就绪但本地无游标 → 注册设备并回溯采纳起点
 * - "poll"  一切就绪 → 常规按游标拉取
 */
export function planChangeFeedRound(input: ChangeFeedDecisionInput): ChangeFeedPlan {
  if (!input.enabled || !input.syncEnabled) return "off";
  if (!input.sidecarUrl || input.sidecarUrl.trim() === "") return "off";
  if (input.syncMode !== "auto") return "off";
  if (!input.deviceId) return "off";
  if (input.cursorRev === null || input.cursorRev === undefined) {
    return input.baselinesReady ? "adopt" : "defer";
  }
  if (input.cursorRev <= 0) return "defer";
  return "poll";
}

// ---------------------------------------------------------------- 分类 ---

export type ChangeAction =
  | { kind: "skip"; reason: "derived-type" | "mtime-only" | "excluded-path"; change: SidecarChange }
  | { kind: "delete"; type: "note" | "file"; change: SidecarChange }
  | { kind: "fetch"; type: "note" | "file"; change: SidecarChange };

const DELETE_ACTIONS = new Set(["soft_delete", "delete"]);

/**
 * 把一条折叠后的变更翻译成本地动作。
 * 契约（施工方案 3.4 / 08-20 生产实测）：
 * - folder 是派生属性、setting 走 v1 配置同步，均跳过；
 * - changed_fields='mtime' 表示内容未变，跳过（免费省流）；
 * - rename 在 sync_log 只有新路径一行 → 按 upsert 新路径处理（fetch），
 *   旧路径残留交给 manifest/digest 对账清理（已写入 sidecar openapi 说明）。
 */
export function classifyChange(change: SidecarChange, isPathExcluded: (path: string) => boolean): ChangeAction {
  if (change.type === "folder" || change.type === "setting") {
    return { kind: "skip", reason: "derived-type", change };
  }
  if (isPathExcluded(change.path)) {
    return { kind: "skip", reason: "excluded-path", change };
  }
  const type = change.type;
  if (DELETE_ACTIONS.has(change.action)) {
    return { kind: "delete", type, change };
  }
  if (change.action === "modify" && change.changed_fields === "mtime") {
    return { kind: "skip", reason: "mtime-only", change };
  }
  return { kind: "fetch", type, change };
}

export interface LocalHashProbe {
  /** 本地已确认基线哈希（fileHashManager.getPathHash），无则 null */
  trackedHash(path: string): string | null;
  /** 本地文件是否存在（变更流删除需要，fetch 需要区分 create/modify 由接收管线处理） */
  fileExists(path: string): boolean;
  /** 本地存在未同步编辑的路径（pending 修改/冲突），须跳过以免覆盖 */
  hasPendingEdit(path: string): boolean;
}

export interface RoundActionCounts {
  fetch: number;
  delete: number;
  skipped: number;
}

/**
 * 汇总分类一批变更：内容已最新的 fetch 降级为 skip；有本地未同步编辑的条目跳过，
 * 交给随后的 v1 announce 上传/冲突流程处理。
 */
export function selectApplicableChanges(
  changes: SidecarChange[],
  probe: LocalHashProbe,
  isPathExcluded: (path: string) => boolean,
): { actions: ChangeAction[]; counts: RoundActionCounts } {
  const actions: ChangeAction[] = [];
  const counts: RoundActionCounts = { fetch: 0, delete: 0, skipped: 0 };
  for (const change of changes) {
    let action = classifyChange(change, isPathExcluded);
    if (action.kind === "fetch") {
      if (probe.hasPendingEdit(change.path)) {
        action = { kind: "skip", reason: "excluded-path", change };
        actions.push(action);
        counts.skipped++;
        continue;
      }
      const local = probe.trackedHash(change.path);
      if (local !== null && local === change.content_hash && probe.fileExists(change.path)) {
        actions.push({ kind: "skip", reason: "mtime-only", change });
        counts.skipped++;
        continue;
      }
      counts.fetch++;
    } else if (action.kind === "delete") {
      if (probe.hasPendingEdit(change.path)) {
        actions.push({ kind: "skip", reason: "excluded-path", change });
        counts.skipped++;
        continue;
      }
      if (!probe.fileExists(change.path)) {
        actions.push({ kind: "skip", reason: "mtime-only", change });
        counts.skipped++;
        continue;
      }
      counts.delete++;
    } else {
      counts.skipped++;
    }
    actions.push(action);
  }
  return { actions, counts };
}

// ---------------------------------------------------------------- 采纳 ---

/**
 * 采纳起点：服务端记得该设备的游标则以服务端为准（幂等重装）；
 * 新设备从 safe_rev 回溯 ADOPTION_BACKTRACK_REVS 条，覆盖离线缺口。
 */
export function computeAdoptionRev(serverCursorRev: number, safeRev: number): number {
  if (serverCursorRev > 0) return serverCursorRev;
  return Math.max(0, safeRev - ADOPTION_BACKTRACK_REVS);
}

/** 按类型推进 lastTime 水位（ms）。取窗口内最新事件的 created_at，与现值取 max。 */
export function advanceWatermark(currentMs: number, changes: SidecarChange[]): number {
  let latest = currentMs;
  for (const c of changes) {
    if (!c.created_at) continue;
    const t = Date.parse(c.created_at.endsWith("Z") || c.created_at.includes("+") ? c.created_at : c.created_at + "Z");
    if (!Number.isNaN(t) && t > latest) latest = t;
  }
  return latest;
}

// ---------------------------------------------------------------- 信封 ---

export class SidecarProtocolError extends Error {
  constructor(message: string, readonly status?: number, readonly code?: string) {
    super(message);
    this.name = "SidecarProtocolError";
  }
  get isStaleCursor(): boolean {
    return this.code === "stale_cursor";
  }
  get isUnauthorized(): boolean {
    return this.code === "unauthorized" || this.status === 401;
  }
}

/** 解析 sidecar 统一信封 {"ok":true,"data":{...}}；非 ok 抛 SidecarProtocolError。 */
export function unwrapEnvelope<T>(body: unknown, status: number): T {
  if (typeof body !== "object" || body === null) {
    throw new SidecarProtocolError(`sidecar returned non-object body (http ${status})`, status);
  }
  const env = body as { ok?: unknown; data?: unknown; error?: unknown; detail?: { error?: unknown } };
  if (env.ok === true) return env.data as T;
  const code = typeof env.error === "string" ? env.error : "unknown";
  throw new SidecarProtocolError(`sidecar error: ${code} (http ${status})`, status, code);
}

export function parseChangesResponse(body: unknown, status: number): ChangesResponse {
  return unwrapEnvelope<ChangesResponse>(body, status);
}

export function parseRegisterResponse(body: unknown, status: number): RegisterResponse {
  return unwrapEnvelope<RegisterResponse>(body, status);
}

/** UUIDv4 校验（INV-6：device_id 由客户端生成，永不由设备名推导）。 */
export function isValidUuidV4(id: string | null | undefined): id is string {
  if (!id) return false;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id);
}

/** 生成 UUIDv4（优先 crypto.randomUUID，测试环境注入 rng）。 */
export function generateUuidV4(rng: () => number = Math.random): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  // RFC4122 v4 fallback：不追求密码学强度，仅要求格式与版本位正确
  const hex = (n: number) => Array.from({ length: n }, () => Math.floor(rng() * 16).toString(16)).join("");
  return `${hex(8)}-${hex(4)}-4${hex(3)}-${(8 + Math.floor(rng() * 4)).toString(16)}${hex(3)}-${hex(12)}`;
}
