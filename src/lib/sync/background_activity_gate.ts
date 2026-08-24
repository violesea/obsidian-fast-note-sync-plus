/**
 * Mobile filesystem activity gate.
 *
 * Keeping the WebSocket alive while the app is backgrounded is safe, but
 * starting new Obsidian vault I/O during an iOS scene transition is not. This
 * gate pauses work at async boundaries and lets the transport remain silent
 * and connected until the app is visible again.
 */

export class BackgroundActivityGate {
  private state: "foreground" | "background" | "closed" = "foreground";
  private waiters = new Set<(open: boolean) => void>();

  get isBackgrounded(): boolean {
    return this.state === "background";
  }

  get isClosed(): boolean {
    return this.state === "closed";
  }

  markBackgrounded(): void {
    if (this.state === "closed") return;
    this.state = "background";
  }

  markForegrounded(): void {
    if (this.state === "closed") return;
    this.state = "foreground";
    this.resolveWaiters(true);
  }

  close(): void {
    if (this.state === "closed") return;
    this.state = "closed";
    this.resolveWaiters(false);
  }

  /**
   * Resolve immediately while visible; otherwise wait without blocking the
   * renderer thread. A closed gate tells callers to abandon their work.
   */
  async waitUntilForeground(): Promise<boolean> {
    if (this.state === "foreground") return true;
    if (this.state === "closed") return false;

    return new Promise<boolean>((resolve) => {
      this.waiters.add(resolve);
    });
  }

  private resolveWaiters(open: boolean): void {
    const waiters = Array.from(this.waiters);
    this.waiters.clear();
    for (const resolve of waiters) resolve(open);
  }
}

export interface BackgroundActivityOwner {
  backgroundActivityGate?: Pick<BackgroundActivityGate, "waitUntilForeground"> & {
    isClosed?: boolean;
  };
}

/** Error used to abandon work after the plugin has been unloaded. */
export class BackgroundActivityClosedError extends Error {
  constructor() {
    super("Background activity gate closed");
    this.name = "BackgroundActivityClosedError";
  }
}

/**
 * Compatibility helper for unit-test doubles and older plugin objects.
 */
export async function waitForForeground(plugin: BackgroundActivityOwner): Promise<boolean> {
  const gate = plugin.backgroundActivityGate;
  if (!gate) return true;
  return gate.waitUntilForeground();
}

/** Wait for a visible app or throw when the plugin is being unloaded. */
export async function requireForeground(plugin: BackgroundActivityOwner): Promise<void> {
  if (!(await waitForForeground(plugin))) throw new BackgroundActivityClosedError();
}

export function isBackgroundActivityClosedError(error: unknown): boolean {
  return error instanceof BackgroundActivityClosedError
    || (error instanceof Error && error.name === "BackgroundActivityClosedError");
}
