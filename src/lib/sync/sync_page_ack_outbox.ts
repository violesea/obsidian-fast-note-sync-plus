export type SyncPageAckType = "note" | "file" | "setting" | "folder";

export interface PendingSyncPageAck {
  type: SyncPageAckType;
  context: string | null;
  initial: boolean;
  pageIndex: number | null;
}

interface AckState {
  context: string | null;
  initial: boolean;
  pageIndex: number | null;
}

type AckSender = (ack: PendingSyncPageAck) => boolean;

/**
 * Durable-in-memory outbox for page acknowledgements.
 *
 * The server treats a page acknowledgement as a context-bound high-water
 * mark. Keeping one state per type and context prevents an older page from
 * replacing a newer one, while retaining an unsent ACK across a transport
 * interruption. The outbox never sends an ACK without a context.
 */
export class SyncPageAckOutbox {
  private readonly pending = new Map<SyncPageAckType, AckState>();
  private activeContext: string | null = null;

  /** Start a new logical sync context and discard all older acknowledgements. */
  beginContext(context: string): void {
    this.activeContext = context || null;
    this.pending.clear();
  }

  /** Update the active context without starting a new logical round. */
  setActiveContext(context: string | null): void {
    this.activeContext = context || null;
    if (!this.activeContext) return;

    for (const [type, state] of this.pending) {
      if (state.context === null) {
        state.context = this.activeContext;
      } else if (state.context !== this.activeContext) {
        this.pending.delete(type);
      }
    }
  }

  /** End a logical context and remove its retained ACKs. */
  clearContext(context?: string): void {
    if (context && this.activeContext && context !== this.activeContext) return;
    this.pending.clear();
    this.activeContext = null;
  }

  /**
   * Queue an initial or page ACK. A missing context is retained but cannot be
   * flushed until a current context is supplied.
   */
  enqueue(type: SyncPageAckType, pageIndex: number, context?: string | null): void {
    const explicitContext = context?.trim() || null;
    if (this.activeContext && explicitContext && explicitContext !== this.activeContext) {
      return;
    }

    const effectiveContext = explicitContext || this.activeContext;
    const current = this.pending.get(type);
    if (current && current.context && effectiveContext && current.context !== effectiveContext) {
      return;
    }

    const state: AckState = current ?? {
      context: effectiveContext,
      initial: false,
      pageIndex: null,
    };
    if (!current || state.context === null) state.context = effectiveContext;

    if (pageIndex === -1) {
      state.initial = true;
    } else if (pageIndex >= 0 && (state.pageIndex === null || pageIndex > state.pageIndex)) {
      state.pageIndex = pageIndex;
    }
    this.pending.set(type, state);
  }

  /** Flush only ACKs for the active context; failed writes remain queued. */
  flush(context: string | null, send: AckSender): void {
    const effectiveContext = context?.trim() || this.activeContext;
    if (!effectiveContext) return;
    if (this.activeContext && effectiveContext !== this.activeContext) return;

    this.setActiveContext(effectiveContext);
    for (const [type, state] of this.pending) {
      if (state.context !== effectiveContext) continue;

      if (state.initial) {
        const initialAck: PendingSyncPageAck = {
          type,
          context: effectiveContext,
          initial: true,
          pageIndex: -1,
        };
        if (!send(initialAck)) continue;
        state.initial = false;
      }

      if (state.pageIndex !== null) {
        const pageAck: PendingSyncPageAck = {
          type,
          context: effectiveContext,
          initial: false,
          pageIndex: state.pageIndex,
        };
        if (!send(pageAck)) continue;
        state.pageIndex = null;
      }

      if (!state.initial && state.pageIndex === null) {
        this.pending.delete(type);
      }
    }
  }

  get size(): number {
    return this.pending.size;
  }

  snapshot(): PendingSyncPageAck[] {
    return Array.from(this.pending, ([type, state]) => ({
      type,
      context: state.context,
      initial: state.initial,
      pageIndex: state.pageIndex,
    }));
  }
}
