export interface ConnectionTransport {
  register: () => Promise<void>;
  unRegister: (setUnregistered?: boolean) => void;
  triggerReconnect: () => void;
  forceReconnect: () => void;
}

type PendingIntent = "register";

/**
 * The only owner of connection intents emitted by the plugin layer.
 * Physical socket replacement remains inside the transport, but lifecycle,
 * settings, network and upgrade callers all pass through this supervisor.
 */
export class ConnectionSupervisor {
  private pendingIntent: PendingIntent | null = null;
  private pumpPromise: Promise<void> | null = null;
  private registrationDesired = false;

  constructor(private readonly transport: ConnectionTransport) {}

  requestRegister(force = false): Promise<void> {
    if (this.registrationDesired && !force) {
      return this.pumpPromise ?? Promise.resolve();
    }
    this.registrationDesired = true;
    this.pendingIntent = "register";
    return this.pump();
  }

  /** Stop the current transport immediately so an in-flight probe is invalidated. */
  requestUnregister(setUnregistered = false): Promise<void> {
    this.registrationDesired = false;
    this.pendingIntent = null;
    this.transport.unRegister(setUnregistered);
    return Promise.resolve();
  }

  /** Replace the socket and then register it again as one settings operation. */
  requestReconfigure(): Promise<void> {
    this.registrationDesired = false;
    this.transport.unRegister(false);
    this.registrationDesired = true;
    this.pendingIntent = "register";
    return this.pump();
  }

  requestReconnect(): void {
    if (!this.registrationDesired) return;
    this.transport.triggerReconnect();
  }

  requestForceReconnect(): void {
    if (!this.registrationDesired) return;
    this.transport.forceReconnect();
  }

  private pump(): Promise<void> {
    if (this.pumpPromise) return this.pumpPromise;

    const task = (async () => {
      while (this.pendingIntent !== null) {
        const intent = this.pendingIntent;
        this.pendingIntent = null;
        if (intent === "register" && this.registrationDesired) {
          await this.transport.register();
        }
      }
    })();
    let tracked: Promise<void>;
    tracked = task.finally(() => {
      if (this.pumpPromise === tracked) this.pumpPromise = null;
    });
    this.pumpPromise = tracked;
    return tracked;
  }
}
