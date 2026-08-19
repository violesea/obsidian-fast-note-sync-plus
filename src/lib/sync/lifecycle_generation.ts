/**
 * Invalidates asynchronous work that belongs to an unloaded plugin instance.
 * A generation check is intentionally synchronous and dependency-free so it
 * can guard callbacks before and after every await boundary.
 */
export class LifecycleGeneration {
  private generation = 0;
  private active = false;

  begin(): number {
    this.generation += 1;
    this.active = true;
    return this.generation;
  }

  invalidate(): void {
    this.generation += 1;
    this.active = false;
  }

  current(): number {
    return this.generation;
  }

  isCurrent(generation: number): boolean {
    return this.active && generation === this.generation;
  }
}
