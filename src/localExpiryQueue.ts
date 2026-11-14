/** An indexed min-heap: one timer, O(log n) insert/update/delete, no stale nodes. */
type ExpiryNode<Key extends string> = { key: Key; expiresAt: number };

const MAX_TIMEOUT_MS = 0x7fffffff;

/**
 * Only used in local mode. Unlike one timer per entry, memory and timer
 * overhead remain bounded by the number of live expiring keys.
 */
export class LocalExpiryQueue<Key extends string> {
  private readonly nodes: Array<ExpiryNode<Key>> = [];
  private readonly positions = new Map<Key, number>();
  private timer?: ReturnType<typeof setTimeout>;
  private disposed = false;

  constructor(private readonly onExpire: (key: Key, expiresAt: number) => void) {}

  /** Move a node up to its earliest-expiration position. */
  private rise(start: number): number {
    let index = start;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.nodes[parent]!.expiresAt <= this.nodes[index]!.expiresAt) break;
      this.swap(parent, index);
      index = parent;
    }
    return index;
  }

  /** Move a node down toward its earliest-expiration position. */
  private sink(start: number): void {
    let index = start;
    const length = this.nodes.length;
    while (true) {
      const left = index * 2 + 1;
      if (left >= length) return;
      const right = left + 1;
      const earlier = right < length && this.nodes[right]!.expiresAt < this.nodes[left]!.expiresAt
        ? right : left;
      if (this.nodes[index]!.expiresAt <= this.nodes[earlier]!.expiresAt) return;
      this.swap(index, earlier);
      index = earlier;
    }
  }

  private swap(a: number, b: number): void {
    const first = this.nodes[a]!;
    const second = this.nodes[b]!;
    this.nodes[a] = second;
    this.nodes[b] = first;
    this.positions.set(first.key, b);
    this.positions.set(second.key, a);
  }

  private removeAt(index: number): void {
    const removed = this.nodes[index]!;
    const last = this.nodes.pop()!;
    this.positions.delete(removed.key);
    if (index < this.nodes.length) {
      this.nodes[index] = last;
      this.positions.set(last.key, index);
      this.sink(this.rise(index));
    }
  }

  /** Assign/remove the expiration for one key. 0 means no expiry. */
  set(key: Key, expiresAt: number): void {
    if (this.disposed) return;
    const previous = this.positions.get(key);
    if (expiresAt === 0) {
      if (previous !== undefined) this.removeAt(previous);
    } else if (previous === undefined) {
      this.positions.set(key, this.nodes.length);
      this.nodes.push({ key, expiresAt });
      this.rise(this.nodes.length - 1);
    } else {
      this.nodes[previous]!.expiresAt = expiresAt;
      this.sink(this.rise(previous));
    }
    this.schedule();
  }

  delete(key: Key): void {
    if (this.disposed) return;
    const previous = this.positions.get(key);
    if (previous !== undefined) {
      this.removeAt(previous);
      this.schedule();
    }
  }

  private schedule(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    if (this.disposed || this.nodes.length === 0) return;
    const ms = Math.min(MAX_TIMEOUT_MS, Math.max(1, this.nodes[0]!.expiresAt - Date.now()));
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.expire();
    }, ms);
    this.timer.unref?.();
  }

  private expire(): void {
    if (this.disposed) return;
    const now = Date.now();
    while (this.nodes.length > 0 && this.nodes[0]!.expiresAt <= now) {
      const expired = this.nodes[0]!;
      this.removeAt(0);
      this.onExpire(expired.key, expired.expiresAt);
    }
    this.schedule();
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.nodes.length = 0;
    this.positions.clear();
  }
}
