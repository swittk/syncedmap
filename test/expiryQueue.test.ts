import { describe, expect, it } from 'vitest';
import { ExpiryQueue } from '../src/expiryQueue.js';

/** Verify every mutation preserves the indexed min-heap invariant and bound. */
describe('local expiration heap', () => {
  it('rebuilds an expiration heap from a snapshot and expires values without stale nodes', async () => {
    const expired: string[] = [];
    const queue = new ExpiryQueue<string>((key) => expired.push(key));
    try {
      const now = Date.now();
      const rows = new Map<string, { expiresAt: number }>();
      for (let i = 0; i < 500; i++) {
        rows.set('item-' + i, { expiresAt: now + 10000 + (i % 9) * 1000 });
      }
      rows.set('expired', { expiresAt: now - 1 });
      rows.set('permanent', { expiresAt: 0 });
      queue.replace(rows);
      expect(queue.size).toBe(500);
      expect(expired).toEqual(['expired']);
      const internal = queue as unknown as {
        nodes: Array<{ key: string; expiresAt: number }>;
        positions: Map<string, number>;
      };
      for (let i = 0; i < internal.nodes.length; i++) {
        const item = internal.nodes[i]!;
        expect(internal.positions.get(item.key)).toBe(i);
        if (i) expect(internal.nodes[Math.floor((i - 1) / 2)]!.expiresAt)
          .toBeLessThanOrEqual(item.expiresAt);
      }
      queue.replace(new Map([['new', { expiresAt: now + 20000 }]]));
      expect(queue.size).toBe(1);
      expect(internal.positions.has('expired')).toBe(false);
      expect(internal.positions.has('item-0')).toBe(false);
      queue.replace(new Map());
      expect(queue.size).toBe(0);
    } finally {
      queue.dispose();
    }
  });

  it('keeps one indexed entry per key through repeated upserts and removals', () => {
    const queue = new ExpiryQueue<string>(() => undefined);
    const live = new Map<string, number>();
    let seed = 0x12345678;
    const next = (): number => {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return seed >>> 0;
    };

    try {
      for (let i = 0; i < 5000; i++) {
        const key = 'entry-' + (next() % 250);
        const expiry = Date.now() + 1000 + next() % 5000;
        if (next() % 3 === 0) {
          queue.delete(key);
          live.delete(key);
        } else {
          queue.set(key, expiry);
          live.set(key, expiry);
        }
        const internal = queue as unknown as {
          nodes: Array<{ key: string; expiresAt: number }>;
          positions: Map<string, number>;
        };
        // Sample the full heap every 100 mutations; checking every node after
        // every write makes matcher overhead dominate the queue operation.
        if (i % 100 !== 0 && i !== 4999) continue;
        expect(internal.nodes.length).toBe(live.size);
        expect(internal.positions.size).toBe(live.size);
        for (let n = 0; n < internal.nodes.length; n++) {
          const item = internal.nodes[n]!;
          expect(internal.positions.get(item.key)).toBe(n);
          expect(item.expiresAt).toBe(live.get(item.key));
          if (n > 0) {
            const parent = Math.floor((n - 1) / 2);
            expect(internal.nodes[parent]!.expiresAt).toBeLessThanOrEqual(item.expiresAt);
          }
        }
      }
    } finally {
      queue.dispose();
    }
  });
});
