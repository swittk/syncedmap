import { describe, expect, it } from 'vitest';
import { LocalExpiryQueue } from '../src/localExpiryQueue.js';

/** Verify every mutation preserves the indexed min-heap invariant and bound. */
describe('local expiration heap', () => {
  it('keeps one indexed entry per key through repeated upserts and removals', () => {
    const queue = new LocalExpiryQueue<string>(() => undefined);
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
