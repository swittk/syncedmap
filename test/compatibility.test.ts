import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { parse } from 'acorn';
import { describe, expect, it } from 'vitest';

describe('ES6 distributable syntax and module exports', () => {
  it.each([
    { file: 'dist/index.js', sourceType: 'module' as const },
    { file: 'dist/index.cjs', sourceType: 'script' as const },
    { file: 'dist/redisLoader.cjs', sourceType: 'script' as const },
  ])('$file parses under strict ECMAScript 2015', ({ file, sourceType }) => {
    const js = readFileSync(resolve(process.cwd(), file), 'utf8');
    expect(() => parse(js, { ecmaVersion: 2015, sourceType })).not.toThrow();
  });

  it('routes TypeScript declarations to the matching ESM/CJS entrypoint', () => {
    const pkg = JSON.parse(readFileSync('package.json', 'utf8')) as {
      exports: { '.': { import: { types: string; default: string }; require: { types: string; default: string } } };
      dependencies: Record<string, string>;
      peerDependencies: Record<string, string>;
      peerDependenciesMeta: Record<string, { optional: boolean }>;
    };
    const exports = pkg.exports['.'];
    expect(exports.import).toEqual({ types: './dist/index.d.ts', default: './dist/index.js' });
    expect(exports.require).toEqual({ types: './dist/index.d.cts', default: './dist/index.cjs' });
    expect(Object.keys(pkg.dependencies)).toEqual(['uniquemutexmanager']);
    expect(pkg.peerDependenciesMeta.ioredis).toEqual({ optional: true });
    expect(pkg.peerDependenciesMeta.redlock).toEqual({ optional: true });
  });
});
