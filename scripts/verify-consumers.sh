#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
redis_pid=""
cleanup() {
  if [[ -n "$redis_pid" ]]; then kill "$redis_pid" >/dev/null 2>&1 || :; wait "$redis_pid" >/dev/null 2>&1 || :; fi
  rm -rf "$work"
}
trap cleanup EXIT
cd "$repo"
npm run build >/dev/null
tarball_name="$(npm pack --json=false --silent --ignore-scripts --pack-destination "$work" | tail -n 1)"
tarball="$work/$tarball_name"
test -f "$tarball"
mkdir -p "$work/consumer"
cd "$work/consumer"
printf '%s\n' '{"private":true,"type":"module"}' > package.json
npm install --silent --no-audit --no-fund --ignore-scripts "$tarball" typescript@5.9.3 @types/node@22 >/dev/null
echo "== Local-only external install without Redis peers =="
node -e "try{require.resolve('ioredis');process.exitCode=1}catch(e){if(e.code!=='MODULE_NOT_FOUND')throw e}"
node -e "try{require.resolve('redlock');process.exitCode=1}catch(e){if(e.code!=='MODULE_NOT_FOUND')throw e}"
cat > local.cjs <<'JS'
const { SyncedMap, SyncedMapConflictError } = require('syncedmap');
if (typeof SyncedMap !== 'function' || typeof SyncedMapConflictError !== 'function') throw Error('CJS exports');
(async () => {
  const map = new SyncedMap({ namespace: 'smoke:local-cjs' });
  await map.set('key', {n: 1});
  if (map.get('key').n !== 1) throw Error('CJS local read');
  await map.dispose();
  console.log('local CJS OK');
})().catch(error => { console.error(error); process.exitCode = 1; });
JS
cat > local.mjs <<'JS'
import { SyncedMap, SyncedMapConflictError } from 'syncedmap';
if (typeof SyncedMap !== 'function' || typeof SyncedMapConflictError !== 'function') throw Error('ESM exports');
const map = new SyncedMap({ namespace: 'smoke:local-esm' });
await map.set('key', {n: 2});
if (map.get('key').n !== 2) throw Error('ESM local read');
await map.dispose();
console.log('local ESM OK');
JS
for exe in "$(command -v node)" "$HOME"/.nvm/versions/node/*/bin/node; do
  if [[ ! -x "$exe" ]]; then continue; fi
  version="$("$exe" -p 'process.versions.node')"
  major="$(printf '%s' "$version" | cut -d. -f1)"
  if (( major < 18 )); then continue; fi
  "$exe" local.cjs
  "$exe" local.mjs
  echo "Node $version CJS+ESM PASS"
done
echo "== Strict TypeScript without Redis peers =="
cat > demo.cts <<'TS'
import { SyncedMap, type SyncedMapOptions } from 'syncedmap';
const config: SyncedMapOptions = {namespace:'typed:consumer'};
const map = new SyncedMap<string, {count:number}>(config);
const value: {count:number} | undefined = map.get('key');
const reader: Promise<number> = map.readLocked('key', async v => v?.count ?? 0);
void map.update('key', old => ({count:(old?.count ?? 0)+1}));
void value;
void reader;
TS
cp demo.cts demo.mts
cp demo.cts demo.ts
cat > nodenext.json <<'JSON'
{"compilerOptions":{"module":"NodeNext","moduleResolution":"NodeNext","target":"ES2015","lib":["ES2015"],"types":["node"],"strict":true,"skipLibCheck":false,"noEmit":true},"files":["demo.cts","demo.mts"]}
JSON
cat > cjs.json <<'JSON'
{"compilerOptions":{"module":"CommonJS","moduleResolution":"Node10","target":"ES2015","lib":["ES2015"],"types":["node"],"strict":true,"skipLibCheck":false,"noEmit":true},"files":["demo.ts"]}
JSON
cat > bundler.json <<'JSON'
{"compilerOptions":{"module":"ESNext","moduleResolution":"Bundler","target":"ES2015","lib":["ES2015"],"types":["node"],"strict":true,"skipLibCheck":false,"noEmit":true},"files":["demo.ts"]}
JSON
for config in nodenext cjs bundler; do
  ./node_modules/.bin/tsc -p "$config.json"
  echo "TypeScript $config PASS"
done
echo "== Redis-enabled external package installation =="
npm install --silent --no-audit --no-fund --ignore-scripts --no-save ioredis@5.11.1 redlock@5.0.0-beta.2 >/dev/null
node -e "require.resolve('ioredis');require.resolve('redlock')"
port="$(node -e "const net=require('node:net');const s=net.createServer();s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>console.log(p))})")"
redis-server --bind 127.0.0.1 --port "$port" --save '' --appendonly no >"$work/redis.log" 2>&1 &
redis_pid="$!"
ready=0
for i in $(seq 1 80); do
  if redis-cli -h 127.0.0.1 -p "$port" ping 2>/dev/null | grep -q PONG; then ready=1; break; fi
  sleep .05
done
if (( ready == 0 )); then cat "$work/redis.log"; exit 1; fi
export COMPAT_REDIS_URL="redis://127.0.0.1:$port"
cat > redis.cjs <<'JS'
const { SyncedMap } = require('syncedmap');
(async () => {
  const map = new SyncedMap({namespace:'smoke:redis',redis:{url:process.env.COMPAT_REDIS_URL}});
  try {
    await map.ready();
    await map.set('count', 1);
    if (await map.readLocked('count', async value => value) !== 1) throw Error('CJS locked read');
    console.log('redis CJS OK');
  } finally {
    await map.dispose();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
JS
cat > redis.mjs <<'JS'
import { SyncedMap } from 'syncedmap';
const map = new SyncedMap({namespace:'smoke:redis',redis:{url:process.env.COMPAT_REDIS_URL}});
try {
  await map.ready();
  if (map.get('count') !== 1) throw Error('ESM hydration');
  await map.update('count', n => n + 1);
  if (await map.getFresh('count') !== 2) throw Error('ESM update');
  console.log('redis ESM OK');
} finally {
  await map.dispose();
}
JS
node redis.cjs
node redis.mjs
echo "External consumers PASS"
