import readline from 'node:readline';
import { SyncedMap } from '../../dist/index.js';

let clusterClient;
let redisConfig = { url: process.argv[2] };
if (process.argv[2].startsWith('cluster://')) {
  const parsed = new URL(process.argv[2]);
  const { Cluster } = await import('ioredis');
  clusterClient = new Cluster([{ host: parsed.hostname, port: Number(parsed.port) }]);
  clusterClient.on('error', () => undefined);
  redisConfig = { client: clusterClient };
}
const map = new SyncedMap({ namespace: process.argv[3], redis: redisConfig });
await map.ready();
process.stdout.write(JSON.stringify({ ready: true }) + '\n');

for await (const line of readline.createInterface({ input: process.stdin })) {
  let message;
  try {
    message = JSON.parse(line);
    let result;
    if (message.op === 'update') {
      for (let i = 0; i < message.count; i++) {
        await map.update(message.key, previous => (previous ?? 0) + 1);
      }
      result = await map.getFresh(message.key);
    } else if (message.op === 'set') {
      await map.set(message.key, message.value);
      result = true;
    } else if (message.op === 'get') {
      result = await map.getFresh(message.key);
    } else if (message.op === 'shutdown') {
      await map.dispose();
      clusterClient?.disconnect();
      process.stdout.write(JSON.stringify({ id: message.id, result: true }) + '\n');
      process.exit(0);
    } else {
      throw new Error('Unknown peer operation');
    }
    process.stdout.write(JSON.stringify({ id: message.id, result }) + '\n');
  } catch (error) {
    process.stdout.write(JSON.stringify({ id: message?.id, error: String(error) }) + '\n');
  }
}
await map.dispose();
clusterClient?.disconnect();
