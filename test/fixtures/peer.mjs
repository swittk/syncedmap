import readline from 'node:readline';
import { SyncedMap } from '../../dist/index.js';

const map = new SyncedMap({ namespace: process.argv[3], redis: { url: process.argv[2] } });
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
