// Effect worker: side effects run after the fact, driven by committed events, never inside
// command handlers. Delivery is at least once, so effects must tolerate repeats.
//
// Usage: node effects.ts <kernel-url> <subscriber.key> [position-file]
import { existsSync, readFileSync, writeFileSync } from "node:fs";

import { Kernable } from "../../sdks/typescript/src/index.ts";

const [url = "unix:/tmp/kernable.sock", key = "app.key", positionFile = "effects.position"] = process.argv.slice(2);
const after = existsSync(positionFile) ? Number(readFileSync(positionFile, "utf8")) : 0;

const subscription = new Kernable({ url, key }).subscribe({ after, types: ["OrderPlaced"] }, async (e) => {
  const { customerId } = e.data as { customerId: string };
  console.log(`sending order confirmation for ${e.aggregateId} to customer ${customerId}`);
  writeFileSync(positionFile, String(e.position)); // after the effect: repeat rather than lose
});
process.on("SIGINT", () => subscription.stop());
await subscription.done;
