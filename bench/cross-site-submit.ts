// One command through the SDK, routed by the partition map the kernel at <url> holds: for
// bench/cross-site.sh. Prints "committed" or "failed: <why>" within <timeout ms>.
// Usage: node cross-site-submit.ts <url> <client.key> <tenant> <aggregate id> [timeout ms]
import { Kernable } from "../sdks/typescript/src/index.ts";

const [url, key, tenant, id, timeout = "15000"] = process.argv.slice(2);
const k = new Kernable({ url, key, log: "off" });
const timer = setTimeout(() => {
  console.log(`failed: no answer within ${timeout} ms`);
  process.exit(1);
}, Number(timeout));
try {
  const o: any = await k.submit({ tenant, domain: "bench", aggregateType: "Bench", aggregateId: id, type: "Write", data: { at: Date.now() }, wait: "committed" });
  console.log(o.committed ? "committed" : `failed: ${JSON.stringify(o)}`);
  process.exit(o.committed ? 0 : 1);
} catch (e) {
  console.log(`failed: ${(e as Error).message.split("\n")[0]}`);
  process.exit(1);
} finally {
  clearTimeout(timer);
}
