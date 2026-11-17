// The workers for bench/cross-site-remote.sh: `bench.Bench` (Write → Written) and `acct.Account`
// (Register → Registered) with a global unique key on the email, so two tenants on two
// partitions cannot register the same address.
// Usage: node cross-site-worker.ts <kernel-url> <worker.key>
import { Kernable, domain } from "../sdks/typescript/src/index.ts";

const bench = domain("bench").aggregate("Bench", 0)
  .apply("Written", (n) => n + 1)
  .command("Write", (_n, data) => [{ type: "Written", data }]);

const account = domain("acct").aggregate("Account", { email: null as string | null })
  .apply("Registered", (_s, d: any) => ({ email: d.email }))
  .command("Register", (_s, d: any) => [{ type: "Registered", data: { email: String(d.email).toLowerCase() } }])
  .unique("email", (s) => s.email, { scope: "global" });

const [url, key] = process.argv.slice(2);
const k = new Kernable({ url, key });
const workers = [k.worker(bench), k.worker(account)];
process.on("SIGTERM", () => workers.forEach((w) => w.stop()));
await Promise.all(workers.map((w) => w.run({ concurrency: 16 })));
