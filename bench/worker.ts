// TypeScript worker for the benchmark: the `Bench` aggregate echoes each command as one event.
// Usage: [WORK_MS=1] node worker.ts <kernel-url> <worker.key>
import { Kernable, aggregate } from "../sdks/typescript/src/index.ts";

// WORK_MS simulates business logic that costs CPU time per command.
const workMs = Number(process.env.WORK_MS ?? 0);

const bench = aggregate("Bench", 0)
  .apply("Written", (n) => n + 1)
  .command("Write", (_n, data) => {
    for (const end = performance.now() + workMs; performance.now() < end; );
    return [{ type: "Written", data }];
  });

const [url, key] = process.argv.slice(2);
const worker = new Kernable({ url, key }).worker(bench);
process.on("SIGTERM", () => worker.stop());
await worker.run({ concurrency: 64 });
