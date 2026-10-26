// Usage: node worker.ts <kernel-url> <worker.key>
import { Kernable } from "../../sdks/typescript/src/index.ts";
import { order } from "./order.ts";

const [url = "unix:/tmp/kernable.sock", key = "worker.key"] = process.argv.slice(2);

const worker = new Kernable({ url, key }).worker(order);
process.on("SIGINT", () => worker.stop());
await worker.run();
