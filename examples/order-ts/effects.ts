// Reactor: side effects run after the fact, driven by committed events, never inside command
// handlers. It runs after the read models (order 10 > 0), so it may query them. Delivery is
// at least once, so effects must tolerate repeats.
//
// Usage: node effects.ts <kernel-url> <listener.key>
import { Kernable } from "../../sdks/typescript/src/index.ts";

const [url = "unix:/tmp/kernable.sock", key = "app.key"] = process.argv.slice(2);

const confirmations = new Kernable({ url, key })
  .reactor("order-confirmations")
  .on<{ customerId: string }>("OrderPlaced", (e) => {
    console.log(`sending order confirmation for ${e.aggregateId} to customer ${e.data.customerId}`);
  });
process.on("SIGINT", () => confirmations.stop());
await confirmations.run();
