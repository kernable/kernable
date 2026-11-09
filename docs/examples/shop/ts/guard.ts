// The guard: a middleware that lets customers act only on themselves and their own orders.
import { Kernable, Rejected } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

// #region guard
const SERVICES = new Set(["svc-confirmations"]);
// What a business process may send (part 10): it acts as `process:<name>`.
const PROCESSES: Record<string, string[]> = { "process:shop.fulfilment": ["Ship", "Cancel"] };
const registered = new Set<string>();
const ownerOf = new Map<string, string>(); // order -> customer

const guardReadmodel = shopDomain.readModel("guard")
  .on("CustomerRegistered", (_data, evt) => void registered.add(evt.aggregateId))
  .on("CustomerErased", (_data, evt) => void registered.delete(evt.aggregateId))
  .on("OrderPlaced", (data: { customerId: string }, evt) => void ownerOf.set(evt.aggregateId, data.customerId))
  .middleware({ types: ["shop.*"] }, (check) => {
    const who = check.request.senderIdentity ?? "";
    if (check.type === "Register") return; // anyone may sign up
    if (check.kind === "query" && SERVICES.has(who)) return; // services may read
    if (check.kind === "command" && PROCESSES[who]?.includes(check.type)) return;
    const data = check.data as { id?: string; customerId?: string };
    const target = check.kind === "command" ? check.aggregateId! : data.id!;
    const owner =
      check.type === "Place" ? data.customerId
      : check.type === "GetCustomer" || check.aggregateType === "customer" ? target
      : ownerOf.get(target);
    if (!registered.has(who) || owner !== who) throw new Rejected(`${check.type} is not allowed for ${who || "anonymous"}`, "forbidden");
  });

await new Kernable().listener(guardReadmodel).run();
// #endregion guard
