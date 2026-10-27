// The guard: a middleware that lets customers act only on themselves and their own orders.
import { Rejected } from "@kernable/sdk";

import { connect } from "./connect.ts";

// #region guard
const SERVICES = new Set(["svc-confirmations"]);
const registered = new Set<string>();
const ownerOf = new Map<string, string>(); // order -> customer

const kernable = connect();
await kernable.readModel("guard")
  .on("shop.CustomerRegistered", (e) => void registered.add(e.aggregateId))
  .on("shop.CustomerErased", (e) => void registered.delete(e.aggregateId))
  .on("shop.OrderPlaced", (e) => void ownerOf.set(e.aggregateId, (e.data as { customerId: string }).customerId))
  .middleware({ types: ["shop.*"] }, (check) => {
    const who = check.request.senderIdentity ?? "";
    if (check.type === "Register") return; // anyone may sign up
    if (check.kind === "query" && SERVICES.has(who)) return; // services may read
    const data = check.data as { id?: string; customerId?: string };
    const target = check.kind === "command" ? check.aggregateId! : data.id!;
    const owner =
      check.type === "Place" ? data.customerId
      : check.type === "GetCustomer" || check.aggregateType === "Customer" ? target
      : ownerOf.get(target);
    if (!registered.has(who) || owner !== who) throw new Rejected(`${check.type} is not allowed for ${who || "anonymous"}`, "forbidden");
  })
  .run();
// #endregion guard
