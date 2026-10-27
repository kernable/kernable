// Part 6: the guard decides who may do what.
import { type Outcome } from "@kernable/sdk";

import { connect } from "./connect.ts";

const kernable = connect();
const show = (what: string, o: Outcome) => console.log(`${what}: ${o.committed ? "committed" : `${o.code} (${o.message})`}`);

// #region try
// Who sends a request is part of its signed request context.
const as = (senderIdentity: string) => ({ context: { senderIdentity } });
const send = (who: string, aggregateType: string, aggregateId: string, type: string, data?: object) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data, ...as(who) });

show("c1 registers", await send("c1", "Customer", "c1", "Register", { email: "ann@example.com", name: "Ann" }));
show("c2 registers", await send("c2", "Customer", "c2", "Register", { email: "bob@example.com", name: "Bob" }));
show("c1 places o1", await send("c1", "Order", "o1", "Place", { customerId: "c1" }));
show("c2 adds to o1", await send("c2", "Order", "o1", "AddItem", { sku: "apple", qty: 1 }));
show("c2 places for c1", await send("c2", "Order", "o2", "Place", { customerId: "c1" }));
const getOrder = (who: string) => kernable.query({ domain: "shop", type: "GetOrder", data: { id: "o1" }, ...as(who) });
console.log("c1 reads o1:", await getOrder("c1"));
await getOrder("c2").catch((err) => console.log(`c2 reads o1: ${err.code} (${err.message})`));
// #endregion try
kernable.close();
