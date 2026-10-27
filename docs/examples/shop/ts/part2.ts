// Part 2: commands, rejections and idempotency.
import { type Outcome } from "@kernable/sdk";

import { connect } from "./connect.ts";

const kernable = connect();
const show = (what: string, o: Outcome) => console.log(`${what}: ${o.committed ? `committed, version ${o.events.at(-1)?.version}` : `${o.code} (${o.message})`}`);

// #region try
const send = (aggregateType: string, aggregateId: string, type: string, data?: object, id?: string) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data, id });

show("register", await send("Customer", "c1", "Register", { email: "ann@example.com", name: "Ann" }));
show("register again", await send("Customer", "c1", "Register", { email: "ann@example.com", name: "Ann" }));
show("place", await send("Order", "o1", "Place", { customerId: "c1" }));
show("add apples", await send("Order", "o1", "AddItem", { sku: "apple", qty: 2 }, "add-1"));
show("same command again", await send("Order", "o1", "AddItem", { sku: "apple", qty: 2 }, "add-1"));
show("submit", await send("Order", "o1", "Submit"));
show("add after submit", await send("Order", "o1", "AddItem", { sku: "pear", qty: 1 }));
// #endregion try
kernable.close();
