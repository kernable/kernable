// Part 2: commands, rejections and idempotency.
import { Kernable, type Outcome } from "@kernable/sdk";

const kernable = new Kernable();
const show = (what: string, o: Outcome) => console.log(`${what}: ${o.committed ? `committed, version ${o.events.at(-1)?.version}` : `${o.code} (${o.message})`}`);

// #region try
const send = (aggregateType: string, aggregateId: string, type: string, data?: object, id?: string) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data, id });

show("register", await send("customer", "c1", "Register", { email: "ann@example.com", name: "Ann" }));
show("register again", await send("customer", "c1", "Register", { email: "ann@example.com", name: "Ann" }));
show("place", await send("order", "o1", "Place", { customerId: "c1" }));
show("add apples", await send("order", "o1", "AddItem", { sku: "apple", qty: 2 }, "add-1"));
show("same command again", await send("order", "o1", "AddItem", { sku: "apple", qty: 2 }, "add-1"));
show("submit", await send("order", "o1", "Submit"));
show("add after submit", await send("order", "o1", "AddItem", { sku: "pear", qty: 1 }));
// #endregion try
kernable.close();
