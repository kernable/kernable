// Part 10: the fulfilment process ships a paid order and cancels an unpaid one.
import { Kernable } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

const kernable = new Kernable({ context: { senderIdentity: "c1" } }).use(shopDomain);
const send = (aggregateType: string, aggregateId: string, type: string, data?: object) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data });

/** The order's status once it is `status`: the process moves it on its own, a little later. */
async function until(id: string, status: string) {
  for (;;) {
    const order = await kernable.query<{ status: string; cancelled?: string }>({ domain: "shop", type: "GetOrder", data: { id } });
    if (order.status === status) return order;
    await new Promise((r) => setTimeout(r, 100));
  }
}

// #region try
await send("customer", "c1", "Register", { email: "ann@example.com", name: "Ann" });
for (const id of ["o1", "o2"]) {
  await send("order", id, "Place", { customerId: "c1" });
  await send("order", id, "AddItem", { sku: "apple", qty: 1 });
  await send("order", id, "Submit"); // confirmed by the reactor: a case starts for the order
}
await send("order", "o1", "Pay");
console.log("o1:", (await until("o1", "shipped")).status);
// o2 is never paid: when its time is up, the process cancels it.
const o2 = await until("o2", "cancelled");
console.log("o2:", o2.status, o2.cancelled);
// #endregion try
kernable.close();
