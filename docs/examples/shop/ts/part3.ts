// Part 3: read your own writes through queries.
import { Kernable } from "@kernable/sdk";

const kernable = new Kernable();
const send = (aggregateType: string, aggregateId: string, type: string, data?: object) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data });

// #region try
const getOrder = (id: string) => kernable.query({ domain: "shop", type: "GetOrder", data: { id } });

await send("customer", "c1", "Register", { email: "ann@example.com", name: "Ann" });
await send("order", "o1", "Place", { customerId: "c1" });
await send("order", "o1", "AddItem", { sku: "apple", qty: 2 });
// The submit returned after the read model processed the event: the query sees it.
console.log(await getOrder("o1"));
console.log(await kernable.query({ domain: "shop", type: "GetCustomer", data: { id: "c1" } }));
await getOrder("o9").catch((err) => console.log(`${err.code}: ${err.message}`));
// #endregion try
kernable.close();
