// Part 4: the reactor confirms a submitted order.
import { connect } from "./connect.ts";

const kernable = connect();
const send = (aggregateType: string, aggregateId: string, type: string, data?: object) =>
  kernable.submit({ domain: "shop", aggregateType, aggregateId, type, data });

// #region try
await send("Customer", "c1", "Register", { email: "ann@example.com", name: "Ann" });
await send("Order", "o1", "Place", { customerId: "c1" });
await send("Order", "o1", "AddItem", { sku: "apple", qty: 2 });
await send("Order", "o1", "Submit");
// The submit settled after the reactor, which confirmed the order before it returned.
console.log(await kernable.query({ domain: "shop", type: "GetOrder", data: { id: "o1" } }));
// #endregion try
kernable.close();
