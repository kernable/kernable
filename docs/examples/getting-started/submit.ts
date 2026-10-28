import { Kernable } from "@kernable/sdk";

const kernable = new Kernable({ key: "app.key" });
const cartC1 = { domain: "shop", aggregateType: "cart", aggregateId: "c1" };

await kernable.submit({ ...cartC1, type: "AddItem", data: { sku: "apple", qty: 2 } });
// submit returned only after every listener processed the event: no polling needed.
let cart = await kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });
console.log(cart);
// { items: 2 }

await kernable.submit({ ...cartC1, type: "CheckOut" });
cart = await kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });
console.log(cart);
// { items: 2, order: '0b9c5e1a-6f3d-8a42-9c1e-5d7f2b8e4a10' }

const outcome = await kernable.submit({ ...cartC1, type: "AddItem", data: { sku: "pear", qty: 1 } });
console.log(outcome);
// { committed: false, code: 'rejected', message: 'Cart is checked out' }
