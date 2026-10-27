import { Kernable } from "@kernable/sdk";

const kernable = new Kernable({ url: `unix:${process.cwd()}/k.sock`, key: "app.key" });
const cart = (type: string, data?: object) =>
  kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: "c1", type, data });
const getCart = () => kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });

await cart("AddItem", { sku: "apple", qty: 2 });
// submit returned only after every listener processed the event: no polling needed.
console.log(await getCart());
await cart("CheckOut");
console.log(await getCart());
console.log(await cart("AddItem", { sku: "pear", qty: 1 })); // rejected: checked out
kernable.close();
