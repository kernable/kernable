// The customers read model with a store: it keeps its rows in a file and continues after a
// restart where it left off, instead of reading every event again.
import { Kernable, Rejected } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

// #region persistent
type CustomerRow = { id: string; email: string; name: string };

const customersReadmodel = shopDomain.readModel("customers", { store: "sqlite:customers.db", schema: "1" })
  .on("CustomerRegistered", (data: { email: string; name: string }, evt, ctx) => {
    ctx.store!.set("customers", evt.aggregateId, { id: evt.aggregateId, email: data.email, name: data.name });
  })
  .on("CustomerErased", (_data, evt, ctx) => ctx.store!.remove("customers", evt.aggregateId))
  .query("GetCustomer", (data: { id: string }, _qry, ctx) => {
    const row = ctx.store!.get<CustomerRow>("customers", data.id);
    if (!row) throw new Rejected(`No customer ${data.id}`, "not_found");
    return row;
  });

await new Kernable().listener(customersReadmodel).run();
// #endregion persistent
