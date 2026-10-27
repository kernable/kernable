// The customers read model with a store: it keeps its rows in a file and continues after a
// restart where it left off, instead of reading every event again.
import { Rejected } from "@kernable/sdk";

import { connect } from "./connect.ts";

// #region persistent
type CustomerRow = { id: string; email: string; name: string };

await connect()
  .readModel("customers", { store: "sqlite:customers.db", schema: "1" })
  .on("shop.CustomerRegistered", (e, ctx) => {
    const { email, name } = e.data as { email: string; name: string };
    ctx.store!.set("customers", e.aggregateId, { id: e.aggregateId, email, name });
  })
  .on("shop.CustomerErased", (e, ctx) => ctx.store!.remove("customers", e.aggregateId))
  .query("shop.GetCustomer", (d: { id: string }, ctx) => {
    const row = ctx.store!.get<CustomerRow>("customers", d.id);
    if (!row) throw new Rejected(`No customer ${d.id}`, "not_found");
    return row;
  })
  .run();
// #endregion persistent
