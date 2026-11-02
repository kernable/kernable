// The reactor: confirms every submitted order to the customer's email.
import { Kernable } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

// #region reactor
// Who this service is (part 6), and the domain's declarations, e.g. which fields are personal (part 7).
const kernable = new Kernable({ context: { senderIdentity: "svc-confirmations" } }).use(shopDomain);

const confirmationsReactor = shopDomain.reactor("confirmations")
  .on("OrderSubmitted", async (_data, evt, ctx) => {
    // The read models (order 0) have processed this event before the reactor (order 10) gets it.
    const order = await ctx.query<{ customerId: string }>("GetOrder", { id: evt.aggregateId });
    const customer = await ctx.query<{ email: string }>("GetCustomer", { id: order.customerId });
    // Same sender and correlation as the submit that caused the event; a rejection parks the event.
    await ctx.submit({
      aggregateType: "order",
      aggregateId: evt.aggregateId,
      type: "Confirm",
      data: { customerId: order.customerId, email: customer.email },
      id: `confirm-${evt.aggregateId}`, // a redelivered event confirms once
    });
  });

await kernable.listener(confirmationsReactor).run();
// #endregion reactor
