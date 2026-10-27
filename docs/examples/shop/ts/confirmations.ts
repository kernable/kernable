// The reactor: confirms every submitted order to the customer's email.
import { connect } from "./connect.ts";
import { shop } from "./domain.ts";

// #region reactor
// Who this service is (part 6), and the domain's declarations, e.g. which fields are personal (part 7).
const kernable = connect({ context: { senderIdentity: "svc-confirmations" } }).use(shop);

const query = <T>(type: string, id: string) => kernable.query<T>({ domain: "shop", type, data: { id } });

await kernable.reactor("confirmations")
  .on("shop.OrderSubmitted", async (e, ctx) => {
    // The read models (order 0) have processed this event before the reactor (order 10) gets it.
    const order = await query<{ customerId: string }>("GetOrder", e.aggregateId);
    const customer = await query<{ email: string }>("GetCustomer", order.customerId);
    await kernable.submit({
      domain: "shop",
      aggregateType: "Order",
      aggregateId: e.aggregateId,
      type: "Confirm",
      data: { customerId: order.customerId, email: customer.email },
      id: `confirm-${e.aggregateId}`, // a redelivered event confirms once
      context: ctx.follow(), // same sender and correlation as the submit that caused it
    });
  })
  .run();
// #endregion reactor
