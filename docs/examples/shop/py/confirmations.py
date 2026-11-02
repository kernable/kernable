"""The reactor: confirms every submitted order to the customer's email."""

from kernable import Kernable, RequestContext

from domain import shop_domain

# region reactor
k = Kernable(context=RequestContext(sender_identity="svc-confirmations")).use(shop_domain)  # parts 6 and 7
confirmations_reactor = shop_domain.reactor("confirmations")


@confirmations_reactor.on("OrderSubmitted")
def _(data, evt, ctx):
    # The read models (order 0) have processed this event before the reactor (order 10) gets it.
    order = ctx.query("GetOrder", {"id": evt.aggregate_id})
    customer = ctx.query("GetCustomer", {"id": order["customerId"]})
    # Same sender and correlation as the submit that caused the event; a rejection parks the event.
    ctx.submit("order", evt.aggregate_id, "Confirm", {"customerId": order["customerId"], "email": customer["email"]},
               id=f"confirm-{evt.aggregate_id}")  # a redelivered event confirms once

k.listener(confirmations_reactor).run()
# endregion reactor
