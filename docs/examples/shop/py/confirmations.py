"""The reactor: confirms every submitted order to the customer's email."""

from kernable import RequestContext

from connect import connect
from domain import shop

# region reactor
k = connect(context=RequestContext(sender_identity="svc-confirmations")).use(shop)  # parts 6 and 7
reactor = k.reactor("confirmations")


@reactor.on("shop.OrderSubmitted")
def _(e, ctx):
    # The read models (order 0) have processed this event before the reactor (order 10) gets it.
    order = k.query("shop", "GetOrder", {"id": e.aggregate_id})
    customer = k.query("shop", "GetCustomer", {"id": order["customerId"]})
    k.submit("shop", "Order", e.aggregate_id, "Confirm", {"customerId": order["customerId"], "email": customer["email"]},
             id=f"confirm-{e.aggregate_id}",  # a redelivered event confirms once
             context=ctx.follow())  # same sender and correlation as the submit that caused it


reactor.run()
# endregion reactor
