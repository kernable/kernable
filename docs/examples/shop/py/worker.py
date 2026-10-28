"""The worker: runs the shop's commands."""

from kernable import Kernable

from domain import customer_aggregate, order_aggregate

# region worker
k = Kernable()
k.worker(customer_aggregate, order_aggregate).run()
# endregion worker
