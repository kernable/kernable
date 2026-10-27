"""The worker: runs the shop's commands."""

from connect import connect
from domain import customer, order

# region worker
k = connect()
k.worker(customer, order).run()
# endregion worker
