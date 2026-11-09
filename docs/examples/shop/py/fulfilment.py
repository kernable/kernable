"""Registers the fulfilment process with the kernel: once per deploy, after the worker is up."""

from kernable import Kernable

from domain import fulfilment

# region register
k = Kernable()
registered = k.register_process(fulfilment)
print(f"{registered.name} version {registered.version}{'' if registered.new else ' (unchanged)'}")
k.close()
# endregion register
