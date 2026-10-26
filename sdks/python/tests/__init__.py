import os

# The tests start the kernel and `kernable verify` as subprocesses while gRPC threads run;
# gRPC's fork handlers then break the child (it dies of SIGPIPE). Must be set before grpc loads.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "false")
