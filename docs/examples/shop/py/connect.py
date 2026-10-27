"""How every process of the shop connects: from the environment (part 8), or to the local kernel
of part 1."""

import os

from kernable import Kernable, Tls


# region connect
def connect(**options) -> Kernable:
    env = os.environ
    tls = Tls(env["KERNABLE_TLS_CA"], env["KERNABLE_TLS_CERT"], env["KERNABLE_TLS_KEY"]) if "KERNABLE_TLS_CA" in env else None
    return Kernable(env.get("KERNABLE_URL", f"unix:{os.getcwd()}/k.sock"), env.get("KERNABLE_KEY", "app.key"), tls=tls,
                    **options)
# endregion connect
