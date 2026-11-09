"""The shop's API (part 9): the kit's domains with the authorization middleware, and the kit's
example HTTP server. People sign in here; the kernel and the kit decide what they may do."""

import os
import sys
import threading

from kernable import Kernable

# The kit's source, copied into the project with `kernable-sdk add kit`; here the repository's.
from example.server import example_server
from kit import kit_domains, relation
from kit.api.api import KitApi
from kit.api.tokens import new_signing_keys
from kit.bootstrap import bootstrap
from kit.shared.mail import use_mailer

from domain import shop_domain

# region relation
# Who owns an order: the customer it was placed for. Declared before the kit's domains start.
relation("shop.order", "owner").set("shop.OrderPlaced", lambda e: e.data["customer_id"])
# endregion relation


# region mail
# In development the kit's mails go to files in mail/, named by address, instead of a mail server.
class FileMailer:
    def send(self, mail) -> None:
        with open(os.path.join("mail", mail.to), "w") as f:
            f.write(mail.token)


os.makedirs("mail", exist_ok=True)
use_mailer(FileMailer())
# endregion mail

# region run
# The kit's domains: its aggregates, read models, reactors and the authorization middleware.
kit = Kernable().app(service="kit", domains=kit_domains())
threading.Thread(target=kit.run, daemon=True).start()
if not kit.ready.wait(30):
    sys.exit("the kit's service did not connect")

# The API's client knows the shop's schemas too: GetOrder declares the order it is about.
kernable = Kernable().use(*kit_domains(), shop_domain)
# Once: the platform tenant, the sign-up policy, and the API's own identity and key.
service = bootstrap(kernable, sign_up={"accounts": "open", "tenants": "active"})["api"]
api = KitApi(kernable, new_signing_keys(), service)
example_server(api, kernable, int(os.environ.get("PORT", "3000"))).serve_forever()
# endregion run
