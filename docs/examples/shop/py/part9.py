"""Part 9: people sign in through the shop's API, and the kit decides what they may do."""

import json
import os
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:3000")


# region client
# A small client of the API: a person's access token and, inside the shop, its tenant.
def call(method: str, path: str, body: dict | None, person: dict | None = None):
    headers = {"content-type": "application/json"}
    if person and person.get("token"):
        headers["authorization"] = f"Bearer {person['token']}"
    if person and person.get("tenant"):
        headers["x-tenant"] = person["tenant"]
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{API}{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request) as res:
            status, raw = res.status, res.read()
    except urllib.error.HTTPError as err:
        status, raw = err.code, err.read()
    return status, (json.loads(raw) if raw and status != 204 else None)


# Registers, confirms the email with the token from the mail (a file in development), signs in.
def sign_up(name: str) -> dict:
    email = f"{name.lower()}@example.com"
    call("POST", "/register", {"email": email, "name": name, "password": "correct horse battery"})
    with open(f"mail/{email}") as f:
        call("POST", "/verify", {"token": f.read()})
    status, signed_in = call("POST", "/sign-in", {"email": email, "password": "correct horse battery"})
    print(f"{name} signs up and signs in: {status}")
    return {"email": email, "token": signed_in["accessToken"]}
# endregion client


# region owner
ann = sign_up("Ann")
opened, shop = call("POST", "/tenants", {"slug": "anns-shop", "name": "Ann's shop"}, ann)
ann["tenant"] = shop["tenant"]
print(f"Ann opens her shop: {opened}")

# Customers may place orders, and work only on orders they own.
grouped, _ = call("POST", "/commands/group/group/CreateGroup", {
    "aggregateId": "customers",
    "data": {
        "name": "Customers",
        "permissions": [
            {"pattern": "shop.Place"},
            {"pattern": "shop.AddItem", "when": "owner"},
            {"pattern": "shop.Submit", "when": "owner"},
            {"pattern": "shop.GetOrder", "when": "owner"},
        ],
    },
}, ann)
print(f"Ann creates the group Customers: {grouped}")

linked, link = call("POST", "/invitation-links", {"groups": [{"group": "customers"}], "label": "the shop's website"}, ann)
print(f"Ann creates an invitation link: {linked}")
# endregion owner

# region customers
bob = sign_up("Bob")
cleo = sign_up("Cleo")
for name, person in (("Bob", bob), ("Cleo", cleo)):
    joined, _ = call("POST", "/invitation-links/join", {"token": link["token"]}, person)
    person["tenant"] = shop["tenant"]
    print(f"{name} joins through the link: {joined}")

_, me = call("GET", "/me", None, bob)
placed, _ = call("POST", "/commands/shop/order/Place", {"aggregateId": "o1", "data": {"customerId": me["senderIdentity"]}}, bob)
print(f"Bob places o1: {placed}")
added, _ = call("POST", "/commands/shop/order/AddItem", {"aggregateId": "o1", "data": {"sku": "apple", "qty": 2}}, bob)
print(f"Bob adds two apples: {added}")
cleo_adds, cleo_adds_body = call("POST", "/commands/shop/order/AddItem", {"aggregateId": "o1", "data": {"sku": "pear", "qty": 1}}, cleo)
print(f"Cleo adds to o1: {cleo_adds} {cleo_adds_body['error']}")

bob_reads, order = call("POST", "/queries/shop/GetOrder", {"data": {"id": "o1"}}, bob)
print(f"Bob reads o1: {bob_reads} {order['status']}, {json.dumps(order['items'], separators=(',', ':'))}")
cleo_reads, refused = call("POST", "/queries/shop/GetOrder", {"data": {"id": "o1"}}, cleo)
print(f"Cleo reads o1: {cleo_reads} {refused['error']}")
# endregion customers

# region rule
_, cleo_me = call("GET", "/me", None, cleo)
for_cleo, for_cleo_body = call("POST", "/commands/shop/order/Place", {"aggregateId": "o2", "data": {"customerId": cleo_me["senderIdentity"]}}, bob)
print(f"Bob places o2 in Cleo's name: {for_cleo} {for_cleo_body['error']}")
# endregion rule

# region explain
_, why = call("POST", "/queries/group/Explain", {
    "data": {"identityId": cleo_me["senderIdentity"], "type": "shop.GetOrder", "aggregateType": "order", "aggregateId": "o1"},
}, cleo)
print(f"Why not? allowed: {str(why['allowed']).lower()}, {why['code']}")

anonymous, nobody = call("POST", "/commands/shop/order/Place", {"aggregateId": "o3", "data": {}}, {"tenant": shop["tenant"]})
print(f"Without signing in: {anonymous} {nobody['error']}")
# endregion explain
