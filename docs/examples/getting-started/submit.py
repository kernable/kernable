from kernable import Kernable

k = Kernable(key="app.key")
cart_c1 = dict(domain="shop", aggregate_type="cart", aggregate_id="c1")

k.submit(**cart_c1, type="AddItem", data={"sku": "apple", "qty": 2})
# submit returned only after every listener processed the event: no polling needed.
cart = k.query(domain="shop", type="GetCart", data={"id": "c1"})
print(cart)
# {'items': 2}

k.submit(**cart_c1, type="CheckOut")
cart = k.query(domain="shop", type="GetCart", data={"id": "c1"})
print(cart)
# {'items': 2, 'order': '0b9c5e1a-6f3d-8a42-9c1e-5d7f2b8e4a10'}

outcome = k.submit(**cart_c1, type="AddItem", data={"sku": "pear", "qty": 1})
print(outcome)
# Outcome(committed=False, events=[], code='rejected', message='Cart is checked out')
