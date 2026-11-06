"""Events, commands and queries as classes (messages.py): typed, snake_case in Python, camelCase in the log."""

import unittest
from typing import Literal

from kernable import Command, CommandContext, Event, Query, Rejected, domain
from kernable.messages import camel, from_wire, snake


class ItemAdded(Event):
    version = 2
    upcasts = {1: lambda d: {"sku": d["sku"], "item_count": d["qty"]}}
    sku: str
    item_count: int


class CartCleared(Event):
    pass


class AddItem(Command):
    secret = ["coupon_code"]
    sku: str
    item_count: int
    coupon_code: str | None = None


class CountItems(Query[int]):
    cart_id: str


class Renamed(Event):
    type = "CartRenamed"
    new_name: str


shop = domain("shop")
cart = shop.aggregate("cart", {"items": 0, "name": None})


@cart.apply(ItemAdded)
def _(s, d: ItemAdded):
    s["items"] += d.item_count


@cart.apply(CartCleared)
def _(s):
    s["items"] = 0


@cart.apply(Renamed)
def _(s, d: Renamed):
    s["name"] = d.new_name


@cart.command(AddItem)
def _(s, d: AddItem, cmd):
    if d.item_count < 1:
        raise Rejected("Add at least one", "invalid_count")
    return [ItemAdded(sku=d.sku, item_count=d.item_count)]


class MessagesTest(unittest.TestCase):
    def test_names_convert_between_snake_case_and_camel_case(self):
        self.assertEqual(camel("refresh_token_hash"), "refreshTokenHash")
        self.assertEqual(snake("refreshTokenHash"), "refresh_token_hash")
        self.assertEqual(camel("ip"), "ip")
        # Keys that are no names (IDs, paths, types) stay as they are.
        self.assertEqual(from_wire({"t1/o-1": {"viaParent": True}, "shop.order": 1}), {"t1/o-1": {"via_parent": True}, "shop.order": 1})

    def test_a_class_is_its_type_and_its_payload_in_camel_case(self):
        events = cart.handle(cart.initial, "AddItem", {"sku": "apple", "itemCount": 2}, CommandContext.new("c-1"))
        self.assertEqual(events, [{"type": "ItemAdded", "data": {"sku": "apple", "itemCount": 2}}])
        self.assertEqual(shop.events["ItemAdded"].schema, 2)
        self.assertEqual(shop.commands["AddItem"].vault.secret, ["couponCode"])

    def test_the_class_upcasts_and_folds(self):
        state = cart.fold([("ItemAdded", 1, {"sku": "pear", "qty": 3}), ("CartRenamed", 1, {"newName": "x"})])
        self.assertEqual(state, {"items": 3, "name": "x"})

    def test_a_missing_or_unknown_field_raises(self):
        with self.assertRaises(TypeError):
            ItemAdded(sku="x")  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            ItemAdded(sku="x", item_count=1, colour="red")  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            ItemAdded("x", 1)  # type: ignore[misc]  keyword-only

    def test_raw_fields_pass_through_as_they_are(self):
        class AddPasskey(Command):
            raw = ["response"]
            response: dict
            label_text: str

        wire = {"response": {"clientDataJSON": "x", "rawId": "y"}, "labelText": "Laptop"}
        self.assertEqual(AddPasskey.from_wire(wire).response, {"clientDataJSON": "x", "rawId": "y"})
        self.assertEqual(AddPasskey.from_wire(wire).to_wire(), wire)

    def test_an_unset_optional_field_is_left_out(self):
        self.assertEqual(AddItem(sku="a", item_count=1).to_wire(), {"sku": "a", "itemCount": 1})

    def test_a_read_model_listens_and_answers_with_classes_also_of_another_domain(self):
        reports = domain("reports")
        counts = reports.read_model("counts")
        seen = []

        @counts.on(ItemAdded)
        def _(d: ItemAdded):
            seen.append(d.item_count)

        @counts.query(CountItems)
        def _(d: CountItems):
            return {"cart_id": d.cart_id, "total_items": sum(seen)}

        self.assertIn("shop.ItemAdded", counts._handlers)
        self.assertIn("reports.CountItems", counts._queries)
        counts._handlers["shop.ItemAdded"]({"sku": "a", "itemCount": 4})
        self.assertEqual(counts._queries["reports.CountItems"]({"cartId": "c1"}), {"cartId": "c1", "totalItems": 4})

    def test_a_renamed_class_and_an_undeclared_one_are_refused(self):
        class e(Event):  # noqa: N801  what a minifier would leave
            pass

        with self.assertRaises(ValueError):
            shop.aggregate("other", {}).apply(e, lambda s: None)

        class Unused(Event):
            pass

        with self.assertRaises(ValueError):
            domain("x").read_model("r").on(Unused, lambda d: None)


if __name__ == "__main__":
    unittest.main()


class ClassAggregateTest(unittest.TestCase):
    def test_an_aggregate_class_is_its_state_and_registers_itself(self):
        from kernable import Aggregate

        library = domain("library")

        class BookAdded(Event):
            title: str

        class AddBook(Command):
            title: str

        class Shelf(Aggregate, domain=library):
            count: int = 0
            titles: list = []

        @Shelf.apply(BookAdded)
        def _(s: Shelf, d: BookAdded):
            s.count += 1
            s.titles.append(d.title)

        @Shelf.command(AddBook)
        def _(s: Shelf, d: AddBook):
            if d.title in s.titles:
                raise Rejected("On the shelf already", "exists")
            return [BookAdded(title=d.title)]

        Shelf.unique("first", lambda s: s.titles[0] if s.titles else None)
        self.assertEqual(library.aggregates[0].name, "shelf")
        state = Shelf.fold([("BookAdded", 1, {"title": "Dune"})])
        self.assertEqual((state.count, state.titles), (1, ["Dune"]))
        self.assertEqual(Shelf().titles, [], "a mutable default is copied for every aggregate")
        self.assertEqual(Shelf.handle(state, "AddBook", {"title": "Emma"}, CommandContext.new("c")), [{"type": "BookAdded", "data": {"title": "Emma"}}])
        with self.assertRaises(Rejected):
            Shelf.handle(state, "AddBook", {"title": "Dune"}, CommandContext.new("c"))
        self.assertEqual([k.value for k in Shelf.index(state).keys], ["Dune"])
        # A snapshot holds the fields as JSON, and comes back as the class.
        definition = Shelf.__aggregate__
        self.assertEqual(definition._to_json(state), {"count": 1, "titles": ["Dune"]})
        self.assertEqual(definition._from_json({"count": 1, "titles": ["Dune"], "gone": 3}), state)
