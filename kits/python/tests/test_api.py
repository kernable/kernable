"""The API's parts that need no kernel: access tokens, and AsyncKitApi offering every KitApi method."""

import inspect
import unittest

from kit.api.aio import AsyncKitApi
from kit.api.api import KitApi
from kit.api.tokens import check_access_token, issue_access_token, new_signing_keys


class TokensTest(unittest.TestCase):
    def test_round_trip(self):
        keys = new_signing_keys()
        claims = check_access_token(keys, issue_access_token(keys, "a1", "s1", ["password"]))
        self.assertEqual((claims.sub, claims.sid, claims.fac), ("a1", "s1", ["password"]))

    def test_refuses_another_key_a_change_and_an_expired_token(self):
        keys = new_signing_keys()
        t = issue_access_token(keys, "a1", "s1", ["password"])
        self.assertIsNone(check_access_token(new_signing_keys(), t))
        head, body, sig = t.split(".")
        self.assertIsNone(check_access_token(keys, f"{head}.{body[:-2]}xx.{sig}"))
        self.assertIsNone(check_access_token(keys, issue_access_token(keys, "a1", "s1", [], now_ms=0)))
        self.assertIsNone(check_access_token(keys, "nonsense"))


class AsyncTest(unittest.TestCase):
    def test_every_method_has_its_async_twin_with_the_same_parameters(self):
        for name, method in inspect.getmembers(KitApi, inspect.isfunction):
            if name.startswith("_"):
                continue
            twin = getattr(AsyncKitApi, name, None)
            self.assertIsNotNone(twin, name)
            self.assertEqual(list(inspect.signature(method).parameters), list(inspect.signature(twin).parameters), name)


if __name__ == "__main__":
    unittest.main()
