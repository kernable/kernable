"""Sign-in through a plain OAuth2 provider (GitHub): its endpoints and which address counts. The
Rust and TypeScript kits decide the same way."""

import unittest

from kit.api.api import github, oauth2_email


class OAuth2(unittest.TestCase):
    def test_the_primary_verified_address_is_the_persons(self):
        self.assertEqual(oauth2_email([
            {"email": "old@example.com", "primary": False, "verified": True},
            {"email": "ada@example.com", "primary": True, "verified": True},
        ]), "ada@example.com")
        # A primary address that is not verified does not count; another verified one does.
        self.assertEqual(oauth2_email([
            {"email": "ada@example.com", "primary": True, "verified": False},
            {"email": "work@example.com", "primary": False, "verified": True},
        ]), "work@example.com")
        self.assertIsNone(oauth2_email([{"email": "ada@example.com", "primary": True, "verified": False}]))
        self.assertIsNone(oauth2_email({"message": "Bad credentials"}))

    def test_github_names_its_endpoints_on_its_web_and_api_hosts(self):
        o = github("id", "secret", "https://github.com/", "https://api.github.com")["oauth2"]
        self.assertEqual(o["authorize"], "https://github.com/login/oauth/authorize")
        self.assertEqual(o["token"], "https://github.com/login/oauth/access_token")
        self.assertEqual(o["user"], "https://api.github.com/user")
        self.assertEqual(o["emails"], "https://api.github.com/user/emails")


if __name__ == "__main__":
    unittest.main()
