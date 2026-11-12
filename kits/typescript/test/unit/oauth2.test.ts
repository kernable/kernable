// Sign-in through a plain OAuth2 provider (GitHub): its endpoints and which address counts. The
// Rust and Python kits decide the same way.

import assert from "node:assert/strict";
import { test } from "node:test";

import { github, oauth2Email } from "../../src/api/api.ts";

test("the primary verified address is the person's", () => {
  assert.equal(oauth2Email([
    { email: "old@example.com", primary: false, verified: true },
    { email: "ada@example.com", primary: true, verified: true },
  ]), "ada@example.com");
  // A primary address that is not verified does not count; another verified one does.
  assert.equal(oauth2Email([
    { email: "ada@example.com", primary: true, verified: false },
    { email: "work@example.com", primary: false, verified: true },
  ]), "work@example.com");
  assert.equal(oauth2Email([{ email: "ada@example.com", primary: true, verified: false }]), null);
  assert.equal(oauth2Email({ message: "Bad credentials" }), null);
});

test("github names its endpoints on its web and API hosts", () => {
  const p = github("id", "secret", "https://github.com/", "https://api.github.com");
  assert.equal(p.oauth2?.authorize, "https://github.com/login/oauth/authorize");
  assert.equal(p.oauth2?.token, "https://github.com/login/oauth/access_token");
  assert.equal(p.oauth2?.user, "https://api.github.com/user");
  assert.equal(p.oauth2?.emails, "https://api.github.com/user/emails");
});
