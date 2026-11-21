# Kit parity

The kit exists in TypeScript, Python, Rust and Go. Each passes the shared fixtures (`../fixtures`),
which hold single commands and authorization checks. Parity goes further: a whole application's
life, run through every pair of languages, must leave exactly the same results, mails and events.

```sh
make parity                                              # all sixteen pairs
node --conditions=source --no-warnings kits/parity/run.ts ts,rs py   # some: host languages, driver languages
```

## How it runs

- `scenario.ts` writes `scenario.json`: about 500 steps that follow the kits' slice tests
  (registration, sign-in, tenants, invitations and links, groups and relations, sessions and
  refresh, password and email changes, TOTP, API keys, passkeys, OAuth and tenant SSO, Explain, a
  process's identity, a restart of the app) and read every list at the end. A step may say what it
  must answer (`expect`).
- Each kit has a runner: `typescript/test/parity.ts`, `python/tests/parity.py`,
  `rust/examples/parity.rs`, `sdks/go/internal/kitparity` (the Go kit lives in the Go SDK's
  module, `sdks/go/kit`). As the **host** it runs the kit's domains and a small shop (orders
  with an owner relation, a parcel process) on a kernel, its mails written to a file. As the
  **driver** it runs the steps through its own `KitApi` and SDK, and writes what each step
  answered, in the shapes the TypeScript kit answers in.
- `run.ts` starts a kernel per pair, the host in one language and the driver in another, restarts
  the host when the scenario asks, then reads every stored event.

## What is compared

Per pair: every step's answer, every mail, and every event (its stream, type, schema, data,
redacted fields and request context), grouped by stream. Values that differ on every run become
placeholders, numbered where they first appear, so a value used twice must be the same value:
IDs, tokens, codes, hashes, times, TOTP steps, local ports. JSON keys and URL parameters are
sorted. Everything else must be equal byte for byte to the TypeScript/TypeScript run. Results land
in `target/parity/` (`<host>-<driver>.json`, `.raw.json` and a `.diff` for each run that differs).

## What it found (2026-11-20)

- The Rust kit listed identities, invitations, links and groups in ID order (random); the others
  list them in the order they were created. Rust now keeps creation order too.
- The Rust kit verified ES256 passkeys only, while the TypeScript and Python kits offer and accept
  EdDSA and RS256 as well: a passkey registered through one of those (Windows Hello may use RS256)
  could not sign in through Rust. Rust now verifies all three, and offers them.
- The passkey options differed (display name, timeout, library extras): all kits now send the
  same options, with the challenge's lifetime as the timeout.
