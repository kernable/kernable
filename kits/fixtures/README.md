# Kit fixtures

The contract every language's kit is held to: the TypeScript, Python and Rust kits run these
same cases. A kit a customer changed may leave them; the kits as shipped pass them all.

## Aggregates: `<domain>.json`

```json
{ "domain": "tenancy", "cases": [{
  "name": "…", "aggregate": "tenant", "aggregateId": "t1", "tenant": "t1", "now": 1000000,
  "given": [{ "type": "TenantCreated", "data": { … } }],
  "when":  { "type": "SuspendTenant", "data": { "reason": "unpaid" } },
  "then":  [{ "type": "TenantSuspended", "data": { "reason": "unpaid", "at": 1000000 } }],
  "index": { "keys": [{ "name": "slug", "value": "acme", "scope": "global" }], "facts": ["exists"] }
}]}
```

Fold `given` (schema 1, vault values in clear), run `when` at `now` (ms) with the aggregate ID and
tenant, and compare the produced events with `then` field by field (no field more, none less), or
the rejection's code with `rejected`. `index`, if present: the unique keys and facts the aggregate
holds after the new events. `requires`, if present: the facts of other aggregates the command
requires (`[{ "domain", "aggregateType", "aggregateId", "fact" }]`, plus `key` and `value` for an
aggregate named by a key, `absent: true` for a fact it must not hold). Values that differ on every run are placeholders: `"<string>"` (a
non-empty string: random IDs and tokens), `"<number>"`, `"<argon2>"` (an argon2id hash), `"<any>"`.

## Authorization: `authorization.json`

`relations` are declared first (`relation(type, name).set(event, e => e.data[field])`). For each
case, the middleware learns the `world` events and the case's `extra` ones (each with its domain,
aggregate type, type, tenant, aggregate ID and data), then checks `check` (a command or query with
its request context): `"allow"`, or refused with the code `expect` names. Expiry is checked
against the clock: past values are 500, future ones far ahead.
