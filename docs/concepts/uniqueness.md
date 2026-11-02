# Unique keys and conditions

An aggregate decides alone, on its own history ([Event sourcing](./event-sourcing#rules-across-aggregates)). Two kinds of rules still reach across aggregates, and the kernel checks both when a command commits:

- **Uniqueness.** Every fund has its own PMDX ID, and no two open funds of a tenant share a name. Each fund decides alone, so two funds launched at the same moment with the same ID would both commit.
- **A condition on another aggregate.** An investor account opens only while its fund is open. A check against a read model before the submit leaves a gap: the fund can be wound up between the check and the commit.

An aggregate declares **unique keys** and **facts**, both computed from its own state, and a command declares the facts it **requires** of other aggregates. Handlers and events do not change.

## Unique keys

A unique key is a name and a function of the state that returns the value the aggregate holds:

::: code-group
<<< @/examples/concepts/uniqueness.ts#fund{ts} [TypeScript]

```python [Python]
from kernable import Rejected, Requirement, domain

funds_domain = domain("funds")
fund_aggregate = funds_domain.aggregate("fund", {"status": "none"})


@fund_aggregate.apply("FundLaunched")
def _(state, data):
    return {"status": "open", **data}


@fund_aggregate.apply("FundWoundUp")
def _(state, data):
    state["status"] = "wound_up"


@fund_aggregate.command("Launch")
def _(state, data):
    if state["status"] != "none":
        raise Rejected("Fund already exists", "exists")
    return [{"type": "FundLaunched", "data": {"pmdxId": data["pmdxId"], "name": data["name"]}}]


@fund_aggregate.command("WindUp")
def _(state, data):
    if state["status"] != "open":
        raise Rejected(f"Cannot wind up a {state['status']} fund", "invalid_state")
    return [{"type": "FundWoundUp"}]


# One PMDX ID per fund in all tenants, kept after winding up.
fund_aggregate.unique("pmdxId", lambda s: s["pmdxId"].strip().upper() if "pmdxId" in s else None, scope="global")
# One name per open fund in each tenant: winding up frees it.
fund_aggregate.unique("name", lambda s: s["name"].strip().lower() if s["status"] == "open" else None)
fund_aggregate.fact("open", lambda s: s["status"] == "open")
```

<<< @/examples/concepts/uniqueness.rs#fund{rust} [Rust]

:::

After every command, also one without events, the worker computes the keys from the new state and sends the full set. The kernel compares it with what the aggregate holds and stores the difference with the commit. So an aggregate always holds exactly what its state yields:

| The function returns | The aggregate holds |
|---|---|
| a string | that value |
| a list of strings | each value once |
| `null`, `undefined`, `None` | nothing |

- **Freeing cannot be forgotten.** A changed value frees the old one in the same commit; a value that becomes null frees it. There is no separate step to release a key.
- **Scope.** `tenant` (the default) makes a value unique within the command's tenant; another tenant may hold the same value. `global` makes it unique across tenants.
- **Per aggregate type.** A key belongs to its domain, aggregate type and name: `pmdxId` of `funds.fund` never collides with a `pmdxId` of another aggregate type.
- **Normalising is yours.** The kernel compares the SHA-256 of the value's UTF-8 bytes, so `LU0001` and `lu0001 ` are two values. Trim, lowercase or format the value in the function.
- **Deterministic**, like `apply`: the function sees only the state. If it throws, the command is rejected as for a handler error (`handler_error`).

## Facts and requirements

A fact is a name and a function of the state that says whether the aggregate holds it: the fund above holds `open` while its status is open. A command declares the facts other aggregates must hold for it to commit:

::: code-group
<<< @/examples/concepts/uniqueness.ts#account{ts} [TypeScript]

```python [Python]
account_aggregate = funds_domain.aggregate("account", {"status": "none"})


@account_aggregate.apply("AccountOpened")
def _(state, data):
    return {"status": "open", "fundId": data["fundId"]}


# Commits only while the fund is open, checked by the kernel at commit.
@account_aggregate.command("OpenAccount",
                           requires=lambda data, state: [Requirement("funds", "fund", data["fundId"], "open")])
def _(state, data):
    if state["status"] != "none":
        raise Rejected("Account already exists", "exists")
    return [{"type": "AccountOpened", "data": {"fundId": data["fundId"]}}]
```

<<< @/examples/concepts/uniqueness.rs#account{rust} [Rust]

:::

- **From the data and the state before the command.** The requirement function gets both; a command that requires a fact of its own aggregate sees what the aggregate holds before it.
- **Same tenant.** A requirement names a domain, an aggregate type, an aggregate ID and a fact, always in the command's tenant. The domain may be another one.
- **At commit.** The kernel checks what the named aggregate holds at the moment the command commits. If the fund was wound up a moment before, the account is rejected with `requirement_missing`.
- **No lock.** A requirement does not hold the fund open afterwards: winding it up later still commits. Blocking a change while other aggregates refer to it is not covered.

### By a key, or absent

A requirement can name its aggregate by a **unique key** of its type instead of its ID: the fund whose `pmdxId` is `FND-lyra`. The kernel finds the aggregate that holds the key when the command commits, always in the command's tenant: a global key held in another tenant names no aggregate here, so a requirement never sees another tenant's facts. No aggregate holds it: then none holds the fact either. So a command needs no lookup of the ID first, and the check stays atomic. A requirement can also be **absent**: the aggregate must not hold the fact. An aggregate without events, or a key nobody holds, holds no fact, so "the vehicle is gone or wound up" is the fact `open` absent. Without a domain, a requirement is of the command's own domain.

::: code-group

```ts [TypeScript]
requires: (data) => [
  // By the fund's PMDX ID, normalised as its key function does.
  { domain: "funds", aggregateType: "fund", key: "pmdxId", value: data.fundPmdxId.toLowerCase(), fact: "open" },
  // Only while the vehicle (own domain) is not wound up, or does not exist.
  { aggregateType: "vehicle", aggregateId: data.vehicleId, fact: "woundUp", absent: true },
],
```

```python [Python]
requires=lambda data, state: [
    Requirement.by_key("fund", "pmdxId", data["fundPmdxId"].lower(), "open", domain="funds"),
    Requirement.of("vehicle", data["vehicleId"], "woundUp", absent=True),
]
```

```rust [Rust]
.requires("OpenAccount", |_, d: Open| [
    Requirement::by_key("funds", "fund", "pmdxId", &d.fund_pmdx_id.to_lowercase(), "open"),
    Requirement::of("vehicle", &d.vehicle_id, "woundUp").absent(),
])
```

:::

- **The value as the key has it.** The kernel compares the SHA-256 of the value: normalise it exactly as the aggregate type's key function does (`toLowerCase` above), or nobody holds it.
- **Not personal keys.** A personal key lives in the vault and cannot name an aggregate.
- **Never the value.** A rejection names the key (`funds.fund with key pmdxId does not hold open`), never its value; `requirement.key` says which, and `requirement.value` is empty.

## Personal keys

A key built from personal data, such as an email, names its subject:

::: code-group
<<< @/examples/concepts/uniqueness.ts#investor{ts} [TypeScript]

```python [Python]
investor_aggregate = funds_domain.aggregate("investor", {})


@investor_aggregate.apply("InvestorRegistered")
def _(state, data):
    return dict(data)


@investor_aggregate.command("RegisterInvestor")
def _(state, data, cmd):
    if "investorId" in state:
        raise Rejected("Investor already registered", "exists")
    return [{"type": "InvestorRegistered", "data": {"investorId": cmd.aggregate_id, "email": data["email"]}}]


# A personal key: kept in the vault, freed when the investor is erased.
investor_aggregate.unique("email", lambda s: s["email"].strip().lower() if s.get("email") else None,
                          personal=lambda s: s.get("investorId"))

funds_domain.command("RegisterInvestor").personal(["email"], lambda d, investor_id: investor_id)
funds_domain.event("InvestorRegistered", 1).personal(["email"], lambda d, investor_id: investor_id)
```

<<< @/examples/concepts/uniqueness.rs#investor{rust} [Rust]

:::

- **Kept in the vault.** The kernel keeps a personal key in the [vault](./data#personal-data-the-vault), as a value of its subject, never in the store. The kernel needs a vault (`--vault-key`); without one, the command is rejected with `no_vault`.
- **Erasing the subject frees it.** After an erasure another aggregate can take the value, and neither the log nor the store keeps a trace of it.
- **No subject, no key.** A personal key whose subject is empty or null is not held.
- **The worker must see the value.** Declare the field personal on the command and the event, as above, and grant the worker's key `vault_read`: without it the state holds `null`, and the aggregate holds no key.

## What the kernel guarantees

The kernel checks keys and requirements while it commits, under the lock that orders commits:

- **Never two holders.** Two aggregates never hold the same key at the same time. Of two commands racing for one value, exactly one commits; the other is rejected with `unique_taken`.
- **Atomic.** The keys and facts an aggregate holds change with its events, in the same commit. A rejected command writes nothing, and is stored as rejected like any other ([Errors](../reference/errors)).
- **Never the value.** A rejection names the key or the fact, not the value, and not the aggregate that holds it.
- **Rebuilt from the store.** The kernel loads what every aggregate holds when it starts; it never runs your fold code.

| Code | Message | Means |
|---|---|---|
| `unique_taken` | `funds.fund key pmdxId is taken` | Another aggregate holds a key the command would leave its aggregate with. |
| `requirement_missing` | `funds.fund f1 does not hold open`, `funds.fund with key pmdxId does not hold open`, `funds.vehicle v1 holds woundUp` | An aggregate the command requires does not hold the fact, or holds one it must not. |
| `invalid_index` | says which limit | Too many keys, facts or requirements, or a malformed name: a bug in the aggregate's definition. |

A rejection also names what failed as data, so you need not parse the message: `key` is the name of the taken key (`pmdxId`), `requirement` the requirement that does not hold (domain, aggregate type, ID, fact). In TypeScript and Python they are `outcome.key` and `outcome.requirement`, in Rust `Rejected::key` and `Rejected::requirement`. Only the kernel sets them, and they are stored with the rejection, so the same command ID gets them back.

- **One reason at a time.** The kernel checks the requirements first, in the order `requires` returns them, then the keys. The rejection names the first requirement that does not hold, or else one taken key. Fix that one and submit again: another requirement or key may fail next.

Workers send keys and facts signed over `kernable.v1.index` ([Signatures](../reference/signing#unique-keys-and-facts)).

## Aggregates from before a key

An aggregate that had events before a key or fact was declared holds nothing of it until its next command. `reindex` runs a command on each of them now:

::: code-group

```ts [TypeScript]
const outcomes = await kernable.reindex({ domain: "funds", aggregateType: "fund", aggregateIds: ["f1", "f2"] });
for (const [id, outcome] of outcomes) if (!outcome.committed) console.log(id, outcome.code, outcome.message);
```

```python [Python]
outcomes = k.reindex("funds", "fund", ["f1", "f2"])
for id, outcome in outcomes.items():
    if not outcome.committed:
        print(id, outcome.code, outcome.message)
```

```rust [Rust]
let outcomes = kernable.reindex(Reindex::new("funds", "fund", ["f1", "f2"])).await?;
for (id, outcome) in outcomes {
    if let Outcome::Rejected(r) = outcome {
        println!("{id}: {} {}", r.code, r.message);
    }
}
```

:::

- **A built-in command.** Every aggregate type with keys or facts handles `_reindex_<aggregateType>` (here `_reindex_fund`). It produces no events; the worker sends the set of the current state, and the kernel stores it. Command types starting with `_reindex_` are reserved for it.
- **Grants.** The worker registers the command, so its key needs `run` for `funds._reindex_fund`, which `run=funds.*` covers; the client's key needs `submit` for it ([Keys per service](../how-to/keys)).
- **An ID without events** holds nothing, and its reindex commits.
- **Duplicates from before.** If two existing funds share a PMDX ID, the first to run a command takes the key. The second is rejected with `unique_taken`, for every command, until a command changes its value or makes it null. Find duplicates with a read model before you declare a key.

## Limits

| Per result (the full set of one aggregate) | At most |
|---|---|
| unique key values | 64 |
| facts | 64 |
| requirements | 64 |
| a key's or fact's name | 128 bytes, not empty, no dots |

The SDKs refuse a name that breaks the rule when the key or fact is declared. A result over a limit is rejected with `invalid_index`.

## What a worker learns

A rejection tells the worker's command something about other aggregates:

- **Facts of its tenant.** A requirement may name any aggregate of the command's tenant, also in another domain, so a worker learns whether that aggregate holds a fact.
- **Values of other tenants.** A global key rejects a value another tenant holds, so a worker learns that the value exists somewhere. It never learns where. Use `tenant` scope unless the value must be unique across tenants.
- **The store holds hashes.** The kernel never gets a value, only its SHA-256, and files a key under a hash of tenant (unless global), domain, aggregate type, name and that SHA-256. A short value, such as an email, can still be guessed from its hash by whoever reads the store: keep keys built from personal data personal.

## Testing

`index(state)` returns the keys and facts an aggregate holds in a state, without a kernel: `fundAggregate.index(state)` in TypeScript, `fund_aggregate.index(state)` in Python, `fund_aggregate.index(&state)?` in Rust. Use it next to `handle` ([Test your domain](../how-to/test-domain)). That two aggregates cannot hold one key is the kernel's job: test it against a real kernel.
