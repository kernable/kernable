# Trust

kernable assumes that any party may lie, and makes every claim checkable.

## Keys and grants

Every process has an **Ed25519 key** (`kernable keys new`). Its **key ID** is the public key in hex, so a signature can be checked without a registry. A signature proves who sent a message, not that the sender may do it: anyone can create a key.

So the kernel trusts **root keys** only (`kernable start --root root.pub`). A root key signs **grants**: a grant says what one key may do, per action and type.

```sh
kernable grant root.key app.pub 'submit=shop.*' 'listen=shop.*'   # writes app.grant
```

| Action | Covers |
|---|---|
| `submit` | commands the key may submit |
| `run` | commands it may run as a worker; it writes events of their domains only |
| `subscribe` | events it may read with a subscription |
| `listen` | events it may read as a listener (read model, reactor) |
| `query` | queries it may ask |
| `answer` | queries it may answer as a read model |
| `check` | commands and queries it may check as a middleware |
| `privileged` | privileged request context switches it may set (`bypass_authorization`) |
| `vault_read` | events and commands whose vault values it gets as a listener or middleware |
| `vault_erase` | events that erase a subject it may write as a worker |
| `data_read` | domains whose files it may read (`shop.*`) |
| `data_write` | domains whose files it may put and delete |
| `cache` | domains whose cache it may use |

A type is a qualified type (`shop.PlaceOrder`), a whole domain (`shop.*`) or everything (`*`). A bare type (`'shop.*'`) means every action on it.

The process sends its grant with every call, in the gRPC metadata entry `kernable-grant`; the SDKs read `<name>.grant` next to the key file. The kernel checks the chain: the grant is signed by a root key, it names the key that signed the request, and neither key is revoked. It keeps the newest grant per key. The kernel's settings hold no keys of services and no permissions, so a new service needs a grant, not a change to the kernel.

The root key is used only to issue grants and revocations, so it can stay offline. Several roots (`--root` repeated) allow replacing one.

A grant cannot be withdrawn. A newer grant for the same key (by its issue time) replaces it, and the kernel refuses the older one once it has seen the newer one. After a kernel restart, it accepts the older one again until the newer one is presented; to take rights away for certain, give the service a new key and revoke the old one.

## What is signed

| Message | Signed by | Proves |
|---|---|---|
| command | client | who asked for exactly this, in which context |
| result (events, snapshot) | worker | which worker decided, based on which version |
| query, subscription | client | who asked; valid 60 s around its timestamp |
| stream opening | worker, listener | possession of the key (challenge) |
| checkpoint | kernel | the state of the whole log at a position |
| grant | root key | what a key may do |
| revoke request | root key | that a root key ended a key; valid 60 s around its timestamp |

Acknowledgements, query answers and middleware verdicts are not signed: they travel on a stream whose key was proven, and are not stored. The exact byte layouts and test vectors are in [Signatures](../reference/signing).

## The hash chain

<FlowDiagram name="trust" />

Every stored event's hash covers its content, its worker's signature, its request context and the previous event's hash. The kernel signs a **checkpoint** of the chain regularly. `kernable verify` recomputes the chain and checks every signature: a changed, removed or reordered event shows, and so does a command without its client's signature. Events after the last checkpoint are protected by the chain only.

## Revoking and changing keys

A key that leaked is **revoked** from the time it may have been used: a root key signs the revocation (`kernable revoke`) and sends it to the running kernel. The kernel stores it signed with its own key, refuses the key from then on, ends its sessions, and `kernable verify` lists everything the key signed since that time as suspect. A revocation is never undone. Keys change without a restart: a new key needs only its grant, a listener moves to a new key whose grant `succeeds` the old one, and the kernel reads its own key file again on SIGHUP. See [Keys per service, and changing a key](../how-to/keys).

## Transport

Over TCP, the kernel requires **mutual TLS**: every process presents a certificate from your CA. Signatures make a message trustworthy; TLS keeps it private and keeps strangers out.

## Privileged switches

The request context is business data and the client decides it, except for **privileged switches** such as `bypass_authorization`, which only keys granted `privileged` for that switch may set. Even then, the middleware decides what a bypass means; the kernel does not skip the chain.
