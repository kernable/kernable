# Trust

kernable assumes that any party may lie, and makes every claim checkable.

<FlowDiagram name="layers" bare silent :autoplay="false" />

- **TLS** keeps strangers off the network: over TCP, every process presents a certificate from your CA.
- **The signature** proves which key sent a command, a query or a result.
- **The grant**, signed by a root key, says what that key may do at all.
- **The middleware** decides what the person behind the request may do, from the request context.
- **The hash chain** with signed checkpoints proves afterwards that nothing stored was changed: `kernable verify`.

## Processes and people

kernable authenticates **processes**, by their keys, not the people using your application. Who the person is, your API says in the request context (`senderIdentity`, `senderTenant`, …), signed with the API's key. The **middleware** decides on it: may this customer see this cart?

So two limits apply to every request, and both must allow it:

- **The key's grant** limits what the process may do at all, whoever it claims to act for. A compromised API can impersonate its users, but only within its own grant: it cannot run commands it may not submit, or read events it may not listen to.
- **The middleware** limits what each person may do. It builds its knowledge from events (who owns which cart, who is blocked) and fails closed while it is away.

Signing in the people themselves (passwords, sessions, tokens) is your application's part; [part 6 of the shop tutorial](../tutorial/shop/6-authorization) shows the pattern.

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
| `manage` | services whose membership it may apply (service names, or `*`) |

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
| membership | a key granted `manage` | what a service consists of; valid 60 s around its timestamp |
| file and cache request | client | who reads, writes or deletes which file or value; valid 60 s around its timestamp |

**Replays** are harmless or refused: a command runs once per command ID, whoever sends it again; a result is bound to its command and the version it was decided on; queries, subscriptions, file and cache requests, revocations and memberships are valid only for 60 s around their timestamp, and mutual TLS keeps others from capturing them.

Acknowledgements, query answers and middleware verdicts are not signed: they travel on a stream whose key was proven, and are not stored. The exact byte layouts and test vectors are in [Signatures](../reference/signing).

## The hash chain

<FlowDiagram name="trust" />

Every stored event's hash covers its content, its worker's signature, its request context and the previous event's hash. The kernel signs a **checkpoint** of the chain regularly. `kernable verify` recomputes the chain and checks every signature: a changed, removed or reordered event shows, and so does a command without its client's signature. Events after the last checkpoint are protected by the chain only.

## Revoking and changing keys

A key that leaked is **revoked** from the time it may have been used: a root key signs the revocation (`kernable revoke`) and sends it to the running kernel. The kernel stores it signed with its own key, refuses the key from then on, ends its sessions, and `kernable verify` lists everything the key signed since that time as suspect. A revocation is never undone. Keys change without a restart: a new key needs only its grant, a listener moves to a new key whose grant `succeeds` the old one, and the kernel reads its own key file again on SIGHUP. See [Keys per service, and changing a key](../how-to/keys).

## If something is compromised

| Compromised | What an attacker can do | What limits it, and what you do |
|---|---|---|
| a client's key | send what its grant allows, as that client, in the name of any person it claims | narrow grants per service; the middleware still decides per person; revoke the key from the suspected time, and `kernable verify` lists everything it signed since as suspect |
| a worker's key | connect as a worker for its command types and decide those commands wrongly | it can only answer commands that clients signed, and write events of its own domains; revoke it, and verify flags its results |
| a listener's key | read the events, and vault values, its grant allows | narrow `listen` and `vault_read` grants; revoke it |
| the kernel's key | sign checkpoints | stored events still carry the client's and worker's signatures, which it cannot forge; replace the key file and reload it (SIGHUP), and verify with every kernel key you had |
| a root key | issue any grant | keep root keys offline; with several roots (`--root` repeated), revoke the compromised one and its grants lose their effect |
| the store file | change, remove or reorder events | `kernable verify` shows every such change up to the last signed checkpoint; only events after it could be cut off unnoticed, so keep the checkpoint interval short |
| the vault key, with the vault file | read personal and secret values | keep both apart from the store and its backups; `kernable vault rekey` re-encrypts with a new key |
| the UI's address | read events, payloads and connections | it listens on loopback only, has no login, and never decrypts the vault; reach it through an SSH tunnel |

## Transport

Over TCP, the kernel requires **mutual TLS**: every process presents a certificate from your CA. Signatures make a message trustworthy; TLS keeps it private and keeps strangers out.

## Privileged switches

The request context is business data and the client decides it, except for **privileged switches** such as `bypass_authorization`, which only keys granted `privileged` for that switch may set. Even then, the middleware decides what a bypass means; the kernel does not skip the chain.
