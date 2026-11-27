# Managed nodes

`kernable node` can be linked to a control point that tells it a few things about its kernel
and that it tells how it stands. kernable Cloud is one such control point; anyone may run
another, since this page is all it takes. Nothing here lets a control point reach into the
machine beyond the fixed list below.

## Joining

```sh
kernable node join https://control.example.net <token> [<dir>] --root <root.pub|ID> [--ca <ca.pem>]
```

`--ca`: a control point whose certificate a CA of your own signed; the node trusts that CA alone
for it, and keeps the path (`ca`) in `managed.toml`.

`--root`: the project's root key, pinned. What the control point carries that the root signed
(below) counts only signed by it.

The node makes the kernel's one key, `instance.key` (unless there is one, with its `.pub`), and
sends, unsigned:

```http
POST /v1/nodes/join
{"token": "<one-time token>", "key": "<the node key's public half, hex>"}
```

A token is good once. The answer names the node and the control point's **settings key**:

```json
{"node": "n-…", "project": "shop", "instance": "eu-1", "settings_key": "<hex>"}
```

The node writes `managed.toml` beside its project file (`url`, `node`, `key`,
`settings-key`, `roots`): the settings key and the roots are pinned there, and only settings the
settings key signed count. Its kernel runs with that key, those roots and `--approvals approvals`
unless its project file names its own.

## Signed requests

Every later request carries three headers, from the node's key:

| Header | Value |
|---|---|
| `x-kernable-key` | the public key, hex |
| `x-kernable-time` | Unix seconds; within 120 s of the control point's clock |
| `x-kernable-nonce` | 32 lowercase hex digits, random per request; the control point takes each key's nonce once, so a request sent again is refused |
| `x-kernable-signature` | Ed25519 over `kernable-state-request\n<METHOD>\n<path>\n<time>\n<nonce>\n<hex SHA-256 of the body>`, hex |

## Settings

Every five seconds:

```http
GET /v1/nodes/<node>/settings       →  {"settings": "<JSON text>", "sig": "<hex>"}
```

404 while there are none. `sig` is the settings key's Ed25519 signature over
`kernable.v1.node-settings\n` followed by the JSON text exactly as sent. The node takes the
settings only if the signature holds, `node` is its own, and `version` is greater than that of the
settings it holds (`managed-settings.json`), so old settings, even signed ones, never come back.

The settings name **only** these fields; settings with any other field are refused:

| Field | What it sets |
|---|---|
| `node`, `version`, `issued_at` | whose they are, their version (growing), when (ms) |
| `role` | `writer`, `standby` or `reader` |
| `follow` | a standby's primary, or a reader's writers or relays (comma-separated) |
| `copy` | a reader of a [relay](../how-to/relays): the copy (its key file `copy.key` unless the node's file names `reader-copy-key`) |
| `push` | a writer: `{"to": "<relay>", "copy": "<copy>"}` for its pusher (with the kernel's instance key, and `copy.key` unless its `[push]` names another) |
| `kernel_version` | run `versions/<version>/kernable` in the project directory, if it is there |
| `max_lag_ms` | a reader's `--reader-max-lag-ms` |
| `witness` | a pair's witness (in its `[pair]` section) |

Keys, grants, secrets, the vault, the stores and the services stay as the project file says:
settings never touch them. A setting the node cannot apply (a kernel version it does not have,
a witness without a `[pair]`) is logged and reported, and the rest applies.

## What the root signed

Every five seconds too:

```http
GET /v1/nodes/<node>/signed         →  {"grants": ["<hex>", …], "approvals": ["<hex>", …]}
```

`grants`: encoded `Grant` messages for the node's key; `approvals`: the project's encoded
`WriterApproval` messages (`storage.proto`). The node takes a grant only if it is for its key and
signed by a pinned root, the newest one, and writes it beside its key (`instance.grant`) when it
is newer than the one there; it never takes one back. It takes an approval only signed by a
pinned root, the newest per copy, and writes it to `approvals/<copy>.approval`; one the control
point no longer carries is removed. A reader's kernel trusts the writer approved for its copy
(`--approvals`), and a kernel taking its copy over writes its approval into its takeover. The
control point checks nothing of this and can make none of it: it carries what the customer's
root signed.

## Reports

Every thirty seconds, and a few seconds after new settings:

```http
POST /v1/nodes/<node>/report
{"version": 2, "kernel_version": "", "binary_version": "0.1.0",
 "role": <the kernel's GET /api/role>, "problems": ["…"], "at": <ms>}
```

The kernel's role (its local socket's `GET /api/role`) carries its own figures in `process`:
`cpu_ms` (CPU time used since it started), `rss_bytes` (memory it holds now) and `store_bytes`
(what its stores take on disk). Nothing of the machine around it: not its other processes, not
its disks beyond the kernel's stores.

## Leaving

Remove `managed.toml` and `managed-settings.json`: the node runs on its own file again.
