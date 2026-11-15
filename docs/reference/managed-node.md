# Managed nodes

`kernable node` can be linked to a control point that tells it a few things about its kernel
and that it tells how it stands. kernable Cloud is one such control point; anyone may run
another, since this page is all it takes. Nothing here lets a control point reach into the
machine beyond the fixed list below.

## Joining

```sh
kernable node join https://control.example.net <token> [<dir>] [--ca <ca.pem>]
```

`--ca`: a control point whose certificate a CA of your own signed; the node trusts that CA alone
for it, and keeps the path (`ca`) in `managed.toml`.

The node makes its key `node.key` (unless there is one), and sends, unsigned:

```http
POST /v1/nodes/join
{"token": "<one-time token>", "key": "<the node key's public half, hex>"}
```

A token is good once. The answer names the node and the control point's **settings key**:

```json
{"node": "n-…", "project": "shop", "instance": "eu-1", "settings_key": "<hex>"}
```

The node writes `managed.toml` beside its project file (`url`, `node`, `key`,
`settings-key`): the settings key is pinned there, and only settings it signed count.

## Signed requests

Every later request carries three headers, from the node's key:

| Header | Value |
|---|---|
| `x-kernable-key` | the public key, hex |
| `x-kernable-time` | Unix seconds; within 120 s of the control point's clock |
| `x-kernable-signature` | Ed25519 over `kernable-state-request\n<METHOD>\n<path>\n<time>\n<hex SHA-256 of the body>`, hex |

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
| `push` | a writer: `{"to": "<relay>", "copy": "<copy>"}` for its pusher (keys `pusher.key`, `copy.key` unless its `[push]` names others) |
| `kernel_version` | run `versions/<version>/kernable` in the project directory, if it is there |
| `max_lag_ms` | a reader's `--reader-max-lag-ms` |
| `witness` | a pair's witness (in its `[pair]` section) |

Keys, grants, secrets, the vault, the stores and the services stay as the project file says:
settings never touch them. A setting the node cannot apply (a kernel version it does not have,
a witness without a `[pair]`) is logged and reported, and the rest applies.

## Reports

Every thirty seconds, and a few seconds after new settings:

```http
POST /v1/nodes/<node>/report
{"version": 2, "kernel_version": "", "binary_version": "0.1.0",
 "role": <the kernel's GET /api/role>, "problems": ["…"], "at": <ms>}
```

## Leaving

Remove `managed.toml` and `managed-settings.json`: the node runs on its own file again.
