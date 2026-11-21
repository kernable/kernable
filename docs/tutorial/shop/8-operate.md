# 8. Operate

The shop works. This part runs it the way you would in production.

## The production kernel

Every party signs what it sends with its own Ed25519 key. A signature says **who** sent a message, but anyone can make a key, so the kernel trusts one key only: your **root key**. The root key signs **grants**, and a grant says **what** a key may do: which actions (submit, run, listen, query, …) on which types. The kernel holds no list of keys or permissions; your processes present their grants.

In part 1, `--dev` made one key, `app`, with a grant for everything, and left the root key next to it. In production:

- Each service gets its **own key and a grant for only what it does**. A stolen key then can do only what its service could.
- The **root key stays offline**. The kernel needs only its public half, `root.pub`, to check grants. You need the root key only to issue a grant or to revoke a key.
- Over a network, the kernel speaks TCP with **mutual TLS**: every process presents a certificate from your CA. TLS decides who may connect; keys and grants still decide what a connection may do. For trying it out, `kernable certs new` makes a development CA and certificates; in production use your own PKI.

```sh
mkdir -p ~/shop-prod && cd ~/shop-prod
```

Make the root key where it will stay:

<<< @/examples/shop/production.sh#root{sh}

Then, on the kernel's machine, the kernel's keys, a key per service, and the certificates. `api` stands for your scripts and your API, the processes that submit and query on behalf of customers:

<<< @/examples/shop/production.sh#keys{sh}

Bring the services' `.pub` files to the root key, grant each what it does, and bring the `.grant` files back next to the keys:

<<< @/examples/shop/production.sh#grants{sh}

| Key | May | Because |
|---|---|---|
| `worker` | run the shop's commands, erase with `CustomerErased` | it runs `Erase` |
| `read-models` | read events, answer queries, read vault values | the customers model shows emails |
| `confirmations` | read `OrderSubmitted`, ask two queries, send `Confirm` | that is all the reactor does |
| `guard` | read three events, check every request | it compares IDs, so it needs no vault values |
| `api` | submit commands, ask queries | the guard decides for which customer |

A key and its grant are public; only the `.key` file is secret. Start the kernel:

<<< @/examples/shop/production.sh#start{sh}

Besides TLS and the keys, four things differ from part 1:

- **Snapshots** go to their own SQLite file. A worker sends a snapshot of an aggregate every 100 events, so a long-lived aggregate does not have to replay its whole history. Snapshots are a cache: delete them, and workers rebuild them.
- **Metrics** and health checks are served on `--metrics-listen`; keep that address private.
- **No UI.** `--dev` turned on the kernel's UI at http://127.0.0.1:7234; in production it is off unless you set `--ui-listen`. It has no login and listens on loopback only, so reach it through an SSH tunnel ([Monitor](../../how-to/monitor#the-ui)).
- Paths point into `data/` and `keys/`. Back up `data/` (the log, the vault) and `keys/`, and keep a copy of the root key, offline too; `kernable verify` checks a log and its signatures at any time.

Every flag can also be set as an environment variable (`KERNABLE_STORE`, `KERNABLE_KERNEL_TLS_CERT`, …), which suits a service manager or a container better than a long command line.

## Connect the processes

The SDKs read the connection from the environment (part 1), so your processes run unchanged. Each one gets its own key and certificate; the SDK finds the grant next to the key:

```sh
export KERNABLE_URL=kernel.example.internal:7233 KERNABLE_TLS_CA=certs/ca.tls.crt
export KERNABLE_KEY=keys/worker.key KERNABLE_TLS_CERT=certs/worker.tls.crt KERNABLE_TLS_KEY=certs/worker.tls.key
node worker.ts
```

Start `read-models`, `confirmations` and `guard` the same way with their keys, and your scripts with `api`. To keep the grant elsewhere, set `KERNABLE_GRANT` to its file.

## Change what a key may do

The kernel stores no permissions; it decides by the newest grant it has seen for a key. So adding a service needs no change to the kernel: make its key, issue its grant, start it.

To give a service more or fewer rights, issue a new grant for the same key and deploy it. Once the service presented it, the kernel holds the older one invalid and checks the service's open sessions against the new one. A grant cannot be withdrawn: after a restart, the kernel accepts an older grant again until it sees the newer one. When it must be gone for sure, rotate the key and revoke the old one.

## Rotate and revoke keys

To **rotate a service's key**, make a new key, grant it, deploy the service with it, then revoke the old key. A listener's registration belongs to its key; to keep its position, the new grant names the key it succeeds:

```sh
kernable keys new keys/read-models-2 >/dev/null
kernable grant offline/root.key keys/read-models-2.pub --succeeds keys/read-models.pub \
  'listen=shop.*' 'answer=shop.*' 'vault_read=shop.*'
```

When the read models connect with `read-models-2`, the kernel moves their registration to it.

To **revoke** a key, because it was replaced or leaked, sign a revocation with the root key and send it to the running kernel. It is the one command that needs the root key and the kernel at once; the revocation is valid for a minute, so it cannot be prepared offline:

```sh
export KERNABLE_TLS_CA=certs/ca.tls.crt KERNABLE_TLS_CERT=certs/api.tls.crt KERNABLE_TLS_KEY=certs/api.tls.key
kernable revoke kernel.example.internal:7233 offline/root.key keys/read-models.pub 2026-10-27T09:00:00Z
```

Over TCP, `kernable` connects with mutual TLS like every other process, with the certificate files in these variables.

From then on the kernel refuses the key and ends its sessions. Give the time from which you no longer trust the key, for a leak the time it may have leaked: `kernable verify` flags everything it signed since. The kernel stores the revocation; it is never undone.

The **kernel's key** signs checkpoints and revocations. Replace `keys/kernel.key` and send the kernel `SIGHUP`; it reads the key again without a restart. Keep the old `kernel.pub`: `kernable verify` needs every key the kernel ever had.

The **root key** is rotated by trusting two for a while: start the kernel with `--root keys/root.pub --root keys/root-2.pub`, issue every grant again with the new root, then restart with the new root only.

## Read models that survive a restart

A read model in memory reads every event again when it starts. With a **store** it keeps its rows in a file, writes them together with its position per batch, and continues where it left off:

::: code-group
<<< @/examples/shop/ts/customers-persistent.ts#persistent [customers-persistent.ts]
<<< @/examples/shop/py/customers_persistent.py#persistent [customers_persistent.py]
<<< @/examples/shop/rs/src/bin/customers_persistent.rs#persistent [customers_persistent.rs]
<<< @/examples/shop/go/customers-persistent/main.go#persistent [customers-persistent/main.go]
:::

When you change how the handlers store data, raise `schema`: the store is cleared and rebuilt from the first event.

## Readiness, deploys and shutdown

- **Tell the kernel what each service consists of**, once per deploy: `kernable apply` with a key granted `manage`, or an SDK app (`kernable.app(...)`) that applies its membership when it starts. Then the kernel waits for exactly that, and what a new version no longer has stops counting ([Remove a listener, middleware or aggregate](../../how-to/remove-listener)).
- **Name each version** with `KERNABLE_DEPLOYMENT` (a build number or commit): its read models and snapshots are its own, a new version warms up beside the serving one and takes over once its read models have caught up, the membership names the version that serves, and going back to the previous one is applying its membership again with `kernable apply --deployment` (an app never switches back by itself) ([Deployments and rollbacks](../../how-to/deployments)).
- `GET /ready` on the metrics address answers 200 once every registered aggregate has a worker connected and every registered listener is connected and has caught up; let your deploy wait for it. `GET /health` answers while the process runs, and `GET /metrics` serves Prometheus metrics.
- **Deploy readers before writers.** When an event's format changes (a new event version), a process that does not know the new version stops at it instead of misreading it.
- On SIGTERM the kernel **drains**: it refuses new requests, lets running ones finish for up to `--drain-timeout-ms`, then stores its state and exits. Commands that did not finish run right after the restart.
- Restarting a worker or a listener loses nothing: commands go to another worker, and listeners continue after their last acknowledged event.
- **Watch for parked events.** A reactor that gives up on an event (a rejected follow-up, or a handler that keeps failing) parks it and moves on; `kernable listeners parked` lists them, `kernable_listener_parked` counts them, and `kernable listeners retry` delivers them again once the cause is fixed ([Parked events](../../how-to/monitor#parked-events)).

The [reference](../../reference/) lists every flag, metric and error.

Next: [9. Accounts and sign-in](./9-sign-in).
