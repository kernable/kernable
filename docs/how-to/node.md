# Run a node from one file

`kernable node` runs one machine of your deployment from a single `project.toml`: the kernel, the [backup follower](./backup#continuous-backup) if you want it, and your own services (workers, read models, reactors, apps) as [Podman](https://podman.io) containers it builds on the machine. It looks after all of them as child processes. It is the same in every place kernable runs, on your own machines or someone else's.

- **A child that exits is started again**, after 1 s, backing off up to 30 s while it keeps failing at once. A kernel crash never takes the node down.
- **A change to the file is applied by itself.** The node reads it every two seconds (or at once on SIGHUP); a child whose command changed is stopped and started again with the new one, and the others run on.
- **An update is a new binary.** Point `binary` at it, or replace the file it names: the node stops the child and starts it on the new binary.
- **A new build of a service on SIGHUP.** Change your code, send the node SIGHUP: it builds every service again, and restarts those whose image changed.
- **SIGTERM or SIGINT stops everything**: your services and the backup follower first, then the kernel, each with SIGTERM, and SIGKILL after `stop-seconds` (default 30) if it has not stopped by then. The kernel drains as it does on its own ([Deploy and upgrade](./deploy)).

## The file

```toml
[kernel]
binary = "/opt/kernable/1.0.1/kernable"   # optional: default the binary that runs the node
stop-seconds = 30                          # optional
flags = { root = "root.pub", store = "sqlite:kernable.db", vault-key = "vault.key", listen = "10.0.0.1:7233" }
env = { KERNABLE_LOG = "info" }            # optional

[backup]                                   # optional
to = "s3://acme-backups/shop"
key = "backup.key"
backup-key = "backup-encryption.key"
follow = "10.0.0.1:7233,10.0.0.2:7233"     # optional: default the kernel's listen
segment-seconds = 5                        # optional, as base-hours and keep
env = { AWS_ACCESS_KEY_ID = "…", AWS_SECRET_ACCESS_KEY = "…" }

[node]                                     # optional
instance = "node-a"                        # KERNABLE_INSTANCE for the services (default the host name)
project = "shop"                           # names the containers (default the file's directory)

[[service]]                                # one per service, as many as you need
name = "worker"
build = "./worker"                         # a directory with a Containerfile or Dockerfile
command = ["node", "app.ts"]               # optional: the image's own by default
key = "keys/worker.key"                    # optional: with keys/worker.grant next to it
env = { SHOP_MAIL = "smtp://mail:25" }     # optional
cpus = "1.5"                               # optional, as memory = "512m"
stop-seconds = 10                          # optional
```

`flags` are the flags of `kernable start` ([Configuration](../reference/configuration)) without their dashes; `dev = true` stands for `--dev`. With a `[backup]` section the kernel gets `--backup on`. Paths are relative to the file's directory, where the children run. Unknown keys are refused, so a typo does not go unnoticed: at start the node exits with 2, later it logs the error and the children run on as they are.

```sh
kernable node project.toml
```

Run it under your service manager (systemd, launchd) so the node itself starts with the machine.

## Your services

Each `[[service]]` is built with `podman build` on the machine, at start, when its section changes, and on SIGHUP, then run with `podman run --rm` as `kernable-<project>-<name>`, after the kernel and stopped before it. It runs on the machine's network and finds the kernel by itself: the node sets `KERNABLE_URL` to the kernel's `listen` and `KERNABLE_INSTANCE` to the node's instance, which the SDKs read. With `key`, the key file and the `.grant` file next to it are mounted read-only and named in `KERNABLE_KEY` and `KERNABLE_GRANT`; create them with `kernable keys new` and `kernable grant`, so the root key never comes near the machine. A service that exits is started again like the kernel.

Install Podman on the machine first (rootless is fine). Services run on Linux; on a Mac, Podman's containers live in a virtual machine and do not share its network.

## Two machines

Each machine of a [standby pair](./standby) runs its own node, with its own `project.toml`: the kernel's flags name the witness and its node key, and `follow` names both kernels, so the backup follower turns to whichever is the primary. Your read models and reactors run on both machines as [instances](./instances).

