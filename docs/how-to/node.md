# Run a node from one file

`kernable node` runs one machine of your deployment from a single `project.toml`: the kernel and, if you want, the [backup follower](./backup#continuous-backup), as child processes it looks after. It is the same in every place kernable runs, on your own machines or someone else's.

- **A child that exits is started again**, after 1 s, backing off up to 30 s while it keeps failing at once. A kernel crash never takes the node down.
- **A change to the file is applied by itself.** The node reads it every two seconds (or at once on SIGHUP); a child whose command changed is stopped and started again with the new one, and the others run on.
- **An update is a new binary.** Point `binary` at it, or replace the file it names: the node stops the child and starts it on the new binary.
- **SIGTERM or SIGINT stops everything**: the backup follower first, then the kernel, each with SIGTERM, and SIGKILL after `stop-seconds` (default 30) if it has not stopped by then. The kernel drains as it does on its own ([Deploy and upgrade](./deploy)).

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
```

`flags` are the flags of `kernable start` ([Configuration](../reference/configuration)) without their dashes; `dev = true` stands for `--dev`. With a `[backup]` section the kernel gets `--backup on`. Paths are relative to the file's directory, where the children run. Unknown keys are refused, so a typo does not go unnoticed: at start the node exits with 2, later it logs the error and the children run on as they are.

```sh
kernable node project.toml
```

Run it under your service manager (systemd, launchd) so the node itself starts with the machine.

## Two machines

Each machine of a [standby pair](./standby) runs its own node, with its own `project.toml`: the kernel's flags name the witness and its node key, and `follow` names both kernels, so the backup follower turns to whichever is the primary. Your read models and reactors run on both machines as [instances](./instances).

Running your own processes (workers, read models, apps) under the node comes later; for now, run them under your service manager next to it.
