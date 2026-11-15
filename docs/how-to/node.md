# Run a node from one file

`kernable node` runs one instance of your deployment on a machine from a single `project.toml`: the kernel, the [backup follower](./backup#continuous-backup) if you want it, and your own services (workers, read models, reactors, apps) as [Podman](https://podman.io) containers it builds on the machine. On Linux the kernel and the backup follower run in Podman too, all in one pod per instance, so [limits](#limits) hold for every part and for the instance as a whole. It looks after all of them as child processes. It is the same in every place kernable runs, on your own machines or someone else's.

- **A child that exits is started again**, after 1 s, backing off up to 30 s while it keeps failing at once. A kernel crash never takes the node down. A child being stopped for a restart stops in the background: the node looks after the others meanwhile, and starts it again once its old process ended.
- **A change to the file is applied by itself.** The node reads it every two seconds (or at once on SIGHUP); a child whose command changed is stopped and started again with the new one, and the others run on.
- **An update is a new binary.** Point `binary` at it, or replace the file it names: the node stops the child and starts it on the new binary. The kernel is not stopped first: the new kernel starts beside it and waits for the stores, the old one gets SIGUSR1, finishes its running requests and lets them go, and writes pause for a moment only. A standby takes its stores over the same way, so both kernels of a pair update one after the other and keep their roles.
- **A new build of a service on SIGHUP.** Change your code, send the node SIGHUP: it builds every service again in the background, and restarts those whose image changed. The other children are looked after meanwhile.
- **One log for everything.** Every line of every child comes out of the node, on stdout or stderr as the child wrote it, prefixed with its name (`worker | …`, `kernel | …`). Under systemd, journald keeps, rotates and searches them: `journalctl -u kernable-node | grep 'worker |'`.
- **Metrics** with `metrics-listen` in `[node]`: see [below](#metrics).
- **SIGHUP also goes to the kernel**, which reads its keys again (and runs `--key-command` again).
- **SIGTERM or SIGINT stops everything**: your services and the backup follower first, then the kernel, each with SIGTERM, and SIGKILL after `stop-seconds` (default 30) if it has not stopped by then. The kernel drains as it does on its own ([Deploy and upgrade](./deploy)).

## The file

```toml
[kernel]
binary = "/opt/kernable/1.0.1/kernable"   # optional: default the binary that runs the node
stop-seconds = 30                          # optional
flags = { root = "root.pub", store = "sqlite:kernable.db", vault-key = "vault.key" }
env = { KERNABLE_LOG = "info" }            # optional
cpus = "2"                                 # optional (Linux), as memory = "2g"
image = "docker.io/library/debian:trixie-slim"  # optional (Linux): the base image, this one by default

[backup]                                   # optional
to = "s3://acme-backups/shop"
key = "backup.key"
backup-key = "backup-encryption.key"
follow = "10.0.0.1:7233,10.0.0.2:7233"     # optional: default the kernel's listen
segment-seconds = 5                        # optional, as base-hours and keep
check-hours = 24                           # optional: reads the backup through that often
env = { AWS_ACCESS_KEY_ID = "…", AWS_SECRET_ACCESS_KEY = "…" }
# signer = "https://signer.example.com/backup"   # instead of credentials: a backup signer presigns each request

[push]                                     # optional: a copy at a relay, for readers elsewhere
to = "relay.example.net:7400"
copy = "shop"
key = "pusher.key"
copy-key = "copy.key"
root = ["root.pub"]                        # optional: new readers may ask for the copy key
follow = "10.0.0.1:7233,10.0.0.2:7233"     # optional: default the kernel's listen

[pair]                                     # optional: one node of a pair
witness = "10.0.0.9:7300"
node-key = "node-a.key"
witness-generation = "witness.generation"  # the witness's generation (kernable witness generation)
advertise = "10.0.0.1:7233"                # optional: default the kernel's listen
peer = "10.0.0.2:7233"                     # the other node's kernel
kernel-keys = ["kernel-a.pub", "kernel-b.pub"]

[node]                                     # optional
instance = "node-a"                        # KERNABLE_INSTANCE for the services (default the host name)
project = "shop"                           # names the containers (default the file's directory)
metrics-listen = "127.0.0.1:7240"          # optional: GET /metrics
cpus = "4"                                 # optional (Linux): the whole instance, as memory = "8g"
network = "host"                           # optional (Linux): see Network; default own
admin-socket = "admin.sock"                # optional: the kernel's local socket, this by default
require-digest = true                      # optional: run images by digest only; see Images
exclusive = true                           # optional: a changed service starts once every old one ended

[[service]]                                # one per service, as many as you need
name = "worker"
build = "./worker"                         # a directory with a Containerfile or Dockerfile
command = ["node", "app.ts"]               # optional: the image's own by default
key = "keys/worker.key"                    # optional: with keys/worker.grant next to it
env = { SHOP_MAIL = "smtp://mail:25" }     # optional
secrets = ["stripe"]                       # optional: see [[secret]]
cpus = "1.5"                               # optional, as memory = "512m"
stop-seconds = 10                          # optional
writable = true                            # optional: see Hardening
data = "/data"                             # optional: the project's app-data/, read-write
expose = 3000                              # optional (Linux): see Network

[[service]]                                # an image built elsewhere, run as it is
name = "web"
image = "registry.example.com/shop/web@sha256:4f1c…"   # instead of build

[[secret]]                                 # one per secret
name = "stripe"
file = "secrets/stripe.key"                # or: command = ["vault", "kv", "get", "-field=key", "shop/stripe"]
```

`flags` are the flags of `kernable start` ([Configuration](../reference/configuration)) without their dashes; `dev = true` stands for `--dev`. With a `[push]` section the node runs the pusher of a [relay](./relays) beside the kernel, which gets `--readers on`. With a `[backup]` section the kernel gets `--backup on`; it takes `cpus` and `memory` too, and `signer`, the URL of a [backup signer](./backup#without-credentials-on-the-server-a-signer), so that the follower needs no storage credentials in its `env`. Paths are relative to the file's directory, where the children run; on Linux every path the kernel and the backup follower use lies inside it, since that directory is all their container sees. Unknown keys are refused, so a typo does not go unnoticed: at start the node exits with 2, later it logs the error and the children run on as they are.

```sh
kernable node project.toml
```

Run it under your service manager (systemd, launchd) so the node itself starts with the machine. The children run in process groups of their own, so a Ctrl-C reaches only the node, which stops them in order; under systemd, set `KillMode=mixed`, so SIGTERM goes to the node alone, and a `TimeoutStopSec` longer than the children's `stop-seconds` together. On Linux, a node that is killed takes its children along: a small watchdog process removes the instance's pod once the node is gone, and a node that starts again replaces whatever pod an earlier one left.

## Your services

Each `[[service]]` is built with `podman build` on the machine, at start, when its section changes, and on SIGHUP, then run with `podman run --rm` as `kernable-<project>-<name>`, after the kernel and stopped before it. The node builds one image at a time, and stops a build that takes over 20 minutes; a build that fails or is stopped is tried again after a pause, and a service that already runs keeps its image meanwhile. It finds the kernel by itself: the node sets `KERNABLE_URL` to the kernel's socket (on Linux, see [Network](#network)) or its `listen`, and `KERNABLE_INSTANCE` to the node's instance, which the SDKs read. With `key`, the key file and the `.grant` file next to it are mounted read-only and named in `KERNABLE_KEY` and `KERNABLE_GRANT`; create them with `kernable keys new` and `kernable grant`, so the root key never comes near the machine. A service that exits is started again like the kernel.

### Images

Instead of `build`, a service may name an `image` to run as it is: `<repository>@sha256:<digest>`, or a tag (`registry.example.com/shop/web:2026-11-12`). Nothing is built, also not on SIGHUP; Podman pulls the image on the first start unless the node's user holds it already (one loaded with `podman load`, say). A change of `image` restarts the service on the new one. `file` goes with `build` only.

A digest names exactly one image, a tag whatever it points at today. With `require-digest = true` in `[node]`, the node runs images by digest only: a service with a tag, a service with `build`, and a `[kernel] image` with a tag are refused (the default base image is pinned by digest), at start with exit code 2 and later with the error logged, the children running on as they are.

A service whose `project.toml` entry changes is stopped and started anew; one that is new (another name) starts with the node's next look, while the old ones may still be stopping. For services that keep their state on the instance's disk without a kernel, that is two versions writing at once. With `exclusive = true` in `[node]`, a new or changed service starts only once every process the node is stopping has ended: stop, then start, with seconds without the app in between.

Install Podman on the machine first (rootless is fine). Services run on Linux; on a Mac, Podman's containers live in a virtual machine and do not share its network.

## Limits

On Linux every part of the instance runs in Podman, in one pod named `kernable-<project>`:

- **The kernel and the backup follower** run in a small base image with the node's kernable binary mounted read-only, so an update is still a new binary. The project directory is mounted at its own path. Name your own base image in `[kernel] image`, for one with your KMS's client for `--key-command`, say. Podman pulls it on the first start.
- **Each part** takes `cpus` and `memory`: `[kernel]`, `[backup]`, `[push]`, every `[[service]]`.
- **The instance as a whole** takes them in `[node]`: the kernel and every service together never use more, so instances side by side on one machine cannot starve each other. A change to these restarts every part in a new pod.
- **Variables** in `env` reach the containers from Podman's own environment, so their values never show in a process list.

On macOS and other systems the kernel and the backup follower run as plain processes, without limits; asking for them there is an error.

## Network

On Linux the instance's pod has a network of its own, so its services see neither the machine's other services nor other instances, and nothing in it can be reached from outside:

- **The kernel listens on a socket**, `run/kernel.sock` in the project directory; the node chooses it, so `flags` takes no `listen`. Each service finds it, read-only, at `/run/kernable/kernel/kernel.sock`, named in `KERNABLE_URL`.
- **A service that serves HTTP** (a web frontend, an API, webhooks) takes `expose = <port>`, the port it listens on in the pod. The node passes `run/<name>.sock` on to it, so the service listens on plain TCP as usual and is reached through its socket.
- **From outside**, a reverse proxy on the machine ends TLS and passes requests on to these sockets: gRPC to `run/kernel.sock` (HTTP/2 without TLS: `grpc_pass unix:…` in nginx), HTTP to `run/<name>.sock`. The sockets let their owner and group connect; grant your proxy's user access with an ACL (`setfacl -m u:<proxy>:x` on the directory and `u:<proxy>:rx,d:u:<proxy>:rw` on `run/`). One of a [pair](#two-machines) names in `[pair] advertise` where the other node reaches its kernel through that proxy (`host:443`).
- **Your services reach the internet** as usual (rootless, through Podman's pasta). The machine's loopback is out of their reach. Run each instance as a user of its own, and keep its user from the machine's other addresses and the provider's metadata service with your firewall.

`network = "host"` puts the whole pod on the machine's network, as on a machine of its own, where nothing else runs beside the instance: the kernel listens where `listen` says (plain TCP on a loopback address only, mutual TLS elsewhere), and the services find it there. A change of `network` starts every part again in a new pod.

## Hardening

Every container the node starts, the kernel's included, gets the same protection, so that code which takes over a process gets as little as possible:

- **No capabilities**, and **no way to gain privileges**: a setuid binary in the image does not work.
- **At most 4096 processes**, so a fork bomb stays inside its container.
- **A read-only root filesystem**, with a fresh, empty `/tmp`, `/var/tmp` and `/run` in memory. What has to last belongs in the kernel.
- **On Linux, Podman's system call filter, tightened**: no new user namespaces, whose "root" would hold capabilities again and reach the kernel paths most container escapes go through, and no kernel keyrings, which nothing separates between containers. The node writes the profile from Podman's own (`/usr/share/containers/seccomp.json`, or yours in `/etc/containers/`) when it makes the pod, so Podman's updates reach it; without one it does not start.

A service's root filesystem is gone when it stops. One that keeps state of its own (files, a SQLite database) names a path for it: `data = "/data"` mounts the project directory's `app-data/` there, read-write, the same directory for every service that names it and for every new version of the service. With the kernel, state lives in its store instead; `data` is for apps that run without it (see `exclusive` above, so two versions never write there at once).

Most images run as they are. One that writes elsewhere (some frameworks keep a cache next to their code) takes `writable = true` in its `[[service]]`; the node logs it at each start. Images that start as root to switch to a user of their own, such as the official `nginx`, do not start without capabilities: use an image that runs as an ordinary user from the start (`nginxinc/nginx-unprivileged`, say). Software that builds a sandbox of its own from user namespaces runs without it, as is usual in containers: Chromium (Puppeteer, Playwright) with `--no-sandbox`; containers inside a service do not run.

## Secrets

A `[[secret]]` names a value your services need (a payment provider's key, a mail password) without writing it into the file: it comes from a file, or from the output of a command such as your KMS's client. A service that lists it in `secrets` finds it, read-only, at `/run/secrets/<name>`.

- **A file** is read again when it changes; **a command** runs at start and on SIGHUP, as `--key-command` does.
- **A new value** restarts the services that read it, and only those.
- **Where the values live:** in a directory only the node's user may enter, in memory where the system has one (`$XDG_RUNTIME_DIR`, or `/run` as root).
- **A secret that cannot be read** stops the node at start; later, the services keep the value they have, and the log names the secret.

## Metrics

With `metrics-listen`, `GET /metrics` answers in Prometheus' text format, a line per child:

| Metric | Type | What |
|---|---|---|
| `kernable_node_child_up{child}` | gauge | 1 while the child runs |
| `kernable_node_child_restarts_total{child}` | counter | times the node started it again |
| `kernable_node_child_uptime_seconds{child}` | gauge | seconds since it last started, while it runs |
| `kernable_node_container_cpu_seconds_total{child}` | counter | CPU time of the child's container: a service, and on Linux the kernel and the backup follower (from its cgroup, every 10 s) |
| `kernable_node_container_memory_bytes{child}` | gauge | memory of the child's container now |

The kernel keeps its own metrics on its `--metrics-listen` ([Monitor kernable](./monitor)). The address is read when the node starts; a change applies after a restart of the node.

## Where the stores live

Keep the project directory, and with it the event store, the vault and the snapshots, on a local disk, never on a network file system such as NFS or SMB. One kernel per store rests on file locks (`<store>.lock`, and the vault's and the snapshots' own), and such file systems do not hold file locks reliably: two kernels could then write one store.

## The kernel's local socket

The node starts the kernel with `--admin-socket admin.sock` in the project directory (`[node] admin-socket` names another place in it; `[kernel] flags` takes no `admin-socket`). It serves `GET /metrics`, `/health`, `/ready` and `/ready?deployment=<d>`, `/api/role` (whether the kernel is the writer or a [reader](./readers), with its standby, readers or lag), the [deployments](./deployments)' warm-up, list and since-report, and `POST /api/deployments/drop` (`kernable deployments drop --socket`), with no login, like a loopback metrics address, but only to the kernel's own user (mode 0600) and root. A tool on the machine reaches it through the file system, without entering the pod's network:

```sh
curl --unix-socket /srv/shop/admin.sock 'http://localhost/ready?deployment=2026-11-12.1'
```

Your services do not see it: their containers get `run/` (the kernel's gRPC socket), not the project directory, so the node refuses an `admin-socket` in `run/`.

## Two machines

Each machine of a [standby pair](./standby) runs its own node, with its own `project.toml` and a `[pair]` section: the witness and its generation file, this node's key, its own address and the other node's (`peer`), and both kernels' kernel keys. From it the node sets the kernel's pair flags (don't repeat them in `flags`), lets the backup follower turn to whichever kernel is the primary, and gives your services both addresses in `KERNABLE_URL`. Your read models and reactors run on both machines as [instances](./instances).

In the pod's own network, `advertise` is required: the address of this machine's reverse proxy that leads to this kernel's socket (`a.example.com:443`), and `peer` the other machine's. The kernel reaches the other one and the witness over TLS with the files its `env` names (`KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT`, `KERNABLE_TLS_KEY`, inside the project directory), and so do your services that turn to the other node; the backup follower stays on this machine's socket.

