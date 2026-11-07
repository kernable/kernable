# Run kernable on your own servers

This page takes a bare Linux machine (Debian 12 or Ubuntu 24.04 and newer, x86_64 or arm64) to a kernel that starts with the machine, keeps its data on a disk you chose, and runs your services next to it. It is the same `kernable node` that runs kernable anywhere else; what it adds is the machine around it: a user, a directory, a systemd unit, a firewall.

## 1. Install

```sh
curl -fsSL https://kernable.com/install.sh | sudo KERNABLE_INSTALL=/usr/local/bin sh
kernable --version
```

The script picks the archive for the machine, checks it against the release's `SHA256SUMS`, and installs one file. For your services the node needs Podman (rootless) and, on Linux, runs the kernel in it too:

```sh
sudo apt-get install -y podman uidmap passt
```

## 2. A user and a directory

The node runs as a user of its own, never as root:

```sh
sudo useradd --system --create-home --home-dir /srv/kernable --shell /usr/sbin/nologin kernable
sudo usermod --add-subuids 200000-265535 --add-subgids 200000-265535 kernable   # for rootless Podman
sudo loginctl enable-linger kernable                                            # its units run without a login
sudo systemctl start user@$(id -u kernable)                                     # its own systemd, now
sudo -u kernable mkdir -p /srv/kernable/shop
```

Put `/srv/kernable` on the disk you want the data on. The store is a SQLite file there: back up the directory's disk as you would a database's, and set up [continuous backup](./backup#continuous-backup) besides.

## 3. Keys

Make the **root key on your own machine**, not on the server: it signs every grant, and whoever has it decides who may do what ([Trust](../concepts/trust)). Copy only its public half to the server:

```sh
kernable keys new root                 # on your machine: root.key stays there
scp root.pub server:/tmp/ && ssh server 'sudo install -o kernable -m 644 /tmp/root.pub /srv/kernable/shop/'
```

On the server, as the user, make the kernel's own keys: the kernel key signs checkpoints, the vault key encrypts personal data ([Passwords and other secrets](./secrets)):

```sh
cd /srv/kernable/shop
sudo -u kernable kernable keys new kernel
sudo -u kernable kernable keys new vault
```

Each service gets a key and a grant from the root key ([Keys per service](./keys)): make the key where the service runs, sign its grant on your machine, copy the grant back.

## 4. The node's file

`/srv/kernable/shop/project.toml`, owned by the user ([Run a node from one file](./node) has every setting):

```toml
[kernel]
flags = { root = "root.pub", kernel-key = "kernel.key", vault-key = "vault.key", store = "sqlite:kernable.db", listen = "unix:/srv/kernable/shop/kernel.sock", metrics-listen = "127.0.0.1:9233" }

[node]
project = "shop"
network = "host"     # the machine is the instance's alone: the kernel listens where listen says

[[service]]
name = "worker"
build = "./worker"
key = "keys/worker.key"
```

With `network = "host"` the instance is on the machine's network, as on a machine of its own: the kernel listens where `listen` says, its services find it there, and `/ready` answers on the loopback address. Clients on other machines reach it over TCP with mutual TLS only (`listen = "0.0.0.0:7233"` with the `tls-*` flags): plain TCP stays on the loopback address ([Mutual TLS with your own PKI](./tls)). Several instances on one machine each get a pod network of their own instead, the kernel on `run/kernel.sock` and a reverse proxy in front ([Network](./node#network)).

## 5. Start it with the machine

A systemd user unit for the user, `/srv/kernable/.config/systemd/user/kernable-node.service`:

```ini
[Unit]
Description=kernable node shop

[Service]
WorkingDirectory=/srv/kernable/shop
ExecStart=/usr/local/bin/kernable node /srv/kernable/shop/project.toml
Restart=always
# The node stops its children in order, each within its stop-seconds.
TimeoutStopSec=120

[Install]
WantedBy=default.target
```

```sh
sudo -u kernable mkdir -p /srv/kernable/.config/systemd/user
# write the unit above to that directory, owned by kernable, then:
sudo systemctl --user -M kernable@ daemon-reload
sudo systemctl --user -M kernable@ enable --now kernable-node
sudo journalctl _UID=$(id -u kernable) -f        # the node's log: every child, prefixed with its name
curl -s 127.0.0.1:9233/ready                     # ready, or what the kernel waits for
```

`systemctl --user -M kernable@ stop kernable-node` stops it the way a deploy should: the services first, then the kernel, which finishes its running requests ([Deploy and upgrade](./deploy)).

## 6. The firewall

Open what clients on other machines use, nothing else: SSH, and the kernel's TLS port if it listens on one. The metrics address stays on the loopback address; scrape it from the machine, or through an SSH tunnel.

## 7. Then

- **Updates** are a new binary: install it, point `[kernel] binary` at it or replace the file, and the node hands the kernel's stores over to it without stopping ([Run a node from one file](./node)).
- **A second machine** that takes over when this one is lost: a standby, or a pair with a witness on a third machine ([Run a standby](./standby)).
- **Backups** to object storage, checked by themselves (`check-hours`), with a restore drill now and then ([Back up and restore](./backup)).
- **Watching it**: `/ready` and `/metrics` on the metrics address, alerts worth having, the UI ([Monitor kernable](./monitor)).
