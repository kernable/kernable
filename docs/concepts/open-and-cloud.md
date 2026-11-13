# What's open, what's kernable Cloud

Everything in this repository is open source under the MIT license. kernable Cloud is a hosted
service built on it and run by kernable; its own code is not open. This page says where the line
runs and why, so you know what you can run yourself and what you would be paying for.

## The test

A feature is open if **someone running their own application on their own servers needs it**.
It belongs to kernable Cloud if it exists **only because many customers' applications run on
servers someone else operates for them**.

So you can run kernable yourself with what is here, in production, without asking anyone. What
the cloud adds is the work of running many tenants on a fleet of servers: placing them, moving
them, scaling them, billing them, and looking after the servers.

## Open: in this repository

- **The kernel** — every mechanism it has, including those the cloud drives: the standby and the
  witness, readers, deployments side by side, backups and restores, the signed usage file, keys
  and grants (delegated ones too), handing stores to a new kernel. There is one kernel, built from
  this repository; the cloud runs the same binary and uses only documented interfaces.
- **The SDKs** (TypeScript, Python, Rust) and **the kits** (accounts, identity, groups,
  invitations and the others).
- **The CLI**, including `kernable cloud …`: a client of kernable Cloud's public API, like any
  other.
- **The UI** that shows a kernel.
- **The reference backup signer** and the formats of backups, exports and images.

## kernable Cloud: not open

- Preparing and running servers: instances as isolated pods, encrypted volumes, limits, updates.
- The ingress that terminates TLS for projects, routes them and spreads reads over copies.
- The control point and the agents: documents signed per server, moves between servers,
  autoscaling, read copies across servers, certificates and DNS.
- Accounts, teams and projects of customers, deploys from source, usage, invoices, the console.

Its API for customers is documented and kept stable: you can build your own tools on it.

## Running it yourself

You need the kernel and the tools you already use for servers:

- [Run kernable on your own servers](/how-to/self-host) — the node, systemd, the firewall;
- [TLS](/how-to/tls) — in front of it, any TLS proxy you like (Caddy, nginx, a load balancer);
- [Backups](/how-to/backup) and [a standby](/how-to/standby) — the kernel does both itself;
- [Deployments and rollbacks](/how-to/deployments), [readers](/how-to/readers).

What you do not get is the fleet: placing, moving and scaling many applications over many servers
is yours to arrange.

## When the line moves

When something only the cloud has becomes common ground, it can move into this repository. That
is decided case by case and said in the release notes.
