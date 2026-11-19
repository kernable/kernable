# What's open, what's kernable Cloud

Everything in this repository is open source under the MIT license. kernable Cloud is a service
run by kernable that manages kernables you run yourself; its own code is not open. This page says
where the line runs and why, so you know what you can run yourself and what you would be paying
for.

## How the two fit together

With kernable Cloud, **your kernel still runs right next to your code**, in your own
infrastructure, built from this repository: your services read and write it locally. kernable
Cloud **keeps it in sync automatically**: your kernel pushes every change, sealed, to a copy in the
EU within seconds, and your read-only kernels anywhere follow that copy. Your kernel stays the
source of your data; the copy is encrypted with keys only your kernels hold. Without kernable
Cloud, you do the same with the open pusher and a relay of your own ([relays](/how-to/relays)).

## The test

A feature is open if **someone running their own application on their own servers needs it**.
It belongs to kernable Cloud if it exists **only because kernable looks after many customers'
kernels from one place**.

So you can run kernable yourself with what is here, in production, without asking anyone.
kernable Cloud never runs your code: your application and its kernels run in your own
infrastructure, wherever you like. What the cloud adds is looking after them from one place.

## Open: in this repository

- **The kernel** — every mechanism it has, including those the cloud drives: the standby and the
  witness, readers, deployments side by side, backups and restores, the signed usage file, keys
  and grants (delegated ones too), handing stores to a new kernel. There is one kernel, built from
  this repository; the cloud uses only documented interfaces.
- **`kernable node`**, which runs a kernel and your services on a machine, and everything that
  runs in your infrastructure when kernable Cloud looks after it: you can read all of it.
- **The SDKs** (TypeScript, Python, Rust) and **the kits** (accounts, identity, groups,
  invitations and the others).
- **The CLI**, including `kernable cloud …`: a client of kernable Cloud's public API, like any
  other.
- **The UI** that shows a kernel.
- **The reference backup signer** and the formats of backups, exports and images.
- **The pusher and the reference relay** (`kernable push`, `kernable relay`): a writer's records
  sent out of its network to a copy that readers elsewhere follow ([relays](/how-to/relays)).

## kernable Cloud: not open

- A copy of each writer's data that your read-only replicas anywhere can follow, kept sealed:
  encrypted so that only your own kernels can read it.
- Backups of that copy, monitoring and alerts, kernel updates, from one place.
- Accounts, teams and projects of customers, usage, invoices, the console.

Its API for customers is documented and kept stable: you can build your own tools on it.

## Running it yourself

You need the kernel and the tools you already use for servers:

- [Run kernable on your own servers](/how-to/self-host) — the node, systemd, the firewall;
- [TLS](/how-to/tls) — in front of it, any TLS proxy you like (Caddy, nginx, a load balancer);
- [Backups](/how-to/backup) and [a standby](/how-to/standby) — the kernel does both itself;
- [Deployments and rollbacks](/how-to/deployments), [readers](/how-to/readers).

What you do not get is looking after it from one place: the sealed copy for replicas anywhere,
its backups, monitoring and updates across all your kernels are yours to arrange.

## When the line moves

When something only the cloud has becomes common ground, it can move into this repository. That
is decided case by case and said in the release notes.
