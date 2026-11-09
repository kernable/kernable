# How-to guides

Short guides for tasks you meet once your application runs. They assume you know the [concepts](../concepts/); the [tutorial](../tutorial/shop/) shows the code they refer to.

## Build and change your application

| Guide | When you need it |
|---|---|
| [Test your domain](./test-domain) | You want to test aggregates without a kernel, or against a real one. |
| [Add tenants, accounts and authorization](../kits/getting-started) | Your application needs tenants, people who sign in, roles and invitations. |
| [Schedule a command](./schedule) | A command should run later: a timeout, a reminder, a deadline. |
| [Change an event's format](./event-versions) | An event must carry something new, but stored events stay as they are. |
| [Rebuild a read model](./rebuild-read-model) | You fixed a handler bug, or a read model should hold something new. |
| [Remove a listener, middleware or aggregate](./remove-listener) | You stop running something, and the kernel should stop waiting for it. Covers memberships. |

## Run it in production

| Guide | When you need it |
|---|---|
| [Deploy and upgrade](./deploy) | You start, stop or upgrade the kernel and your processes. |
| [Back up and restore](./backup) | You set up backups, or you need to restore. |
| [Move a kernel with export and import](./export) | You leave a hosted kernel, move to another machine, or change the store backend. |
| [Share tenants across kernels](./partitions) | One kernel is not enough for all your tenants: several share them by a signed map. |
| [Run a standby](./standby) | A second kernel should hold every write and take over, with a witness by itself, when the primary's machine is lost. |
| [Run a listener on several machines](./instances) | The same read models and reactors run on more than one machine, each with its own copy. |
| [Run a node from one file](./node) | One file says what runs on a machine: the kernel and its backup follower, started again when they exit or the file changes. |
| [Run kernable on your own servers](./self-host) | From a bare Linux machine to a node that starts with it: install, a user, keys, the node's file, systemd, the firewall. |
| [Monitor kernable](./monitor) | You want metrics, health checks, alerts and logs, or a reactor parked events. |

## Security

| Guide | When you need it |
|---|---|
| [Keys per service, and changing a key](./keys) | You give each service its own key and grant, or you change, revoke or replace a key. |
| [Mutual TLS with your own PKI](./tls) | You run over TCP with your own CA instead of development certificates. |
| [Passwords and other secrets](./secrets) | A command carries a value that must never be stored. |

## Data

| Guide | When you need it |
|---|---|
| [Handle an erasure request](./erasure) | A person asks you to erase their personal data. |
| [Store files and cache values](./data-and-cache) | A domain needs files (an invoice PDF) or values that expire (a rate limit). |

## Extend kernable

| Guide | When you need it |
|---|---|
| [Write an SDK for another language](./new-sdk) | You want processes in a language without an SDK. |
