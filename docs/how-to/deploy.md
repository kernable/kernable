# Deploy and upgrade

Use this page when you put a new version of a service into production, or upgrade the kernel itself. Both can happen while the other side keeps running: processes reconnect on their own, and the kernel keeps commands queued until a worker takes them.

Before you start, give the kernel a metrics address (`--metrics-listen 127.0.0.1:9233`): `GET /ready` there tells you when a deploy is done ([Monitor kernable](./monitor#health-checks)).

## A new version of a service

1. **Issue grants for anything new.** A new service, or a service that needs more rights, gets a new grant from the root key ([Keys per service](./keys)). The kernel does not change.
2. **Deploy readers before writers** when an event's format changes: listeners that read the new format go first, workers that write it after ([Change an event's format](./event-versions)).
3. **Apply the service's membership, once per deploy.** It lists the workers (aggregate types), listeners and middlewares the kernel waits for. From a pipeline, with a key granted `manage=<service>`:

   ```sh
   kernable apply 127.0.0.1:7233 keys/deploy.key shop.json
   # shop.json: {"service": "shop", "workers": ["shop.cart"], "listeners": ["carts", "mails"],
   #             "middlewares": ["guard"], "metadata": {"version": "1.4.0"}}
   ```

   It answers `applied shop: 1 workers, 2 listeners, 1 middlewares`. From code, call `kernable.apply(...)` in a deploy script; an app (`kernable.app({ service, version, domains }).run()`) builds the membership from its domains and applies it itself on every start, so its key needs `manage` for the service.
4. **Start the new processes and stop the old ones.** Workers and listeners can restart at any time: a worker's commands go to another worker, a listener continues after its last acknowledged event.
5. **Wait for `/ready`.** It answers 200 once every listed worker and listener is connected and the listeners have caught up; until then 503 names what it waits for:

   ```sh
   $ curl -s 127.0.0.1:9233/ready
   waiting for listeners: carts
   $ curl -s 127.0.0.1:9233/ready
   ready
   ```

6. **Check the result.** `kernable services <store>` shows each applied membership, who applied it and when; a listed member that never connected is marked `(never registered)`. The kernel UI shows the same live ([Monitor kernable](./monitor#the-ui)).

Name each version (`KERNABLE_DEPLOYMENT`) to run versions side by side and roll back safely: each builds its own read models, and the membership names the one that serves ([Deployments and rollbacks](./deployments)).

What the new membership no longer lists stops counting: the kernel stops waiting for it, and a middleware that is no longer listed stops checking ([Remove a listener, middleware or aggregate](./remove-listener)).

### Pitfalls

- **The newest apply wins.** The kernel refuses a membership issued before the one it holds, so a late or replayed apply cannot bring an old list back. Apply from one place per deploy, not from every process. With `app()`, every start applies: an old replica that restarts during a rolling deploy brings the old list back until a new one starts. Finish the rollout, or apply from the pipeline afterwards.
- **One service is one membership.** Two different apps with the same service name replace each other's list.
- **Commands of unknown types are refused.** A worker registers its aggregate and command types when it connects; until then, commands for an aggregate or command type that no worker registered and the membership does not list get `NOT_FOUND`. Start the worker of a new aggregate before clients send its commands, or list it in the membership: commands of a listed aggregate wait in the queue until its worker comes.
- **Middleware is always checked.** While a listed middleware restarts, the requests it checks are refused. Keep the restart short, or run a second instance under a different name.

## The kernel

A [pair with a witness](./standby#a-pair-with-a-witness) updates without downtime: [update the standby, switch over, update the old primary](./standby#update-a-pair-without-downtime). A single kernel:

1. **Back up** the store and the vault ([Back up and restore](./backup)).
2. **Stop the kernel with SIGTERM** (or SIGINT). It refuses new submits and queries, lets running ones finish for `--drain-timeout-ms` (default 10 s), then signs a checkpoint, saves listener positions and exits. Commands that did not finish stay queued and run again after the restart.
3. **Install the new `kernable` binary.**
4. **Start the kernel** with the same flags, and wait until `GET /ready` answers 200. Before the first release a store of an earlier development build does not carry over: formats change in place, without migrations.

While the kernel is away, workers and listeners reconnect on their own (their console shows `connection lost (…), reconnecting`, then `connected again`), waiting a little longer after each failed attempt, up to 30 s. A client's requests wait for the kernel up to their timeout (60 s by default); a request that gives up gets `DEADLINE_EXCEEDED`, and should be resubmitted with the same command ID: a command ID runs once.

**SDK versions.** Every SDK sends its protocol version. A kernel accepts a range and refuses others with a message saying whether the SDK or the kernel needs an update. Upgrade the kernel first when a release says so. Before the first release, deploy SDKs and kernel of the same build together.

## What needs a restart

| Change | What to do |
|---|---|
| a new service, new rights | issue a grant; no change to the kernel |
| a leaked key | `kernable revoke` against the running kernel ([Keys](./keys#revoking-a-stolen-key)) |
| the kernel's key | replace the file and send SIGHUP ([Keys](./keys#the-kernel-s-key)) |
| TLS certificates, the vault key, any other flag of `kernable start` | restart the kernel ([kernable start](../reference/configuration)) |
