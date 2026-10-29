# Deploy and upgrade

## The kernel

1. **Back up** the store and the vault ([Back up and restore](./backup)).
2. **Stop the kernel with SIGTERM.** It refuses new requests, lets running ones finish for `--drain-timeout-ms` (default 10 s), saves listener positions and a checkpoint, and exits. Commands that did not finish stay queued.
3. **Install the new `kernable` binary.**
4. **Migrate the store if needed.** `kernable migrate <store>` says "nothing to migrate" when the format is current; otherwise it upgrades the store. A kernel refuses to start on a store of another format, so you cannot forget this step.
5. **Start the kernel** and wait until `GET /ready` on its metrics address answers 200: every registered aggregate has a worker connected, and every registered listener is connected and has caught up.

New services, new grants and revocations need no change to the kernel: issue a grant, or send a revocation with `kernable revoke`. A new kernel key needs SIGHUP only. Every other setting is a flag of `kernable start` and needs a restart ([Keys](./keys)).

Processes reconnect on their own while the kernel is away; clients get connection errors and should resubmit with the same command ID.

## Your processes

- **Apply the service's membership once per deploy** (`kernable apply` or a deploy script calling `kernable.apply`): it lists the workers, listeners and middlewares the kernel waits for; what the new version no longer has stops counting, and a middleware it no longer lists stops checking ([Remove a listener, middleware or aggregate](./remove-listener)). Not from every process: the latest apply wins.
- **Workers and listeners restart safely at any time.** A worker's commands go to another worker; a listener continues after its last acknowledged event.
- **Deploy readers before writers** when an event's format changes ([Change an event's format](./event-versions)).
- **Middleware is checked always.** While a middleware restarts, the requests it checks are refused. Run two instances only if they have different names, or keep the restart short.
- **SDK versions.** Every SDK sends its protocol version. A kernel accepts a range and refuses others with a message saying whether the SDK or the kernel needs an update: upgrade the kernel first when a release says so.
