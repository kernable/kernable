# Deployments and rollbacks

Use this page when you put a new version of your code into production and want to be able to go back to the one before. A **deployment** is one version of your code: the workers, read models, reactors and processes built from it. kernable never changes the events for a deployment. Each deployment builds its own read models and snapshots from the events, so switching between deployments, forward or back, only changes which one serves.

## Name every deployment

Give every process of a version the same deployment name, through the environment:

```sh
KERNABLE_DEPLOYMENT=2026-11-11.2 node app.js
```

or in code (`new Kernable({ deployment })`, `Kernable(deployment=...)`, `Options::deployment` in Rust). A name has 1 to 128 letters, digits, `-`, `_` and `.`; a git commit or a build number works well. A process without one behaves as before deployments existed.

With a name set, the process tells the kernel which code it runs whenever it connects, and:

- **Every event records the deployment that wrote it** (`Event.deployment`, part of the event's hash). `kernable deployments <store>` lists each deployment with its first and last event.
- **Snapshots are kept per deployment.** A worker only gets snapshots its own deployment made, so an older version never reads a newer state format.
- **Read models and middlewares are kept per deployment.** The read model `carts` of deployment `v2` is registered as `carts@v2`, beside `carts@v1`, with its own position: it builds from the first event while `v1`'s keeps serving. Both stay registered, so a switch back finds `v1`'s read model where it stopped, and it catches up on what happened since.
- **Reactors are not.** A reactor (a listener that answers no queries and checks nothing) keeps one registration across deployments, so the new deployment goes on after the old one's position and no email is sent twice. Run one deployment's reactors at a time.

## Which deployment serves

A service's [membership](./deploy#a-new-version-of-a-service) names the deployment that serves it. The deployment that serves:

- runs the commands: workers of the service that declare another deployment stay connected and idle;
- counts: its listeners are the ones the kernel waits for (`/ready`, settling submits, the order of delivery); another deployment's read models keep catching up but hold nobody back;
- answers queries: a query goes to a read model of the asker's own deployment if there is one, else to the serving deployment's;
- checks requests: of a middleware registered by several deployments, the serving one's checks, or, while it is away, another connected one.

## A new deployment warms up, then takes over

A new deployment does not take over the moment its processes start: its read models first rebuild from the events beside the serving deployment's, while the serving one goes on. The kernel switches a service to another deployment only once that deployment has **caught up**:

- for every aggregate the membership lists, a worker of the deployment is connected;
- every read model and middleware it lists is registered for the deployment (`carts@2026-11-11.2`) and within `--deployment-switch-lag` events (default 100) of the newest one. A reactor keeps one registration across deployments and needs no warm-up.

Until then, a membership that would switch is refused with `FAILED_PRECONDITION` `not_caught_up`, naming what it waits for:

```
not_caught_up: deployment 2026-11-11.2 has not caught up: carts@2026-11-11.2 at 1200 of 2215; it takes shop over once it has (apply again), or force the switch
```

A deployment that wrote events before (it served, and an operator goes back to it) takes over at once: its read models are kept where they stopped and catch up the gap in moments. One whose read models were [dropped](#clean-up-old-deployments) warms up like a new one. Going back is always an operator's decision: an app never switches back by itself ([below](#who-switches)).

### Who switches

An app (`kernable.app(...).run()`) applies its membership when it starts. Its `switch` option (`AppOptions.switch`, `switch=` in Python, `App::switch` in Rust; default `KERNABLE_SWITCH`) says what happens when another deployment serves:

| `switch` | The app |
|---|---|
| `caught-up` (default) | for a deployment that never served: runs beside the serving deployment, its workers idle and its read models building, applies its membership again every second, and takes over once it has caught up. For a deployment that served before (it wrote events): runs beside like `never`, refused with `served_before`, until an operator rolls back to it. |
| `never` | runs beside the serving deployment until an operator switches (below); it applies its membership only while its own deployment serves, and notices the switch within a second. |
| `now` | takes over at once, caught up or not (what applying did before deployments warmed up). |

A service that has no membership yet, or is served by the app's own deployment already, takes the membership at once in every mode.

So a restart never switches back. During a rolling deploy from `v1` to `v2`, a `v1` process that restarts (a crash, a node drained) after `v2` took over comes up beside `v2` instead of taking the service back: `v1` served before, so only an explicit rollback switches to it again. A `v2` process that restarts applies the membership of the deployment that serves, which changes nothing. The app's `ready` resolves (is set) once it serves; `connected` once its workers and listeners are connected, also while it warms up; `serving` once its deployment serves. Its processes register once it serves.

From a pipeline, apply the manifest with the deployment, and let it wait for the warm-up:

```sh
kernable apply 127.0.0.1:7233 keys/deploy.key shop.json --deployment 2026-11-11.2 --wait 600
```

Without `--wait` a refusal fails at once (exit code 3, so a script can tell it from other errors); `--force` switches without waiting. In code, `Kernable.apply` takes the same choice (`Manifest.switch`), and `waitsForSwitch` (`waits_for_switch`, `Error::waits_for_switch`) tells the refusal from other errors.

### Is it caught up?

- `GET /ready?deployment=2026-11-11.2` on the metrics address (`--metrics-listen`): 200 once every read model and middleware registered for the deployment is within the lag (or it served before) and some of its processes are connected, else 503 naming what it waits for.
- `GET /api/deployments/warm-up?deployment=2026-11-11.2` in the kernel UI: per read model its registration, position, how far behind the newest event, whether it is connected and caught up; the aggregates the deployment's workers run.
- `GET /ready` keeps answering for the deployment that serves.
- The same on the kernel's local socket (`--admin-socket`; `kernable node` makes `admin.sock` in the project directory): `curl --unix-socket admin.sock http://localhost/ready?deployment=2026-11-11.2`, and `/api/deployments/warm-up?deployment=…`. A tool on the machine reaches it through the file system, so it need not enter the network namespace the kernel runs in (the node's pod); the socket is the kernel's user's only (mode 0600), and root's.

Before the switch, nothing of the new deployment counts: its read models hold nobody back and settling submits do not wait for them. After it, the old deployment's processes idle; stop them when you no longer want to go back quickly.

## Before you roll back: what the newer deployment left behind

Events are never undone. Before you switch back, look at what the newer deployment wrote since it started, from the position before its first event (`kernable deployments <store>` shows it) or from a time:

```sh
$ kernable deployments since kernable.db 1840 --version 2026-11-11.2
after position 1840 (up to 2215), events of 2026-11-11.2
  shop.ItemAdded schema 2  312 events in 97 aggregates, by 2026-11-11.2  NEW SCHEMA
  shop.CartShared schema 1  4 events in 4 aggregates, by 2026-11-11.2  NEW TYPE
  shop.OrderPlaced schema 1  51 events in 51 aggregates, by 2026-11-11.2
  scheduled command 7d1c… shop.RemindCustomer on shop.cart/c-17, due 1762876800000
  running case o-88#1 of shop.fulfilment (tenant t1), last StepReached
  process shop.fulfilment version 4 registered
```

`--at 2026-11-11T09:00:00Z` starts from a time instead, `--json` prints it for a tool; the kernel UI serves the same at `GET /api/deployments/since?position=1840&version=…`. What each part means for a rollback:

| What it shows | After a rollback to the older deployment |
|---|---|
| Rows without a mark | Safe: the older code knows these events. |
| `NEW SCHEMA`, `NEW TYPE` | The aggregates listed hold events the older code does not know: their commands are refused with `unknown_history` until a deployment that knows them serves again. Every other aggregate works. The older read models skip these events and count them. |
| Scheduled commands | They run when due, on the older workers, as the older code decides. A command type the older code does not run waits in the queue until a deployment that runs it serves. Cancel the ones that must not run (`Kernable.cancel`). |
| Process timers, running cases | Cases keep the process version they started on, and the kernel runs them: their steps send commands to whatever serves. A newer version's case may send commands the older code does not know. |
| Process versions registered | New cases start on the newest registered version unless the older deployment registers its own again (an app does when it starts). |

No mark anywhere means the rollback is safe. Marks mean it is still possible, but those aggregates wait for the next roll forward.

## Roll back

1. Start the older deployment's processes again, if they stopped. Its read models connect under their registrations and catch up on the events written meanwhile.
2. Apply its manifest with `--deployment <older>` (`kernable apply 127.0.0.1:7233 keys/deploy.key shop.json --deployment 2026-11-11.1`): it switches as soon as its workers are connected, as it served before. Starting its app is not enough: an app does not switch back to a deployment that served before (`served_before`), it waits beside until you apply. In code, `Kernable.apply` with switch `now` rolls back as well. From then on its workers run the commands, and the newer deployment's processes idle; stop them.
3. Watch `/ready` and the `unknown_history` rejections in the UI.

Rolling forward again is the same with the newer deployment.

## The unknown-history refusal

When a worker with a deployment gets a command, the kernel first looks at the aggregate's events (those after the snapshot of that deployment, or all of them). It refuses the command with `unknown_history`, before the worker sees it, if one of them is

- of a schema above the one the worker declares for its type (`Domain.event(type, schema)`, the event classes' `version`), or
- of a type the worker does not declare at all, that its deployment never wrote, and that another deployment first wrote after this deployment's first event.

Refusing is safer than deciding on state the code cannot build: an aggregate that misses an event can accept what it must refuse. The second rule lets a deployment that is just a new build of the same code read types older than itself, but not types a newer deployment introduced. Declare every event type your aggregates read (the SDKs send every type that has an `apply` or a declaration).

A worker without a deployment is not checked this way. Instead, as before, it may not register while the store holds a newer schema than it writes.

## Read models skip what they do not know

A read model that gets an event of a newer schema than its code reads skips it instead of stopping: it moves on, counts it (`kernable_listener_skipped`, the listener's row in the UI) and logs a warning once per type and schema. A reactor parks such an event (`unknown_event`) instead, also one that erases a subject, so that once code that reads it serves, `kernable listeners retry` handles it. After rolling forward, a read model that skipped events should [rebuild](./rebuild-read-model); in-memory read models do on every start.

## Fixing what went wrong

A deployment that wrote wrong events, or sent something it should not have, is fixed forward:

- **Wrong events of a known type:** correct them with new events (a command whose handler writes the correction, `PriceCorrected` after a wrong `PriceSet`), never by deleting. The log keeps both, and read models apply the correction.
- **Side effects outside:** undo them with actions of their own, a reactor or a command that refunds, cancels or tells.
- **Events the older code cannot read:** roll forward to code that reads them; or write the code that reads them into a new deployment of the older version.

## Clean up old deployments

A deployment's read model registrations (with their positions) and snapshots stay until you drop them, so a rollback can always find them. Drop one that you will not go back to, while the kernel runs, through its local socket:

```sh
kernable deployments drop --socket admin.sock 2026-11-01.1 --force
```

or with the kernel stopped, from the store:

```sh
kernable deployments drop kernable.db 2026-11-01.1 --force
```

The drop is refused (exit code 3; codes in [Errors](../reference/errors#refusals-of-a-deployment-drop)):

- **`serving`**: a service's membership names the deployment. Switch first.
- **`running`**: on a running kernel, a worker or read model of the deployment is connected. Its read models' state is in use, as during a grace period in which the old deployment keeps running beside the new one: stop its processes first.
- **`served_before`**, unless `--force`: the deployment wrote events, so a rollback may need its read models. The kernel cannot know how long you mean to keep it (a grace period is yours, not the kernel's), so you say so with `--force`. A deployment that never wrote an event, a preview's or one that never took over, drops without it.

Or let the kernel drop deployments that no membership names, with none of their read models connected for a number of days: `--deployment-retention-days 30`. Each logs what it dropped; the events stay. A dropped deployment that comes back starts afresh: its read models rebuild, and for the unknown-history refusal it counts from its first event, as before.

## Output for tools

The deployment commands print JSON with `--json`, for a platform or a script. Fields are camelCase; positions are numbers, times milliseconds since the epoch (UTC). New fields may be added; none is renamed or removed while the protocol stays 1.

`kernable deployments <store> --json`:

```json
{
  "deployments": [
    { "deployment": "2026-11-11.1", "events": 1840, "firstPosition": 1, "lastPosition": 1840,
      "lastAt": 1762851200000, "listeners": ["carts@2026-11-11.1"] }
  ],
  "services": [ { "service": "shop", "deployment": "2026-11-11.2" } ]
}
```

`deployments` lists every deployment that wrote events or has read models registered (`listeners`: their registrations, `name@deployment`); one without events has `events` 0. `services`: the deployment each service's membership names (`""`: every deployment serves).

`kernable deployments since <store> <position> --json` (and `GET /api/deployments/since`):

```json
{
  "from": 1840, "fromTime": 1762851200000, "head": 2215, "deployment": "2026-11-11.2",
  "types": [
    { "type": "shop.CartShared", "schema": 1, "deployment": "2026-11-11.2", "events": 4,
      "aggregates": 4, "firstPosition": 1901, "lastAt": 1762860000000,
      "newType": true, "newSchema": false }
  ],
  "scheduled": [
    { "commandId": "7d1c…", "tenant": "t1", "type": "shop.RemindCustomer",
      "aggregate": "shop.cart/c-17", "receivedAt": 1762855000000, "deliverAt": 1762876800000 }
  ],
  "timers": [],
  "cases": [ { "process": "shop.fulfilment", "case": "o-88#1", "tenant": "t1",
               "lastEvent": "StepReached", "lastAt": 1762859000000 } ],
  "processVersions": [ { "process": "shop.fulfilment", "version": 4, "recordedAt": 1762852000000 } ]
}
```

`deployment` is `null` without `--version`; `timers` has the fields of `scheduled`.

`kernable deployments drop … --json`: `{"deployment": "2026-11-01.1", "readModels": ["carts@2026-11-01.1"], "snapshots": 12, "forced": true}`; refused, `{"error": "served_before", "message": "…"}` (exit code 3).

`GET /api/deployments/warm-up?deployment=<d>` (UI and local socket):

```json
{
  "deployment": "2026-11-11.2", "head": 2215, "maxLag": 100, "servedBefore": false,
  "caughtUp": false, "waitingFor": "deployment 2026-11-11.2 has not caught up: carts@2026-11-11.2 at 1990 of 2215",
  "listeners": [ { "name": "carts", "registration": "carts@2026-11-11.2", "position": 1990,
                   "behind": 225, "connected": true, "caughtUp": false } ],
  "workers": [ { "aggregate": "shop.Cart", "connected": true } ]
}
```

`waitingFor` is `null` once caught up; a listener not registered yet has `registration` `null` and only `name` and `caughtUp`. `GET /ready?deployment=<d>` answers 200 `caught up` or 503 with the `waitingFor` text.

## Check a deployment before switching

Before a new deployment serves, you can see what it would have done with the commands production really received: `kernable replay` sends production's recent commands, in their original order, to a kernel on a **masked copy** ([Previews and copies](./previews)) where the new deployment's workers run, and compares each outcome with the one production stored. It reports which commands write other events (types, schemas, numbers, payload fields), which ones are rejected now or with another code, and which event types or schemas the new code writes that the store never held. Nothing in production changes, and the copy holds no personal data.

### 1. A masked copy from before the commands

The commands to replay are the ones production stored after the copy's position, so make the copy at a point before them. From the backup, at a time in the past (an hour ago, say):

```sh
kernable restore s3://acme-backups/shop --backup-key backup-encryption.key \
  --at 2026-11-12T08:00:00Z \
  --mask-vault --store sqlite:check/kernable.db --vault-store sqlite:check/vault.db \
  --vault-key check/vault.key --data-store file:check/data
```

or copy the store now and replay the commands that arrive in the next hour (`kernable copy`). Either prints the copy's mark with its position, `up to event 41870`; `kernable verify` on the copy shows it again.

### 2. A kernel of the check, and the new deployment's workers

Start a kernel on the copy with `--replay on`, a new snapshot store and an address of its own:

```sh
kernable start --root root.pub --replay on --listen unix:check/k.sock \
  --store sqlite:check/kernable.db --vault-store sqlite:check/vault.db \
  --vault-key check/vault.key --snapshots sqlite:check/snapshots.db --data-store file:check/data
```

Such a kernel starts only on a masked copy, and only does what the check needs:

- it takes only the commands `kernable replay` sends, each run **as if received at its original time** (the submit carries it). As the command also keeps its ID, `ctx.now()` and `ctx.id()` return what they returned in production, so the same code writes the same events;
- it refuses every other submit (`FAILED_PRECONDITION`, `replay_only`), so the new deployment's reactors and processes send nothing;
- it **fires no scheduled command and no process step**.

Start the new deployment's processes against it, with their deployment name (`KERNABLE_DEPLOYMENT=2026-11-12.1`) and outbound connections pointed at test accounts, as for a preview. The workers matter, and the middlewares if its memberships list any (a submit waits for them); read models may run, reactors get nothing to do. The copy's memberships name the deployment that served production, so switch to the new one at once:

```sh
kernable apply unix:check/k.sock keys/deploy.key shop.json --deployment 2026-11-12.1 --force
```

### 3. Replay

```sh
kernable replay --from sqlite:kernable.db --since 41870 \
  --to unix:check/k.sock --deployment 2026-11-12.1
```

`--from` is production's store on this machine (read, also while its kernel runs), an export file, or a running kernel (`--from 127.0.0.1:7233 --key ops.key`, a key granted `export`). `--since` is the copy's position (or a UTC time, the last event before it); `--limit 5000` replays only the oldest commands after it. The commands go one at a time, each completed before the next.

```text
replayed the commands of sqlite:kernable.db after position 41870 (up to 42412) on unix:check/k.sock, deployment 2026-11-12.1
  581 commands: 548 replayed (544 identical, 3 different, 1 error), 28 skipped, not sent: 4 scheduled commands and 1 process step
new event types and schemas:
  shop.CartShared schema 1  NEW TYPE  (1 command)
new rejections:
  too_many  (1 command)
differences:
  9b1e… shop.AddItem on shop.Cart: payload of shop.ItemAdded: changed price; added discount
  77c4… shop.ShareCart on shop.Cart: events: shop.CartUpdated/1 -> shop.CartUpdated/1, shop.CartShared/1
  2d0a… shop.AddItem on shop.Cart: outcome: committed shop.ItemAdded/1 -> rejected too_many
  c51f… shop.Archive on shop.Cart: outcome: committed shop.CartArchived/1 -> refused NOT_FOUND (no worker registered shop.Archive for shop.Cart)
skipped:
  e8a2… shop.Register on shop.Customer: carries personal values, which the copy does not hold
  ...
FAILED: 4 differences, more than --max-differences 0
```

`--json` prints the same for a tool. The exit code gates the switch: **0** when at most `--max-differences` commands differ (default 0; a number, or a percent of the commands replayed such as `1%`), **3** when more do, **1** when the check could not run (the kernel at `--to` does not run a check, stays unavailable, or its events come from another deployment than `--deployment`). Then throw the copy away: stop its kernel and delete its stores and its vault key.

### What it compares

Per command: the events production committed against the events the new deployment commits, by type, schema and number, then payload by payload, naming the fields that changed, were added or were removed; or the rejection codes; or that one side committed and the other rejected. A command the kernel of the check refuses (a command type no worker of the new deployment runs: `NOT_FOUND`) counts as an error, which fails the check like a difference. Equal outcomes count as identical; event positions, versions, commit times and the producing deployment are not compared.

The report never holds a value: command IDs, command, aggregate and event types, schemas, rejection codes and field names, no aggregate IDs, no payload values, no rejection messages.

### What it does not send

| Commands | Why | In the report |
|---|---|---|
| with personal values (vault references) | the copy holds none of production's values, and a placeholder would change both the command and what the code decides on it | `skipped`, reason `personal` |
| scheduled ones, and those of process steps | they ran at their own time in production; the kernel of the check fires neither | listed with their original outcome |
| not completed in production | there is nothing to compare | `skipped`, `notCompleted` |
| later commands of an aggregate one of the above touched | they would run on another state than they did in production | `skipped`, `afterSkipped` |

Personal values that the copy reads as `null` in an aggregate's history can still change a decision. Payload fields that hold a personal value in production and `null` (or a new value) in the check count as equal; a difference on an aggregate that held personal values is marked as such (`masked`, "its aggregate holds masked personal values"), and one that follows an earlier difference on the same aggregate is marked too (`afterDifference`), as it ran on another state. The check does not follow effects across aggregates (unique keys, requirements) of commands it skipped, and it does not check read models, reactors or queries: watch the [warm-up](#a-new-deployment-warms-up-then-takes-over) for those.

## Previews

A preview of a new deployment on a copy of production needs a copy of the store too. Make a **masked copy** for it ([Previews and copies](./previews)): the events and the history without the personal data, which reads as `null` as after an erasure, from the backup, a store or an export. kernable Cloud makes such copies for you.
