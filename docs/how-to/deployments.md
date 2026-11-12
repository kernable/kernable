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

A deployment's read model registrations (with their positions) and snapshots stay until you drop them, so a rollback can always find them. Drop one that you will not go back to, with the kernel stopped:

```sh
kernable deployments drop kernable.db 2026-11-01.1
```

or let the kernel drop deployments that no membership names, with none of their read models connected for a number of days: `--deployment-retention-days 30`. Either logs what it dropped; the events stay. A dropped deployment that comes back starts afresh: its read models rebuild, and for the unknown-history refusal it counts from its first event, as before.

## Previews and replay checks

A preview of a new deployment on a copy of production, or a check that replays recent commands on a copy before switching, needs a copy of the store. Make a **masked copy** for it ([Previews and copies](./previews)): the events and the history without the personal data, which reads as `null` as after an erasure, from the backup, a store or an export. kernable Cloud makes such copies for you.
