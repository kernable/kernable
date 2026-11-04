# Share tenants across kernels

When one kernel is not enough for all your tenants, run several, each serving a share of them: **partitions**. A tenant lives on exactly one partition, so everything kernable guarantees within a tenant holds as before: one writer per aggregate, unique keys and requirements, listener order, settling, erasure. A signed **partition map** says where each tenant lives, and the SDKs send every request to its tenant's partition by themselves.

Every partition is an ordinary kernel, or a [pair with a standby](./standby). One of them, the **control partition**, also holds the map and the trust records (grants, revocations, memberships); the others take them from it every second.

## 1. Keys

Every partition takes the trust records, and reserves global unique keys, with a node key granted `replicate` (a pair's node key does both). The control partition needs one too, to ask the others about global keys:

```sh
kernable keys new node-p2 && kernable grant root.key node-p2.pub 'replicate=*'
```

Every partition trusts the same root keys (`--root`).

## 2. Start the partitions

```sh
# p1: the control partition (no --control)
kernable start --root root.pub --partition p1 --listen 10.0.1.1:7233 …

# p2: takes the map and the trust records from p1
kernable start --root root.pub --partition p2 --control 10.0.1.1:7233 \
  --node-key node-p2.key --listen 10.0.2.1:7233 …
```

With a pair per partition, give `--control` both kernels of the control partition, comma-separated. A partition serves no tenant before it has a map: two partitions must never take the same tenant.

## 3. Apply the map

```json
{
  "epoch": 1,
  "partitions": {
    "p1": ["10.0.1.1:7233", "10.0.1.2:7233"],
    "p2": ["10.0.2.1:7233", "10.0.2.2:7233"]
  },
  "default": "p1",
  "control": "p1",
  "tenants": { "acme": "p2" }
}
```

```sh
kernable partitions apply 10.0.1.1:7233 ops.key map.json   # ops.key: granted manage for *
kernable partitions show 10.0.2.1:7233 ops.key             # p2 has it within seconds
```

Tenants the map lists live on their partition; every other tenant on the default partition, so new tenants need no change to the map. Every new map needs a higher `epoch`. Keep the map file in version control: it is the answer to "where is tenant X?".

Change a tenant's partition in the map only before it has data. A tenant with data moves with `kernable partitions move` (step 5); while a move runs, `partitions apply` refuses until it is finished.

## 4. Point your processes at it

Give every process the address of one partition (or of all, comma-separated): the SDK reads the map there and

- sends each submit, query, file and cache request to its tenant's partition;
- runs its workers and listeners on every partition. A read model sees the events of all of them, in order within each partition; with a store, it keeps its position per partition;
- applies memberships on the control partition;
- reads the map again when a partition answers `wrong_partition` (the map changed), and sends the request anew.

A subscription reads every partition too, from their beginnings, with events in order within a partition but not across them.

A **read model across tenants** (totals over all customers, an admin list) holds the events of every partition, but a query reaches it through one partition, its tenant's, and the kernel there only knows how far the read model got on that partition. To get an answer that holds everything committed anywhere before the query, ask with `allPartitions`:

::: code-group

```ts [TypeScript]
const total = await kernable.query<number>({ domain: "billing", type: "Revenue", allPartitions: true });
```

```python [Python]
total = k.query("billing", "Revenue", all_partitions=True)
```

```rust [Rust]
let total: u64 = kernable.query(Query::new("billing", "Revenue").all_partitions()).await?;
```

:::

The SDK first sends a wait-only query to every partition at once; each returns as soon as the read model has processed that partition's events up to that moment, and then the query itself goes to the tenant's partition. It costs one round trip per partition, in parallel, plus however long the read model needs to catch up. Without partitions it waits for the one kernel the same way.

## 5. Move a tenant

```sh
kernable partitions move 10.0.1.1:7233 ops.key acme p3
```

The command moves `acme` from p2 to p3 in two parts, and applies a map for each:

1. **While `acme` runs on.** A map marks `acme` as being copied to p3. p3 asks p2 for every command of the tenant that has an outcome, with its events, vault values and files, and commits the events again under the same commands and worker signatures: new positions in p3's log, the same aggregate versions, and on each copy where it was first committed (`origin_partition`, `origin_position`). Listeners and subscriptions do not get the copies; the originals were delivered on p2. p3 holds none of `acme`'s unique keys yet.
2. **The rest, with `acme` stopped.** A map marks `acme` as moving. Both partitions answer its requests with `UNAVAILABLE` (`moving: …`); the SDKs wait and send them again, for up to 30 seconds. Once p2's queue holds none of `acme`'s commands, p2 sends only what p3 lacks (commands that completed since the first part) and what each of `acme`'s aggregates holds: its unique keys and facts.
3. p3 files those keys and tells the control partition, which hands it `acme`'s global unique keys.
4. The final map puts `acme` on p3. p2 drops `acme`'s unique keys, vault values and files; its events stay in p2's log as history.

A listener (with the SDK) keeps a tenant's order across the move: it handles `acme`'s new events on p3 only after its stream of p2 has passed `acme`'s last event there. A subscription sees each partition in order, not across them.

If the command stops (or a partition does), run it again: it continues the move, and p3 keeps what it copied already. Moving a tenant back works the same way; only what happened since it left is copied.

The tenant is unavailable only for the second part. Measured on one Apple M2 Max with both partitions on it (`bench/move.sh`, a command per event, 1 KB each): with SQLite about a second, whatever the tenant's size (1 s for 268,000 events); with the file log, which reads its whole log for the rest, it grows with the log (2 s for 422,000 events, 1 GB). The move as a whole takes about 60 seconds per million events with SQLite and 30 with the file log, nearly all of it in the first part. Files go along in both parts, so a tenant with many large files stops longer.

## What it gains

Every partition commits on its own, so throughput grows with partitions on machines of their own. Measured on one Apple M2 Max (12 CPUs, one SSD), where every partition also brings its worker and a load generator of 64 clients, so all of them share the same CPUs and disk (`bench/partitions.sh`, results in `bench/`):

| Store, durability | 1 partition | 2 | 3 | 4 |
|---|---|---|---|---|
| SQLite, `normal` | 7,008/s | 10,279/s | 11,820/s | 12,287/s |
| file log, `normal` | 12,670/s | 19,217/s | 20,909/s | 21,456/s |
| SQLite, `full` | 3,052/s | 3,042/s | 3,675/s | 4,062/s |

On one machine, two partitions give about 1.5 times one; beyond that the shared CPUs and disk are the limit, not kernable. Measure your own machines.

## What happens when

| When | What happens |
|---|---|
| A request names a tenant of another partition | `FAILED_PRECONDITION` `wrong_partition: tenant acme is on partition p2 (map epoch 3)`; the SDKs read the map again and send it there |
| A key is revoked on the control partition | Every partition refuses it within about a second |
| The control partition is down | The others serve on with the map and trust records they hold; new maps, grants and memberships wait, and so do commits that file a new global key |
| A partition that was down starts again | It takes what it missed from the control partition first thing |
| A tenant moves | While it is copied it runs on; then its requests wait (`UNAVAILABLE`, `moving: …`) for the rest, about a second; the SDKs send them again |
| The target of a move stops during the copy | It copies again when it is back; what it holds already it keeps |

## Limits

- **One tenant never grows beyond one partition.** Give a big tenant a partition of its own.
- **Across tenants, there is no order and no atomicity**: a command changes one aggregate of one tenant, as always; a read model across partitions sees each partition's events in order, not one global order. A query with `allPartitions` sees everything committed before it, but no order between partitions either.
- **Unique keys with `scope: global`** are unique across all partitions: before a partition commits one its aggregate did not hold, it reserves the key on the control partition, which asks the partition that had it whether it still does (a moment of extra latency for those commits, and none while the control partition is down: they wait). Personal global keys are unique within each partition only.
- New partitions in the map reach running workers and listeners when they start again.
- A move copies the tenant's vault values only to a target with a vault. The source's standby drops the tenant's vault values and files only once it serves as the primary.
