# Schedule a command

Some commands should run later: cancel an order that is not paid within 3 days, remind a
customer tomorrow, close a poll at midnight. Enqueue the command now with the time it should run
at; the kernel keeps it and runs it then.

::: code-group
```ts [TypeScript]
const id = await kernable.enqueue(CancelOrder.new({ reason: "unpaid" }), {
  aggregateId: "o1",
  at: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000), // in 3 days
});
```
```python [Python]
from datetime import datetime, timedelta, timezone

command_id = kernable.enqueue(
    CancelOrder(reason="unpaid"),
    aggregate_id="o1",
    at=datetime.now(timezone.utc) + timedelta(days=3),
)
```
```rust [Rust]
use std::time::{Duration, SystemTime};

let id = kernable
    .enqueue(
        Submit::command(CancelOrder { reason: "unpaid".into() }, "o1")
            .at(SystemTime::now() + Duration::from_secs(3 * 24 * 60 * 60)), // in 3 days
    )
    .await?;
```
:::

`enqueue` returns at once with the command's ID. The time is part of the signed command.

## What happens until then

- **The command is stored now.** Like any queued command it survives restarts, reaches the
  standby before you hear back, and moves with its tenant to another partition.
- **The aggregate is not blocked.** Commands for an aggregate run one at a time, in order; a
  scheduled command waits beside that queue, so the order's other commands run meanwhile. At its
  time it joins the queue behind them.
- **It is checked twice.** When you enqueue it: the signature, the key's grant and the
  [middleware](../concepts/queries-and-middleware#middleware), so a command nobody may send is
  refused at once. When it is due: the middleware again, with what it knows then, and whether its
  key was revoked meanwhile. A customer suspended in the meantime does not get the command run.
- **Late runs late.** If the kernel is down when the command is due, it runs as soon as the kernel
  is back. A command that must not run late takes `until` as well: past that time it is rejected
  `expired` instead.
- **At most `--schedule-max-days` ahead** (366 by default).

## Cancel it

When the order is paid after a day, the cancellation must not happen:

::: code-group
```ts [TypeScript]
const result = await kernable.cancel(id);
// { cancelled: true }, or { cancelled: false, code: "too_late" | "not_found" | <a middleware's code>, message }
```
```python [Python]
result = kernable.cancel(command_id)
# CancelOutcome(cancelled=True), or cancelled=False with code "too_late", "not_found" or a middleware's
```
```rust [Rust]
let outcome = kernable.cancel(Cancel::new(&id)).await?;
// CancelOutcome::Cancelled, or CancelOutcome::Rejected(r) with r.code "too_late", "not_found" or a middleware's
```
:::

- **Who may cancel:** a key granted `submit` for the command's type, the same as could send it.
  Middlewares check the cancel as a request of its own kind (`check.kind` is `"cancel"`), with the
  command's type, aggregate and data. The [kit](../kits/authorization) decides a cancel exactly as
  it would decide the command.
- **Afterwards** the command's ID answers `cancelled`, also to a resubmit.
- **Too late:** a command that is due already, running or done answers `too_late`. Undo its
  effect with another command if you need to.

## Its result

Submit the same command ID again to get the outcome. Before the command's time, this waits for
it, up to the kernel's submit timeout; afterwards it returns the stored outcome at once, as for
any command.

## Watch it

`kernable_scheduled_commands` counts the scheduled commands waiting. Cancelled and expired ones
count as rejected in `kernable_commands_total`, and appear in the UI's list of rejections with
their code.
