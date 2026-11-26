# Troubleshooting

Most of what goes wrong shows on a reader's `/ready`: it answers 503 with the reason.

## A reader is not ready

| Why | What to do |
|---|---|
| the writer is away | The pusher has not been heard from for a few seconds. Check that the writer runs and that its node runs the pusher (`[push]` in `project.toml`); the pusher's log names why it cannot connect. |
| refused: the key is not named, stopped or revoked | The relay does not let the reader in. `kernable cloud projects links <project>` lists each partition's keys with their role and state: add the reader's key (`kernable cloud projects key <project> <partition> reader.pub read`) or let a stopped one in again (`… activate`). A revoked key stays out: use a new one. |
| no copy key | The reader's `--reader-copy-key` file is missing. Copy the partition's copy key there yourself: kernable Cloud never has it. |
| behind | The reader is further behind the writer than `--reader-max-lag-ms` (30 s by default) and refuses queries until it catches up. Look at what slows it: its machine, its network to the relay. |

A refused reader keeps asking (at most every 60 s) and follows as soon as it is let in, without a
restart.

## The pusher cannot connect

- **Not linked.** `kernable cloud projects links <project>` must list the partition with the
  pusher's key ID. A new link takes a few seconds to reach the relay.
- **Ended.** An unlinked partition refuses its pusher; link it again (within 30 days, its copy
  comes back).
- **The copy's name.** `copy` in `[push]` is `<team>.<project>.<partition>`, the team by its slug:
  `kernable cloud projects show <project>` names it.
- **Network.** The pusher opens one TLS connection out to the relay's port (7400); nothing comes
  in. The relay's certificate is a public one.

## The copy stopped following

It reached its cap: the relay refuses pushes that would grow it further, and its readers report
themselves not ready. Your writer is not affected. See [usage and prices](/cloud/usage#a-copy-s-limit).

## Commands to a reader fail with `writer_away`

A reader passes writes back through the relay to the pusher. While no pusher is connected, it
answers `writer_away`, and the SDKs try again. Once the pusher is back, they go through.

## Still stuck

Write to [support](/cloud/monitoring#support) with the copy's name and what the reader's `/ready` and
the pusher's log say. More on how readers fail and recover:
[readers: when something fails](/how-to/readers#when-something-fails).
