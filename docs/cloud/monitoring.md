# Monitoring and support

kernable Cloud watches its own side around the clock; your kernels, in your infrastructure, are
yours to watch.

## What kernable watches

Every minute: the relays and their copies, the backups of each copy, the servers behind them,
and your [managed nodes](/cloud/link-kernels#_8-managed-nodes-optional) as they report. An operator
bot restarts what fails and asks a person about anything else. Upgrades and fixes on kernable's
side need nothing from you.

What kernable sees of a copy is what a relay may see: its size, its head, when its pusher and
readers last connected. Never a record's content.

## What you watch

Your writer, its standby, the pusher and your readers run with you. Watch them as you would
without kernable Cloud:

- a reader's `/ready` answers 503, with the reason, while it is not following or too far
  behind; route your load balancer by it;
- the metrics `kernable_reader_lag` and `kernable_reader_following` on readers,
  `kernable_readers_lag_max` on the writer;
- the pusher logs each failed connection and tries again by itself.

More: [monitor kernable](/how-to/monitor),
[readers: watch it](/how-to/readers#watch-it). When something does not
work, see [troubleshooting](/cloud/troubleshooting).

## Alerts

kernable Cloud mails your team when something on your side needs a look, once it starts and again
once it is over:

| Alert | When | |
|---|---|---|
| A copy's writer is away | its pusher has not been connected for 5 minutes (a copy that never held anything excepted) | critical |
| A reader is behind | a connected reader is more than 10,000 records behind its copy for 5 minutes | warning |
| A reader is away | a reader whose key the partition names, active, was connected and has not been for 5 minutes (a key named but never connected, stopped or revoked is not missed) | warning |
| A reader holds an old copy key | a joined reader's node reports another copy key than the writer's node, for 5 minutes (after a rotation it asks for the new one; granted `copy=<copy>`, it is handed it) | warning |
| A copy is filling up | over 80 % of its limit (critical over 95 %); past it, the writer stops pushing | warning |
| A managed node is silent | no report for 2 minutes | critical |
| A managed node reports a problem | as it reports it | warning |

The mails go to the team's owners and admins, and to the addresses set for the project (at most
10): on the project's page in the console under **Alerts**, or

```sh
kernable cloud projects alerts shop set ops@example.com plant-a@example.com
kernable cloud projects alerts shop          # where they go now
kernable cloud projects alerts shop set      # only the owners and admins again
```

The addresses are personal data: a new list erases the one before it, and the audit log counts
them without naming them.

kernable's own alerts (its servers, relays and backups) go to kernable's operators. During Preview
Access there is no on-call: what fails outside business hours is restarted by the bot or waits for
the next morning.

## Support

Write to the address in your invitation's mail, with your team's slug and, for a copy, its name
(`<team>.<project>.<partition>`). In Preview Access: business hours, best effort, no SLA.
