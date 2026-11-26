# Usage and prices

## Prices in Preview Access

A monthly fee per team, and usage on top only beyond what the fee includes (excluding VAT):

| | |
|---|---|
| **Monthly fee per team** | **€149** |
| Included every month | 20 GB stored across the team's copies, their backups included; 200 GB sent to your readers |
| Also included | any number of partitions, readers, members, CI tokens and managed nodes; support in business hours |
| Stored beyond 20 GB | €0.50 per GB-month |
| Sent beyond 200 GB | €0.05 per GB |

The fee is billed monthly in advance, usage beyond it in arrears. Your team can cancel at the end of any month
([leaving](/cloud/leaving)).

If a payment fails, Stripe tries again over the following days, and the team's page says so.
If the invoices stay unpaid, the team can open no new project and link no new partition until
they are paid in the billing portal. Once a subscription has ended, billing is set up again from
the team's page. Running copies go on for 30 days either way; then they are unlinked (nothing
pushes to or reads them) and kept another 30 days, as any unlinked copy: paying or setting
billing up again and linking the partitions brings them back. The team's page says when.

## What is counted

For each copy:

- **what it holds**, in GB-months: its size, hour by hour;
- **what it sent to your readers**, in GB.

Nothing of a record's content is needed for either. A GB is 10⁹ bytes; a GB-month is a GB kept
for 730 hours. The team's page in the console shows the month so far against what the fee
includes, each project's page its partitions, and so does `kernable cloud usage` (or
`kernable cloud projects usage <name>`). The invoice lists what went beyond what the fee
includes; usage is reported to billing every hour.

## A copy's limit

A copy keeps everything its writer wrote: kernable cannot compact what it cannot read. Each copy
has a cap. A push that would grow it beyond the cap is refused: the copy stops following, and its
readers report themselves not ready (`/ready` answers 503) until the cap rises. Your writer and
your application are not affected.

If a copy nears its cap, write to support: in Preview Access, caps are raised by hand.
