# Leaving

Your data stays where it always was: in your own kernels. kernable Cloud holds a sealed copy and
its backups, which it cannot read. Leaving takes nothing away from your application.

Your team can cancel at the end of any month, in the billing portal (`kernable cloud teams
billing <team>`, or **Invoices, payment method, cancelling** on the team's page in the console).
Deleting the team cancels it too. The month you leave in is the last one billed. Until the
month ends, the team's page says when billing ends; changed your mind, renew it in the portal.

## Stop using a copy

```sh
kernable cloud projects unlink shop eu
```

Nothing pushes to or reads the copy from then on. Take the `[push]` section out of the writer's
node, and point your readers at the writer directly or at
[a relay of your own](/how-to/relays): the same open kernable, with no
change to your code. The copy and its backups are kept 30 days, and linking the partition again
brings it back; after that, they are deleted.

## Close the team

```sh
kernable cloud teams delete acme
```

The team is suspended at once and kept 30 days, restorable with
`kernable cloud teams restore acme`. Then its projects' copies and their backups are deleted.
Billing records are kept as tax law requires.

## Your account

Your account stays until you ask for it to be deleted: write to support.
