# Security and data

## What kernable Cloud holds

| What | Where | Readable by kernable |
|---|---|---|
| Your account: email, name, password hash, second factors | the customer service, in its vault | yes, to sign you in |
| Teams, members, roles, projects, CI tokens (their hashes) | the customer service | yes |
| The sealed copies of your writers | relays in the EU | no: positions, hashes and sizes only |
| Their backups, seven days | object storage in the EU | no |
| Usage: bytes stored, bytes sent to readers | the control point | yes, to bill it |

Your code, your services and your kernels' stores are never at kernable Cloud.

## Keys you hold

- **Your root key**: offline, with you. It grants every other key of your kernels.
- **The copy key**, one per partition: made on your writer's machine, with its pusher and your
  readers. The pusher hands it to a reader your root key granted `copy=<copy>`, sealed to that
  reader's own key; kernable carries the sealed key and can never open it. Or you keep it by hand
  (`copy-key-handover = false`).
- **Each instance's own key**: made on its machine. kernable Cloud knows only its public half,
  with its role and state for the partition (active, stopped, revoked).

kernable signs only what concerns its own side: the settings of managed nodes (a fixed list,
never keys, grants or secrets) and the documents of your account.

## Signing in

Registration is by invitation. Sign-in takes a password with a second factor (an authenticator
app or a passkey), a passkey alone, or GitHub for an account that linked it. The console keeps its
session in cookies scripts cannot read, accepted only from the console itself.

## Region

kernable Cloud, every copy and every backup are in the EU.

## Ending

Deleting a team keeps it 30 days, restorable; then its copies and their backups are deleted. Your
data stays in your own kernels. Billing records are kept as tax law requires.

## Reporting a problem

Write to the address in your invitation's mail. Please do not open public issues for anything
that could affect other customers.
