# Teams, members and tokens

Every account has a personal team. Open more for your company or your projects:

```sh
kernable cloud teams create acme "Acme"
```

## Roles

| Role | May |
|---|---|
| **owner** | everything: the terms, the payment method, deleting the team, inviting owners |
| **admin** | members, invitations, CI tokens, projects |
| **developer** | work with projects |
| **viewer** | read |

Owners act only with a second factor. A team's owners can require one of every member:

```sh
kernable cloud teams two-factor acme on
```

## Members

```sh
kernable cloud members --team acme invite ada@example.com --role developer
kernable cloud members --team acme role <identity> admin
kernable cloud members --team acme remove <identity>
```

The invitation goes out by mail. Someone without an account makes one while accepting it; they
get a personal team of their own too.

## Tokens for CI

A token is a role in one team, maybe for one project only, never an owner's. It is shown once:

```sh
kernable cloud tokens --team acme create ci --role developer --project shop --expires 90
```

CI sets `KERNABLE_CLOUD_TOKEN` to it. Revoke it with `kernable cloud tokens --team acme revoke <id>`.

## Audit log

Every change in a team is recorded with who made it: members invited, joining, their roles
changed or removed, CI tokens created and revoked, projects created, partitions linked and
unlinked, join tokens made, the terms, the second-factor rule, deletion and restore. Admins and
owners read it in the console, on the team's page, or with:

```sh
kernable cloud audit --team acme
# 2026-11-18T09:12:03Z  ada@example.com  member.invited     bob@example.com  developer
# 2026-11-18T09:40:51Z  bob@example.com  member.joined
# 2026-11-18T10:02:17Z  ci               partition.linked   shop.eu          pushers 3f9a1c0e77b2; roots 81c2d4e5f6a7
```

A CI token's changes name the token by its label. Entries are kept as long as the team, and
nobody, kernable included, changes or removes one. Reads are not recorded, nor are the
operators' actions on kernable's own servers: those are in kernable's own audit log.

## Deleting a team

```sh
kernable cloud teams delete acme
```

The team is suspended at once and kept 30 days, during which `kernable cloud teams restore acme`
brings it back. After that, its projects' copies and their backups are deleted. Your data stays
where it always was: in your own kernels.
