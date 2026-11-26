# The CLI: `kernable cloud`

The open `kernable` CLI talks to kernable Cloud through its [API](/cloud/reference/api), at
`https://api.kernable.cloud` unless `--api` or `KERNABLE_CLOUD_API` names another. `--team`
defaults to your personal team; `--json` prints the API's answer as it is.

## Session

| Command | What it does |
|---|---|
| `kernable cloud login [--api <url>] [--no-browser]` | Signs in by device code: open the page shown, enter the code, sign in and approve. The session goes into the OS keychain, else `~/.config/kernable/cloud.json` (0600). |
| `kernable cloud logout` | Ends the session and forgets it. |
| `kernable cloud whoami` | The account, its teams and roles, and what each team still lacks for its first project; for a CI token, its team. |

In CI, set `KERNABLE_CLOUD_TOKEN` to a team's [token](/cloud/teams#tokens-for-ci) instead of signing in.

## Teams

| Command | What it does |
|---|---|
| `kernable cloud teams` | Your teams. |
| `kernable cloud teams create <slug> [<name>]` | Opens a team, you its owner. |
| `kernable cloud teams accept-terms <team>` | Accepts the terms and the Preview Access notice. |
| `kernable cloud teams verify-payment <team>` | Sets up the team's billing: prints the payment page (Stripe) to open ([prices](/cloud/usage)). |
| `kernable cloud teams billing <team>` | Prints the team's billing portal: invoices, the payment method, cancelling. |
| `kernable cloud teams two-factor <team> on\|off` | Requires a second factor of every member. |
| `kernable cloud teams delete <team>` | Deletes the team: kept 30 days, restorable. |
| `kernable cloud teams restore <team>` | Takes the deletion back. |

## Members

| Command | What it does |
|---|---|
| `kernable cloud members [--team <team>]` | Members and their roles. |
| `kernable cloud members invite <email> --role <role>` | Invites by mail: `owner`, `admin`, `developer` or `viewer`. |
| `kernable cloud members role <identity> <role>` | Changes a member's role. |
| `kernable cloud members remove <identity>` | Removes a member. |

## Tokens

| Command | What it does |
|---|---|
| `kernable cloud tokens [--team <team>]` | The team's CI tokens, without secrets. |
| `kernable cloud tokens create <label> --role <role> [--project <project>] [--expires <days>]` | A token, shown once; never an owner's. |
| `kernable cloud tokens revoke <id>` | Revokes it. |

## Projects

| Command | What it does |
|---|---|
| `kernable cloud projects [--team <team>]` | The team's projects. |
| `kernable cloud projects create <name>` | Records a project, once the team passed the checks before its first one. |
| `kernable cloud projects show <name>` | The project. |
| `kernable cloud projects usage <name> [--month <yyyy-mm>]` | Its partitions' copies this month: what each holds, GB-months stored, GB sent. |
| `kernable cloud usage [--month <yyyy-mm>]` | The team's copies this month, against what the monthly fee includes. |
| `kernable cloud projects links <name>` | Its partitions linked to your kernels, and the relay they push to. |
| `kernable cloud projects root <name>` | (Next release.) The project's root key, and the old one during a replacement. |
| `kernable cloud projects root <name> set <root.pub\|ID> [--now]` | Sets the root key; a later one [replaces it](/cloud/link-kernels#replacing-it), keeping the old one valid until `finish` (7 days at most), or `--now` ends it at once. Admins and owners. |
| `kernable cloud projects root <name> finish` | Finishes a replacement: the old root no longer counts. Admins and owners. |
| `kernable cloud projects link <name> <partition> --pusher <pusher.pub\|ID> [--root <root.pub\|ID>]` | Links a partition: that pusher may push its copy, readers granted by the project's root follow it; `--root`, if given, must be the project's root. Developers and up. |
| `kernable cloud projects unlink <name> <partition>` | Ends the link at once; the copy is kept 30 days. Admins and owners. |
| `kernable cloud projects alerts <name> [set <email>...]` | Where the project's [alerts](/cloud/monitoring#alerts) go besides the team's owners and admins (at most 10); `set` without addresses leaves only them. Admins and owners. |
| `kernable cloud projects node-token <name> <instance>` | A one-time join token (24 h) for a [managed node](/cloud/link-kernels#_6-managed-nodes-optional). Admins and owners. |

## The audit log

| Command | What it does |
|---|---|
| `kernable cloud audit [--team <team>] [--before <position>] [--limit <n>]` | The team's [audit log](/cloud/teams#audit-log), newest first: when, who, what, on what. Prints how to page back. Admins and owners. |
