# The customer API

The API of kernable Cloud's customer service: what the `kernable cloud` CLI and the console use,
and what your own tools may use. It is kept stable: a change that would break a client gets a new
path, the old one keeps working until it is announced gone.

**Base URL:** the customer service's address (for kernable Cloud, `https://api.kernable.cloud`, the CLI's default).
**Bodies:** JSON, `content-type: application/json`. **Errors:** a status code and
`{"error": "<code>", "message": "<what happened>"}`.

## Authentication

Every call but those marked *public* needs an `Authorization` header, one of:

- `Bearer <access token>` — an account's session: from `POST /v1/sessions` (or a passkey, GitHub,
  or the CLI's device login). Access tokens are short-lived; `POST /v1/sessions/refresh` gives a
  new pair.
- `ApiKey <token>` — an API token (`POST /v1/teams/{team}/tokens`): for CI and tools. It is made
  for one project of one team, with a role (for CI usually `developer`), and reaches nothing else
  of the team; it needs no second factor and ends when it expires or is revoked.

**The console** (`console.kernable.cloud`) holds its session in cookies instead: `POST
/v1/sessions`, `/v1/sessions/refresh` and `/v1/passkeys/sign-in` from the console's origin with
the header `X-Kernable-Console: 1` answer with `HttpOnly`, `Secure`, `SameSite=Strict` cookies
(`kn_access`, and `kn_refresh` on `/v1/sessions`) and no tokens in the body; a request carrying
the cookie is taken only with that origin and header. The API answers CORS for the console's
origin alone.

**Roles** in a team, each including the ones below it: `viewer` (read), `developer` (works with projects),
`admin` (projects, members, tokens), `owner` (agreements, payment, the team itself).
Owners must have a second factor; writes by an owner without one in the session are refused.

## Accounts and sessions

| Call | Body | Answers | Who |
|---|---|---|---|
| `POST /v1/accounts` | `email`, `name`, `password` (optional with passkey/GitHub) | `202`, a verification mail goes out; `403` `sign_up_closed` where registration is by invitation (kernable Cloud) | public, rate-limited |
| `POST /v1/sign-up-invitations/peek` | `token` (from the invitation's link) | `email`, the invited address; `400` `invalid_token` once it expired, was used or revoked | public, rate-limited |
| `POST /v1/sign-up-invitations/accept` | `token`, `name`, `password` | `201`, `account`: the account, verified, with its personal team; `409` `account_exists` if the address has one | public, rate-limited |
| `POST /v1/accounts/verify` | `token` (from the mail) | `200` | public |
| `POST /v1/sessions` | `email`, `password`, `code` (TOTP, if set up) | `access_token`, `refresh_token` | public, rate-limited |
| `POST /v1/sessions/refresh` | `refresh_token` | a new pair | public |
| `DELETE /v1/sessions` | — | `204` | the session |
| `GET /v1/me` | — | the account: email, name, second factors, teams | the session |
| `POST /v1/me/totp` | — | a TOTP secret to scan | the session |
| `POST /v1/me/totp/confirm` | `code` | `200`, TOTP active | the session |
| `POST /v1/me/passkeys/options` | — | WebAuthn creation options | the session |
| `POST /v1/me/passkeys` | `label`, `response` (the browser's credential) | `200` | the session |
| `POST /v1/passkeys/options` | — | WebAuthn request options | public |
| `POST /v1/passkeys/sign-in` | the browser's assertion | `access_token`, `refresh_token` | public |
| `POST /v1/oidc/{provider}/start` | — | `url` to send the browser to (`github`) | public |

## The CLI's device login

| Call | Body | Answers | Who |
|---|---|---|---|
| `POST /v1/cli/device` | `client` | `device_code`, `user_code`, `verification_uri`, `verification_uri_complete`, `expires_in`, `interval` (RFC 8628) | public |
| `POST /v1/cli/token` | `device_code` | `access_token`, `refresh_token` once approved; `400` with `authorization_pending` (or `slow_down`, `expired_token`, `access_denied`) until then | public |
| `POST /v1/cli/device/approve` | `user_code` | `200` | the session (with its second factor) |

## Teams, members, tokens

| Call | Body | Answers | Role |
|---|---|---|---|
| `GET /v1/teams` | — | your teams with your role in each; `missing`: what keeps a team from a new project; `billing_status` (Stripe's: `active`, `past_due`, `unpaid`, `canceled`, …) and `billing_ends_at` (ms), if it is set to end | any account |
| `POST /v1/teams` | `slug` | the new team (you its owner) | any account |
| `DELETE /v1/teams/{team}` | — | deletion requested: the team goes after 30 days | owner |
| `POST /v1/teams/{team}/restore` | — | the deletion withdrawn | owner |
| `POST /v1/teams/{team}/agreements` | — | the terms and the Preview Access accepted | owner |
| `POST /v1/teams/{team}/payment-method` | — | `url`: the payment page (Stripe Checkout) to send the owner to; billing counts once it is paid. `409` `billing_set_up` if it is (and has not ended) | owner |
| `GET /v1/teams/{team}/usage?month=<yyyy-mm>` | — | the month's usage of all the team's copies (this month by default, UTC): `copies`, `total` (`stored_gb_months`, `sent_gb`); `included`: what the monthly fee includes (`stored_gb_months`, `sent_gb`) | viewer |
| `POST /v1/teams/{team}/billing/portal` | — | `url`: the billing portal (invoices, the payment method, cancelling) | owner |
| `PUT /v1/teams/{team}/two-factor` | `required` (bool) | every member must have a second factor | owner |
| `GET /v1/teams/{team}/members` | — | members and roles | viewer |
| `PUT /v1/teams/{team}/members/{identity}` | `role` | the new role (not above your own) | admin |
| `DELETE /v1/teams/{team}/members/{identity}` | — | removed | admin |
| `POST /v1/teams/{team}/invitations` | `email`, `role` | `invitation`; a mail goes out | admin (owner to invite an owner) |
| `POST /v1/invitations/accept` | `token`; without a session `token`, `name`, `password` | joined; without a session, an account for the invited address is made and joins (`sign_in_first` if it has one) | the invited account, or the invited person |
| `GET /v1/teams/{team}/tokens` | — | the team's API tokens (no secrets) | admin |
| `POST /v1/teams/{team}/tokens` | `label`, `role`, `project`, `expires_days` (optional) | the token, shown once | admin |
| `DELETE /v1/teams/{team}/tokens/{id}` | — | revoked | admin |

## The audit log

Every change made through this API in a team is recorded: by whom, what, on what. Entries are
never changed or removed while the team exists.

| Call | Body | Answers | Role |
|---|---|---|---|
| `GET /v1/teams/{team}/audit?before=<position>&limit=<n>` | — | up to `limit` entries (100 by default, 500 at most) older than `before`, newest first: `position`, `at` (ms), `actor` (`account:<id>` or `token:<id>`) and `actor_name` (an email, a token's label), `action`, `subject` and `subject_name`, `detail` | admin |

The actions: `team.created`, `team.terms`, `team.payment`, `team.two-factor`, `team.deletion`,
`team.restored`, `member.invited`, `member.joined`, `member.role`, `member.removed`,
`token.created`, `token.revoked`, `project.created`, `project.root_set`, `project.root_replacing`,
`project.root_replaced` (also by `kernable`, when the 7 days are up), `project.root_replaced_now`,
`hosted.allowed` (by `kernable`), `hosted.on`, `hosted.client_ca`, `hosted.off`,
`partition.linked`, `partition.unlinked`,
`node.token`, and `billing.status` (by `stripe`: the subscription changed there, e.g. cancelled in the portal). To page back, pass the last entry's `position` as `before`.

## Projects

kernable Cloud runs no project's code: a project is the team's record that its kernels link to.

| Call | Body | Answers | Role |
|---|---|---|---|
| `GET /v1/teams/{team}/projects` | — | the team's projects | viewer |
| `POST /v1/teams/{team}/projects` | `name`, `plan` (optional) | the project, recorded | admin |
| `GET /v1/teams/{team}/projects/{name}` | — | the project, with its `root` (`key_id`, `since`) and, during a replacement, `previous_root` (`key_id`, `until`) | viewer |
| `PUT /v1/teams/{team}/projects/{name}/root` | `key`: the content of `root.pub` (64 hex digits); `now` (optional): `true` ends the old root at once | the project. The first root is set; a later one keeps the old root valid until `finish`, 7 days at most. `409` `replacing` during a replacement, `same_root` | admin |
| `GET /v1/teams/{team}/projects/{name}/hosted` | — | `offered`, `allowed` and `used` (hosted kernels the team may run and runs), its `hosted` kernel (`name`, `since`, `client_ca`) or `null`, its `url`, and its `status` at the kernel server (`state`, `ready`, `kernel_key`, `pusher_key`, `server_ca`, `bytes`, `problem`) | viewer |
| `PUT /v1/teams/{team}/projects/{name}/hosted` | `client_ca`: one PEM certificate | turns the hosted kernel on, or gives it another client CA; as `GET`. `403` `hosted_limit`, `409` `no_root`, `400` `invalid_ca` | admin |
| `DELETE /v1/teams/{team}/projects/{name}/hosted` | — | turns it off (its data is kept 30 days); as `GET`. `409` `hosted_off` | admin |
| `POST /v1/teams/{team}/projects/{name}/root/finish` | — | the project: the old root no longer valid; `409` `not_replacing` | admin |
| `GET /v1/teams/{team}/projects/{name}/usage?month=<yyyy-mm>` | — | the month's usage of its partitions' copies (this month by default, UTC): `copies` (`copy`, `project`, `partition`, `stored_gb` now, `stored_gb_months`, `sent_gb`, `at`), `total`, `included` | viewer |
| `GET /v1/teams/{team}/projects/{name}/links` | — | `relay`, `project` and its `links`: each partition's copy and `keys` (`key`, `role` `push` or `read`, `state` `active`, `stopped` or `revoked`, `since`, `by`), and those ended within 30 days | viewer |
| `PUT /v1/teams/{team}/projects/{name}/links/{partition}` | `pusher`: its key ID (64 hex digits) | `relay` to push to and the `link`; `409` `pusher_active` while another pusher of the partition is active | developer |
| `PUT /v1/teams/{team}/projects/{name}/links/{partition}/keys/{key}` | `role` (a new key only): `push` or `read`; `state` (default `active`): `active`, `stopped` or `revoked` | `relay` and the `link`; `404` `not_linked`, `409` `pusher_active`, `409` `revoked` (for good), `400` `other_role` | developer |
| `DELETE /v1/teams/{team}/projects/{name}/links/{partition}` | — | the link, ended (`removed_at`): the copy is kept 30 days | admin |
| `POST /v1/teams/{team}/projects/{name}/nodes/tokens` | `instance` | `token`: a one-time join token for a managed node, valid 24 hours | admin |
