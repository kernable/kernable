# The kernable kit (Go)

Tenancy, accounts, identities, groups, invitations and authorization for an application on
kernable, as source code in your project. It is yours: change fields, rules and flows, or delete
what you do not need. The kit is not a dependency you update; a newer one is copied anew, and you
take over what you want.

```sh
go run github.com/kernable/kernable/sdks/go/cmd/kernable-sdk@latest add kit [dir]   # default dir: kit
go mod tidy
go test ./kit/...                   # the contract: every case passes in the kit as shipped
```

Run it inside your module: the copy is a package of it, named after the directory (`kit`,
`authkit`), at the SDK version you ran. Pin the SDK in your `go.mod` to the same version.

## What is in it

| Files | What they do |
|---|---|
| `tenancy*.go` | a Tenant per tenant: pending → active ⇄ suspended → deleted, a slug unique across tenants, its owners, MFA and sign-up policies |
| `account*.go` | a person's Account (registration, email verification, password, TOTP and recovery codes, passkeys, OAuth/OIDC links), a Session per sign-in, a tenant's single sign-on (`Sso`) |
| `identity*.go` | a person or service in one tenant, its profile, group memberships (scoped, expiring) and API keys |
| `group.go` | named sets of permissions per tenant; `owners` is virtual |
| `invitation*.go` | invitations by email, accepted by a signed-in account or a new person, and invitation links |
| `authorization_*.go` | the middleware that decides every request, and `Relation`, `Visibility`, `Explain` |
| `api*.go` | `KitAPI`: what only your API can do (sign-in, tokens, OIDC, passkey challenges, API keys) |
| `bootstrap.go` | `Bootstrap` and `KitDomains` |
| `example/` | an HTTP API over `KitAPI` on `net/http` |

Every domain has the same files: `<domain>.go` (its types, and the function that registers the
parts), `_events.go`, `_aggregates.go`, `_commands.go`, `_readmodels.go`, `_queries.go`, and
`_reactors.go` where it has reactors. The constants you are most likely to change are in
`constants.go`.

## Running it

```go
// Your relations first: who stands in which relation to your aggregates, from your events.
kit.Relation("shop.order", "owner").Set("shop.OrderPlaced", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["customer"]) })
kit.UseAuthorizationStore("sqlite:authorization.db")
// Passkeys are bound to your site: its domain, a name, the origins your pages are served from.
kit.UsePasskeys(kit.RelyingParty{RPID: "example.com", RPName: "Example", Origins: []string{"https://example.com"}})
client, _ := kernable.New(kernable.Options{Key: "kit.key"})
app, _ := client.App(kernable.AppOptions{Service: "kit", Domains: append(kit.KitDomains(), shop.Domain)})
log.Fatal(app.Run(ctx))
```

Once, with a key granted `privileged` for `bypass_authorization`, `kit.Bootstrap` creates the
platform tenant, the sign-up policy, the API's service identity and its key, and optionally the
first platform administrator. `example/server.go` shows an HTTP API over `KitAPI` on `net/http`;
`example.Run` starts the kit, the bootstrap and that server at once.

## Keys and grants

| Process | Needs |
|---|---|
| the kit's service | `run` on the kit's domains, `listen`, `answer`, `check` on `*`, `vault_read`, `vault_erase`, `reserved_order` (`*`: its listeners run before yours), `privileged` for `bypass_authorization` (its reactors), `process` on `*` (it gives processes their identities and keys) |
| your API | `submit` and `query` on what it sends, `cache` on `account` (passkey challenges, OIDC state) |

## Changing it

The events are the contract other processes read (`_events.go` in each domain), the same as the
other languages' kits: fields are camelCase in the log. `fixtures/` holds the cases every
language's kit passes. Change a rule and its fixture together, and the tests tell you what else
moved. Every domain, flow and setting is documented at https://kernable.com/docs/kits/, with a
reference of its commands, events and queries; the design with every decision is
`design/kits.md` in the kernable repository.
