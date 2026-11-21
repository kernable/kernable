# Passwords and other secrets

Use this page when a command carries a value that must never be stored: a password, a one-time code, a token. Mark the field **secret**: the handler sees it in clear, the log never does, and the kernel deletes it as soon as the command is done.

## Set it up

1. **Turn the vault on:** start the kernel with `--vault-key` (`kernable start --dev` does). Secrets are kept in the vault while their command runs; without a vault, the kernel refuses commands with secret fields (`FAILED_PRECONDITION`).
2. **Mark the field secret** on the command, as the shop does for `SetPassword` and `SignIn` (highlighted below). Only top-level fields of commands can be secret.
3. **Give the client the domain**, so it seals the field before signing: `new Kernable().use(shopDomain)`, Python `Kernable().use(shop_domain)`, Rust `.with_domain(&shop_domain)`. A client without it sends the password in clear, and it is stored in the command log for good. `kernable submit` cannot seal fields: never use it for such commands.
4. **Store a hash, not the value**, in the handler (see the rules below). The example stores an scrypt hash in `PasswordSet`, marked personal, so it is erased with the customer.

::: code-group
<<< @/examples/shop/ts/domain.ts#vault{7-8} [domain.ts]
<<< @/examples/shop/py/domain.py#vault{7-8} [domain.py]
<<< @/examples/shop/rs/src/domain.rs#vault{29-30} [domain.rs]
<<< @/examples/shop/go/domain.go#vault [domain.go]
:::

## What happens to the value

- The client moves the field to the vault before signing the command; the stored command keeps a reference, `{"$vault": "…"}`. The reference is part of the command's signature: nobody can swap the value without breaking it.
- The worker gets the value in clear for as long as the command runs, also when it is delivered again after a crash. So do middlewares granted `vault_read` for the command.
- The vault deletes it as soon as the command completed, committed or rejected. A kernel that restarts deletes those of commands that completed meanwhile. A command that waits in the queue keeps its secret until it completes.
- A middleware that rejects the command before it is queued leaves nothing behind: the value never reaches the vault.

Check it in the stores: the command holds the reference only, and no secret is left once no command runs.

```sh
sqlite3 kernable.db "SELECT payload FROM commands WHERE command_type = 'SetPassword'"   # {"password":{"$vault":"…"}}
sqlite3 vault.db "SELECT count(*) FROM vault WHERE secret = 1"                         # 0
```

## Rules for handlers

- **Never put a secret into an event**, a log line or an error message. Store a hash, salted and slow (scrypt, Argon2), and mark it personal so it is erased with the person ([Handle an erasure request](./erasure)).
- **Derive the salt with `ctx.id()`.** Handlers must decide the same way when a command runs again; a random salt would differ.
- **Compare in a command**, like `SignIn`: the stored hash stays in the aggregate, and the answer is the outcome (committed, or rejected with `denied`). Use the same message for an unknown person and a wrong password.
- **One-time codes:** store a hash with an expiry in an event, and send the code itself as a secret in the command that uses it.

How the vault works: [Personal data: the vault](../concepts/data#personal-data-the-vault).
