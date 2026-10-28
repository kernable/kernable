# 7. Personal data

Events are kept forever and form a signed hash chain: you cannot change or delete one. But customers have a right to have their personal data erased, and passwords should not be stored at all. kernable keeps such values in a **vault** next to the log:

- The log stores a reference instead of the value: `{"$vault": "3fa9…"}`. The reference is salted, so it reveals nothing about the value, also once the value is gone.
- The vault keeps the values encrypted, and deletes them for real.
- **Personal** values belong to a person (a subject) and are deleted when an event erases that person.
- **Secret** values, like passwords, are deleted as soon as their command completed.

The kernel you started in part 1 already has a vault: `--dev` turned it on with `vault.key`, and it keeps the values in `vault.db`.

## Declare what is personal

In `domain.ts`, declare per command and event which fields go to the vault, and for whom; add passwords and erasure to the customer:

::: code-group
<<< @/examples/shop/ts/domain.ts#vault [domain.ts]
<<< @/examples/shop/py/domain.py#vault [domain.py]
<<< @/examples/shop/rs/src/domain.rs#vault [domain.rs]
:::

The handlers do not change: they still see `email` and `password` in clear. The SDK moves the fields to the vault before a command or event is signed, and puts them back for the processes allowed to read them.

Two actions of a grant decide who that is:

- `vault_read` lists the events and commands whose vault values a listener or middleware gets in clear. A read model that shows email addresses needs it; a guard that only compares IDs does not, and sees the references.
- `vault_erase` lists the erasing events a worker may write. Erasing is final, so grant it only for the events meant for it: `vault_erase=shop.CustomerErased`.

`app`, which `--dev` lets do everything, has both. Part 8 grants them per service.

Read models hold values too, so they must forget an erased customer. In `read-models.ts`:

::: code-group
<<< @/examples/shop/ts/read-models.ts#erased [read-models.ts]
<<< @/examples/shop/py/read_models.py#erased [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#erased [read_models.rs]
:::

Clients that send commands with personal or secret fields need the declarations: create them with `.use(shopDomain)`, as the reactor in part 4 already does.

## Try it

Restart every process, then:

::: code-group
<<< @/examples/shop/ts/part7.ts#try [part7.ts]
<<< @/examples/shop/py/part7.py#try [part7.py]
<<< @/examples/shop/rs/src/bin/part7.rs#try [part7.rs]
:::

::: code-group
<<< @/examples/shop/ts/part7.out{text} [TypeScript]
<<< @/examples/shop/py/part7.out{text} [Python]
<<< @/examples/shop/rs/part7.out{text} [Rust]
:::

After `Erase`, the customer's email and name are gone from the vault, in the customer's stream and in every order's confirmation alike. A read model rebuilt from the log reads `null` where they were. The events themselves stay, and the hash chain still verifies.

Next: [8. Operate](./8-operate).
