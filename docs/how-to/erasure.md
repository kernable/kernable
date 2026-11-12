# Handle an erasure request

A person asks you to erase their personal data, for example under the GDPR. Events cannot be deleted, so kernable keeps personal values in the **vault** and the log keeps references only ([Personal data: the vault](../concepts/data#personal-data-the-vault)). With that set up, an erasure is one command. The set-up has to be in place **before** the data arrives: a value that went into the log in clear stays there forever.

## Before the first request: declare

1. **Turn the vault on:** start the kernel with `--vault-key` (`kernable start --dev` does). Without it, commands and events with vault fields are refused.
2. **Mark every personal field**, in commands and events, with the person it belongs to (`personal(fields, subject)`; [tutorial, part 7](../tutorial/shop/7-personal-data)). Fields of other aggregates and domains count too: the shop marks the email in an order's confirmation as personal data of the customer. Only top-level fields can be marked. A worker rejects a command (`personal_in_clear`) whose handler puts a field the command marks personal, under the same name, into an event that does not: it would be stored in clear. After an erasure, and for a reader without `vault_read`, such a field is `null`: in TypeScript, type it `Personal<string>` so the compiler finds the handlers that must allow for that.
3. **Declare an erasing event**, e.g. `CustomerErased` with `erases(subject)`, and a command that writes it, e.g. `Erase` on the customer.
4. **Grant the worker** that writes it `vault_erase` for that event type:

   ```sh
   kernable grant keys/root.key keys/shop-worker.pub 'run=shop.*' vault_erase=shop.CustomerErased
   ```

   Without it, the command is rejected with `erase_not_allowed`.
5. **Make every read model that holds personal data forget the person** when it sees the erasing event. A read model's store then also removes the old values from its write-ahead log, so they are not left on the disk. Do the same for files: an erasure does not touch them ([Personal data in files](./data-and-cache#personal-data-in-files)).
6. **Let clients seal the fields:** a client that sends commands with personal fields needs the domain's declarations (`.use(shopDomain)`, Python `.use(shop_domain)`, Rust `.with_domain(&shop_domain)`). Without them, it sends the values in clear and they are stored in the command log.

The shop's read models forget an erased customer like this:

::: code-group
<<< @/examples/shop/ts/read-models.ts#erased [read-models.ts]
<<< @/examples/shop/py/read_models.py#erased [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#erased [read_models.rs]
:::

The subject is a string, such as the customer ID `c1`. An erasure deletes every value of that subject in the erasing command's tenant, in every stream and domain; the same subject in another tenant keeps its values. Use one subject per person, unique within the tenant.

## The request

1. **Authenticate** the person, as for any request.
2. **Send the erasing command:**

   ```ts
   const outcome = await kernable.submit({ domain: "shop", aggregateType: "customer", aggregateId: "c1", type: "Erase" });
   ```

   When it is committed, the kernel has deleted the person's values from the vault, in every stream and domain of its tenant, and dropped the snapshots of every stream that used them. Unique keys built from the person's data are free again ([personal keys](../concepts/uniqueness#personal-keys)). Secrets have no subject and are not affected. The kernel stores the erasing event first and erases right after, before it answers; after a crash in between, it finishes the erasure when it starts, before it serves anything. So the log records every erasure that happened, and a rejected erasing command erases nothing.
3. **Read models** forget the person as they process the event; rebuilt, they read `null` where the values were.
4. **Delete the person's files** if a reactor does not do it for you.
5. **Backups** of the vault still hold the values until they expire ([Back up and restore](./backup#backups-and-erasure)). Masked copies for previews never held them ([Previews and copies](./previews)).
6. **Confirm** to the person.

## Check that it worked

The kernel logs `erased a subject's vault values` with the command ID, and keeps a proof in the vault's `erasures` table: the SHA-256 of the subject, the erasing command, how many values went, and when. It does not keep the subject itself:

```sh
sqlite3 vault.db "SELECT subject_hash, tenant, command_id, erased, datetime(at / 1000, 'unixepoch') FROM erasures"
printf '%s' c1 | shasum -a 256   # the subject_hash of c1
sqlite3 vault.db "SELECT count(*) FROM vault WHERE subject = 'c1'"   # 0
```

Then check that nothing personal is stored in clear. Payloads in the SQLite store are JSON text, so you can search them for a test person's email; it must not appear:

```sh
sqlite3 kernable.db "SELECT count(*) FROM events WHERE payload LIKE '%ann@example.com%'"     # 0
sqlite3 kernable.db "SELECT count(*) FROM commands WHERE payload LIKE '%ann@example.com%'"   # 0
```

The vault values are encrypted, so a search of `vault.db` finds nothing even before an erasure. The kernel UI shows the references, never the values.

## What stays

The events and commands stay, with references instead of values, and the hash chain still verifies: you keep the history (an order was placed and confirmed) without the person's data. These are not erased:

- **IDs**: aggregate IDs, command IDs, and the subject that the erasing event names. Use IDs that do not identify a person by themselves, not email addresses.
- **The request context** of commands, such as `senderIdentity`, stored in clear with every command.
- **Fields not marked personal**, and **files and cache values**.
