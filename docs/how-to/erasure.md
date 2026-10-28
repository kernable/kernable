# Handle an erasure request

A person asks you to erase their personal data. With the vault, that is one command.

## Before the request: declare

1. Mark every personal field, in commands and events, with the person it belongs to ([tutorial, part 7](../tutorial/shop/7-personal-data)). Fields of other domains count too: the shop marks the email in an order's confirmation as personal data of the customer.
2. Declare an erasing event, e.g. `CustomerErased`, with `erases(...)`.
3. Grant the worker that writes it `vault_erase` for that event type (`kernable grant root.key worker.pub vault_erase=shop.CustomerErased …`).
4. Make every read model that holds personal data forget the person when it sees the erasing event.

## The request

1. **Authenticate** the person, as for any request.
2. **Send the erasing command** (`Erase`). When it is committed, the kernel has deleted the person's values from the vault, in every stream and domain, and dropped the snapshots that might hold them.
3. **Read models** forget the person as they process the event; rebuilt, they read `null`.
4. **Backups** of the vault still hold the values until they expire ([Back up and restore](./backup)).
5. **Confirm** to the person.

## What stays

The events stay, with references instead of values, and the hash chain still verifies: you keep the history (an order was placed and confirmed) without the person's data. IDs such as `c1` are not erased: use IDs that do not identify a person by themselves.

Check that nothing personal is stored in clear: search the store files for a test person's email; it must not appear.
