# Back up and restore

## What to back up

| What | Why | If lost |
|---|---|---|
| the event store (`store`) | all commands and events | everything is lost |
| the vault (`[vault] store`) | personal and secret values | personal data reads as `null` everywhere |
| the vault key (`[vault] key`) | decrypts the vault | the vault cannot be read: same as losing it |
| the kernel key (`kernel_key`) | signs checkpoints and revocations | change it ([Keys](./keys)); keep every old public key for `kernable verify` |
| process keys | sign requests | create new ones and change the allowlists |
| the snapshot store | a cache | nothing: workers rebuild snapshots |
| read model stores | caches of your views | nothing: read models rebuild from the events |

Back up the store and the vault **together**, from the same moment: stop the kernel, or copy both SQLite files with SQLite's online backup (`sqlite3 shop.db ".backup shop.bak"`) right after each other. A file log can be copied while the kernel runs; the copy may end in a torn record, which is cut off when it opens.

Keep keys apart from the data, as you would keep any secret.

## Restore

1. Stop the kernel.
2. Put the store, the vault and the keys back.
3. Check the store: `kernable verify <store> <kernel.pub>...`, with every key the kernel ever had, must say `OK`.
4. Start the kernel. Listeners with stores that are ahead of the restored log should be rebuilt ([Rebuild a read model](./rebuild-read-model)).

## Backups and erasure

A backup keeps personal data that was erased after it was taken. Keep vault backups only as long as your retention rules allow. If you restore a vault older than an erasure, erase the person again: send the erasing command once more.
