# Store files and cache values

Use this page when a process needs to keep something that is not an event: a **file** of a domain (an invoice PDF, an upload) or a **cache value** that may expire (a rate limit, a computed answer). The kernel keeps both for your processes, so you need no separate file server or cache. Both are kept under tenant, domain and key, and every call is signed and checked against the key's grant, like a query.

Neither is part of the signed event log: they are not hash-chained, not verified by `kernable verify`, and **not erased with the vault**. If a file holds personal data, delete it yourself when you erase the person ([below](#personal-data-in-files)).

| | Files | Cache |
|---|---|---|
| Kept | until deleted | until they expire, are deleted, or the kernel restarts |
| Largest value | 16 MiB | 1 MiB |
| Key | 1 to 512 bytes | 1 to 512 bytes |
| Grant | `data_read`, `data_write` | `cache` |
| Flag | `--data-store` (default `file:data`) | `--cache-store` (only `memory:`) |

## 1. Grant access

| Action | Allows |
|---|---|
| `data_read` | getting the domain's files |
| `data_write` | putting and deleting them |
| `cache` | setting, getting and deleting cache values |

Grant them per domain:

```sh
kernable grant keys/root.key keys/api.pub 'data_write=shop.*' 'data_read=shop.*' 'cache=shop.*'
```

`kernable start --dev` lets `app` do everything. A key without the grant gets `PERMISSION_DENIED` (`key may not read files in shop`). More on grants: [Keys per service](./keys).

## 2. Put and get files

Any client can use them: `kernable.data` on a `Kernable` (`new Kernable()`, Python `Kernable()`, Rust `Kernable::new(…)`).

::: code-group
```ts [TypeScript]
await kernable.data.put("shop", "invoices/o-1.pdf", pdf, { contentType: "application/pdf" });
const file = await kernable.data.get("shop", "invoices/o-1.pdf"); // { data, contentType } or undefined
await kernable.data.delete("shop", "invoices/o-1.pdf");           // whether it existed
```
```python [Python]
k.data.put("shop", "invoices/o-1.pdf", pdf, "application/pdf")
file = k.data.get("shop", "invoices/o-1.pdf")  # Data(data, content_type) or None
k.data.delete("shop", "invoices/o-1.pdf")      # whether it existed
```
```rust [Rust]
kernable.data().put("shop", "invoices/o-1.pdf", pdf, "application/pdf").await?;
let file = kernable.data().get("shop", "invoices/o-1.pdf").await?; // Option<Data>
kernable.data().delete("shop", "invoices/o-1.pdf").await?;         // whether it existed
```
:::

- Putting a file replaces the one under the same key. A reader gets the old file or the new one, never half of it.
- Text is stored as UTF-8. Without a content type, a string is stored as `text/plain; charset=utf-8`, bytes as `application/octet-stream`.
- The tenant is the client's; TypeScript (`{ tenant }`) and Python (`tenant=`) can name another per call.
- There is no listing: choose keys you can compute again, such as `invoices/<order ID>.pdf`.

## 3. Set and get cache values

::: code-group
```ts [TypeScript]
await kernable.cache.set("shop", "quote/c-1", JSON.stringify(quote), { ttlMs: 60_000 });
const cached = await kernable.cache.get("shop", "quote/c-1"); // bytes or undefined
```
```python [Python]
k.cache.set("shop", "quote/c-1", json.dumps(quote), ttl_ms=60_000)
cached = k.cache.get("shop", "quote/c-1")  # bytes or None
```
```rust [Rust]
kernable.cache().set("shop", "quote/c-1", serde_json::to_vec(&quote)?, Some(Duration::from_secs(60))).await?;
let cached = kernable.cache().get("shop", "quote/c-1").await?; // Option<Vec<u8>>
```
:::

Without a TTL (`0`, `None`), a value lives until it is deleted or the kernel restarts. A restart always empties the cache: keep in it only what you can compute again. Every process connected to the kernel sees the same values.

## Personal data in files

An erasure deletes vault values only. To delete a person's files with them, handle the erasing event in a reactor whose key may `listen` to it and has `data_write`, and delete the files under keys you can derive from the person:

```ts
const erasedFiles = shopDomain.reactor("erased-files")
  .on("CustomerErased", async (_data, evt) => {
    await kernable.data.delete("shop", `customers/${evt.aggregateId}/avatar.png`);
  });
await kernable.listener(erasedFiles).run();
```

Deleting a missing file is no error, so a redelivered event does no harm.

Keep personal data out of cache values, or give them a short TTL. See [Handle an erasure request](./erasure).

## Where they live

| Flag | Values | Notes |
|---|---|---|
| `--data-store` | `file:<directory>` (default `file:data`), `memory:` | The directory is created on the first file. Each file is `<directory>/<tenant>/<domain>/<key>`, every part hex-encoded. `memory:` loses the files on a restart. |
| `--cache-store` | `memory:` | The only backend. |

With `file:`, a key becomes a file name of twice its length in bytes. Most file systems allow 255 bytes per name, so keep keys under 128 bytes there.

Back up the data directory together with the event store ([Back up and restore](./backup)). All flags: [Configuration](../reference/configuration).
