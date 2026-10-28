# Store files and cache values

Besides commands and events, the kernel keeps two things for your processes: **files** of a domain (an invoice PDF, an upload) in its data store, and **cache values** that may expire (a rate limit, a computed answer) in its cache. Both are kept under tenant, domain and key, and every call is signed and checked against the key's grant, like a query.

Neither is part of the signed event log: they are not hash-chained, not verified, and not erased with the vault. If a file holds personal data, delete it yourself when you erase the person, for example in the listener that handles the erasing event.

## Grant access

| Action | Allows |
|---|---|
| `data_read` | getting the domain's files |
| `data_write` | putting and deleting them |
| `cache` | setting, getting and deleting cache values |

Grant them per domain: `kernable grant root.key api.pub 'data_write=shop.*' 'data_read=shop.*' 'cache=shop.*'`. `kernable start --dev` lets `app` do everything.

## Files

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

A key is any string of up to 512 bytes; a file has at most 16 MiB. Putting a file replaces the one under the same key. Text is stored as UTF-8.

## Cache values

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

A value has at most 1 MiB and lives until it expires, is deleted, or the kernel restarts: keep in the cache only what you can compute again.

## Where they live

The data store is set with `--data-store`: `file:data` (the default, a directory next to the kernel's working directory) or `memory:`. Back up the directory with the event log. The cache store, `--cache-store`, is `memory:`. See [kernable start](../reference/configuration).
