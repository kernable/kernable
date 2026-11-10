# Store files and cache values

Use this page when a process needs to keep something that is not an event: a **file** of a domain (an invoice PDF, an upload) or a **cache value** that may expire (a rate limit, a computed answer). The kernel keeps both for your processes, so you need no separate file server or cache. Both are kept under tenant, domain and key, and every call is signed and checked against the key's grant, like a query.

Neither is part of the signed event log: they are not hash-chained, not verified by `kernable verify`, and **not erased with the vault**. If a file holds personal data, delete it yourself when you erase the person ([below](#personal-data-in-files)).

| | Files | Cache |
|---|---|---|
| Kept | until deleted | until they expire, are deleted, or the kernel restarts |
| Largest value | 16 MiB (`--max-data-bytes`, up to 128 MiB) | 1 MiB |
| Key | 1 to 512 bytes | 1 to 512 bytes |
| Grant | `data_read`, `data_write` | `cache` |
| Flag | `--data-store` (default `file:data`) | `--cache-store` (only `memory:`) |

## 1. Grant access

| Action | Allows |
|---|---|
| `data_read` | getting and listing the domain's files |
| `data_write` | putting and deleting them |
| `cache` | setting, getting and deleting cache values |

Grant them per domain:

```sh
kernable grant keys/root.key keys/api.pub 'data_write=shop.*' 'data_read=shop.*' 'cache=shop.*'
```

`kernable start --dev` lets `app` do everything. A key without the grant gets `PERMISSION_DENIED` (`key may not read files in shop`). More on grants: [Keys per service](./keys).

Grants decide per key and domain; **who** may read which file, and in which tenant, a middleware decides. Every file and cache request carries the client's request context (its default one, or one per call) and passes the middleware chain under the types `shop.PutData`, `shop.GetData`, `shop.ListData`, `shop.DeleteData`, `shop.SetCache`, `shop.GetCache` and `shop.DeleteCache`. A middleware covering `shop.*` checks them like commands; a refusal is `PERMISSION_DENIED` with its code. Use it, for example, to keep tenants apart: refuse when `senderTenant` is not the request's tenant ([Queries and middleware](../concepts/queries-and-middleware#middleware)).

## 2. Put and get files

Any client can use them: `kernable.data` on a `Kernable` (`new Kernable()`, Python `Kernable()`, Rust `Kernable::new(…)`).

::: code-group
```ts [TypeScript]
const sha256 = await kernable.data.put("shop", "invoices/o-1.pdf", pdf, { contentType: "application/pdf" });
const file = await kernable.data.get("shop", "invoices/o-1.pdf"); // { data, contentType, sha256 } or undefined
const { files, next } = await kernable.data.list("shop", "invoices/"); // [{ key, contentType, size }]
await kernable.data.delete("shop", "invoices/o-1.pdf");           // whether it existed
```
```python [Python]
sha256 = k.data.put("shop", "invoices/o-1.pdf", pdf, "application/pdf")
file = k.data.get("shop", "invoices/o-1.pdf")  # Data(data, content_type, sha256) or None
page = k.data.list("shop", "invoices/")        # Listing(files=[Listed(key, content_type, size)], next)
k.data.delete("shop", "invoices/o-1.pdf")      # whether it existed
```
```rust [Rust]
let sha256 = kernable.data().put("shop", "invoices/o-1.pdf", pdf, "application/pdf").await?;
let file = kernable.data().get("shop", "invoices/o-1.pdf").await?; // Option<Data>
let page = kernable.data().list("shop", "invoices/", "").await?;   // Listing { files, next }
kernable.data().delete("shop", "invoices/o-1.pdf").await?;         // whether it existed
```
:::

- Putting a file replaces the one under the same key. A reader gets the old file or the new one, never half of it.
- Text is stored as UTF-8. Without a content type, a string is stored as `text/plain; charset=utf-8`, bytes as `application/octet-stream`.
- The tenant is the client's; TypeScript (`{ tenant }`) and Python (`tenant=`) can name another per call.
- A put and a get answer the SHA-256 of the file's data (hex), as the kernel computed it. Name it in the event that records the file (`InvoiceStored { key, sha256 }`): the signed log then says which bytes belong to it, and a reader can check them.
- A listing gives the files whose keys start with a prefix (empty for all of the domain), with their content type and size, in the order of their keys' bytes, up to 1,000 per page; pass `next` as `after` for the next page. Use it to find files no event names (a crash between a put and the command that records it leaves one), or to see what a tenant stores. Keys you can compute again, such as `invoices/<order ID>.pdf`, still need no listing to be found.
- Each file travels whole in one message: the kernel and the SDK hold it in memory while it is put or read. Raise `--max-data-bytes` (up to 128 MiB) for larger files; for anything bigger, keep the bytes in object storage and their address and hash in an event.
- A put or delete runs at most once per signed request. One that failed may still have taken effect, so the same request is refused as a replay (`ALREADY_EXISTS`); send it again as a new call, which the SDKs sign anew.

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
| `--data-store` | `file:<directory>` (default `file:data`), `memory:` | The directory is created on the first file. Each file is `<directory>/<tenant>/<domain>/<key>`, every part hex-encoded; a part over 123 bytes, whose hex would not fit in a file name, is `h-` and the SHA-256 of it instead. Each file starts with its tenant, domain, key and content type, so a listing and a standby know every name. `memory:` loses the files on a restart. |
| `--cache-store` | `memory:` | The only backend. |

A listing reads the domain's directory, so it takes longer the more files a tenant keeps in a domain.

Back up the data directory together with the event store ([Back up and restore](./backup)). All flags: [Configuration](../reference/configuration).

## Watching them

The UI's **Explore → Data** shows the files per tenant and domain (how many, their size, and a domain as folders by `/` in its keys) and the cache (entries, memory against `--max-cache-bytes`, the hit rate, values that expired or were evicted, and the keys with how long until they expire). It lists names and sizes only: it never opens a file or shows a cache value ([Monitor kernable](./monitor#the-ui)).
