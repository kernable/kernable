//! Files and cache values: bytes stored by the kernel under a key of a domain, outside the
//! signed event log. Each request is signed with this process's key and needs a grant for the
//! domain: `data_read` to read files, `data_write` to put and delete them, `cache` for the cache.
//! Each request carries a signed request context (the client's default, see
//! [`crate::Options::context`], or one set with [`Files::context`] or [`Cache::context`]), and
//! the kernel runs the domain's middlewares before it (types `<domain>.PutData`, `.GetData`,
//! `.DeleteData`, `.ListData`, `.SetCache`, `.GetCache`, `.DeleteCache`).

use std::time::Duration;

use kernable_core::{
    PROTOCOL,
    proto::{
        CacheRequest, DataRequest, cache_service_client::CacheServiceClient,
        data_service_client::DataServiceClient,
    },
    signing,
};

use crate::{Client, Error, RequestContext, issued_now};

/// Room for the largest file a kernel takes (`--max-data-bytes`, at most 128 MiB) and the rest
/// of the message.
const MAX_MESSAGE_BYTES: usize = 129 << 20;

/// A file as stored by the kernel, returned by [`Files::get`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Data {
    /// The file's bytes.
    pub data: Vec<u8>,
    /// The media type given when the file was put, e.g. `image/png`.
    pub content_type: String,
    /// The SHA-256 of `data`, hex, as the kernel computed it.
    pub sha256: String,
}

/// A file as [`Files::list`] names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub content_type: String,
    /// The file's length in bytes.
    pub size: u64,
}

/// One page of [`Files::list`]: up to 1,000 files, and `next` (to pass as `after`) if there
/// may be more.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    pub files: Vec<Listed>,
    pub next: Option<String>,
}

/// Files of up to 16 MiB (the kernel's `--max-data-bytes`) under keys of 1 to 512 bytes, per tenant and domain; see
/// [`crate::Kernable::data`]. Files are not part of the signed event log, and erasing a vault
/// subject does not delete them, so keep personal data in vault fields instead. A refused or
/// invalid request is [`Error::Status`]; a middleware that refuses it (checking `PutData`,
/// `GetData`, `DeleteData` or `ListData` of the domain) makes it `PERMISSION_DENIED` with
/// `"<code>: <message>"`. Example:
/// `kernable.data().put("shop", "invoices/c1.pdf", pdf, "application/pdf").await?`, then
/// `kernable.data().get("shop", "invoices/c1.pdf").await?` returns `Some(Data { .. })`.
#[derive(Clone)]
pub struct Files {
    client: Client,
    tenant: String,
    context: RequestContext,
}

impl Files {
    pub(crate) fn new(client: Client, tenant: String, context: RequestContext) -> Files {
        Files {
            client,
            tenant,
            context,
        }
    }

    /// The tenant to keep files under. Default: the connection's ([`crate::Options::tenant`]).
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<String>) -> Files {
        self.tenant = tenant.into();
        self
    }

    /// Replaces the client's default context for the requests of this handle (signed with
    /// each; middlewares decide on it); inside a listener, pass `ctx.follow()`. Example:
    /// `kernable.data().context(alice).get("shop", "k").await?`.
    #[must_use]
    pub fn context(mut self, context: RequestContext) -> Files {
        self.context = context;
        self
    }

    /// Stores `data` as `key` of `domain` (granted `data_write`), replacing any file there.
    /// Returns the SHA-256 of `data` (hex) as the kernel computed it: for an event to name.
    pub async fn put(
        &self,
        domain: &str,
        key: &str,
        data: impl Into<Vec<u8>>,
        content_type: &str,
    ) -> Result<String, Error> {
        let request = self.request("put", domain, key, data.into(), content_type, "");
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call(None, request, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.put_data(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(hex::encode(res.sha256))
    }

    /// The file `key` of `domain` (granted `data_read`); `None` if there is none.
    pub async fn get(&self, domain: &str, key: &str) -> Result<Option<Data>, Error> {
        let request = self.request("get", domain, key, Vec::new(), "", "");
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call_with(None, request, true, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.get_data(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(res.found.then_some(Data {
            data: res.data,
            content_type: res.content_type,
            sha256: hex::encode(res.sha256),
        }))
    }

    /// The files of `domain` whose keys start with `prefix` (granted `data_read`), in the order
    /// of their keys' bytes, up to 1,000 after the key `after` (`""` for the first page). To
    /// find files no event names, or what a tenant stores. Example: `let mut after =
    /// String::new(); loop { let page = files.list("shop", "invoices/", &after).await?; …;
    /// match page.next { Some(n) => after = n, None => break } }`.
    pub async fn list(&self, domain: &str, prefix: &str, after: &str) -> Result<Listing, Error> {
        let request = self.request("list", domain, prefix, Vec::new(), "", after);
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call_with(None, request, true, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.list_data(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(Listing {
            files: res
                .files
                .into_iter()
                .map(|f| Listed {
                    key: f.key,
                    content_type: f.content_type,
                    size: f.size,
                })
                .collect(),
            next: (!res.next.is_empty()).then_some(res.next),
        })
    }

    /// Deletes the file `key` of `domain` (granted `data_write`); whether there was one.
    pub async fn delete(&self, domain: &str, key: &str) -> Result<bool, Error> {
        let request = self.request("delete", domain, key, Vec::new(), "", "");
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call(None, request, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.delete_data(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(res.found)
    }

    fn service(channel: tonic::transport::Channel) -> DataServiceClient<tonic::transport::Channel> {
        DataServiceClient::new(channel)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
    }

    fn request(
        &self,
        op: &str,
        domain: &str,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
        after: &str,
    ) -> DataRequest {
        let mut r = DataRequest {
            tenant_id: self.tenant.clone(),
            domain: domain.to_owned(),
            key: key.to_owned(),
            data,
            content_type: content_type.to_owned(),
            issued_at: issued_now(),
            client_sig: None,
            protocol: PROTOCOL,
            context: Some(self.context.to_wire()),
            after: after.to_owned(),
        };
        r.client_sig = Some(self.client.sign(&signing::data_message(op, &r)));
        r
    }
}

/// Values of up to 1 MiB under keys of 1 to 512 bytes, per tenant and domain, held in the
/// kernel's memory (granted `cache`); see [`crate::Kernable::cache`]. A restart of the kernel
/// empties the cache; values are not part of the signed event log, and erasing a vault subject
/// does not delete them. For rate limits, short-lived tokens and computed values. A refused
/// or invalid request is [`Error::Status`]; a middleware that refuses it (checking `SetCache`,
/// `GetCache` or `DeleteCache` of the domain) makes it `PERMISSION_DENIED` with
/// `"<code>: <message>"`. Example:
/// `kernable.cache().set("shop", "rate:c1", "3", Some(Duration::from_secs(60))).await?`, then
/// `kernable.cache().get("shop", "rate:c1").await?` returns `Some(b"3".to_vec())`.
#[derive(Clone)]
pub struct Cache {
    client: Client,
    tenant: String,
    context: RequestContext,
}

impl Cache {
    pub(crate) fn new(client: Client, tenant: String, context: RequestContext) -> Cache {
        Cache {
            client,
            tenant,
            context,
        }
    }

    /// The tenant to keep values under. Default: the connection's ([`crate::Options::tenant`]).
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<String>) -> Cache {
        self.tenant = tenant.into();
        self
    }

    /// Replaces the client's default context for the requests of this handle (signed with
    /// each; middlewares decide on it); inside a listener, pass `ctx.follow()`. Example:
    /// `kernable.cache().context(alice).get("shop", "k").await?`.
    #[must_use]
    pub fn context(mut self, context: RequestContext) -> Cache {
        self.context = context;
        self
    }

    /// Stores `value` as `key` of `domain` (granted `cache`), replacing any value there. With a
    /// `ttl`, the value expires after it; without, it stays until deleted or the kernel
    /// restarts. A `&str` or `String` is stored as UTF-8.
    pub async fn set(
        &self,
        domain: &str,
        key: &str,
        value: impl Into<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> Result<(), Error> {
        // A TTL under a millisecond would mean none on the wire.
        let ttl_ms = ttl.map_or(0, |t| (t.as_millis() as u64).max(1));
        let request = self.request("set", domain, key, value.into(), ttl_ms);
        self.client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call(None, request, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.set_cache(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(())
    }

    /// The value `key` of `domain`; `None` if there is none or it expired.
    pub async fn get(&self, domain: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let request = self.request("get", domain, key, Vec::new(), 0);
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call_with(None, request, true, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.get_cache(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(res.found.then_some(res.value))
    }

    /// Deletes the value `key` of `domain`; whether there was one.
    pub async fn delete(&self, domain: &str, key: &str) -> Result<bool, Error> {
        let request = self.request("delete", domain, key, Vec::new(), 0);
        let res = self
            .client
            .routed(&request.tenant_id.clone(), |client| {
                let request = request.clone();
                async move {
                    client
                        .call(None, request, |channel, r| {
                            let mut service = Self::service(channel);
                            async move { service.delete_cache(r).await }
                        })
                        .await
                }
            })
            .await?;
        Ok(res.found)
    }

    fn service(
        channel: tonic::transport::Channel,
    ) -> CacheServiceClient<tonic::transport::Channel> {
        CacheServiceClient::new(channel)
    }

    fn request(
        &self,
        op: &str,
        domain: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> CacheRequest {
        let mut r = CacheRequest {
            tenant_id: self.tenant.clone(),
            domain: domain.to_owned(),
            key: key.to_owned(),
            value,
            ttl_ms,
            issued_at: issued_now(),
            client_sig: None,
            protocol: PROTOCOL,
            context: Some(self.context.to_wire()),
        };
        r.client_sig = Some(self.client.sign(&signing::cache_message(op, &r)));
        r
    }
}
