//! Files and cache values: bytes stored by the kernel under a key of a domain, outside the
//! signed event log. Each request is signed with this process's key and needs a grant for the
//! domain: `data_read` to read files, `data_write` to put and delete them, `cache` for the cache.

use std::time::Duration;

use kernable_core::{
    PROTOCOL,
    proto::{
        CacheRequest, DataRequest, cache_service_client::CacheServiceClient,
        data_service_client::DataServiceClient,
    },
    signing,
};

use crate::{Client, Error, now_ms};

/// Room for the kernel's largest file (16 MiB) and the rest of the message.
const MAX_MESSAGE_BYTES: usize = 17 << 20;

/// A file as stored by the kernel, returned by [`Files::get`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Data {
    /// The file's bytes.
    pub data: Vec<u8>,
    /// The media type given when the file was put, e.g. `image/png`.
    pub content_type: String,
}

/// Files of up to 16 MiB under keys of 1 to 512 bytes, per tenant and domain; see
/// [`crate::Kernable::data`]. Files are not part of the signed event log, and erasing a vault
/// subject does not delete them, so keep personal data in vault fields instead. A refused or
/// invalid request is [`Error::Status`]. Example:
/// `kernable.data().put("shop", "invoices/c1.pdf", pdf, "application/pdf").await?`, then
/// `kernable.data().get("shop", "invoices/c1.pdf").await?` returns `Some(Data { .. })`.
#[derive(Clone)]
pub struct Files {
    client: Client,
    tenant: String,
}

impl Files {
    pub(crate) fn new(client: Client, tenant: String) -> Files {
        Files { client, tenant }
    }

    /// Stores `data` as `key` of `domain` (granted `data_write`), replacing any file there.
    pub async fn put(
        &self,
        domain: &str,
        key: &str,
        data: impl Into<Vec<u8>>,
        content_type: &str,
    ) -> Result<(), Error> {
        let request = self.request("put", domain, key, data.into(), content_type);
        self.service().put_data(request).await?;
        Ok(())
    }

    /// The file `key` of `domain` (granted `data_read`); `None` if there is none.
    pub async fn get(&self, domain: &str, key: &str) -> Result<Option<Data>, Error> {
        let request = self.request("get", domain, key, Vec::new(), "");
        let res = self.service().get_data(request).await?.into_inner();
        Ok(res.found.then_some(Data {
            data: res.data,
            content_type: res.content_type,
        }))
    }

    /// Deletes the file `key` of `domain` (granted `data_write`); whether there was one.
    pub async fn delete(&self, domain: &str, key: &str) -> Result<bool, Error> {
        let request = self.request("delete", domain, key, Vec::new(), "");
        Ok(self
            .service()
            .delete_data(request)
            .await?
            .into_inner()
            .found)
    }

    fn service(&self) -> DataServiceClient<tonic::transport::Channel> {
        DataServiceClient::new(self.client.channel.clone())
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
    ) -> tonic::Request<DataRequest> {
        let mut r = DataRequest {
            tenant_id: self.tenant.clone(),
            domain: domain.to_owned(),
            key: key.to_owned(),
            data,
            content_type: content_type.to_owned(),
            issued_at: now_ms(),
            client_sig: None,
            protocol: PROTOCOL,
        };
        r.client_sig = Some(self.client.sign(&signing::data_message(op, &r)));
        self.client.request(r)
    }
}

/// Values of up to 1 MiB under keys of 1 to 512 bytes, per tenant and domain, held in the
/// kernel's memory (granted `cache`); see [`crate::Kernable::cache`]. A restart of the kernel
/// empties the cache; values are not part of the signed event log, and erasing a vault subject
/// does not delete them. For rate limits, short-lived tokens and computed values. A refused
/// or invalid request is [`Error::Status`]. Example:
/// `kernable.cache().set("shop", "rate:c1", "3", Some(Duration::from_secs(60))).await?`, then
/// `kernable.cache().get("shop", "rate:c1").await?` returns `Some(b"3".to_vec())`.
#[derive(Clone)]
pub struct Cache {
    client: Client,
    tenant: String,
}

impl Cache {
    pub(crate) fn new(client: Client, tenant: String) -> Cache {
        Cache { client, tenant }
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
        self.service().set_cache(request).await?;
        Ok(())
    }

    /// The value `key` of `domain`; `None` if there is none or it expired.
    pub async fn get(&self, domain: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let request = self.request("get", domain, key, Vec::new(), 0);
        let res = self.service().get_cache(request).await?.into_inner();
        Ok(res.found.then_some(res.value))
    }

    /// Deletes the value `key` of `domain`; whether there was one.
    pub async fn delete(&self, domain: &str, key: &str) -> Result<bool, Error> {
        let request = self.request("delete", domain, key, Vec::new(), 0);
        Ok(self
            .service()
            .delete_cache(request)
            .await?
            .into_inner()
            .found)
    }

    fn service(&self) -> CacheServiceClient<tonic::transport::Channel> {
        CacheServiceClient::new(self.client.channel.clone())
    }

    fn request(
        &self,
        op: &str,
        domain: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> tonic::Request<CacheRequest> {
        let mut r = CacheRequest {
            tenant_id: self.tenant.clone(),
            domain: domain.to_owned(),
            key: key.to_owned(),
            value,
            ttl_ms,
            issued_at: now_ms(),
            client_sig: None,
            protocol: PROTOCOL,
        };
        r.client_sig = Some(self.client.sign(&signing::cache_message(op, &r)));
        self.client.request(r)
    }
}
