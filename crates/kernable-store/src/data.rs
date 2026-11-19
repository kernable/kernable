//! The data store: files of a domain, under (tenant, domain, key), apart from the event log.
//! They are not part of the hash chain and not erased with the vault: a domain that
//! keeps personal data in files deletes them itself.
//!
//! Backends, selected by URL: `file:<directory>` and `memory:`. A file backend keeps each file
//! as `<directory>/<tenant>/<domain>/<key>`, every part hex-encoded, so no key reaches outside
//! the directory; the file holds its tenant, domain and key, the content type, then the data.

use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{Mutex, MutexGuard, PoisonError},
};

use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// Where a file lives.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataKey {
    pub tenant_id: String,
    pub domain: String,
    pub key: String,
}

/// A stored file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data {
    pub content_type: String,
    pub data: Vec<u8>,
}

pub trait DataStore: Send + Sync {
    fn get(&self, key: &DataKey) -> Result<Option<Data>>;

    /// Stores the file, replacing one under the same key.
    fn put(&self, key: &DataKey, data: &Data) -> Result<()>;

    /// Deletes the file; whether there was one.
    fn delete(&self, key: &DataKey) -> Result<bool>;

    /// Calls `f` with every file, by its place in the store (`tenant/domain/key`, each part
    /// as [`file_name`] spells it) and its bytes as stored: a full copy for a standby.
    fn dump(&self, f: &mut dyn FnMut(&str, &[u8]) -> Result<()>) -> Result<()>;

    /// Keeps a file of another store's [`DataStore::dump`] at its place.
    fn restore(&self, place: &str, stored: &[u8]) -> Result<()>;

    /// Deletes every file of `tenant`, for a tenant that moved to another partition;
    /// returns how many.
    fn drop_tenant(&self, tenant: &str) -> Result<usize>;

    /// Up to `limit` files of `tenant` and `domain` whose keys start with `prefix`, after the
    /// key `after`, in the order of their keys' bytes.
    fn list(
        &self,
        tenant: &str,
        domain: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Listed>>;

    /// Per tenant and domain, how many files and how many bytes they take, for the UI;
    /// sorted by tenant and domain.
    fn summary(&self) -> Result<Vec<DataSummary>>;
}

/// The files of one tenant's domain, counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSummary {
    pub tenant: String,
    pub domain: String,
    pub files: u64,
    /// What they take in the store (with each file's header, in a file store).
    pub bytes: u64,
}

/// A file as [`DataStore::list`] names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub content_type: String,
    /// The file's length in bytes.
    pub size: u64,
}

/// Whether `key` belongs in a listing of `prefix` after `after`.
fn listed(key: &str, prefix: &str, after: &str) -> bool {
    key.starts_with(prefix) && key > after
}

/// The beginning of every place of `tenant`'s files in a [`DataStore::dump`].
pub fn tenant_place(tenant: &str) -> String {
    format!("{}/", file_name(tenant))
}

/// The key of a file as a [`DataStore::dump`] gives it: stored with it.
pub fn key_of(stored: &[u8]) -> Result<DataKey> {
    Ok(unstored("a dumped file", stored)?.0)
}

/// The three parts of a place from [`DataStore::dump`]; refused unless each is a name
/// [`file_name`] makes, so a place never reaches outside the store.
fn parts(place: &str) -> Result<[&str; 3]> {
    let valid = |p: &&str| {
        let hex = p.strip_prefix("h-").unwrap_or(p);
        !hex.is_empty()
            && hex
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    };
    let parts: Vec<&str> = place.split('/').collect();
    match <[&str; 3]>::try_from(parts) {
        Ok(parts) if parts.iter().all(valid) => Ok(parts),
        _ => Err(Error::Corrupt(format!(
            "{place} is not a place in a data store"
        ))),
    }
}

/// A file as a data store keeps it: its tenant, domain, key and content type, each as its
/// length (`u32`, big-endian) and its bytes, then the data. The names travel with the file, so
/// a name too long to be spelled out as a file name is known all the same.
fn stored(key: &DataKey, data: &Data) -> Vec<u8> {
    let parts = [&key.tenant_id, &key.domain, &key.key, &data.content_type];
    let mut bytes =
        Vec::with_capacity(16 + parts.iter().map(|p| p.len()).sum::<usize>() + data.data.len());
    for part in parts {
        bytes.extend((part.len() as u32).to_be_bytes());
        bytes.extend(part.as_bytes());
    }
    bytes.extend(&data.data);
    bytes
}

/// The names and the content type of a stored file, and where its data begins.
fn header(name: &str, bytes: &[u8]) -> Result<(DataKey, String, usize)> {
    let corrupt = || Error::Corrupt(format!("data file {name} is cut short"));
    let mut at = 0;
    let mut part = || -> Result<String> {
        let len = u32::from_be_bytes(
            bytes
                .get(at..at + 4)
                .ok_or_else(corrupt)?
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let text = bytes.get(at + 4..at + 4 + len).ok_or_else(corrupt)?;
        at += 4 + len;
        String::from_utf8(text.to_vec()).map_err(|_| corrupt())
    };
    let key = DataKey {
        tenant_id: part()?,
        domain: part()?,
        key: part()?,
    };
    let content_type = part()?;
    Ok((key, content_type, at))
}

fn unstored(name: &str, bytes: &[u8]) -> Result<(DataKey, Data)> {
    let (key, content_type, at) = header(name, bytes)?;
    Ok((
        key,
        Data {
            content_type,
            data: bytes[at..].to_vec(),
        },
    ))
}

/// The header of the stored file at `path`, read alone, and the file's length.
fn read_header(path: &std::path::Path) -> Result<Option<(DataKey, String, u64)>> {
    use std::io::Read;
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let len = file.metadata()?.len();
    // Four length-prefixed names of up to 2,068 bytes in all (a key of 512, a tenant, domain
    // and content type of their limits) fit the first 4 KiB; a longer header is read whole.
    let mut head = vec![0; len.min(4096) as usize];
    file.read_exact(&mut head)?;
    let name = path.display().to_string();
    let parsed = match header(&name, &head) {
        Ok(parsed) => parsed,
        Err(_) if (head.len() as u64) < len => header(&name, &fs::read(path)?)?,
        Err(e) => return Err(e),
    };
    let (key, content_type, at) = parsed;
    Ok(Some((key, content_type, len - at as u64)))
}

/// Opens a data store: `file:<directory>` or `memory:`.
pub fn open(url: &str) -> Result<Box<dyn DataStore>> {
    Ok(match url.split_once(':') {
        Some(("memory", _)) => Box::new(MemoryData::default()),
        Some(("file", dir)) if !dir.is_empty() => Box::new(FileData(PathBuf::from(dir))),
        _ => {
            return Err(Error::Corrupt(format!(
                "data store {url}: use file:<directory> or memory:"
            )));
        }
    })
}

#[derive(Default)]
pub struct MemoryData(Mutex<HashMap<DataKey, Data>>);

impl MemoryData {
    fn map(&self) -> MutexGuard<'_, HashMap<DataKey, Data>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl DataStore for MemoryData {
    fn get(&self, key: &DataKey) -> Result<Option<Data>> {
        Ok(self.map().get(key).cloned())
    }

    fn put(&self, key: &DataKey, data: &Data) -> Result<()> {
        self.map().insert(key.clone(), data.clone());
        Ok(())
    }

    fn delete(&self, key: &DataKey) -> Result<bool> {
        Ok(self.map().remove(key).is_some())
    }

    fn dump(&self, f: &mut dyn FnMut(&str, &[u8]) -> Result<()>) -> Result<()> {
        let files: Vec<(DataKey, Data)> = self
            .map()
            .iter()
            .map(|(k, d)| (k.clone(), d.clone()))
            .collect();
        for (k, data) in files {
            let place = format!(
                "{}/{}/{}",
                file_name(&k.tenant_id),
                file_name(&k.domain),
                file_name(&k.key)
            );
            f(&place, &stored(&k, &data))?;
        }
        Ok(())
    }

    fn restore(&self, place: &str, bytes: &[u8]) -> Result<()> {
        parts(place)?;
        let (key, data) = unstored(place, bytes)?;
        self.map().insert(key, data);
        Ok(())
    }

    fn list(
        &self,
        tenant: &str,
        domain: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Listed>> {
        let mut files: Vec<Listed> = self
            .map()
            .iter()
            .filter(|(k, _)| {
                k.tenant_id == tenant && k.domain == domain && listed(&k.key, prefix, after)
            })
            .map(|(k, d)| Listed {
                key: k.key.clone(),
                content_type: d.content_type.clone(),
                size: d.data.len() as u64,
            })
            .collect();
        files.sort_by(|a, b| a.key.cmp(&b.key));
        files.truncate(limit);
        Ok(files)
    }

    fn summary(&self) -> Result<Vec<DataSummary>> {
        let mut by: std::collections::BTreeMap<(String, String), (u64, u64)> = Default::default();
        for (k, d) in self.map().iter() {
            let e = by
                .entry((k.tenant_id.clone(), k.domain.clone()))
                .or_default();
            e.0 += 1;
            e.1 += d.data.len() as u64;
        }
        Ok(by
            .into_iter()
            .map(|((tenant, domain), (files, bytes))| DataSummary {
                tenant,
                domain,
                files,
                bytes,
            })
            .collect())
    }

    fn drop_tenant(&self, tenant: &str) -> Result<usize> {
        let mut map = self.map();
        let before = map.len();
        map.retain(|k, _| k.tenant_id != tenant);
        Ok(before - map.len())
    }
}

/// Files in a directory, created on the first put.
pub struct FileData(PathBuf);

impl FileData {
    /// Writes `bytes` as the file at `path`, durably: a reader sees the old file or the new
    /// one, never half.
    fn write(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let dir = path.parent().expect("has a directory");
        let created = !dir.exists();
        fs::create_dir_all(dir)?;
        let partial = path.with_extension("partial");
        let mut file = fs::File::create(&partial)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&partial, path)?;
        // The rename, and the directories created for it, survive a power loss.
        sync_directory(path)?;
        if created {
            sync_directory(dir)?;
            sync_directory(dir.parent().expect("has a directory"))?;
        }
        Ok(())
    }

    fn path(&self, k: &DataKey) -> PathBuf {
        self.0
            .join(file_name(&k.tenant_id))
            .join(file_name(&k.domain))
            .join(file_name(&k.key))
    }
}

/// A name's file: its hex, which keeps it readable, or for a name too long for a file system
/// (255 bytes, with room for `.partial`), `h-` and its SHA-256, which no hex name collides
/// with. Every name that fits is spelled as before.
fn file_name(name: &str) -> String {
    const LONGEST: usize = 123;
    if name.len() <= LONGEST {
        hex::encode(name)
    } else {
        format!("h-{}", hex::encode(Sha256::digest(name)))
    }
}

impl DataStore for FileData {
    fn get(&self, key: &DataKey) -> Result<Option<Data>> {
        let bytes = match fs::read(self.path(key)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let (held, data) = unstored(&key.key, &bytes)?;
        // A hashed name another key shares would need a SHA-256 collision; refused anyway.
        Ok((held == *key).then_some(data))
    }

    fn put(&self, key: &DataKey, data: &Data) -> Result<()> {
        self.write(&self.path(key), &stored(key, data))
    }

    fn dump(&self, f: &mut dyn FnMut(&str, &[u8]) -> Result<()>) -> Result<()> {
        let entries = |dir: &std::path::Path| -> Result<Vec<String>> {
            let mut names = Vec::new();
            match fs::read_dir(dir) {
                Ok(read) => {
                    for entry in read {
                        let name = entry?.file_name().to_string_lossy().into_owned();
                        // Half-written files (`.partial`) and anything foreign stay out.
                        if !name.contains('.') {
                            names.push(name);
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            names.sort();
            Ok(names)
        };
        for tenant in entries(&self.0)? {
            for domain in entries(&self.0.join(&tenant))? {
                for key in entries(&self.0.join(&tenant).join(&domain))? {
                    let place = format!("{tenant}/{domain}/{key}");
                    if parts(&place).is_err() {
                        continue;
                    }
                    match fs::read(self.0.join(&tenant).join(&domain).join(&key)) {
                        Ok(bytes) => f(&place, &bytes)?,
                        // Deleted meanwhile: its record follows.
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Ok(())
    }

    fn restore(&self, place: &str, bytes: &[u8]) -> Result<()> {
        let [tenant, domain, key] = parts(place)?;
        self.write(&self.0.join(tenant).join(domain).join(key), bytes)
    }

    fn delete(&self, key: &DataKey) -> Result<bool> {
        match fs::remove_file(self.path(key)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn summary(&self) -> Result<Vec<DataSummary>> {
        // Tenants and domains are spelled as hex (their names are short); the files are
        // counted from the directory, their sizes from the file system.
        let name = |n: &std::ffi::OsStr| -> Option<String> {
            hex::decode(n.to_string_lossy().as_bytes())
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
        };
        let tenants = match fs::read_dir(&self.0) {
            Ok(read) => read,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for t in tenants {
            let t = t?;
            let Some(tenant) = name(&t.file_name()) else {
                continue;
            };
            if !t.file_type()?.is_dir() {
                continue;
            }
            for d in fs::read_dir(t.path())? {
                let d = d?;
                let Some(domain) = name(&d.file_name()) else {
                    continue;
                };
                if !d.file_type()?.is_dir() {
                    continue;
                }
                let (mut files, mut bytes) = (0, 0);
                for f in fs::read_dir(d.path())? {
                    let f = f?;
                    if f.file_name().to_string_lossy().contains('.') {
                        continue; // half-written
                    }
                    files += 1;
                    bytes += f.metadata()?.len();
                }
                out.push(DataSummary {
                    tenant: tenant.clone(),
                    domain,
                    files,
                    bytes,
                });
            }
        }
        out.sort_by(|a, b| (&a.tenant, &a.domain).cmp(&(&b.tenant, &b.domain)));
        Ok(out)
    }

    fn drop_tenant(&self, tenant: &str) -> Result<usize> {
        let dir = self.0.join(file_name(tenant));
        let mut count = 0;
        match fs::read_dir(&dir) {
            Ok(domains) => {
                for domain in domains {
                    count += fs::read_dir(domain?.path())?.count();
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        }
        fs::remove_dir_all(&dir)?;
        sync_directory(&dir)?;
        Ok(count)
    }

    fn list(
        &self,
        tenant: &str,
        domain: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Listed>> {
        let dir = self.0.join(file_name(tenant)).join(file_name(domain));
        let read = match fs::read_dir(&dir) {
            Ok(read) => read,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        // A spelled-out name is its key; a hashed one is known from its file.
        let mut found: Vec<(String, PathBuf)> = Vec::new();
        for entry in read {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.contains('.') {
                continue; // half-written (`.partial`) or foreign
            }
            let key = if name.starts_with("h-") {
                match read_header(&entry.path())? {
                    Some((k, _, _)) => k.key,
                    None => continue,
                }
            } else {
                match hex::decode(&name)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                {
                    Some(key) => key,
                    None => continue,
                }
            };
            if listed(&key, prefix, after) {
                found.push((key, entry.path()));
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0));
        let mut files = Vec::with_capacity(found.len().min(limit));
        for (key, path) in found {
            if files.len() == limit {
                break;
            }
            // Deleted meanwhile: left out.
            if let Some((_, content_type, size)) = read_header(&path)? {
                files.push(Listed {
                    key,
                    content_type,
                    size,
                });
            }
        }
        Ok(files)
    }
}

/// Syncs the directory holding `path`, so a newly created file is found after a power loss.
fn sync_directory(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => std::path::Path::new("."),
        };
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
