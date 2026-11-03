//! The data store: files of a domain, under (tenant, domain, key), apart from the event log. They are not part of the hash chain and not erased with the vault: a domain that
//! keeps personal data in files deletes them itself.
//!
//! Backends, selected by URL: `file:<directory>` and `memory:`. A file backend keeps each file
//! as `<directory>/<tenant>/<domain>/<key>`, every part hex-encoded, so no key reaches outside
//! the directory; the file holds the content type, then the data.

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

/// A file as a file data store keeps it: the content type's length, the type, the data.
fn stored(data: &Data) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + data.content_type.len() + data.data.len());
    bytes.extend((data.content_type.len() as u32).to_be_bytes());
    bytes.extend(data.content_type.as_bytes());
    bytes.extend(&data.data);
    bytes
}

fn unstored(name: &str, bytes: &[u8]) -> Result<Data> {
    let corrupt = || Error::Corrupt(format!("data file {name} is cut short"));
    let len = u32::from_be_bytes(
        bytes
            .get(..4)
            .ok_or_else(corrupt)?
            .try_into()
            .expect("4 bytes"),
    );
    let end = 4 + len as usize;
    let content_type = bytes.get(4..end).ok_or_else(corrupt)?;
    Ok(Data {
        content_type: String::from_utf8_lossy(content_type).into_owned(),
        data: bytes[end..].to_vec(),
    })
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
            f(&place, &stored(&data))?;
        }
        Ok(())
    }

    fn restore(&self, place: &str, bytes: &[u8]) -> Result<()> {
        let name = |part: &str| -> Result<String> {
            hex::decode(part)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "{place}: a memory data store keeps names of up to 123 bytes only"
                    ))
                })
        };
        let [tenant, domain, key] = parts(place)?;
        let key = DataKey {
            tenant_id: name(tenant)?,
            domain: name(domain)?,
            key: name(key)?,
        };
        let data = unstored(&key.key, bytes)?;
        self.map().insert(key, data);
        Ok(())
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
        crate::file::sync_directory(path)?;
        if created {
            crate::file::sync_directory(dir)?;
            crate::file::sync_directory(dir.parent().expect("has a directory"))?;
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
        unstored(&key.key, &bytes).map(Some)
    }

    fn put(&self, key: &DataKey, data: &Data) -> Result<()> {
        self.write(&self.path(key), &stored(data))
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
}
