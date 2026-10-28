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
}

/// Files in a directory, created on the first put.
pub struct FileData(PathBuf);

impl FileData {
    fn path(&self, k: &DataKey) -> PathBuf {
        self.0
            .join(hex::encode(&k.tenant_id))
            .join(hex::encode(&k.domain))
            .join(hex::encode(&k.key))
    }
}

impl DataStore for FileData {
    fn get(&self, key: &DataKey) -> Result<Option<Data>> {
        let bytes = match fs::read(self.path(key)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let corrupt = || Error::Corrupt(format!("data file {} is cut short", key.key));
        let len = u32::from_be_bytes(bytes.get(..4).ok_or_else(corrupt)?.try_into().unwrap());
        let end = 4 + len as usize;
        let content_type = bytes.get(4..end).ok_or_else(corrupt)?;
        Ok(Some(Data {
            content_type: String::from_utf8_lossy(content_type).into_owned(),
            data: bytes[end..].to_vec(),
        }))
    }

    fn put(&self, key: &DataKey, data: &Data) -> Result<()> {
        let path = self.path(key);
        fs::create_dir_all(path.parent().expect("has a directory"))?;
        // Write next to it and rename: a reader sees the old file or the new one, never half.
        let partial = path.with_extension("partial");
        let mut file = fs::File::create(&partial)?;
        file.write_all(&(data.content_type.len() as u32).to_be_bytes())?;
        file.write_all(data.content_type.as_bytes())?;
        file.write_all(&data.data)?;
        file.sync_all()?;
        fs::rename(&partial, &path)?;
        Ok(())
    }

    fn delete(&self, key: &DataKey) -> Result<bool> {
        match fs::remove_file(self.path(key)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
