//! A whole kernel in one file, and back: the exit into a self-hosted kernel.
//!
//! An export holds what a new standby gets ([`crate::replication`]): every record of the store
//! in one consistent view, the vault values they refer to and every file of the data store.
//! Vault values stay sealed with the source's vault key ([`Vault::export_sealed`]), so the file
//! holds no personal data in clear; whoever imports it needs that key. Snapshots and the cache
//! are left out: workers rebuild the one, the other expires.
//!
//! A running kernel streams its export ([`ExportService`], `kernable export <addr>`); stopped
//! stores are exported directly (`kernable export <file> --store ...`). Both write the same
//! file (see `export.proto`): the magic, a header, batches, and an end whose digest covers
//! every byte before it. [`import`] fills empty stores from it and checks the store with
//! `verify`.

use std::{
    collections::BTreeSet,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use kernable_core::{
    proto::{
        DataFile, ExportEnd, ExportFrame, ExportHeader, ExportRequest, ImageBatch, LogRecord,
        export_frame::Frame, export_service_server::ExportService, log_record::Record,
    },
    signing,
};
use kernable_store::{Store, data::DataStore, vault::Vault};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    Api, BoxError, MAX_REQUEST_AGE_MS, config, now_ms, protocol,
    replication::{BATCH, BATCH_BYTES, vault_refs},
};

/// The first bytes of every export file.
pub const MAGIC: &[u8; 8] = b"KNBLEXP\n";
/// The version of the export format ([`ExportHeader::format`]).
pub const FORMAT: u32 = 1;
/// The largest frame a reader takes: a batch of records, or files up to the data limit.
const MAX_FRAME: usize = 512 << 20;

/// The stores an export is read from.
pub struct Source<'a> {
    pub store: &'a dyn Store,
    pub vault: Option<&'a Vault>,
    pub data: &'a dyn DataStore,
    /// The kernel's `--partition`, if any.
    pub partition: &'a str,
    /// Leave the vault values out, also where the store refers to them: for a masked copy
    /// ([`crate::copy`]), which needs no vault key.
    pub masked: bool,
}

/// Calls `f` with the frames of an export of `source`: the header, the batches, and the end
/// (without its digest, which [`Writer`] sets).
pub fn produce(
    source: &Source,
    f: &mut dyn FnMut(ExportFrame) -> Result<(), BoxError>,
) -> Result<(), BoxError> {
    let frame = |frame| ExportFrame { frame: Some(frame) };
    f(frame(Frame::Header(ExportHeader {
        format: FORMAT,
        created_at: now_ms(),
        partition: source.partition.to_owned(),
        vault: source.vault.is_some() && !source.masked,
    })))?;
    let mut end = ExportEnd::default();
    let mut kernel_keys = BTreeSet::new();
    let mut failed: Option<BoxError> = None;
    let (mut batch, mut bytes) = (Vec::new(), 0);
    let flush = |batch: &mut Vec<Record>,
                 end: &mut ExportEnd,
                 f: &mut dyn FnMut(ExportFrame) -> Result<(), BoxError>|
     -> Result<(), BoxError> {
        if batch.is_empty() {
            return Ok(());
        }
        let refs = vault_refs(batch.iter());
        let vault = match source.vault {
            _ if source.masked => Vec::new(),
            Some(vault) => vault.export_sealed(&refs)?,
            None if refs.is_empty() => Vec::new(),
            None => {
                return Err(
                    "the store refers to vault values: give the vault and its key to export them"
                        .into(),
                );
            }
        };
        end.records += batch.len() as u64;
        end.vault_values += vault.len() as u64;
        f(frame(Frame::Batch(ImageBatch {
            sealed: Vec::new(),
            records: batch
                .drain(..)
                .map(|r| LogRecord { record: Some(r) })
                .collect(),
            vault,
            files: Vec::new(),
        })))
    };
    source.store.snapshot(&mut |record| {
        if failed.is_some() {
            return Ok(());
        }
        match &record {
            Record::Checkpoint(c) => {
                kernel_keys.extend(c.kernel_sig.iter().map(|s| s.key_id.clone()))
            }
            Record::Revocation(r) => {
                kernel_keys.extend(r.kernel_sig.iter().map(|s| s.key_id.clone()))
            }
            _ => {}
        }
        bytes += LogRecord {
            record: Some(record.clone()),
        }
        .encoded_len();
        batch.push(record);
        if batch.len() >= BATCH || bytes >= BATCH_BYTES {
            bytes = 0;
            if let Err(e) = flush(&mut batch, &mut end, f) {
                failed = Some(e);
            }
        }
        Ok(())
    })?;
    if let Some(e) = failed {
        return Err(e);
    }
    flush(&mut batch, &mut end, f)?;
    let (mut files, mut bytes) = (Vec::new(), 0);
    let mut failed: Option<BoxError> = None;
    source.data.dump(&mut |place, stored| {
        if failed.is_some() {
            return Ok(());
        }
        bytes += stored.len();
        files.push(DataFile {
            path: place.to_owned(),
            data: stored.to_vec(),
            ..Default::default()
        });
        if bytes >= BATCH_BYTES {
            bytes = 0;
            end.files += files.len() as u64;
            let files = std::mem::take(&mut files);
            if let Err(e) = f(frame(Frame::Batch(ImageBatch {
                files,
                ..Default::default()
            }))) {
                failed = Some(e);
            }
        }
        Ok(())
    })?;
    if let Some(e) = failed {
        return Err(e);
    }
    if !files.is_empty() {
        end.files += files.len() as u64;
        f(frame(Frame::Batch(ImageBatch {
            files,
            ..Default::default()
        })))?;
    }
    end.kernel_keys = kernel_keys.into_iter().collect();
    f(frame(Frame::End(end)))
}

/// Writes frames to an export file, hashing every byte; [`Writer::finish`] writes the end with
/// the digest.
pub struct Writer<W: Write> {
    out: W,
    hash: Sha256,
}

impl<W: Write> Writer<W> {
    pub fn new(mut out: W) -> std::io::Result<Self> {
        out.write_all(MAGIC)?;
        let mut hash = Sha256::new();
        hash.update(MAGIC);
        Ok(Writer { out, hash })
    }

    /// Writes a frame; the end frame gets its digest and is the last.
    pub fn write(&mut self, mut frame: ExportFrame) -> std::io::Result<bool> {
        let end = match &mut frame.frame {
            Some(Frame::End(end)) => {
                end.digest = self.hash.clone().finalize().to_vec();
                true
            }
            _ => false,
        };
        let bytes = frame.encode_length_delimited_to_vec();
        if !end {
            self.hash.update(&bytes);
        }
        self.out.write_all(&bytes)?;
        if end {
            self.out.flush()?;
        }
        Ok(end)
    }

    pub fn into_inner(self) -> W {
        self.out
    }
}

/// Reads the frames of an export file and checks its digest at the end.
pub struct Reader<R: Read> {
    input: R,
    hash: Sha256,
    done: bool,
}

impl<R: Read> Reader<R> {
    pub fn new(mut input: R) -> Result<Self, BoxError> {
        let mut magic = [0u8; 8];
        input
            .read_exact(&mut magic)
            .map_err(|_| "not a kernable export: the file is too short")?;
        if &magic != MAGIC {
            return Err("not a kernable export".into());
        }
        let mut hash = Sha256::new();
        hash.update(MAGIC);
        Ok(Reader {
            input,
            hash,
            done: false,
        })
    }

    /// The next frame; `None` after the end, whose digest it checked.
    pub fn frame(&mut self) -> Result<Option<ExportFrame>, BoxError> {
        if self.done {
            return Ok(None);
        }
        let mut prefix = Vec::with_capacity(10);
        let len = loop {
            let mut byte = [0u8; 1];
            if self.input.read(&mut byte)? == 0 {
                return Err("the export ends early: it is cut short".into());
            }
            prefix.push(byte[0]);
            if byte[0] & 0x80 == 0 {
                break prost::decode_length_delimiter(prefix.as_slice())?;
            }
            if prefix.len() >= 10 {
                return Err("the export is damaged: a frame length is invalid".into());
            }
        };
        if len > MAX_FRAME {
            return Err(format!("the export is damaged: a frame of {len} bytes").into());
        }
        let mut body = vec![0u8; len];
        self.input
            .read_exact(&mut body)
            .map_err(|_| "the export ends early: it is cut short")?;
        let frame = ExportFrame::decode(body.as_slice())
            .map_err(|_| "the export is damaged: a frame does not decode")?;
        if let Some(Frame::End(end)) = &frame.frame {
            let digest = self.hash.clone().finalize();
            if end.digest != digest.as_slice() {
                return Err("the export is damaged: its digest does not match".into());
            }
            let mut rest = [0u8; 1];
            if self.input.read(&mut rest)? != 0 {
                return Err("the export is damaged: bytes follow its end".into());
            }
            self.done = true;
        } else {
            self.hash.update(&prefix);
            self.hash.update(&body);
        }
        Ok(Some(frame))
    }
}

/// The stores an export is imported into; they must be empty.
pub struct Target<'a> {
    pub store: &'a dyn Store,
    pub vault: Option<&'a Vault>,
    pub data: &'a dyn DataStore,
    /// A masked copy ([`crate::copy`]): the export's vault values are left out, and `vault`
    /// is the copy's own, new one.
    pub mask: Option<crate::copy::Mask>,
}

/// What [`import`] took.
#[derive(Debug, Default)]
pub struct Imported {
    pub header: ExportHeader,
    pub end: ExportEnd,
}

/// Fills the empty stores of `target` from the export in `file`. The file is read twice: first
/// to check it whole (its digest, its counts, and that `target`'s vault opens its values),
/// then to apply it, with the checks of [`Store::apply_image`]. If applying fails, the stores
/// are left half-filled: delete them and import again.
pub fn import(file: &Path, target: &Target) -> Result<Imported, BoxError> {
    if !empty(target.store)? {
        return Err("the store holds data already: import into new, empty stores".into());
    }
    read(file, &mut |batch| {
        if target.mask.is_some() {
            return Ok(());
        }
        crate::copy::refuse_marked(
            batch.records.iter().filter_map(|l| l.record.as_ref()),
            "the export",
        )?;
        if let Some(row) = batch.vault.first() {
            let vault = target.vault.ok_or(
                "the export holds vault values: give the vault and the source's vault key",
            )?;
            vault.check_sealed(std::slice::from_ref(row))?;
        }
        Ok(())
    })?;
    read(file, &mut |batch| apply(target, batch))
}

/// Applies one batch of an export to `target`: its files, its vault values (none for a masked
/// copy), its records.
pub fn apply(target: &Target, mut batch: ImageBatch) -> Result<(), BoxError> {
    if let Some(mask) = &target.mask {
        mask.image(&mut batch);
    }
    for file in &batch.files {
        target.data.restore(&file.path, &file.data)?;
    }
    if let Some(vault) = target.vault {
        vault.import(&batch.vault)?;
    }
    let records: Vec<Record> = batch.records.into_iter().filter_map(|l| l.record).collect();
    target.store.apply_image(&records)?;
    Ok(())
}

/// Reads the export in `file` and calls `f` with each batch; returns its header and end once
/// it reached the end and the counts match it.
pub fn read(
    file: &Path,
    f: &mut dyn FnMut(ImageBatch) -> Result<(), BoxError>,
) -> Result<Imported, BoxError> {
    let input = std::fs::File::open(file).map_err(|e| format!("{}: {e}", file.display()))?;
    let mut reader = Reader::new(std::io::BufReader::new(input))?;
    let header = match reader.frame()?.and_then(|f| f.frame) {
        Some(Frame::Header(h)) => h,
        _ => return Err("the export is damaged: it does not start with its header".into()),
    };
    if header.format != FORMAT {
        return Err(format!(
            "the export has format {}; this kernel reads format {FORMAT}",
            header.format
        )
        .into());
    }
    let mut taken = ExportEnd::default();
    loop {
        match reader.frame()?.and_then(|f| f.frame) {
            Some(Frame::Batch(batch)) => {
                taken.records += batch.records.len() as u64;
                taken.vault_values += batch.vault.len() as u64;
                taken.files += batch.files.len() as u64;
                f(batch)?;
            }
            Some(Frame::End(end)) => {
                if (taken.records, taken.vault_values, taken.files)
                    != (end.records, end.vault_values, end.files)
                {
                    return Err("the export is damaged: its counts do not match its end".into());
                }
                return Ok(Imported { header, end });
            }
            Some(Frame::Header(_)) => return Err("the export is damaged: a second header".into()),
            None => return Err("the export is damaged: an empty frame".into()),
        }
    }
}

/// Whether nothing was ever written to the store.
pub fn empty(store: &dyn Store) -> Result<bool, BoxError> {
    Ok(store.head_position()? == 0
        && store.command_ids()?.is_empty()
        && store.listeners()?.is_empty()
        && store.grants()?.is_empty()
        && store.workers()?.is_empty())
}

#[tonic::async_trait]
impl ExportService for Api {
    type ExportStream = ReceiverStream<Result<ExportFrame, Status>>;

    async fn export(
        &self,
        request: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let kernel = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("the export request is not signed"))?;
        signing::verify(&sig, &signing::export_message(req.issued_at))
            .map_err(|_| Status::unauthenticated("invalid export request signature"))?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "export request expired; check the clock",
            ));
        }
        kernel.present(&metadata, &sig.key_id)?;
        if !config::allowed(&kernel.permissions().export, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "the key may not export the kernel: its grant needs export for *",
            ));
        }
        // Every event of the export anchored by a checkpoint.
        kernel
            .store
            .checkpoint(&kernel.key(), now_ms())
            .map_err(|e| Status::internal(e.to_string()))?;
        tracing::warn!(key = %sig.key_id, "exporting the whole kernel");
        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            let partition = kernel
                .config()
                .partition
                .as_ref()
                .map(|p| p.name.clone())
                .unwrap_or_default();
            let source = Source {
                store: kernel.store.as_ref(),
                vault: kernel.vault.as_ref(),
                data: kernel.data.as_ref(),
                partition: &partition,
                masked: false,
            };
            let sent = produce(&source, &mut |frame| {
                tx.blocking_send(Ok(frame))
                    .map_err(|_| "the exporting client left".into())
            });
            if let Err(e) = sent {
                tracing::error!(error = %e, "export failed");
                let _ = tx.blocking_send(Err(Status::internal(format!("export failed: {e}"))));
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
