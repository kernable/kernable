//! Business processes (design/processes.md), stage 1: registering models and removing
//! processes. Every version of a model is kept as a signed record in the store (replicated and
//! exported like memberships), and each process is active or removed: a removed one starts no
//! new cases (a membership no longer lists it, or `kernable processes remove`); registering it
//! again makes it active.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, TryLockError},
};

use kernable_core::{
    proto::{
        Membership, ProcessRecord, ProcessRegistered, ProcessRegistration, ProcessRemoval,
        ProcessRemoved, Signature, process_service_server::ProcessService,
    },
    signing,
};
use tonic::{Request, Response, Status};

use crate::{
    Api, Kernel, MAX_REQUEST_AGE_MS, config, internal, now_ms,
    process_model::{self, Kind, Model},
    protocol,
};

/// What the kernel holds of one process.
#[derive(Debug, Clone, Default)]
pub(crate) struct Entry {
    /// Every version, oldest first.
    pub versions: Vec<ProcessRecord>,
    pub removed: bool,
}

#[derive(Default)]
pub(crate) struct Processes {
    all: RwLock<BTreeMap<String, Entry>>,
    /// Held while a process changes, so versions are numbered one after the other.
    changing: Mutex<()>,
}

impl Processes {
    pub(crate) fn load(records: Vec<ProcessRecord>) -> Self {
        let mut all: BTreeMap<String, Entry> = BTreeMap::new();
        for r in records {
            let entry = all.entry(r.name.clone()).or_default();
            if r.version == 0 {
                entry.removed = r.removed;
            } else {
                entry.versions.push(r);
            }
        }
        for entry in all.values_mut() {
            entry.versions.sort_by_key(|v| v.version);
        }
        Processes {
            all: RwLock::new(all),
            changing: Mutex::default(),
        }
    }

    /// The process `name`, if it was ever registered.
    #[allow(dead_code)] // the engine (stage 2) and the UI read processes
    pub(crate) fn get(&self, name: &str) -> Option<Entry> {
        self.all
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// Held while a process changes; a second change waits as a write waits for the standby.
    fn changing(&self) -> MutexGuard<'_, ()> {
        match self.changing.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => crate::replication::blocking(|| {
                self.changing.lock().unwrap_or_else(PoisonError::into_inner)
            }),
        }
    }

    fn set(&self, name: &str, entry: Entry) {
        self.all
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), entry);
    }

    /// Keeps `model` (its canonical form `canonical`) as the next version of `name` unless it
    /// equals the newest, and makes the process active. Returns its version and whether it is
    /// new.
    fn register(
        &self,
        kernel: &Kernel,
        name: &str,
        model: &[u8],
        canonical: Vec<u8>,
        issued_at: i64,
        sig: &Signature,
    ) -> Result<(u32, bool), Status> {
        let _changing = self.changing();
        let mut entry = self.get(name).unwrap_or_default();
        let now = now_ms();
        let (version, new) = match entry.versions.last() {
            Some(v) if v.canonical == canonical => (v.version, false),
            Some(v) => (v.version + 1, true),
            None => (1, true),
        };
        if new {
            let record = ProcessRecord {
                name: name.to_owned(),
                version,
                model: model.to_vec(),
                canonical,
                issued_at,
                sig: Some(sig.clone()),
                recorded_at: now,
                removed: false,
            };
            kernel.store.save_process(&record).map_err(internal)?;
            entry.versions.push(record);
        }
        let revived = entry.removed;
        if revived {
            kernel
                .store
                .save_process(&state(name, false, issued_at, sig))
                .map_err(internal)?;
            entry.removed = false;
        }
        if new || revived {
            kernel.sync(0);
            self.set(name, entry);
        }
        if revived {
            tracing::info!(process = %name, version, "a removed process is active again");
        }
        Ok((version, new))
    }

    /// Removes the process `name`: it starts no new cases. Returns false if it was removed
    /// already; NOT_FOUND if it was never registered.
    fn remove(
        &self,
        kernel: &Kernel,
        name: &str,
        issued_at: i64,
        sig: &Signature,
    ) -> Result<bool, Status> {
        let _changing = self.changing();
        let Some(mut entry) = self.get(name) else {
            return Err(Status::not_found(format!("no process {name}")));
        };
        if entry.removed {
            return Ok(false);
        }
        kernel
            .store
            .save_process(&state(name, true, issued_at, sig))
            .map_err(internal)?;
        kernel.sync(0);
        entry.removed = true;
        self.set(name, entry);
        tracing::info!(process = %name, "process removed: it starts no new cases");
        Ok(true)
    }

    /// Removes the processes `before` listed that `after` (the membership of the same service
    /// applied now) no longer lists.
    pub(crate) fn unlisted(
        &self,
        kernel: &Kernel,
        before: Option<&Membership>,
        after: &Membership,
        sig: &Signature,
    ) -> Result<(), Status> {
        let Some(before) = before else {
            return Ok(());
        };
        for name in before
            .processes
            .iter()
            .filter(|p| !after.processes.contains(p))
        {
            match self.remove(kernel, name, after.issued_at, sig) {
                Ok(_) => {}
                // Listed, but never registered: nothing to remove.
                Err(s) if s.code() == tonic::Code::NotFound => {}
                Err(s) => return Err(s),
            }
        }
        Ok(())
    }
}

/// A change of state of process `name`.
fn state(name: &str, removed: bool, issued_at: i64, sig: &Signature) -> ProcessRecord {
    ProcessRecord {
        name: name.to_owned(),
        version: 0,
        issued_at,
        sig: Some(sig.clone()),
        recorded_at: now_ms(),
        removed,
        ..Default::default()
    }
}

/// Refuses a model whose signer lacks a grant it needs: `process` for the process, `submit` for
/// each command, `query` for each query, `listen` for the start event and each waited event.
fn authorize(kernel: &Kernel, key_id: &str, model: &Model) -> Result<(), Status> {
    let permissions = kernel.permissions();
    let denied = |action: &str, name: &str| {
        Status::permission_denied(format!(
            "key may not register process {}: it needs a grant {action} for {name}",
            model.name
        ))
    };
    if !config::allowed(&permissions.process, key_id, &model.name) {
        return Err(denied("process", &model.name));
    }
    for command in model.commands() {
        let (_, name) = process_model::split_command(command);
        if !config::allowed(&permissions.submit, key_id, &name) {
            return Err(denied("submit", &name));
        }
    }
    for query in model.queries() {
        if !config::allowed(&permissions.query, key_id, query) {
            return Err(denied("query", query));
        }
    }
    for event in model.events() {
        if !config::allowed(&permissions.listen, key_id, event) {
            return Err(denied("listen", event));
        }
    }
    Ok(())
}

/// Refuses a model that sends a command no worker registered and no membership lists, as Submit
/// refuses the command.
fn known_commands(kernel: &Kernel, model: &Model) -> Result<(), Status> {
    for step in &model.steps {
        let Kind::Command { command, .. } = &step.kind else {
            continue;
        };
        let (aggregate, name) = process_model::split_command(command);
        if !kernel.workers.runs(kernel, &aggregate, &name) {
            return Err(Status::invalid_argument(format!(
                "process {}, step {}: no worker registered {name} for {aggregate}, and no \
                 membership lists {aggregate}",
                model.name, step.name
            )));
        }
    }
    Ok(())
}

/// Checks a signed request's signature, grant header and age.
fn signed<'a>(
    kernel: &Kernel,
    metadata: &tonic::metadata::MetadataMap,
    sig: Option<&'a Signature>,
    message: &[u8],
    issued_at: i64,
    what: &str,
) -> Result<&'a Signature, Status> {
    let sig = sig.ok_or_else(|| Status::unauthenticated(format!("the {what} is not signed")))?;
    signing::verify(sig, message)
        .map_err(|_| Status::unauthenticated(format!("invalid {what} signature")))?;
    kernel.present(metadata, &sig.key_id)?;
    if (now_ms() - issued_at).abs() > MAX_REQUEST_AGE_MS {
        return Err(Status::unauthenticated(format!(
            "the {what} expired; check the clock"
        )));
    }
    Ok(sig)
}

/// The registration of `model` (checked, its signer authorized) by `sig`: its version and
/// whether it is new.
pub(crate) fn register(
    kernel: &Arc<Kernel>,
    json: &[u8],
    issued_at: i64,
    sig: &Signature,
) -> Result<ProcessRegistered, Status> {
    let (model, warnings) =
        process_model::parse(json).map_err(|e| Status::invalid_argument(e.to_string()))?;
    authorize(kernel, &sig.key_id, &model)?;
    known_commands(kernel, &model)?;
    for warning in &warnings {
        tracing::warn!(%warning, "a process reads a field that may be absent");
    }
    let value: serde_json::Value = serde_json::from_slice(json)
        .map_err(|e| Status::invalid_argument(format!("the model is not JSON: {e}")))?;
    let canonical = process_model::canonical(&value);
    let (version, new) =
        kernel
            .processes
            .register(kernel, &model.name, json, canonical, issued_at, sig)?;
    if new {
        tracing::info!(process = %model.name, version, key = %sig.key_id, "process registered");
    }
    Ok(ProcessRegistered {
        name: model.name,
        version,
        new,
    })
}

#[tonic::async_trait]
impl ProcessService for Api {
    async fn register(
        &self,
        request: Request<ProcessRegistration>,
    ) -> Result<Response<ProcessRegistered>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let _running = kernel.request()?;
        let sig = signed(
            kernel,
            &metadata,
            req.sig.as_ref(),
            &signing::process_message(&req.model, req.issued_at),
            req.issued_at,
            "process registration",
        )?;
        register(kernel, &req.model, req.issued_at, sig).map(Response::new)
    }

    async fn remove(
        &self,
        request: Request<ProcessRemoval>,
    ) -> Result<Response<ProcessRemoved>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let _running = kernel.request()?;
        let sig = signed(
            kernel,
            &metadata,
            req.sig.as_ref(),
            &signing::process_remove_message(&req.name, req.issued_at),
            req.issued_at,
            "process removal",
        )?;
        if !process_model::valid_process_name(&req.name) {
            return Err(Status::invalid_argument(format!(
                "{} is not a qualified process name (domain.process)",
                req.name
            )));
        }
        if !config::allowed(&kernel.permissions().process, &sig.key_id, &req.name) {
            return Err(Status::permission_denied(format!(
                "key may not remove process {}: it needs a grant process for it",
                req.name
            )));
        }
        let removed = kernel
            .processes
            .remove(kernel, &req.name, req.issued_at, sig)?;
        Ok(Response::new(ProcessRemoved { removed }))
    }
}
