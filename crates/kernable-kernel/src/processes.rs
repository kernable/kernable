//! Business processes (design/processes.md): registering models and removing processes.
//! Every version of a model is kept as a signed record in the store (replicated and
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
        ProcessRemoved, RequestContext, Signature, process_service_server::ProcessService,
    },
    signing,
};
use tonic::{Request, Response, Status};

use crate::{
    Api, Kernel, config, internal, now_ms,
    process_model::{self, Kind, Model},
    protocol,
};

/// What the kernel holds of one process.
#[derive(Debug, Clone, Default)]
pub(crate) struct Entry {
    /// Every version, oldest first.
    pub versions: Vec<ProcessRecord>,
    pub removed: bool,
    /// As whom it acts (`ProcessService.Act`); none: as `process:<name>`.
    pub acting: Option<RequestContext>,
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
                entry.acting = r.acting;
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
    pub(crate) fn get(&self, name: &str) -> Option<Entry> {
        self.all
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// Every process.
    pub(crate) fn all(&self) -> BTreeMap<String, Entry> {
        self.all
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
                acting: None,
            };
            kernel.store.save_process(&record).map_err(internal)?;
            entry.versions.push(record);
        }
        let revived = entry.removed;
        if revived {
            kernel
                .store
                .save_process(&state(name, false, entry.acting.clone(), issued_at, sig))
                .map_err(internal)?;
            entry.removed = false;
        }
        if new || revived {
            kernel.sync(0);
            self.set(name, entry);
            announce(kernel, name);
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
            .save_process(&state(name, true, entry.acting.clone(), issued_at, sig))
            .map_err(internal)?;
        kernel.sync(0);
        entry.removed = true;
        self.set(name, entry);
        announce(kernel, name);
        tracing::info!(process = %name, "process removed: it starts no new cases");
        Ok(true)
    }

    /// Takes a process record of the control partition: a version this kernel lacks, or a
    /// state that differs from the one it holds.
    pub(crate) fn take(&self, kernel: &Kernel, record: ProcessRecord) -> Result<(), Status> {
        let _changing = self.changing();
        let mut entry = self.get(&record.name).unwrap_or_default();
        if record.version == 0 {
            if entry.removed == record.removed && entry.acting == record.acting {
                return Ok(());
            }
            entry.removed = record.removed;
            entry.acting = record.acting.clone();
        } else if entry.versions.iter().any(|v| v.version == record.version) {
            return Ok(());
        } else {
            process_model::parse(&record.model)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            entry.versions.push(record.clone());
            entry.versions.sort_by_key(|v| v.version);
        }
        kernel.store.save_process(&record).map_err(internal)?;
        kernel.sync(0);
        let name = record.name.clone();
        self.set(&name, entry);
        tracing::info!(process = %name, version = record.version, removed = record.removed, "process taken from the control partition");
        Ok(())
    }

    /// Sets as whom the process `name` acts. Returns false if it acted so already; NOT_FOUND if
    /// it was never registered.
    fn act(
        &self,
        kernel: &Kernel,
        name: &str,
        acting: RequestContext,
        issued_at: i64,
        sig: &Signature,
    ) -> Result<bool, Status> {
        let _changing = self.changing();
        let Some(mut entry) = self.get(name) else {
            return Err(Status::not_found(format!("no process {name}")));
        };
        if entry.acting.as_ref() == Some(&acting) {
            return Ok(false);
        }
        kernel
            .store
            .save_process(&state(
                name,
                entry.removed,
                Some(acting.clone()),
                issued_at,
                sig,
            ))
            .map_err(internal)?;
        kernel.sync(0);
        tracing::info!(process = %name, identity = %acting.sender_identity, tenant = %acting.sender_tenant, "the process acts as a new identity");
        entry.acting = Some(acting);
        self.set(name, entry);
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
        // A partition takes the processes' states from the control partition.
        if follows_control(kernel) {
            return Ok(());
        }
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

/// Whether this kernel is a partition that takes its processes from the control partition.
fn follows_control(kernel: &Kernel) -> bool {
    kernel
        .config
        .partition
        .as_ref()
        .is_some_and(|p| p.control.is_some())
}

/// A change of state of process `name`.
fn state(
    name: &str,
    removed: bool,
    acting: Option<RequestContext>,
    issued_at: i64,
    sig: &Signature,
) -> ProcessRecord {
    ProcessRecord {
        name: name.to_owned(),
        version: 0,
        issued_at,
        sig: Some(sig.clone()),
        recorded_at: now_ms(),
        removed,
        acting,
        ..Default::default()
    }
}

/// Tells the engine to announce the process's change in the registry stream (on the control
/// partition, or the only kernel).
fn announce(kernel: &Kernel, name: &str) {
    if !follows_control(kernel) {
        kernel
            .engine
            .send(crate::process_engine::Input::Announce(name.to_owned()));
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

/// The registration of `model` (checked, its signer authorized) by `sig`: its version and
/// whether it is new.
fn register(
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
    // A partition holds the control partition's versions: it registers nothing of its own, but
    // answers for a model the control partition has (an app connects to every partition).
    if follows_control(kernel) {
        let known = kernel.processes.get(&model.name).and_then(|e| {
            e.versions
                .iter()
                .find(|v| v.canonical == canonical)
                .cloned()
        });
        return match known {
            Some(v) => Ok(ProcessRegistered {
                name: model.name,
                version: v.version,
                new: false,
            }),
            None => Err(Status::failed_precondition(format!(
                "process {}: register process models on the control partition; this partition \
                 takes them from there",
                model.name
            ))),
        };
    }
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
        let (sig, _slot) = kernel.signed(
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
        let (sig, _slot) = kernel.signed(
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
        if follows_control(kernel) {
            return Err(Status::failed_precondition(
                "remove processes on the control partition; this partition takes them from there",
            ));
        }
        let removed = kernel
            .processes
            .remove(kernel, &req.name, req.issued_at, sig)?;
        Ok(Response::new(ProcessRemoved { removed }))
    }

    async fn act(
        &self,
        request: Request<kernable_core::proto::ProcessActing>,
    ) -> Result<Response<kernable_core::proto::ProcessActed>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let _running = kernel.request()?;
        let (sig, _slot) = kernel.signed(
            &metadata,
            req.sig.as_ref(),
            &signing::process_act_message(&req.name, req.context.as_ref(), req.issued_at),
            req.issued_at,
            "process acting",
        )?;
        if !config::allowed(&kernel.permissions().process, &sig.key_id, &req.name) {
            return Err(Status::permission_denied(format!(
                "key may not set as whom process {} acts: it needs a grant process for it",
                req.name
            )));
        }
        if follows_control(kernel) {
            return Err(Status::failed_precondition(
                "set as whom a process acts on the control partition; this partition takes it \
                 from there",
            ));
        }
        let acting = req.context.unwrap_or_default();
        if acting.sender_identity.is_empty() {
            return Err(Status::invalid_argument(
                "the context names no sender identity",
            ));
        }
        let changed = kernel
            .processes
            .act(kernel, &req.name, acting, req.issued_at, sig)?;
        Ok(Response::new(kernable_core::proto::ProcessActed {
            changed,
        }))
    }

    async fn resolve(
        &self,
        request: Request<kernable_core::proto::ProcessResolution>,
    ) -> Result<Response<kernable_core::proto::ProcessResolved>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let _running = kernel.request()?;
        let (sig, _slot) = kernel.signed(
            &metadata,
            req.sig.as_ref(),
            &signing::process_resolve_message(
                &req.process,
                &req.case_id,
                &req.tenant_id,
                &req.action,
                req.issued_at,
            ),
            req.issued_at,
            "case resolution",
        )?;
        if !config::allowed(&kernel.permissions().process, &sig.key_id, &req.process) {
            return Err(Status::permission_denied(format!(
                "key may not resolve cases of process {}: it needs a grant process for it",
                req.process
            )));
        }
        if !req.tenant_id.is_empty() {
            kernel.owns(&req.tenant_id)?;
        }
        let (reply, answer) = tokio::sync::oneshot::channel();
        kernel.engine.send(crate::process_engine::Input::Resolve {
            process: req.process,
            case_id: req.case_id,
            tenant: req.tenant_id,
            action: req.action,
            by: sig.key_id.clone(),
            reply,
        });
        let tenant_id = answer
            .await
            .map_err(|_| Status::unavailable("the process engine does not run; retry"))??;
        Ok(Response::new(kernable_core::proto::ProcessResolved {
            tenant_id,
        }))
    }
}
