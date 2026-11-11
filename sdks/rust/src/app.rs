//! Apps: one service made of whole domains.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::task::JoinSet;

use crate::{
    Domain, Error, Kernable, ListenerHandle, Manifest, Process, Switch, Worker,
    domain::Runnable,
    log::{self, Level, Overview, Row},
};

/// An app: one service made of whole domains, for [`Kernable::app`]. Every aggregate, read
/// model and reactor created through its domains runs in it. Built with methods that each
/// return the changed app:
///
/// `let app = kernable.app(App::new().service("shop").version("1.0.0").domain(&shop_domain))?;`
/// then `app.run().await?`. To do something once everything is connected (e.g. in a test),
/// spawn it and wait: `let running = tokio::spawn({ let app = app.clone(); async move {
/// app.run().await } }); app.ready().await;`.
#[derive(Clone, Default)]
pub struct App {
    service: Option<String>,
    version: Option<String>,
    description: Option<String>,
    metadata: Vec<(String, String)>,
    domains: Vec<Domain>,
    switch: Option<Switch>,
}

impl App {
    /// An app without domains for the connection's service; add them with [`App::domain`].
    pub fn new() -> App {
        App::default()
    }

    /// The service it is. Default: the connection's ([`crate::Options::service`]).
    pub fn service(mut self, service: impl Into<String>) -> App {
        self.service = Some(service.into());
        self
    }

    /// Goes into the membership's metadata as `version`.
    pub fn version(mut self, version: impl Into<String>) -> App {
        self.version = Some(version.into());
        self
    }

    /// Goes into the membership's metadata as `description`.
    pub fn description(mut self, description: impl Into<String>) -> App {
        self.description = Some(description.into());
        self
    }

    /// More metadata, after `version` and `description`; kept in the order given.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> App {
        self.metadata.push((key.into(), value.into()));
        self
    }

    /// A domain of the app: its aggregates, read models and reactors run in it. Call it once per
    /// domain.
    pub fn domain(mut self, domain: &Domain) -> App {
        self.domains.push(domain.clone());
        self
    }

    /// When the app's deployment takes the service over from another one that serves it (see
    /// [`Switch`]): by default once its read models have caught up, if the deployment never
    /// served before; meanwhile it runs beside the serving deployment, its workers idle and its
    /// read models building. One that served before (an older deployment's app restarting) runs
    /// beside until an operator rolls back. Default:
    /// `KERNABLE_SWITCH` (`caught-up`, `now`, `never`), else [`Switch::CaughtUp`].
    pub fn switch(mut self, switch: Switch) -> App {
        self.switch = Some(switch);
        self
    }
}

struct AppInner {
    kernable: Kernable,
    /// The names of the app's domains.
    domains: Vec<String>,
    aggregates: Vec<Arc<dyn Runnable>>,
    manifest: Manifest,
    worker: Option<Worker>,
    listeners: Vec<ListenerHandle>,
    /// The domains' business processes, registered once the worker is ready.
    processes: Vec<Process>,
    /// Whether they are registered (also when there are none).
    registered: tokio::sync::watch::Sender<bool>,
    /// When it switches the service to its deployment.
    switch: Switch,
    /// Whether its membership is applied: its deployment serves the service.
    serving: tokio::sync::watch::Sender<bool>,
}

/// An [`App`] on a connection; see [`Kernable::app`]. Cloning gives another handle to the same
/// app, e.g. to call `stop()` or `ready()` while another task awaits `run()`.
#[derive(Clone)]
pub struct AppHandle {
    inner: Arc<AppInner>,
}

impl AppHandle {
    pub(crate) fn new(kernable: &Kernable, app: App) -> Result<AppHandle, Error> {
        let switch = match app.switch {
            Some(switch) => switch,
            None => Switch::from_env()?,
        };
        let service = app.service.unwrap_or_else(|| kernable.service.clone());
        if service.is_empty() {
            return Err(Error::Config(
                "an app needs a service: set it with App::service or Options::service".into(),
            ));
        }
        let aggregates: Vec<_> = app.domains.iter().flat_map(Domain::aggregates).collect();
        let listeners: Vec<_> = app.domains.iter().flat_map(Domain::listeners).collect();
        let mut manifest = Manifest::new().service(&service);
        manifest.workers = aggregates
            .iter()
            .map(|a| format!("{}.{}", a.domain().name(), a.name()))
            .collect();
        for listener in listeners.iter().filter(|l| !l.is_live()) {
            manifest = if listener.is_middleware() {
                manifest.middleware(listener)
            } else {
                manifest.listener(listener)
            };
        }
        let described = [("version", app.version), ("description", app.description)];
        for (key, value) in described {
            if let Some(value) = value {
                manifest = manifest.metadata(key, value);
            }
        }
        manifest.metadata.extend(app.metadata);
        let processes: Vec<Process> = app.domains.iter().flat_map(Domain::processes).collect();
        for process in &processes {
            // A typo stops the start, not a case.
            process.model()?;
            manifest = manifest.process(process);
        }
        let worker = (!aggregates.is_empty()).then(|| {
            let worker = Worker::new(kernable.client.clone(), service.clone());
            for aggregate in &aggregates {
                worker.runnable(Arc::clone(aggregate));
            }
            worker
        });
        let listeners = listeners
            .iter()
            .map(|l| ListenerHandle::new(kernable, l, service.clone()))
            .collect();
        let mut domains: Vec<String> = Vec::new();
        for domain in &app.domains {
            if !domains.iter().any(|d| d == domain.name()) {
                domains.push(domain.name().to_owned());
            }
        }
        Ok(AppHandle {
            inner: Arc::new(AppInner {
                kernable: kernable.clone(),
                domains,
                aggregates,
                manifest,
                worker,
                listeners,
                registered: tokio::sync::watch::Sender::new(processes.is_empty()),
                processes,
                switch,
                serving: tokio::sync::watch::Sender::new(false),
            }),
        })
    }

    /// The membership `run()` applies.
    pub fn manifest(&self) -> &Manifest {
        &self.inner.manifest
    }

    /// The worker of all the app's aggregates; none if it has none.
    pub fn worker(&self) -> Option<&Worker> {
        self.inner.worker.as_ref()
    }

    /// Every read model and reactor of the app, live ones too.
    pub fn listeners(&self) -> &[ListenerHandle] {
        &self.inner.listeners
    }

    /// Returns once the worker and every listener of the app are connected, its deployment
    /// serves the service ([`AppHandle::serving`]) and the domains' processes are registered;
    /// waits forever if one never connects, so wrap it in `tokio::time::timeout` where that
    /// matters.
    pub async fn ready(&self) {
        self.connected().await;
        self.serving().await;
        let mut registered = self.inner.registered.subscribe();
        let _ = registered.wait_for(|done| *done).await;
    }

    /// Returns once the worker and every listener of the app are connected; while another
    /// deployment serves, they are warming up beside it.
    pub async fn connected(&self) {
        if let Some(worker) = &self.inner.worker {
            worker.ready().await;
        }
        for listener in &self.inner.listeners {
            listener.ready().await;
        }
    }

    /// Returns once the app's membership is applied: its deployment serves the service. Right
    /// at the start, unless another deployment serves; then once the app's deployment has
    /// caught up and taken over ([`App::switch`]), or an operator switched to it.
    pub async fn serving(&self) {
        let mut serving = self.inner.serving.subscribe();
        let _ = serving.wait_for(|s| *s).await;
    }

    /// Ends `run`: stops the worker and every listener.
    pub fn stop(&self) {
        if let Some(worker) = &self.inner.worker {
            worker.stop();
        }
        for listener in &self.inner.listeners {
            listener.stop();
        }
    }

    /// Applies the membership (see [`Kernable::apply`]; it lists the domains' processes), then
    /// runs the worker and every listener until `stop()`. If another deployment serves the
    /// service, the kernel switches to this one only as [`App::switch`] says: until then the app
    /// runs beside it (workers idle, read models building) and applies its membership again
    /// every second. Once it serves and the worker is ready, it registers the domains' processes
    /// ([`Kernable::register_process`]): a process whose commands no worker runs yet would be
    /// refused. Once all are connected and it serves, it prints an overview of the app (unless
    /// `KERNABLE_LOG` is below `info`) instead of their connection lines. Fails only if the
    /// kernel refuses the membership for another reason than the switch ([`Error::Status`]); a
    /// failing handler does not end it (a command handler's error rejects the command, a
    /// listener handler's error makes the listener reconnect and get the event again), or the
    /// kernel refuses a process (then it stops). A panic in a worker or listener task is passed
    /// on.
    pub async fn run(&self) -> Result<(), Error> {
        let started = Instant::now();
        let manifest = self.inner.manifest.clone().switch(self.inner.switch);
        let applied = match self.inner.kernable.apply(manifest.clone()).await {
            Ok(()) => true,
            Err(e) if e.waits_for_switch() => {
                self.beside(&e);
                false
            }
            Err(e) => return Err(e),
        };
        if let Some(worker) = &self.inner.worker {
            worker.announce(false);
        }
        for listener in &self.inner.listeners {
            listener.announce(false);
        }
        let overview = {
            let app = self.clone();
            tokio::spawn(async move {
                app.ready().await;
                app.overview(started.elapsed().as_millis());
            })
        };
        let mut running = JoinSet::new();
        if let Some(worker) = self.inner.worker.clone() {
            running.spawn(async move { worker.run().await });
        }
        for listener in &self.inner.listeners {
            let listener = listener.clone();
            running.spawn(async move { listener.run().await });
        }
        let failed = Arc::new(Mutex::new(None));
        let setup = {
            let (app, failed) = (self.clone(), Arc::clone(&failed));
            tokio::spawn(async move {
                if !applied {
                    app.switch_later(manifest).await;
                }
                app.inner.serving.send_replace(true);
                if let Err(e) = app.register().await {
                    *failed.lock().unwrap() = Some(e);
                    app.stop();
                }
            })
        };
        while let Some(done) = running.join_next().await {
            if let Err(e) = done
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        }
        overview.abort();
        setup.abort();
        let failed = failed.lock().unwrap().take();
        failed.map_or(Ok(()), Err)
    }

    /// Logs why the app runs beside the deployment that serves.
    fn beside(&self, refused: &Error) {
        let deployment = self.inner.kernable.client.deployment.clone();
        let why = match refused {
            Error::Status(s) => s.message().to_owned(),
            other => other.to_string(),
        };
        let log = &self.inner.kernable.client.log;
        log.info("app", || {
            format!("deployment {deployment} runs beside the one that serves ({why})")
        });
    }

    /// Applies the membership every second until the kernel takes it.
    async fn switch_later(&self, manifest: Manifest) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self.inner.kernable.apply(manifest.clone()).await {
                Ok(()) => break,
                Err(e) if e.waits_for_switch() => {}
                Err(e) => {
                    let log = &self.inner.kernable.client.log;
                    log.warn("app", || format!("applying the membership failed: {e}"));
                }
            }
        }
        let deployment = self.inner.kernable.client.deployment.clone();
        let service = self.inner.manifest.service.clone().unwrap_or_default();
        let log = &self.inner.kernable.client.log;
        log.info("app", || {
            format!("deployment {deployment} serves {service} now")
        });
    }

    /// Registers the domains' processes once the worker is ready.
    async fn register(&self) -> Result<(), Error> {
        if self.inner.processes.is_empty() {
            return Ok(());
        }
        if let Some(worker) = &self.inner.worker {
            worker.ready().await;
        }
        for process in &self.inner.processes {
            self.inner.kernable.register_process(process).await?;
        }
        self.inner.registered.send_replace(true);
        Ok(())
    }

    /// Prints the app's overview, `ms` after `run()` started, if `info` lines are enabled.
    fn overview(&self, ms: u128) {
        let inner = &self.inner;
        let log = &inner.kernable.client.log;
        if !log.enabled(Level::Info) {
            return;
        }
        let domains = &inner.domains;
        // A single domain's own names go without it; with several, every name is qualified.
        let short = |domain: &str, name: &str| {
            if domains.len() == 1 && domains[0] == domain {
                name.to_owned()
            } else {
                format!("{domain}.{name}")
            }
        };
        let mut rows = Vec::new();
        for aggregate in &inner.aggregates {
            let mut types = aggregate.command_types();
            types.sort_unstable();
            rows.push(Row {
                kind: "worker",
                name: short(aggregate.domain().name(), aggregate.name()),
                details: vec![format!("runs {}", types.join(", "))],
            });
        }
        for handle in &inner.listeners {
            let listener = handle.listener();
            rows.push(Row {
                kind: if listener.is_live() {
                    "live"
                } else {
                    "listener"
                },
                name: short(listener.domain().name(), listener.name()),
                details: listener.details(handle.started_at(), &|t| log::relative(t, domains)),
            });
        }
        let m = &inner.manifest;
        let meta = |key: &str| {
            m.metadata
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        };
        let overview = Overview {
            service: m.service.clone().unwrap_or_default(),
            version: meta("version"),
            description: meta("description"),
            peer: log.peer.to_string(),
            workers: m.workers.len(),
            listeners: m.listeners.len(),
            middlewares: m.middlewares.len(),
            rows,
            ms,
        };
        log::write(&log::render_overview(&overview, log.color));
    }
}
