use std::{
    collections::HashMap,
    fs,
    io::{IsTerminal, Write},
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kernable_core::{
    proto::{
        ApplyRequest, Attribute, Command, ListenerRecord, Membership, Permission, RevokeRequest,
        SubmitRequest, When, WorkerRecord, command_result::Outcome,
        command_service_client::CommandServiceClient,
        membership_service_client::MembershipServiceClient,
        trust_service_client::TrustServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{BoxError, Config, GRANT_HEADER, Kernel, config, grants, serve};
use prost::Message;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "usage:
  kernable start --root <root.pub> [<flag> <value>]...
  kernable start --dev [<flag> <value>]...
                                          runs the kernel, trusting grants of the root keys; SIGHUP
                                          reads the kernel key again, SIGTERM or SIGINT finishes
                                          running requests and stops (flags: kernable start --help);
                                          --dev creates missing keys and a grant that lets app.key do
                                          everything, and turns the vault on, for trying kernable out
  kernable keys new <name>                writes <name>.key (secret) and <name>.pub (key ID)
  kernable grant <root.key> <name.pub> [--succeeds <old.pub>] <permission>...
                                          writes <name>.grant, signed by the root key; a permission
                                          is <action>=<type>,<type> or a bare type for every action
                                          (actions: submit run subscribe listen query answer check
                                          privileged vault_read vault_erase data_read data_write
                                          cache manage; manage takes service names)
  kernable revoke <addr> <root.key> <key.pub> <since>
                                          revokes a key from <since> on (UTC, 2026-10-27T09:00:00Z),
                                          for good; the kernel must be running (over TCP with
                                          mutual TLS: KERNABLE_TLS_CA, _CERT and _KEY)
  kernable checkpoint <store> <kernel.key>  signs the current head of the event log (kernel stopped)
  kernable verify <store> <kernel.pub>...   checks hash chain, checkpoints and signatures, with every
                                          key the kernel ever had (store: sqlite:<path>, file:<path>
                                          or a bare SQLite path); exit 2: intact, but something was
                                          signed by a revoked key (SUSPECT lines)
  kernable certs new <dir> <client>...    writes a development CA, a kernel certificate and one
                                          client certificate per name, for mTLS
  kernable listeners <store>              lists registered listeners with order, position, middleware
                                          and when they were last seen
  kernable listeners remove <store> <name>  removes a listener, and its middleware (kernel stopped)
  kernable apply <addr> <key> <manifest.json>
                                          applies a service's membership (JSON with service, workers,
                                          listeners, middlewares and metadata); once per deploy,
                                          with a key granted manage for the service (TLS as for
                                          revoke)
  kernable services <store>               lists the applied memberships, what they list, and what
                                          their services run beyond them
  kernable workers <store>                lists registered aggregates with their command types and
                                          when a worker for them was last seen
  kernable workers remove <store> <domain.Aggregate>
                                          unregisters an aggregate: its commands are refused (kernel
                                          stopped)
  kernable vault rekey <vault> <old.key> <new.key>
                                          re-encrypts the vault with a new key (kernel stopped)
  kernable schemas <store>                lists the highest stored payload version per event type
  kernable migrate <store>                upgrades a store to this build's data format (kernel stopped;
                                          back the store up first)
  kernable submit <addr> <client.key> <tenant> <domain> <aggregate_type> <aggregate_id> <command_type> <payload>
                                          signs and submits a command (addr: http://host:port, or
                                          unix:/path on Unix; TLS as for revoke), with <client>.grant if it exists";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match args.as_slice() {
        ["start", "--help"] => {
            println!("{}", start_help());
            return ExitCode::SUCCESS;
        }
        ["start", flags @ ..] => {
            let dev = flags.contains(&"--dev");
            let mut flags: Vec<&str> = flags.iter().copied().filter(|f| *f != "--dev").collect();
            let mut notes = None;
            if dev {
                match development_setup() {
                    Ok(n) => notes = Some(n),
                    Err(e) => {
                        eprintln!("error: {e}");
                        return ExitCode::FAILURE;
                    }
                }
                let given = |flag: &str, var: &str| {
                    flags.iter().any(|f| f.starts_with(flag)) || std::env::var_os(var).is_some()
                };
                let root = !given("--root", "KERNABLE_ROOT");
                let vault = !given("--vault-key", "KERNABLE_VAULT_KEY");
                if !given("--ui-listen", "KERNABLE_UI_LISTEN") {
                    flags.extend(["--ui-listen", "127.0.0.1:7234"]);
                }
                if root {
                    flags.extend(["--root", "root.pub"]);
                }
                if vault {
                    flags.extend(["--vault-key", "vault.key"]);
                }
            }
            return match Config::from_args(&flags, |var| std::env::var(var).ok()) {
                Ok(config) => start(config, notes).await,
                Err(e) => {
                    eprintln!("error: {e}\n\n{}", start_help());
                    ExitCode::from(2)
                }
            };
        }
        ["grant", root, key, permissions @ ..] if !permissions.is_empty() => {
            grant(root, key, permissions)
        }
        ["revoke", addr, root, key, since] => revoke(addr, root, key, since).await,
        ["keys", "new", name] => keys_new(name),
        ["checkpoint", store, key] => checkpoint(store, key),
        ["verify", store, keys @ ..] if !keys.is_empty() => {
            return verify(store, keys).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            });
        }
        ["vault", "rekey", vault, old, new] => vault_rekey(vault, old, new),
        ["certs", "new", dir, clients @ ..] if !clients.is_empty() => {
            match kernable_core::certs::create(std::path::Path::new(dir), clients) {
                Ok(()) => {
                    println!(
                        "wrote ca.tls.crt, kernel.tls.crt/.tls.key and client certificates to {dir}"
                    );
                    Ok(true)
                }
                Err(e) => Err(e.to_string().into()),
            }
        }
        ["listeners", store] => listeners(store),
        ["schemas", store] => schemas(store),
        ["migrate", store] => migrate(store),
        ["listeners", "remove", store, name] => remove_listener(store, name),
        ["workers", store] => workers(store),
        ["services", store] => services(store),
        ["apply", addr, key, manifest] => apply(addr, key, manifest).await,
        ["workers", "remove", store, aggregate] => remove_worker(store, aggregate),
        [
            "submit",
            addr,
            key,
            tenant,
            domain,
            agg_type,
            agg_id,
            cmd_type,
            payload,
        ] => {
            let cmd = Command {
                command_id: random_id(),
                tenant_id: tenant.to_string(),
                domain: domain.to_string(),
                aggregate_type: agg_type.to_string(),
                aggregate_id: agg_id.to_string(),
                command_type: cmd_type.to_string(),
                payload: payload.as_bytes().to_vec(),
                client_sig: None,
                context: None,
                vault_refs: Vec::new(),
            };
            submit(addr, key, cmd).await
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// `kernable start --dev`: creates what is missing of `root.key`, `kernel.key`, `vault.key`,
/// `app.key` and `app.grant` in the working directory, with a grant that lets `app` do everything.
/// Returns what it did, to show after the banner.
fn development_setup() -> Result<Vec<String>> {
    let mut notes = Vec::new();
    for name in ["root", "kernel", "vault", "app"] {
        if !Path::new(&format!("{name}.key")).exists() {
            write_key(name)?;
            notes.push(format!("created {name}.key"));
        }
    }
    if !Path::new("app.grant").exists() {
        let root = signing::read_signing_key("root.key")?;
        let key_id = key_id_of("app.pub")?;
        let all = vec![Permission {
            action: "*".into(),
            types: vec!["*".into()],
        }];
        let grant = signing::grant(&root, &key_id, all, "", now_ms());
        fs::write("app.grant", format!("{}\n", hex(&grant.encode_to_vec())))?;
        notes.push("created app.grant".into());
    }
    notes.push(
        "development mode: app.key may do everything. In production, start with --root only and \
         grant each service what it needs (kernable grant)."
            .into(),
    );
    Ok(notes)
}

fn start_help() -> String {
    let mut help = String::from(
        "kernable start flags (each also as an environment variable):\n  \
         --dev                      creates root.key, kernel.key, vault.key, app.key and app.grant\n                             \
         (app may do everything) if missing; trusts root.pub unless --root is given and\n                             \
         uses vault.key unless --vault-key is given\n",
    );
    for (flag, var, default, _) in config::FLAGS {
        let default = if default.is_empty() {
            String::new()
        } else {
            format!(" (default {default})")
        };
        help.push_str(&format!("  {flag:<26} {var}{default}\n"));
    }
    help
}

/// Runs the kernel; `dev_notes` is what `--dev` set up, if it was given.
async fn start(config: Config, dev_notes: Option<Vec<String>>) -> ExitCode {
    // KERNABLE_LOG sets the level filter (default `info`); KERNABLE_LOG_FORMAT=json logs JSON.
    let filter = EnvFilter::try_from_env("KERNABLE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let logs = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if std::env::var("KERNABLE_LOG_FORMAT").as_deref() == Ok("json") {
        logs.json().init();
    } else {
        // For a person at a terminal only; services and pipes get the log lines alone.
        if std::io::stderr().is_terminal() {
            let dir = std::env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_default();
            eprintln!("{}", banner(&config, dev_notes.is_some(), &dir));
        }
        logs.init();
    }
    for note in dev_notes.iter().flatten() {
        eprintln!("{note}");
    }
    match run_kernel(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "kernel stopped");
            ExitCode::FAILURE
        }
    }
}

/// The start banner: the name, the version and platform, and where the kernel keeps what.
fn banner(config: &Config, dev: bool, dir: &str) -> String {
    const LOGO: [&str; 3] = [
        r" _     _ _______ ______  __   _ _______ ______  _       _______",
        r" |____/  |______ |_____/ | \  | |_____| |_____] |       |______",
        r" |    \_ |______ |    \_ |  \_| |     | |_____] |_____  |______",
    ];
    // `sqlite:kernable.db` → `sqlite - kernable.db`, `memory:` → `memory`.
    let store = |s: &str| match s.split_once(':') {
        Some((kind, "")) => kind.to_owned(),
        Some((kind, place)) => format!("{kind} - {place}"),
        None => s.to_owned(),
    };
    let listen = if config.listen.starts_with("unix:") {
        format!("{} (Unix socket)", config.listen)
    } else if config.tls.is_some() {
        format!("{} (mutual TLS)", config.listen)
    } else {
        format!("{} (plain TCP, this machine only)", config.listen)
    };
    let roots = match config.roots.as_slice() {
        [one] => format!("root {}…", &one[..one.len().min(12)]),
        many => format!("{} roots", many.len()),
    };
    let rows = [
        ("Working Directory", dir.to_owned()),
        ("Listen", listen),
        ("Event Store", store(&config.store)),
        ("Command Store", store(&config.store)),
        ("Snapshot Store", store(&config.snapshots)),
        ("Data Store", store(&config.data)),
        ("Cache Store", store(&config.cache)),
        (
            "Vault",
            config
                .vault
                .as_ref()
                .map_or("disabled".into(), |v| store(&v.store)),
        ),
        (
            "Keys",
            format!("{roots} · kernel {}", config.kernel_key.display()),
        ),
        (
            "Metrics",
            config
                .metrics_listen
                .clone()
                .unwrap_or_else(|| "disabled".into()),
        ),
        (
            "UI",
            config
                .ui_listen
                .as_ref()
                .map_or("disabled".into(), |addr| format!("http://{addr}")),
        ),
        (
            "Mode",
            if dev {
                "development (--dev)"
            } else {
                "production"
            }
            .into(),
        ),
    ];
    let mut out = format!(
        "\n{}\n{}   » {}\n{}   » {} » {}\n\n",
        LOGO[0],
        LOGO[1],
        env!("CARGO_PKG_VERSION"),
        LOGO[2],
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    for (name, value) in rows {
        out.push_str(&format!(" » {name:.<22} {value}\n"));
    }
    out
}

async fn run_kernel(config: Config) -> std::result::Result<(), BoxError> {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %config.listen,
        tls = config.tls.is_some(),
        "kernel starting"
    );
    let kernel = Kernel::open(config)?;
    let mut server = tokio::spawn(serve(Arc::clone(&kernel)));
    let mut hangup = Hangup::new()?;
    loop {
        tokio::select! {
            result = &mut server => return result?.and_then(|()| kernel.shutdown()),
            _ = hangup.recv() => match kernel.reload() {
                Ok(()) => tracing::info!("kernel key read again"),
                Err(e) => tracing::error!(error = %e, "reading the kernel key failed; the previous key stays"),
            },
            signal = shutdown_signal() => {
                // Workers and listeners stay connected, so running commands can finish and settle.
                tracing::info!(signal = signal?, "shutting down: finishing running requests");
                if !kernel.drain().await {
                    tracing::warn!("drain timed out: unfinished commands run again after a restart");
                }
                server.abort();
                return kernel.shutdown();
            }
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = term.recv() => Ok("SIGTERM"),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<&'static str> {
    tokio::signal::ctrl_c().await.map(|()| "Ctrl+C")
}

/// SIGHUP on Unix; elsewhere it never comes.
#[cfg(unix)]
struct Hangup(tokio::signal::unix::Signal);

#[cfg(unix)]
impl Hangup {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Hangup(signal(SignalKind::hangup())?))
    }

    async fn recv(&mut self) -> Option<()> {
        self.0.recv().await
    }
}

#[cfg(not(unix))]
struct Hangup;

#[cfg(not(unix))]
impl Hangup {
    fn new() -> std::io::Result<Self> {
        Ok(Hangup)
    }

    async fn recv(&mut self) -> Option<()> {
        std::future::pending().await
    }
}

/// Writes `<name>.grant` next to `<name>.pub`: the grant, signed by the root key, hex encoded.
fn grant(root: &str, key: &str, args: &[&str]) -> Result<bool> {
    let root = signing::read_signing_key(root)?;
    let key_id = key_id_of(key)?;
    let mut succeeds = String::new();
    let mut permissions: Vec<Permission> = Vec::new();
    let mut rest = args.iter();
    while let Some(&arg) = rest.next() {
        if arg == "--succeeds" {
            succeeds = key_id_of(rest.next().ok_or("--succeeds needs a key")?)?;
            continue;
        }
        let (action, types) = arg.split_once('=').unwrap_or(("*", arg));
        permissions.push(Permission {
            action: action.to_owned(),
            types: types.split(',').map(str::to_owned).collect(),
        });
    }
    let grant = signing::grant(&root, &key_id, permissions, &succeeds, now_ms());
    if let Some(reason) = grants::malformed(&grant) {
        return Err(reason.into());
    }
    let path = format!("{}.grant", key.strip_suffix(".pub").unwrap_or(key));
    fs::write(&path, format!("{}\n", hex(&grant.encode_to_vec())))?;
    for p in &grant.permissions {
        println!("{} {}", p.action, p.types.join(","));
    }
    if !succeeds.is_empty() {
        println!("succeeds {succeeds}");
    }
    println!("wrote {path}");
    Ok(true)
}

async fn revoke(addr: &str, root: &str, key: &str, since: &str) -> Result<bool> {
    let root = signing::read_signing_key(root)?;
    let key_id = key_id_of(key)?;
    let since = config::utc_ms(since)
        .ok_or_else(|| format!("{since} is not a UTC time like 2026-10-27T09:00:00Z"))?;
    let issued_at = now_ms();
    let request = RevokeRequest {
        root_sig: Some(signing::sign(
            &root,
            &signing::revoke_message(&key_id, since, issued_at),
        )),
        key_id: key_id.clone(),
        since,
        issued_at,
        protocol: kernable_core::PROTOCOL,
    };
    TrustServiceClient::new(connect(addr).await?)
        .revoke(request)
        .await?;
    println!("revoked {key_id}");
    Ok(true)
}

/// A channel to the kernel at `addr`, over mutual TLS when `KERNABLE_TLS_CA`,
/// `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY` name the files, as for the SDKs.
async fn connect(addr: &str) -> Result<tonic::transport::Channel> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    match (
        var("KERNABLE_TLS_CA"),
        var("KERNABLE_TLS_CERT"),
        var("KERNABLE_TLS_KEY"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let tls = transport::Tls {
                ca: ca.into(),
                cert: cert.into(),
                key: key.into(),
            };
            Ok(transport::connect_tls(addr, &tls)
                .await
                .map_err(|e| e.to_string())?)
        }
        _ => Ok(transport::connect(addr).await?),
    }
}

/// A key ID given as itself or as the path of a `.pub` file.
fn key_id_of(key: &str) -> Result<String> {
    if signing::verifying_key_from_hex(key).is_ok() {
        return Ok(key.to_owned());
    }
    Ok(signing::key_id(&signing::read_verifying_key(key)?))
}

fn keys_new(name: &str) -> Result<bool> {
    println!("{}", write_key(name)?);
    Ok(true)
}

/// Writes `<name>.key` (secret, never overwritten) and `<name>.pub`; returns the key ID.
fn write_key(name: &str) -> Result<String> {
    let key = signing::generate_key();
    let key_id = signing::key_id(&key.verifying_key());
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    // Readable by the owner only; on other systems the directory's permissions apply.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(format!("{name}.key"))?
        .write_all(format!("{}\n", hex(key.as_bytes())).as_bytes())?;
    fs::write(format!("{name}.pub"), format!("{key_id}\n"))?;
    Ok(key_id)
}

fn checkpoint(store: &str, key: &str) -> Result<bool> {
    let key = signing::read_signing_key(key)?;
    match kernable_store::open(store)?.checkpoint(&key, now_ms())? {
        Some(cp) => println!("checkpoint at position {}", cp.global_position),
        None => println!("no events yet"),
    }
    Ok(true)
}

/// 0: intact; 1: tampered; 2: intact, but something was signed by a revoked key.
fn verify(store: &str, keys: &[&str]) -> Result<ExitCode> {
    let key_ids = keys
        .iter()
        .map(|k| Ok(signing::key_id(&signing::read_verifying_key(k)?)))
        .collect::<Result<Vec<_>>>()?;
    let report = kernable_store::open_read_only(store)?.verify(&key_ids)?;
    println!(
        "{} events, {} commands, {} checkpoints, {} events after the last checkpoint",
        report.events, report.commands, report.checkpoints, report.unanchored
    );
    for p in &report.problems {
        println!("PROBLEM {p}");
    }
    for s in &report.suspects {
        println!("SUSPECT {s}");
    }
    Ok(if !report.is_ok() {
        println!("TAMPERED");
        ExitCode::FAILURE
    } else if !report.suspects.is_empty() {
        println!("OK, {} suspect", report.suspects.len());
        ExitCode::from(2)
    } else {
        println!("OK");
        ExitCode::SUCCESS
    })
}

fn vault_rekey(vault: &str, old: &str, new: &str) -> Result<bool> {
    let read = |path: &str| -> Result<[u8; 32]> {
        let text = fs::read_to_string(path)?;
        let text = text.trim();
        let bytes: Option<Vec<u8>> = (0..text.len())
            .step_by(2)
            .map(|i| {
                text.get(i..i + 2)
                    .and_then(|b| u8::from_str_radix(b, 16).ok())
            })
            .collect();
        bytes
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| format!("{path}: not a 32-byte key in hex").into())
    };
    let count = kernable_store::vault::Vault::open(vault, &read(old)?)?.rekey(&read(new)?)?;
    println!("re-encrypted {count} values; start the kernel with --vault-key <new.key>");
    Ok(true)
}

fn listeners(store: &str) -> Result<bool> {
    let store = kernable_store::open_read_only(store)?;
    let memberships = memberships_of(store.as_ref())?;
    for l in store.listeners()? {
        let types = match l.event_types.as_slice() {
            [] => "*".to_owned(),
            types => types.join(","),
        };
        let middleware: String = l
            .middlewares
            .iter()
            .map(|m| {
                let when = if m.when() == When::After {
                    "after"
                } else {
                    "before"
                };
                format!(
                    " middleware {when} order {} checks {}",
                    m.order,
                    m.types.join(",")
                )
            })
            .collect();
        let unlisted = memberships.get(&l.service).is_some_and(|m| {
            !m.listeners
                .iter()
                .chain(&m.middlewares)
                .any(|n| *n == l.name)
        });
        let service = match (l.service.as_str(), unlisted) {
            ("", _) => String::new(),
            (service, false) => format!(" service {service}"),
            (service, true) => format!(" service {service} (not listed: counts for nobody)"),
        };
        println!(
            "{} order {} position {} types {types} key {}{service}{middleware} last seen {}",
            l.name,
            l.order,
            l.position,
            l.key_id,
            config::utc(l.last_seen)
        );
    }
    Ok(true)
}

fn workers(store: &str) -> Result<bool> {
    for w in kernable_store::open_read_only(store)?.workers()? {
        let service = match w.service.as_str() {
            "" => String::new(),
            service => format!(" service {service}"),
        };
        println!(
            "{} commands {} key {}{service} last seen {}",
            w.aggregate_type,
            w.command_types.join(","),
            w.key_id,
            config::utc(w.last_seen)
        );
    }
    Ok(true)
}

/// The applied memberships by service.
fn memberships_of(store: &dyn kernable_store::Store) -> Result<HashMap<String, Membership>> {
    Ok(store
        .memberships()?
        .into_iter()
        .filter_map(|r| r.membership)
        .map(|m| (m.service.clone(), m))
        .collect())
}

fn services(store: &str) -> Result<bool> {
    let store = kernable_store::open_read_only(store)?;
    let listeners = store.listeners()?;
    let workers = store.workers()?;
    for r in store.memberships()? {
        let (Some(m), Some(sig)) = (r.membership, r.sig) else {
            continue;
        };
        let metadata: String = m
            .metadata
            .iter()
            .map(|a| format!(" {}={}", a.key, a.value))
            .collect();
        println!(
            "{}{metadata} (applied {} by {})",
            m.service,
            config::utc(r.applied_at),
            sig.key_id
        );
        let registered = |found: bool| if found { "" } else { " (never registered)" };
        for a in &m.workers {
            let found = workers.iter().any(|w| w.aggregate_type == *a);
            println!("  worker {a}{}", registered(found));
        }
        for (kind, names) in [("listener", &m.listeners), ("middleware", &m.middlewares)] {
            for name in names {
                let found = listeners.iter().any(|l| l.name == *name);
                println!("  {kind} {name}{}", registered(found));
            }
        }
        let listed = |name: &String| m.listeners.iter().chain(&m.middlewares).any(|n| n == name);
        for l in listeners
            .iter()
            .filter(|l| l.service == m.service && !listed(&l.name))
        {
            println!("  dynamic listener {}", l.name);
        }
    }
    Ok(true)
}

/// Applies the membership in a manifest file, signed with `key_path` (granted `manage`).
async fn apply(addr: &str, key_path: &str, manifest: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let json: serde_json::Value = serde_json::from_str(&fs::read_to_string(manifest)?)
        .map_err(|e| format!("{manifest}: {e}"))?;
    let strings = |field: &str| -> Result<Vec<String>> {
        match json.get(field) {
            None => Ok(Vec::new()),
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|_| format!("{manifest}: {field} is a list of strings").into()),
        }
    };
    let metadata: Vec<Attribute> = match json.get("metadata") {
        None => Vec::new(),
        Some(serde_json::Value::Object(map)) => map
            .iter()
            .map(|(key, value)| Attribute {
                key: key.clone(),
                value: value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned),
            })
            .collect(),
        Some(_) => return Err(format!("{manifest}: metadata is an object").into()),
    };
    let membership = Membership {
        service: json
            .get("service")
            .and_then(|s| s.as_str())
            .ok_or_else(|| format!("{manifest}: no service"))?
            .to_owned(),
        workers: strings("workers")?,
        listeners: strings("listeners")?,
        middlewares: strings("middlewares")?,
        metadata,
        issued_at: now_ms(),
    };
    let mut request = tonic::Request::new(ApplyRequest {
        sig: Some(signing::sign(
            &key,
            &signing::membership_message(&membership),
        )),
        membership: Some(membership.clone()),
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    MembershipServiceClient::new(connect(addr).await?)
        .apply(request)
        .await?;
    println!(
        "applied {}: {} workers, {} listeners, {} middlewares",
        membership.service,
        membership.workers.len(),
        membership.listeners.len(),
        membership.middlewares.len()
    );
    Ok(true)
}

/// Sends the key's grant with `request`, if it lies next to the key file.
fn with_key_grant<T>(request: &mut tonic::Request<T>, key_path: &str) -> Result<()> {
    let grant = format!(
        "{}.grant",
        key_path.strip_suffix(".key").unwrap_or(key_path)
    );
    if let Ok(grant) = fs::read_to_string(&grant) {
        request
            .metadata_mut()
            .insert(GRANT_HEADER, grant.trim().parse()?);
    }
    Ok(())
}

fn remove_worker(store: &str, aggregate: &str) -> Result<bool> {
    let store = kernable_store::open(store)?;
    if !store
        .workers()?
        .iter()
        .any(|w| w.aggregate_type == aggregate)
    {
        println!("no aggregate {aggregate}");
        return Ok(false);
    }
    store.save_workers(&[WorkerRecord {
        aggregate_type: aggregate.to_owned(),
        removed: true,
        ..Default::default()
    }])?;
    println!("removed {aggregate}");
    Ok(true)
}

fn schemas(store: &str) -> Result<bool> {
    for (event_type, schema) in kernable_store::open_read_only(store)?.event_schemas()? {
        println!("{event_type} schema {schema}");
    }
    Ok(true)
}

fn migrate(store: &str) -> Result<bool> {
    let (backend, path) = kernable_store::backend(store);
    if backend != "memory" && !std::path::Path::new(path).exists() {
        return Err(format!("no store at {path}").into());
    }
    let format = kernable_store::FORMAT;
    match kernable_store::migrate(store) {
        Ok(from) if from == format => println!("store is at format {format}; nothing to migrate"),
        Ok(from) if backend == "file" => println!(
            "migrated from format {from} to {format}; the old log is kept as {path}.format-{from}"
        ),
        Ok(from) => println!("migrated from format {from} to {format}"),
        Err(kernable_store::Error::Format(found)) if found < format => {
            println!(
                "no migration from format {found}: it comes from a development build; \
                 create a new store"
            );
            return Ok(false);
        }
        Err(e) => return Err(e.into()),
    }
    Ok(true)
}

fn remove_listener(store: &str, name: &str) -> Result<bool> {
    let store = kernable_store::open(store)?;
    if !store.listeners()?.iter().any(|l| l.name == name) {
        println!("no listener {name}");
        return Ok(false);
    }
    store.save_listeners(&[ListenerRecord {
        name: name.to_owned(),
        removed: true,
        ..Default::default()
    }])?;
    println!("removed {name}");
    Ok(true)
}

async fn submit(addr: &str, key_path: &str, mut cmd: Command) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    cmd.client_sig = Some(signing::sign(&key, &signing::command_message(&cmd)));
    println!("command {}", cmd.command_id);
    let mut client = CommandServiceClient::new(connect(addr).await?);
    let mut request = tonic::Request::new(SubmitRequest {
        command: Some(cmd),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    });
    with_key_grant(&mut request, key_path)?;
    match client.submit(request).await?.into_inner().outcome {
        Some(Outcome::Committed(c)) => {
            for e in c.events {
                let payload = String::from_utf8_lossy(&e.payload);
                println!(
                    "committed #{} v{} {} {payload}",
                    e.global_position, e.version, e.event_type
                );
            }
            if !c.settled {
                println!("not settled: some listeners have not processed the events yet");
            }
            Ok(true)
        }
        Some(Outcome::Rejected(r)) => {
            println!("rejected {}: {}", r.code, r.message);
            Ok(false)
        }
        Some(Outcome::Accepted(_)) | None => Ok(false),
    }
}

fn random_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("OS randomness unavailable");
    hex(&bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_banner_names_where_the_kernel_keeps_what() {
        let root = signing::key_id(&signing::generate_key().verifying_key());
        let args = [
            "--root",
            &root,
            "--vault-key",
            "vault.key",
            "--metrics-listen",
            "127.0.0.1:9233",
            "--ui-listen",
            "127.0.0.1:7234",
        ];
        let config = Config::from_args(&args, |_| None).unwrap();
        let banner = banner(&config, true, "/tmp/demo");
        assert!(banner.contains("|____/"), "{banner}");
        assert!(banner.contains(&format!("» {}", env!("CARGO_PKG_VERSION"))));
        for line in [
            " » Working Directory..... /tmp/demo",
            " » Listen................ 127.0.0.1:7233 (plain TCP, this machine only)",
            " » Event Store........... sqlite - kernable.db",
            " » Snapshot Store........ memory",
            " » Data Store............ file - data",
            " » Cache Store........... memory",
            " » Keys.................. root ",
            " » Vault................. sqlite - vault.db",
            " » Metrics............... 127.0.0.1:9233",
            " » UI.................... http://127.0.0.1:7234",
            " » Mode.................. development (--dev)",
        ] {
            assert!(banner.contains(line), "{line:?} missing in\n{banner}");
        }
    }
}
