#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use std::{
    collections::HashMap,
    fs,
    io::{self, IsTerminal, Write},
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kernable_core::{
    proto::{
        ApplyRequest, Attribute, Command, ListenerRecord, Membership, Permission, RetryRequest,
        RevokeRequest, SubmitRequest, When, WorkerRecord, command_result::Outcome,
        command_service_client::CommandServiceClient,
        listener_service_client::ListenerServiceClient,
        membership_service_client::MembershipServiceClient,
        trust_service_client::TrustServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{BoxError, Config, GRANT_HEADER, Kernel, config, grants, serve};
use prost::Message;
use tracing_subscriber::EnvFilter;
use zeroize::Zeroizing;

const USAGE: &str = "usage:

Run the kernel:
  kernable start --root <root.pub> [<flag> <value>]...
  kernable start --dev [<flag> <value>]...
                                          Runs the kernel in the foreground, trusting grants signed
                                          by the root keys; the flags are listed by kernable start
                                          --help. On Unix, SIGHUP reads the kernel key again.
                                          SIGTERM or SIGINT refuses new requests, lets running ones
                                          finish and stops. --dev is for trying kernable out: it
                                          creates root.key, kernel.key, vault.key, app.key and
                                          app.grant in the working directory if missing (app may do
                                          everything) and defaults to --root root.pub, --vault-key
                                          vault.key and --ui-listen 127.0.0.1:7234. Exit 2 on
                                          invalid flags; exit 1 if the kernel stops on an error.
  kernable demo [--ui-listen <addr>]      Runs kernable for presentations: a kernel on a new store
                                          in a temporary directory, with a small bank built in,
                                          shown step by step on the Stage at http://<addr>/demo
                                          (default 127.0.0.1:7234). Reset starts again on a new
                                          store; Ctrl-C ends it and removes everything.

Keys and trust:
  kernable keys new <name>                Writes <name>.key (the secret key, readable by the owner
                                          only, never overwritten) and <name>.pub (the key ID), and
                                          prints the key ID.
  kernable grant <root.key> <name.pub> [--succeeds <old.pub>] <permission>...
                                          Writes <name>.grant, signed by the root key, and prints
                                          its permissions. A permission is <action>=<type>,<type>...
                                          or a bare <type> for every action. Actions: submit run
                                          subscribe listen query answer check privileged vault_read
                                          vault_erase data_read data_write cache manage. A type is
                                          *, <domain>.* or <domain>.<Type>; manage takes service
                                          names, privileged takes bypass_authorization. --succeeds
                                          lets this key take over the aggregates and listeners
                                          registered with <old.pub>. A newer grant for the same key
                                          replaces the older one.
  kernable revoke <addr> <root.key> <key.pub> <since>
                                          Revokes a key for good from <since> on (UTC, like
                                          2026-10-27T09:00:00Z): the kernel refuses it, ends its
                                          connections and rejects its queued commands accepted since
                                          then (key_revoked). The kernel must be running.
  kernable certs new <dir> <client>...    Writes a development CA (ca.tls.crt), a kernel certificate
                                          (kernel.tls.crt, kernel.tls.key) and a certificate per
                                          <client> (<client>.tls.crt, <client>.tls.key) to <dir>,
                                          for mutual TLS. The CA key is not kept: issue a new set to
                                          add clients. Production brings its own PKI.

Deploy and inspect:
  kernable apply <addr> <key> <manifest.json>
                                          Applies a service's membership: a JSON manifest with
                                          service, workers, listeners, middlewares and metadata.
                                          Signed with <key>, whose grant needs manage=<service>; the
                                          grant file next to the key (app.grant for app.key) is sent
                                          along. The kernel must be running. Once per deploy.
  kernable services <store>               Lists each applied membership: the service, its metadata,
                                          who applied it and when, and the workers, listeners and
                                          middlewares it lists, marking those that never registered;
                                          then the service's dynamic listeners, registered but not
                                          listed.
  kernable workers <store>                Lists the registered aggregate types with their command
                                          types, key, service and when a worker for them was last
                                          seen.
  kernable listeners <store>              Lists the registered listeners with order, position, event
                                          types, key, service, middleware and when they were last
                                          seen. A listener its service's membership does not list is
                                          marked: it counts for nobody.
  kernable listeners parked <store>       Lists the events listeners parked: the listener, position,
                                          event type, code, attempts, when, whether a retry is
                                          asked for, and the message (for a handler error only the
                                          error's type; the listener's log has the rest).
  kernable schemas <store>                Lists the highest stored schema version per event type.

Parked events, with the kernel running:
  kernable listeners retry <addr> <key> <listener> <position|--all>
                                          Delivers the listener's parked event at <position> again,
                                          or all of them, the next time it is connected; if it
                                          fails again, it is parked again. Signed with <key>, whose
                                          grant needs manage=<service> of the listener (manage=*
                                          for one without a service); the grant file next to the
                                          key is sent along. Exit 1 if none of them is parked.
  kernable listeners dismiss <addr> <key> <listener> <position|--all>
                                          Removes the parked event at <position>, or all of them,
                                          without delivering them again. Like retry otherwise.

Check the store:
  kernable verify <store> <kernel.pub>...
                                          Recomputes the hash chain and checks every checkpoint and
                                          signature, with every key the kernel ever had: give all
                                          its .pub files. Read-only, so the kernel may run. Prints
                                          the counts, PROBLEM and SUSPECT lines, then OK or
                                          TAMPERED. Exit 0: intact. Exit 1: tampered, or the check
                                          failed. Exit 2: intact, but something was signed by a key
                                          at or after its revocation (SUSPECT lines). Events after
                                          the last checkpoint are counted: cutting them off could
                                          not be detected.
  kernable checkpoint <store> <kernel.key>
                                          Signs a checkpoint of the newest event now, as the running
                                          kernel does every --checkpoint-interval-ms. Kernel
                                          stopped.

Maintenance, with the kernel stopped:
  kernable listeners remove <store> <name>
                                          Removes a listener and its middleware, so they no longer
                                          count. If a listener connects again under that name, it
                                          starts at the newest event. Exit 1 if there is no such
                                          listener.
  kernable workers remove <store> <domain.Aggregate>
                                          Unregisters an aggregate type: its commands are refused
                                          (NOT_FOUND) unless a membership lists it. Exit 1 if there
                                          is no such aggregate.
  kernable vault rekey <vault> <old.key> <new.key>
                                          Re-encrypts every vault value with a new key; then start
                                          the kernel with --vault-key <new.key>. <vault> is the
                                          --vault-store, for example sqlite:vault.db. A key
                                          encrypts at most 2^32 values (each with a random nonce):
                                          the kernel warns at startup past half of that and refuses
                                          new values at the limit, until the vault is rekeyed.

Scripting:
  kernable submit <addr> <client.key> <tenant> <domain> <aggregate_type> <aggregate_id> <command_type> <payload>
                                          Signs a command with a new ID and submits it, waiting
                                          until it has settled; the grant file next to the key is
                                          sent along. <payload> is sent as given, usually JSON.
                                          Prints the command ID, then a committed line per event
                                          with its global position, version, type and payload, or
                                          rejected <code>: <message> with exit 1. It cannot seal
                                          vault fields: not for commands with personal or secret
                                          values.

Arguments and exit codes:
  <addr> is host:port, http://host:port, or unix:<path> on Unix. To reach a kernel over mutual TLS,
  set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY to PEM files, as for the SDKs. <store>
  is the kernel's --store: sqlite:<path>, a bare SQLite path, or file:<path>. Exit codes, unless a
  command says otherwise: 0 success, 1 failure, 2 unknown command or wrong arguments.";

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
                let ui = !given("--ui-listen", "KERNABLE_UI_LISTEN");
                let durability = !given("--durability", "KERNABLE_DURABILITY");
                if ui {
                    flags.extend(["--ui-listen", "127.0.0.1:7234"]);
                }
                if durability {
                    flags.extend(["--durability", "normal"]);
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
        ["demo"] => demo("127.0.0.1:7234").await,
        ["demo", "--ui-listen", addr] => demo(addr).await,
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
        ["listeners", "remove", store, name] => remove_listener(store, name),
        ["listeners", "parked", store] => parked(store),
        [
            "listeners",
            op @ ("retry" | "dismiss"),
            addr,
            key,
            name,
            which,
        ] => retry(addr, key, name, which, *op == "dismiss").await,
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

async fn demo(ui: &str) -> Result<bool> {
    kernable_kernel::demo::run(ui)
        .await
        .map_err(|e| e.to_string())?;
    Ok(true)
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
         (app may do everything) if missing; trusts root.pub unless --root is given,\n                             \
         uses vault.key unless --vault-key is given, and serves the UI on\n                             \
         127.0.0.1:7234 unless --ui-listen is given\n",
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
            "Durability",
            match config.durability {
                kernable_store::Durability::Full => "full (on the disk at every commit)",
                kernable_store::Durability::Normal => "normal (survives a crash, not a power loss)",
            }
            .into(),
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
        .write_all(Zeroizing::new(format!("{}\n", hex::encode(key.as_bytes()))).as_bytes())?;
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
    let read = |path: &str| -> Result<Zeroizing<[u8; 32]>> {
        signing::read_secret(path).map_err(|e| match e.kind() {
            io::ErrorKind::InvalidData => format!("{path}: not a 32-byte key in hex").into(),
            _ => e.into(),
        })
    };
    let (old, new) = (read(old)?, read(new)?);
    let count = kernable_store::vault::Vault::open(vault, &old)?.rekey(&new)?;
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
        let parked = match l.parked.len() {
            0 => String::new(),
            n => format!(" parked {n}"),
        };
        println!(
            "{} order {} position {} types {types} key {}{service}{middleware}{parked} last seen {}",
            l.name,
            l.order,
            l.position,
            l.key_id,
            config::utc(l.last_seen)
        );
    }
    Ok(true)
}

fn parked(store: &str) -> Result<bool> {
    for l in kernable_store::open_read_only(store)?.listeners()? {
        for p in &l.parked {
            let retry = if p.retry { " retrying" } else { "" };
            println!(
                "{} position {} {} {} attempts {} parked {}{retry}: {}",
                l.name,
                p.position,
                p.event_type,
                p.code,
                p.attempts,
                config::utc(p.parked_at),
                p.message
            );
        }
    }
    Ok(true)
}

/// Delivers a listener's parked events again, or dismisses them: `which` is a position or
/// `--all`. Exit 1 if none of them is parked.
async fn retry(addr: &str, key_path: &str, name: &str, which: &str, dismiss: bool) -> Result<bool> {
    let positions = match which {
        "--all" => Vec::new(),
        p => vec![
            p.parse::<u64>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| format!("{p} is not a position; give one, or --all"))?,
        ],
    };
    let key = signing::read_signing_key(key_path)?;
    let issued_at = now_ms();
    let mut request = tonic::Request::new(RetryRequest {
        sig: Some(signing::sign(
            &key,
            &signing::retry_message(name, &positions, dismiss, issued_at),
        )),
        listener: name.to_owned(),
        positions,
        dismiss,
        issued_at,
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    let applied = ListenerServiceClient::new(connect(addr).await?)
        .retry(request)
        .await?
        .into_inner()
        .positions;
    if applied.is_empty() {
        println!("no parked event {which} of {name}");
        return Ok(false);
    }
    let what = if dismiss { "dismissed" } else { "retrying" };
    let list: Vec<String> = applied.iter().map(u64::to_string).collect();
    println!(
        "{what} {} parked events of {name}: {}",
        applied.len(),
        list.join(" ")
    );
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
            " » Snapshot Store........ sqlite - kernable-snapshots.db",
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
