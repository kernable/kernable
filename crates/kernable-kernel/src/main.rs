#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use std::{
    collections::HashMap,
    fs,
    io::{self, IsTerminal, Write},
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kernable_core::{
    proto::{
        ApplyRequest, Attribute, Command, ExportRequest, ListenerRecord, MapRequest, Membership,
        MoveStatusRequest, Partition, PartitionMap, Permission, PromoteRequest, RetryRequest,
        RevokeRequest, SubmitRequest, SwitchoverRequest, TenantMove, TenantPartition, When,
        WorkerRecord, command_result::Outcome, command_service_client::CommandServiceClient,
        export_service_client::ExportServiceClient, listener_service_client::ListenerServiceClient,
        membership_service_client::MembershipServiceClient,
        partition_service_client::PartitionServiceClient,
        replication_service_client::ReplicationServiceClient,
        trust_service_client::TrustServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{
    BoxError, Config, GRANT_HEADER, Kernel, backup, config, export, grants, node,
    partitions::partition_of, serve, standby, witness,
};
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
                                          finish and stops; SIGTERM to a pair's primary hands its
                                          role to the standby in sync first. SIGUSR1 stops the same
                                          way, never handing over (a new binary takes the stores
                                          over). --dev is for trying kernable out: it
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
                                          vault_erase data_read data_write cache manage replicate
                                          export backup reserved_order. A type is *, <domain>.* or
                                          <domain>.<Type>; manage takes service names, privileged
                                          takes bypass_authorization; replicate, export, backup and
                                          reserved_order (negative listener orders, for kits) take *.
                                          --succeeds lets this key take over the aggregates and
                                          listeners registered with <old.pub>. A newer grant for the
                                          same key replaces the older one.
  kernable revoke <addr> <root.key> <key.pub> <since>
                                          Revokes a key for good from <since> on (UTC, like
                                          2026-10-27T09:00:00Z): the kernel refuses it, ends its
                                          connections and rejects its queued commands accepted since
                                          then (key_revoked). The kernel must be running.
  kernable witness --listen <addr> --root <root.pub> --state <file> [--lease-ms <ms>] [--tls-...]
                                          Runs the witness of a pair of kernels started with
                                          --witness: it decides which one is the primary, renews
                                          the primary's lease (3000 ms by default) and lets the
                                          standby in sync take its place when the lease runs out.
                                          It keeps the term and the roles in <file> (default
                                          witness.state); listens on 127.0.0.1:7300 by default,
                                          elsewhere only with mutual TLS (--tls-cert, --tls-key,
                                          --tls-client-ca).
  kernable partitions apply <addr> <key> <map.json>
                                          Applies a partition map on the control partition at
                                          <addr>: {'epoch': 2, 'partitions': {'p1': ['host:7233'],
                                          'p2': [...]}, 'default': 'p1', 'control': 'p1',
                                          'tenants': {'acme': 'p2'}} (JSON, with double quotes).
                                          Tenants not listed live on
                                          the default partition; the epoch must grow with every
                                          map. <key> needs a grant for manage on *. The other
                                          partitions take it from the control partition.
  kernable partitions show <addr> <key>   Prints the partition map the kernel at <addr> holds.
  kernable partitions move <addr> <key> <tenant> <to>
                                          Moves <tenant> with its data to partition <to>: applies
                                          a map that marks it moving on the control partition at
                                          <addr>, waits until <to> has copied it, then applies the
                                          map with the tenant on <to>. Meanwhile the tenant's
                                          requests are answered UNAVAILABLE. Run it again to finish
                                          a move it did not. <key> needs manage on *.
  kernable promote <addr> <key>           Makes the standby at <addr> (started with --follow) the
                                          primary: it stops following and serves. <key> needs a
                                          grant for manage on *. Promote only a standby whose
                                          primary is gone: a primary that still runs is fenced (it
                                          tells nobody of a write from then on), and rejoins as a
                                          standby of the new one. Without a witness, a standby the
                                          primary never told it is in sync is refused: the primary
                                          may have told writes it lacks.
  kernable switchover <addr> <key>        Hands the primary role over to the standby in sync now:
                                          the primary at <addr> refuses new requests (the SDKs retry
                                          them on the other kernel), lets running ones finish, and
                                          waits until the standby holds every record (at most 10 s).
                                          In a pair (--witness) it tells the witness; without one it
                                          tells the standby on its stream. The standby serves at
                                          once, and the old primary stops: in a pair, started again,
                                          it follows; without a witness, it starts only with
                                          --follow. For updates and moves: update or start the
                                          standby, switch over, update or retire the old primary.
                                          <key> needs a grant for manage on *.
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

Export and import:
  kernable export <addr> <key> <file>
                                          Writes everything the running kernel at <addr> holds to
                                          <file>: every record of its store, every file of its data
                                          store and the vault values, which stay encrypted with its
                                          vault key. The kernel signs a checkpoint first. <key>
                                          needs a grant for export on *; the grant file next to the
                                          key is sent along. <file> must not exist; it is written
                                          readable by its owner only, and deleted if the export does
                                          not finish. Snapshots and the cache are not exported.
                                          Prints the counts and the kernel keys that signed the
                                          store.
  kernable export <file> [--store <s>] [--data-store <d>] [--vault-store <v> --vault-key <k>]
                                          The same from stores read directly, kernel stopped. The
                                          flags name the stores as for kernable start, with its
                                          defaults; without --vault-key the vault is left out, which
                                          fails if the store refers to vault values.
  kernable import <file> [--store <s>] [--data-store <d>] [--vault-store <v> --vault-key <k>] [--kernel-pub <kernel.pub>]...
                                          Fills new, empty stores from an export, then checks the
                                          store like verify, with the same output and exit codes.
                                          --vault-key must be the source's vault key. The whole file
                                          is checked before anything is written: a copy cut short or
                                          changed, or a wrong vault key, is refused. Without
                                          --kernel-pub, the store is checked with the kernel keys
                                          the export names; compare them with the source's .pub
                                          files, or name those. Keep the source's kernel .pub files
                                          for verify after the kernel runs on the new stores.

Run a node:
  kernable node <project.toml>            Runs one node of a project: the kernel and, with a
                                          [backup] section, the backup follower, as child processes
                                          it restarts when they exit. It reads the file again every
                                          two seconds or on SIGHUP, and restarts a child whose
                                          command changed or whose binary was replaced. SIGTERM or
                                          SIGINT stops the children (SIGKILL after stop-seconds,
                                          default 30). The file has [kernel] (flags: the flags of
                                          kernable start without their dashes, env, binary,
                                          stop-seconds), [backup] (to, key, backup-key, follow,
                                          segment-seconds, base-hours, keep, check-hours, env), [node]
                                          (instance, project, podman, metrics-listen: GET /metrics)
                                          and any number of [[service]]
                                          (name, build, file, command, key, env, secrets, cpus,
                                          memory, stop-seconds, writable, expose): containers built
                                          with podman on the node,
                                          rebuilt on SIGHUP, run after the kernel with KERNABLE_URL,
                                          KERNABLE_INSTANCE and their key. Every child's output comes
                                          out of the node prefixed with its name. Paths are relative
                                          to its directory. Exit 2 if the file is invalid at start.
  kernable expose <socket>=<host:port>... Passes every connection to each Unix socket on to its
                                          address, both ways: a node runs it in an instance's pod,
                                          for a service's port to be reached through the socket.

Continuous backup:
  kernable backup key <file>              Writes a new backup key (32 random bytes in hex) to
                                          <file>, readable by its owner only; never overwrites one.
                                          Keep it apart from the backups, as you keep the root key:
                                          without it a backup cannot be read.
  kernable backup --follow <addr>[,<addr>] --key <key> --to <target> --backup-key <file> [--segment-seconds <s>] [--base-hours <h>] [--keep <n>] [--check-hours <h>] [--status-file <file>]
                                          Follows the kernel at <addr> (started with --backup on;
                                          both kernels of a pair, comma-separated) and writes its
                                          records to <target>, encrypted with the backup key, until
                                          stopped: a base, then a segment every --segment-seconds
                                          (default 5) or 16 MiB, a new generation every
                                          --base-hours (default 24), the newest --keep (default 7)
                                          kept. <key> needs a grant for backup on *; the grant file
                                          next to it is sent along. <target> is file:<dir> or
                                          s3://<bucket>/<prefix>, with KERNABLE_S3_ENDPOINT,
                                          KERNABLE_S3_REGION, AWS_ACCESS_KEY_ID and
                                          AWS_SECRET_ACCESS_KEY. Vault values stay encrypted with the
                                          kernel's vault key besides. With --check-hours, it also
                                          checks the backup that often, as backup check does, and
                                          logs what it found. With --status-file, it notes there
                                          (JSON) when it last uploaded, up to which record, and when
                                          it last checked and what it found.
  kernable backup list <target> --backup-key <file>
                                          Prints the generations of a backup: each base's record
                                          and time, and how far its segments reach.
  kernable backup check <target> --backup-key <file>
                                          Reads the newest generation all through, without writing
                                          a store: every object opens with the key and decodes, and
                                          the records chain from the base to the last segment.
                                          Prints the record a restore reaches and how old it is;
                                          exits 1 if the backup does not check out.
  kernable restore <target> --backup-key <file> [--at <time> | --at-record <n>] [--store <s>] [--data-store <d>] [--vault-store <v> --vault-key <k>] [--kernel-pub <kernel.pub>]...
                                          Fills new, empty stores from a backup: the newest
                                          generation that begins before --at (UTC, like
                                          2026-11-04T13:40:00Z) or --at-record, its base, then its
                                          segments up to there (default: everything), then checks
                                          the store like verify, with the same output and exit
                                          codes. --vault-key must be the source's vault key; the
                                          flags name the stores as for kernable start. Without
                                          --kernel-pub, the store is checked with the kernel keys
                                          the backup names.

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
  kernable --version                      Prints the version, the system and the architecture.
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
        ["--version" | "version"] => {
            println!(
                "kernable {} ({} {})",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH
            );
            return ExitCode::SUCCESS;
        }
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
        ["promote", addr, key] => promote(addr, key).await,
        ["switchover", addr, key] => switchover(addr, key).await,
        ["partitions", "apply", addr, key, map] => partitions_apply(addr, key, map).await,
        ["partitions", "show", addr, key] => partitions_show(addr, key).await,
        ["partitions", "move", addr, key, tenant, to] => {
            partitions_move(addr, key, tenant, to).await
        }
        ["witness", flags @ ..] => return run_witness(flags).await,
        ["node", project] => return run_node(project).await,
        ["expose", pairs @ ..] if !pairs.is_empty() => return expose(pairs).await,
        ["backup", "key", file] => backup_key(file),
        ["backup", "list", to, flags @ ..] if !to.starts_with("--") => backup_list(to, flags),
        ["backup", "check", to, flags @ ..] if !to.starts_with("--") => backup_check(to, flags),
        ["backup", flags @ ..] => return run_backup(flags).await,
        ["restore", from, flags @ ..] if !from.starts_with("--") => {
            return restore(from, flags).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            });
        }
        ["keys", "new", name] => keys_new(name),
        ["checkpoint", store, key] => checkpoint(store, key),
        ["verify", store, keys @ ..] if !keys.is_empty() => {
            return verify(store, keys).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            });
        }
        ["vault", "rekey", vault, old, new] => vault_rekey(vault, old, new),
        ["export", addr, key, file] if ![addr, key, file].iter().any(|a| a.starts_with("--")) => {
            export_running(addr, key, file).await
        }
        ["export", file, flags @ ..] if !file.starts_with("--") => export_stopped(file, flags),
        ["import", file, flags @ ..] if !file.starts_with("--") => {
            return import(file, flags).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            });
        }
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
    // A standby follows until it is promoted, then runs as the kernel on what it holds.
    let ran = if config.replication.pair.is_some() {
        let leaving = leave_on_sigterm();
        match node::become_primary(config).await {
            Ok(primary) => {
                leaving.abort();
                run_kernel_on(primary.kernel, Some(primary.lease)).await
            }
            Err(e) => Err(e),
        }
    } else if config.replication.follow.is_some() {
        let leaving = leave_on_sigterm();
        match standby::run(config.clone()).await {
            Ok(()) => {
                leaving.abort();
                run_kernel(config).await
            }
            Err(e) => Err(e),
        }
    } else {
        run_kernel(config).await
    };
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "kernel stopped");
            // At once: a deposed primary's writes that wait for a standby must never be told,
            // and the runtime would wait for their threads forever. Their clients see the
            // connection end and submit again on the new primary.
            std::process::exit(1)
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
    kernable_kernel::refuse_if_handed_over(&config)?;
    let kernel = Kernel::open(config)?;
    let keeper = kernable_kernel::switchover_keeper(&kernel);
    run_kernel_on(kernel, keeper).await
}

/// Serves `kernel` until a signal; with `lease`, also until it ends: a primary of a pair the
/// witness deposed, or one that handed its role over to its standby. SIGTERM stops a pair's
/// primary after it handed its role to its standby; SIGUSR1 (the node, when a new kernel
/// binary waits for the stores, P21) lets the stores go without that.
async fn run_kernel_on(
    kernel: Arc<Kernel>,
    lease: Option<std::pin::Pin<Box<dyn Future<Output = BoxError> + Send>>>,
) -> std::result::Result<(), BoxError> {
    let mut lease = lease.unwrap_or_else(|| Box::pin(std::future::pending()));
    let mut server = tokio::spawn(serve(Arc::clone(&kernel)));
    let mut hangup = Hangup::new()?;
    // A stop that waits for the switchover to the standby first.
    let mut handing: Option<
        tokio::sync::oneshot::Receiver<
            std::result::Result<kernable_core::proto::SwitchoverResult, String>,
        >,
    > = None;
    loop {
        let stopping = handing.is_some();
        let handed = async {
            match handing.as_mut() {
                Some(rx) => rx
                    .await
                    .unwrap_or_else(|_| Err("the lease keeper ended".into())),
                None => std::future::pending().await,
            }
        };
        let signal = tokio::select! {
            result = &mut server => return result?.and_then(|()| kernel.shutdown()),
            why = &mut lease => {
                server.abort();
                let _ = kernel.shutdown();
                return if stopping { Ok(()) } else { Err(why) };
            }
            _ = hangup.recv() => {
                match kernel.reload() {
                    Ok(()) => tracing::info!("kernel key read again"),
                    Err(e) => tracing::error!(error = %e, "reading the kernel key failed; the previous key stays"),
                }
                continue;
            }
            result = handed => match result {
                // The lease keeper ends next, with the handover.
                Ok(r) => {
                    tracing::warn!(standby = %r.standby, record = r.seq, "stopping: the standby serves now");
                    continue;
                }
                Err(why) => {
                    tracing::warn!(%why, "stopping without handing over");
                    "SIGTERM"
                }
            },
            signal = shutdown_signal(), if !stopping => {
                let signal = signal?;
                if signal == "SIGTERM"
                    && let Some(rx) = kernable_kernel::node::hand_over_on_stop(&kernel)
                {
                    tracing::info!("shutting down: handing the primary role to the standby first");
                    handing = Some(rx);
                    continue;
                }
                signal
            }
        };
        // Workers and listeners stay connected, so running commands can finish and settle.
        tracing::info!(signal, "shutting down: finishing running requests");
        if !kernel.drain().await {
            tracing::warn!("drain timed out: unfinished commands run again after a restart");
        }
        server.abort();
        return kernel.shutdown();
    }
}

/// While the kernel follows a primary: SIGTERM tells the primary that it leaves, so a
/// pair's primary commits alone at once, and ends the process. SIGUSR1 (a new binary takes the
/// stores, P21) still ends it at once: that standby is back within moments.
fn leave_on_sigterm() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let Ok(mut term) = signal(SignalKind::terminate()) else {
                return;
            };
            if term.recv().await.is_some() {
                tracing::info!("stopping: telling the primary that this standby leaves");
                standby::leave().await;
                std::process::exit(0);
            }
        }
    })
}

/// SIGINT, SIGTERM, or SIGUSR1 (let the stores go to a new kernel binary).
#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut usr1 = signal(SignalKind::user_defined1())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = term.recv() => Ok("SIGTERM"),
        _ = usr1.recv() => Ok("SIGUSR1"),
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

/// `kernable witness --listen <addr> --root <root.pub>... --state <file> [--lease-ms <ms>]
/// [--tls-cert <pem> --tls-key <pem> --tls-client-ca <pem>]`.
async fn run_witness(flags: &[&str]) -> ExitCode {
    let parsed = (|| -> Result<witness::WitnessConfig> {
        let mut given: std::collections::HashMap<&str, Vec<String>> = Default::default();
        let mut rest = flags.iter();
        while let Some(&flag) = rest.next() {
            let known = [
                "--listen",
                "--root",
                "--state",
                "--lease-ms",
                "--tls-cert",
                "--tls-key",
                "--tls-client-ca",
            ];
            if !known.contains(&flag) {
                return Err(format!("unknown flag {flag}").into());
            }
            let value = rest.next().ok_or(format!("{flag} needs a value"))?;
            given.entry(flag).or_default().push((*value).to_owned());
        }
        let one = |flag: &str| given.get(flag).and_then(|v| v.last().cloned());
        let roots = given
            .get("--root")
            .ok_or("no root key: pass --root <key ID or .pub file>")?
            .iter()
            .map(|r| key_id_of(r))
            .collect::<Result<Vec<_>>>()?;
        let tls = match (one("--tls-cert"), one("--tls-key"), one("--tls-client-ca")) {
            (Some(cert), Some(key), Some(client_ca)) => Some(config::Tls {
                cert: cert.into(),
                key: key.into(),
                client_ca: client_ca.into(),
            }),
            (None, None, None) => None,
            _ => return Err("TLS needs --tls-cert, --tls-key and --tls-client-ca".into()),
        };
        Ok(witness::WitnessConfig {
            listen: one("--listen").unwrap_or_else(|| "127.0.0.1:7300".into()),
            roots,
            state: one("--state")
                .unwrap_or_else(|| "witness.state".into())
                .into(),
            lease_ms: match one("--lease-ms") {
                Some(ms) => ms
                    .parse()
                    .map_err(|_| format!("--lease-ms: {ms} is not a number"))?,
                None => witness::LEASE_MS,
            },
            tls,
        })
    })();
    let config = match parsed {
        Ok(config) => config,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    let filter = EnvFilter::try_from_env("KERNABLE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    tokio::select! {
        result = witness::run(config) => match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "witness stopped");
                ExitCode::FAILURE
            }
        },
        _ = shutdown_signal() => ExitCode::SUCCESS,
    }
}

/// Flags given as `--flag value`, each at most once, of the `known` ones; `--kernel-pub` may
/// repeat.
fn flag_values<'a>(flags: &[&'a str], known: &[&str]) -> Result<HashMap<&'a str, Vec<&'a str>>> {
    let mut values: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut rest = flags.iter();
    while let Some(&flag) = rest.next() {
        if !known.contains(&flag) {
            return Err(format!("unknown flag {flag}").into());
        }
        let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let given = values.entry(flag).or_default();
        if !given.is_empty() && flag != "--kernel-pub" {
            return Err(format!("{flag} is given twice").into());
        }
        given.push(value);
    }
    Ok(values)
}

/// `kernable expose <socket>=<host:port>...`: each Unix socket passes its connections on to its
/// address, both ways, until stopped. A node runs it in an instance's pod, so that what reaches
/// the socket from outside (the ingress) reaches a service's port in the pod's own network.
async fn expose(pairs: &[&str]) -> ExitCode {
    use std::os::unix::fs::FileTypeExt;
    for pair in pairs {
        let Some((socket, to)) = pair.split_once('=') else {
            eprintln!("error: {pair}: <socket>=<host:port>");
            return ExitCode::from(2);
        };
        // A stale socket from an earlier run, but never any other file.
        if std::fs::metadata(socket).is_ok_and(|m| m.file_type().is_socket()) {
            let _ = std::fs::remove_file(socket);
        }
        let listener = match tokio::net::UnixListener::bind(socket) {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("error: {socket}: {e}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(e) = kernable_kernel::socket_mode(socket) {
            eprintln!("error: {socket}: {e}");
            return ExitCode::FAILURE;
        }
        let to = to.to_owned();
        tokio::spawn(async move {
            loop {
                let mut inbound = match listener.accept().await {
                    Ok((inbound, _)) => inbound,
                    Err(e) => {
                        eprintln!("accepting on the socket failed: {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let to = to.clone();
                tokio::spawn(async move {
                    match tokio::net::TcpStream::connect(&to).await {
                        Ok(mut outbound) => {
                            let _ = outbound.set_nodelay(true);
                            let _ =
                                tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                        Err(e) => eprintln!("{to}: {e}"),
                    }
                });
            }
        });
    }
    eprintln!("exposing {}", pairs.join(" "));
    std::future::pending::<()>().await;
    ExitCode::SUCCESS
}

/// `kernable node <project.toml>`: runs one node of a project until stopped.
async fn run_node(project: &str) -> ExitCode {
    let filter = EnvFilter::try_from_env("KERNABLE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    match kernable_kernel::supervisor::run(Path::new(project)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

/// `kernable backup key <file>`.
fn backup_key(file: &str) -> Result<bool> {
    backup::crypt::BackupKey::create(Path::new(file)).map_err(boxed)?;
    let key = backup::crypt::BackupKey::read(Path::new(file)).map_err(boxed)?;
    println!("wrote the backup key {} to {file}", key.id());
    Ok(true)
}

/// `kernable backup check <target> --backup-key <file>`.
fn backup_check(to: &str, flags: &[&str]) -> Result<bool> {
    let values = flag_values(flags, &["--backup-key"])?;
    let key = values
        .get("--backup-key")
        .and_then(|v| v.first())
        .ok_or("--backup-key is missing")?;
    let crypt = backup::crypt::BackupKey::read(Path::new(key)).map_err(boxed)?;
    let target = backup::target::open(to).map_err(boxed)?;
    let started = std::time::Instant::now();
    match backup::check(target.as_ref(), &crypt) {
        Ok(c) => {
            println!(
                "generation {}: base at record {} ({} records), {} segments to record {}, {} bytes, \
                 the newest taken {} s ago; read in {:.1} s",
                c.generation,
                c.base,
                c.image_records,
                c.segments,
                c.record,
                c.bytes,
                (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as i64)
                    - c.taken_at)
                    / 1000,
                started.elapsed().as_secs_f64()
            );
            println!("OK");
            Ok(true)
        }
        Err(e) => {
            println!("the backup does not check out: {e}");
            Ok(false)
        }
    }
}

/// `kernable backup list <target> --backup-key <file>`.
fn backup_list(to: &str, flags: &[&str]) -> Result<bool> {
    let values = flag_values(flags, &["--backup-key"])?;
    let key = values
        .get("--backup-key")
        .and_then(|v| v.first())
        .ok_or("--backup-key is missing")?;
    let crypt = backup::crypt::BackupKey::read(Path::new(key)).map_err(boxed)?;
    let target = backup::target::open(to).map_err(boxed)?;
    let gens = backup::generations(target.as_ref(), &crypt).map_err(boxed)?;
    if gens.is_empty() {
        println!("no generations in {}", target.describe());
    }
    for g in gens {
        match &g.base {
            Some(b) => {
                let reach = g.segments.last().map_or(b.seq, |s| s.last);
                println!(
                    "{}  base at record {} ({}, {} records, {} vault values, {} files), {} segments up to record {}",
                    g.name,
                    b.seq,
                    config::utc(b.created_at),
                    b.records,
                    b.vault_values,
                    b.files,
                    g.segments.len(),
                    reach
                );
            }
            None => println!(
                "{}  unfinished: its base was not written to the end",
                g.name
            ),
        }
    }
    Ok(true)
}

/// `kernable backup --follow ...`: runs the backup follower until stopped.
async fn run_backup(flags: &[&str]) -> ExitCode {
    let parsed = (|| -> Result<backup::Follower> {
        let values = flag_values(
            flags,
            &[
                "--follow",
                "--key",
                "--to",
                "--backup-key",
                "--segment-seconds",
                "--base-hours",
                "--keep",
                "--check-hours",
                "--status-file",
            ],
        )?;
        let one = |flag: &str| values.get(flag).and_then(|v| v.first().copied());
        let need = |flag: &str| one(flag).ok_or_else(|| format!("{flag} is missing"));
        let number = |flag: &str, default: u64| -> Result<u64> {
            match one(flag) {
                None => Ok(default),
                Some(v) => match v.parse::<u64>() {
                    Ok(n) if n > 0 => Ok(n),
                    _ => Err(format!("{flag}: {v} is not a positive number").into()),
                },
            }
        };
        let key_path = need("--key")?;
        let key =
            signing::read_signing_key(key_path).map_err(|e| format!("--key {key_path}: {e}"))?;
        let grant_path = Path::new(key_path).with_extension("grant");
        let grant = fs::read_to_string(&grant_path)
            .map_err(|e| format!("the backup key's grant {}: {e}", grant_path.display()))?
            .trim()
            .to_owned();
        let addrs: Vec<String> = need("--follow")?
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_owned)
            .collect();
        if addrs.is_empty() {
            return Err("--follow needs an address".into());
        }
        Ok(backup::Follower {
            addrs,
            key,
            grant,
            target: Arc::from(backup::target::open(need("--to")?).map_err(boxed)?),
            crypt: Arc::new(
                backup::crypt::BackupKey::read(Path::new(need("--backup-key")?)).map_err(boxed)?,
            ),
            segment_every: Duration::from_secs(number("--segment-seconds", 5)?),
            base_every: Duration::from_secs(number("--base-hours", 24)? * 3600),
            keep: number("--keep", 7)? as usize,
            check_every: match one("--check-hours") {
                None => None,
                Some(_) => Some(Duration::from_secs(number("--check-hours", 24)? * 3600)),
            },
            status_file: one("--status-file").map(std::path::PathBuf::from),
        })
    })();
    let follower = match parsed {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    let filter = EnvFilter::try_from_env("KERNABLE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    tokio::select! {
        result = backup::run(follower) => match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "the backup follower stopped");
                ExitCode::FAILURE
            }
        },
        _ = shutdown_signal() => ExitCode::SUCCESS,
    }
}

/// `kernable restore <target> [flags]`: fills new, empty stores, then checks the store.
fn restore(from: &str, flags: &[&str]) -> Result<ExitCode> {
    // Every flag takes a value: the backup's go here, the stores' to StoreFlags.
    let (mut mine, mut rest) = (Vec::new(), Vec::new());
    for pair in flags.chunks(2) {
        match ["--backup-key", "--at", "--at-record"].contains(&pair[0]) {
            true => mine.extend_from_slice(pair),
            false => rest.extend_from_slice(pair),
        }
    }
    let values = flag_values(&mine, &["--backup-key", "--at", "--at-record"])?;
    let one = |flag: &str| values.get(flag).and_then(|v| v.first().copied());
    let until = match (one("--at"), one("--at-record")) {
        (Some(_), Some(_)) => return Err("give --at or --at-record, not both".into()),
        (Some(at), None) => backup::Until::Time(
            config::utc_ms(at)
                .ok_or_else(|| format!("--at {at}: not a UTC time like 2026-11-04T13:40:00Z"))?,
        ),
        (None, Some(n)) => backup::Until::Record(
            n.parse()
                .map_err(|_| format!("--at-record {n}: not a number"))?,
        ),
        (None, None) => backup::Until::End,
    };
    let key = one("--backup-key").ok_or("--backup-key is missing")?;
    let crypt = backup::crypt::BackupKey::read(Path::new(key)).map_err(boxed)?;
    let target = backup::target::open(from).map_err(boxed)?;
    let flags = StoreFlags::parse(&rest, true)?;
    let stores = standby::Stores {
        // Nothing serves from it until it is filled: one flush at the end (`sync`), not one
        // per batch.
        store: kernable_store::open_with(&flags.store, kernable_store::Durability::Normal)?,
        vault: flags.open_vault()?,
        data: kernable_store::data::open(&flags.data)?,
        snapshots: kernable_store::snapshots::open("memory:")?,
    };
    let kernel_keys = flags
        .kernel_pubs
        .iter()
        .map(|k| key_id_of(k))
        .collect::<Result<Vec<_>>>()?;
    let restored =
        backup::restore(target.as_ref(), &crypt, &stores, until, &kernel_keys).map_err(boxed)?;
    println!(
        "restored generation {}: base at record {}, up to record {} (taken {})",
        restored.generation,
        restored.base,
        restored.record,
        config::utc(restored.taken_at)
    );
    if let Some(short) = &restored.short {
        println!("note: {short}");
    }
    if kernel_keys.is_empty() && !restored.kernel_keys.is_empty() {
        println!(
            "checked with the kernel keys the backup names: {}; compare them with the source's \
             kernel .pub files, or name those with --kernel-pub",
            restored.kernel_keys.join(", ")
        );
    }
    let report = stores.store.verify(&restored.kernel_keys)?;
    Ok(report_verify(&report))
}

async fn partitions_apply(addr: &str, key_path: &str, path: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let json: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path)?).map_err(|e| format!("{path}: {e}"))?;
    let text = |field: &str| -> Result<String> {
        Ok(json
            .get(field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("{path}: {field} is a partition's name"))?
            .to_owned())
    };
    let partitions = json
        .get("partitions")
        .and_then(|v| v.as_object())
        .ok_or_else(|| format!("{path}: partitions maps names to address lists"))?
        .iter()
        .map(|(name, addresses)| {
            let addresses: Vec<String> = serde_json::from_value(addresses.clone())
                .map_err(|_| format!("{path}: partition {name} needs a list of addresses"))?;
            Ok(Partition {
                name: name.clone(),
                addresses,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tenants = match json.get("tenants") {
        None => Vec::new(),
        Some(serde_json::Value::Object(map)) => map
            .iter()
            .map(|(tenant, partition)| {
                Ok(TenantPartition {
                    tenant: tenant.clone(),
                    partition: partition
                        .as_str()
                        .ok_or_else(|| format!("{path}: tenant {tenant} names a partition"))?
                        .to_owned(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => return Err(format!("{path}: tenants maps tenants to partitions").into()),
    };
    let mut map = PartitionMap {
        epoch: json
            .get("epoch")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| format!("{path}: epoch is a number"))?,
        partitions,
        tenants,
        default: text("default")?,
        control: text("control")?,
        issued_at: now_ms(),
        sig: None,
        protocol: kernable_core::PROTOCOL,
        moving: Vec::new(),
        moved: Vec::new(),
        copying: Vec::new(),
    };
    // Moves stay as the held map has them: one in progress is finished with `partitions move`.
    if let Some(held) = held_map(addr, key_path).await? {
        if let Some(m) = held.moving.first().or(held.copying.first()) {
            return Err(format!(
                "tenant {} is moving from {} to {}; finish it first: kernable partitions move {addr} {key_path} {} {}",
                m.tenant, m.from, m.to, m.tenant, m.to
            )
            .into());
        }
        map.moved = held.moved;
    }
    apply_map(addr, key_path, &key, &mut map).await?;
    println!(
        "applied partition map epoch {} ({} partitions, {} tenants listed, default {})",
        map.epoch,
        map.partitions.len(),
        map.tenants.len(),
        map.default
    );
    Ok(true)
}

/// Signs `map` and applies it on the control partition at `addr`.
async fn apply_map(
    addr: &str,
    key_path: &str,
    key: &ed25519_dalek::SigningKey,
    map: &mut PartitionMap,
) -> Result<()> {
    map.issued_at = now_ms();
    map.protocol = kernable_core::PROTOCOL;
    map.sig = Some(signing::sign(key, &signing::partitions_message(map)));
    let mut request = tonic::Request::new(map.clone());
    with_key_grant(&mut request, key_path)?;
    PartitionServiceClient::new(connect(addr).await?)
        .apply(request)
        .await?;
    Ok(())
}

/// The map the kernel at `addr` holds, if any.
async fn held_map(addr: &str, key_path: &str) -> Result<Option<PartitionMap>> {
    let mut request = tonic::Request::new(MapRequest {
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    match PartitionServiceClient::new(connect(addr).await?)
        .map(request)
        .await
    {
        Ok(map) => Ok(Some(map.into_inner())),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
        Err(s) => Err(s.into()),
    }
}

/// Waits until partition `to` (at `addresses`) has copied `tenant` for the move or copy of
/// `epoch`.
async fn wait_copied(
    addresses: &[String],
    key_path: &str,
    tenant: &str,
    to: &str,
    epoch: u64,
) -> Result<kernable_core::proto::MoveStatus> {
    let started = std::time::Instant::now();
    let mut said = std::time::Instant::now();
    loop {
        for address in addresses {
            let mut request = tonic::Request::new(MoveStatusRequest {
                tenant: tenant.to_owned(),
                protocol: kernable_core::PROTOCOL,
            });
            with_key_grant(&mut request, key_path)?;
            let Ok(channel) = connect(address).await else {
                continue;
            };
            match PartitionServiceClient::new(channel).moving(request).await {
                Ok(s) if s.get_ref().done && s.get_ref().epoch == epoch => {
                    return Ok(s.into_inner());
                }
                Ok(_) => break,
                Err(_) => continue,
            }
        }
        if said.elapsed() > std::time::Duration::from_secs(10) {
            said = std::time::Instant::now();
            println!(
                "still copying ({}s); partition {to}'s log says why if it takes long",
                started.elapsed().as_secs()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

async fn partitions_move(addr: &str, key_path: &str, tenant: &str, to: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let held = held_map(addr, key_path)
        .await?
        .ok_or("the control partition holds no partition map; apply one first")?;
    let addresses = held
        .partitions
        .iter()
        .find(|p| p.name == to)
        .map(|p| p.addresses.clone())
        .ok_or_else(|| format!("the map lists no partition {to}"))?;
    let started = std::time::Instant::now();
    // The move, and when the tenant stopped.
    let (m, stopped) = match held.moving.iter().find(|m| m.tenant == tenant) {
        Some(m) if m.to == to => {
            println!(
                "tenant {tenant} is moving from {} to {to} already; waiting for the copy",
                m.from
            );
            (m.clone(), std::time::Instant::now())
        }
        Some(m) => {
            return Err(format!(
                "tenant {tenant} is moving to {} already; finish that first",
                m.to
            )
            .into());
        }
        None => {
            // 1. Copy the tenant while it runs, unless that is under way already.
            let c = match held.copying.iter().find(|m| m.tenant == tenant) {
                Some(c) if c.to == to => {
                    println!("tenant {tenant} is being copied to {to} already; waiting for it");
                    c.clone()
                }
                Some(c) => {
                    return Err(format!(
                        "tenant {tenant} is being copied to {} already; finish that first",
                        c.to
                    )
                    .into());
                }
                None => {
                    let from = partition_of(&held, tenant).to_owned();
                    if from == to {
                        return Err(format!("tenant {tenant} is on partition {to} already").into());
                    }
                    let mut map = held.clone();
                    map.epoch += 1;
                    let c = TenantMove {
                        tenant: tenant.to_owned(),
                        from,
                        to: to.to_owned(),
                        epoch: map.epoch,
                        ..Default::default()
                    };
                    map.copying.push(c.clone());
                    apply_map(addr, key_path, &key, &mut map).await?;
                    println!(
                        "map epoch {}: tenant {tenant} is copied from {} to {to} while it runs",
                        map.epoch, c.from
                    );
                    c
                }
            };
            let copied = wait_copied(&addresses, key_path, tenant, to, c.epoch).await?;
            println!(
                "copied while it ran in {}s (source head {})",
                started.elapsed().as_secs(),
                copied.source_head
            );
            // 2. The map that stops the tenant for the rest.
            let mut map = held_map(addr, key_path)
                .await?
                .ok_or("the control partition lost its partition map")?;
            map.epoch += 1;
            map.copying.retain(|x| x.tenant != tenant);
            let m = TenantMove {
                epoch: map.epoch,
                ..c
            };
            map.moving.push(m.clone());
            let stopped = std::time::Instant::now();
            apply_map(addr, key_path, &key, &mut map).await?;
            println!(
                "map epoch {}: tenant {tenant} moves from {} to {to}; its requests wait meanwhile",
                map.epoch, m.from
            );
            (m, stopped)
        }
    };
    // 3. Wait until the target has copied the rest.
    let status = wait_copied(&addresses, key_path, tenant, to, m.epoch).await?;
    // 4. The map with the tenant on the target, from the map held now.
    let mut map = held_map(addr, key_path)
        .await?
        .ok_or("the control partition lost its partition map")?;
    map.epoch += 1;
    map.moving.retain(|x| x.tenant != tenant);
    map.tenants.retain(|t| t.tenant != tenant);
    if map.default != to {
        map.tenants.push(TenantPartition {
            tenant: tenant.to_owned(),
            partition: to.to_owned(),
        });
    }
    map.moved.push(TenantMove {
        source_head: status.source_head,
        target_after: status.target_after,
        ..m.clone()
    });
    apply_map(addr, key_path, &key, &mut map).await?;
    println!(
        "map epoch {}: tenant {tenant} moved from {} to {to} in {}s, stopped for {} ms (source head {}, target after {})",
        map.epoch,
        m.from,
        started.elapsed().as_secs(),
        stopped.elapsed().as_millis(),
        status.source_head,
        status.target_after
    );
    Ok(true)
}

async fn partitions_show(addr: &str, key_path: &str) -> Result<bool> {
    let map = held_map(addr, key_path)
        .await?
        .ok_or("this kernel holds no partition map")?;
    println!(
        "epoch {}, default {}, control {}",
        map.epoch, map.default, map.control
    );
    for p in &map.partitions {
        println!("partition {}: {}", p.name, p.addresses.join(", "));
    }
    for t in &map.tenants {
        println!("tenant {} -> {}", t.tenant, t.partition);
    }
    for m in &map.copying {
        println!(
            "tenant {} being copied {} -> {} while it runs",
            m.tenant, m.from, m.to
        );
    }
    for m in &map.moving {
        println!("tenant {} moving {} -> {}", m.tenant, m.from, m.to);
    }
    for m in &map.moved {
        println!(
            "tenant {} moved {} -> {} (source head {}, target after {})",
            m.tenant, m.from, m.to, m.source_head, m.target_after
        );
    }
    Ok(true)
}

async fn promote(addr: &str, key_path: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let issued_at = now_ms();
    let mut request = tonic::Request::new(PromoteRequest {
        issued_at,
        sig: Some(signing::sign(&key, &signing::promote_message(issued_at))),
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    let result = ReplicationServiceClient::new(connect(addr).await?)
        .promote(request)
        .await?
        .into_inner();
    println!(
        "promoted {addr}: it serves as the primary from record {}",
        result.seq
    );
    Ok(true)
}

/// `kernable switchover <addr> <key>`: the primary hands its role to its standby in sync.
async fn switchover(addr: &str, key_path: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let issued_at = now_ms();
    let mut request = tonic::Request::new(SwitchoverRequest {
        issued_at,
        sig: Some(signing::sign(&key, &signing::switchover_message(issued_at))),
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    let result = ReplicationServiceClient::new(connect(addr).await?)
        .switchover(request)
        .await?
        .into_inner();
    println!(
        "switched over: {} is the primary now, from record {}; {addr} stops (to follow it, start it again: in a pair as before, otherwise with --follow)",
        result.standby, result.seq
    );
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
    Ok(report_verify(&report))
}

/// Prints a verify report; the exit code of `kernable verify`.
fn report_verify(report: &kernable_store::Report) -> ExitCode {
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
    if !report.is_ok() {
        println!("TAMPERED");
        ExitCode::FAILURE
    } else if !report.suspects.is_empty() {
        println!("OK, {} suspect", report.suspects.len());
        ExitCode::from(2)
    } else {
        println!("OK");
        ExitCode::SUCCESS
    }
}

/// The flags of `export` and `import`: the stores as `kernable start` names them.
struct StoreFlags {
    store: String,
    data: String,
    /// The vault store and its key, with `--vault-key`.
    vault: Option<(String, String)>,
    kernel_pubs: Vec<String>,
}

impl StoreFlags {
    fn parse(flags: &[&str], kernel_pubs: bool) -> Result<Self> {
        let mut values: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut known = vec!["--store", "--data-store", "--vault-store", "--vault-key"];
        if kernel_pubs {
            known.push("--kernel-pub");
        }
        let mut rest = flags.iter();
        while let Some(flag) = rest.next() {
            if !known.contains(flag) {
                return Err(format!("unknown flag {flag}").into());
            }
            let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
            values.entry(flag).or_default().push(value);
        }
        let one = |flag: &str, default: &str| -> Result<String> {
            match values.get(flag).map(Vec::as_slice) {
                None => Ok(default.to_owned()),
                Some([v]) => Ok((*v).to_owned()),
                Some(_) => Err(format!("{flag} is given twice").into()),
            }
        };
        let vault = match values.contains_key("--vault-key") {
            true => Some((
                one("--vault-store", "sqlite:vault.db")?,
                one("--vault-key", "")?,
            )),
            false if values.contains_key("--vault-store") => {
                return Err("--vault-store needs --vault-key".into());
            }
            false => None,
        };
        Ok(StoreFlags {
            store: one("--store", "sqlite:kernable.db")?,
            data: one("--data-store", "file:data")?,
            vault,
            kernel_pubs: values
                .get("--kernel-pub")
                .map(|v| v.iter().map(|s| (*s).to_owned()).collect())
                .unwrap_or_default(),
        })
    }

    fn open_vault(&self) -> Result<Option<kernable_store::vault::Vault>> {
        let Some((store, key)) = &self.vault else {
            return Ok(None);
        };
        let key = signing::read_secret(key).map_err(|e| match e.kind() {
            io::ErrorKind::InvalidData => format!("{key}: not a 32-byte key in hex").into(),
            _ => Box::<dyn std::error::Error>::from(format!("{key}: {e}")),
        })?;
        Ok(Some(kernable_store::vault::Vault::open(store, &key)?))
    }
}

fn boxed(e: BoxError) -> Box<dyn std::error::Error> {
    e
}

/// A new export file, readable by its owner only; never overwrites one.
fn create_export(file: &str) -> Result<export::Writer<io::BufWriter<fs::File>>> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let out = options.open(file).map_err(|e| format!("{file}: {e}"))?;
    Ok(export::Writer::new(io::BufWriter::new(out))?)
}

fn exported(file: &str, end: &kernable_core::proto::ExportEnd) {
    println!(
        "exported {} records, {} vault values and {} files to {file}",
        end.records, end.vault_values, end.files
    );
    if !end.kernel_keys.is_empty() {
        println!("signed by the kernel keys {}", end.kernel_keys.join(", "));
    }
}

/// Writes the frames to `file`; deletes it if they do not reach their end.
fn write_export(
    file: &str,
    frames: impl FnOnce(&mut dyn FnMut(kernable_core::proto::ExportFrame) -> Result<bool>) -> Result<()>,
) -> Result<bool> {
    let mut writer = create_export(file)?;
    let mut end = None;
    let outcome = frames(&mut |frame| {
        if let Some(kernable_core::proto::export_frame::Frame::End(e)) = &frame.frame {
            end = Some(e.clone());
        }
        Ok(writer.write(frame)?)
    });
    let done = outcome.and_then(|()| {
        let file = writer
            .into_inner()
            .into_inner()
            .map_err(|e| e.into_error())?;
        file.sync_all()?;
        end.ok_or_else(|| "the export ended early".into())
    });
    match done {
        Ok(end) => {
            exported(file, &end);
            Ok(true)
        }
        Err(e) => {
            let _ = fs::remove_file(file);
            Err(e)
        }
    }
}

/// `kernable export <addr> <key> <file>`: the running kernel streams its export.
async fn export_running(addr: &str, key_path: &str, file: &str) -> Result<bool> {
    let key = signing::read_signing_key(key_path)?;
    let issued_at = now_ms();
    let mut request = tonic::Request::new(ExportRequest {
        issued_at,
        sig: Some(signing::sign(&key, &signing::export_message(issued_at))),
        protocol: kernable_core::PROTOCOL,
    });
    with_key_grant(&mut request, key_path)?;
    let mut stream = ExportServiceClient::new(connect(addr).await?)
        .max_decoding_message_size(usize::MAX)
        .export(request)
        .await?
        .into_inner();
    let mut writer = create_export(file)?;
    let mut end = None;
    let outcome: Result<()> = async {
        while let Some(frame) = stream.message().await? {
            if let Some(kernable_core::proto::export_frame::Frame::End(e)) = &frame.frame {
                end = Some(e.clone());
            }
            if writer.write(frame)? {
                break;
            }
        }
        let out = writer
            .into_inner()
            .into_inner()
            .map_err(|e| e.into_error())?;
        out.sync_all()?;
        Ok(())
    }
    .await;
    match (outcome, end) {
        (Ok(()), Some(end)) => {
            exported(file, &end);
            Ok(true)
        }
        (Ok(()), None) => {
            let _ = fs::remove_file(file);
            Err("the kernel ended the export early".into())
        }
        (Err(e), _) => {
            let _ = fs::remove_file(file);
            Err(e)
        }
    }
}

/// `kernable export <file> [flags]`: stopped stores, read directly.
fn export_stopped(file: &str, flags: &[&str]) -> Result<bool> {
    let flags = StoreFlags::parse(flags, false)?;
    let store = kernable_store::open_read_only(&flags.store)?;
    let vault = flags.open_vault()?;
    let data = kernable_store::data::open(&flags.data)?;
    let source = export::Source {
        store: store.as_ref(),
        vault: vault.as_ref(),
        data: data.as_ref(),
        partition: "",
    };
    write_export(file, |write| {
        export::produce(&source, &mut |frame| {
            write(frame).map(|_| ()).map_err(|e| e.to_string().into())
        })
        .map_err(boxed)?;
        Ok(())
    })
}

/// `kernable import <file> [flags]`: fills new, empty stores, then checks the store.
fn import(file: &str, flags: &[&str]) -> Result<ExitCode> {
    let flags = StoreFlags::parse(flags, true)?;
    let store = kernable_store::open(&flags.store)?;
    let vault = flags.open_vault()?;
    let data = kernable_store::data::open(&flags.data)?;
    let target = export::Target {
        store: store.as_ref(),
        vault: vault.as_ref(),
        data: data.as_ref(),
    };
    let imported = export::import(Path::new(file), &target).map_err(boxed)?;
    let end = &imported.end;
    println!(
        "imported {} records, {} vault values and {} files",
        end.records, end.vault_values, end.files
    );
    let key_ids = match flags.kernel_pubs.is_empty() {
        false => flags
            .kernel_pubs
            .iter()
            .map(|k| key_id_of(k))
            .collect::<Result<Vec<_>>>()?,
        true => {
            if !end.kernel_keys.is_empty() {
                println!(
                    "checked with the kernel keys the export names: {}; compare them with the \
                     source's kernel .pub files, or name those with --kernel-pub",
                    end.kernel_keys.join(", ")
                );
            }
            end.kernel_keys.clone()
        }
    };
    let report = store.verify(&key_ids)?;
    Ok(report_verify(&report))
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
