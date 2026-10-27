use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use kernable_core::{
    proto::{
        Command, ListenerRecord, SubmitRequest, command_result::Outcome,
        command_service_client::CommandServiceClient,
    },
    signing, transport,
};

const USAGE: &str = "usage:
  kernable keys new <name>                writes <name>.key (secret) and <name>.pub (key ID)
  kernable checkpoint <store> <kernel.key>  signs the current head of the event log (kernel stopped)
  kernable verify <store> <kernel.pub>...   checks hash chain, checkpoints and signatures, with every
                                          key the kernel ever had (store: sqlite:<path>, file:<path>
                                          or a bare SQLite path); exit 2: intact, but something was
                                          signed by a revoked key (SUSPECT lines)
  kernable certs new <dir> <client>...    writes a development CA, a kernel certificate and one
                                          client certificate per name, for mTLS
  kernable listeners <store>              lists registered listeners with order, position and middleware
  kernable listeners remove <store> <name>  removes a listener, and its middleware (kernel stopped)
  kernable vault rekey <vault> <old.key> <new.key>
                                          re-encrypts the vault with a new key (kernel stopped)
  kernable schemas <store>                lists the highest stored payload version per event type
  kernable migrate <store>                upgrades a store to this build's data format (kernel stopped;
                                          back the store up first); 1 to 2 drops the snapshots, which
                                          workers rebuild into the snapshot store
  kernable submit <addr> <client.key> <tenant> <domain> <aggregate_type> <aggregate_id> <command_type> <payload>
                                          signs and submits a command (addr: unix:/path or host:port)";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match args.as_slice() {
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

fn keys_new(name: &str) -> Result<bool> {
    let key = signing::generate_key();
    let key_id = signing::key_id(&key.verifying_key());
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(format!("{name}.key"))?
        .write_all(format!("{}\n", hex(key.as_bytes())).as_bytes())?;
    fs::write(format!("{name}.pub"), format!("{key_id}\n"))?;
    println!("{key_id}");
    Ok(true)
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
    println!("re-encrypted {count} values; put the new key into [vault] key");
    Ok(true)
}

fn listeners(store: &str) -> Result<bool> {
    for l in kernable_store::open_read_only(store)?.listeners()? {
        let types = match l.event_types.as_slice() {
            [] => "*".to_owned(),
            types => types.join(","),
        };
        let middleware = l.middleware.map_or(String::new(), |m| {
            format!(" middleware order {} checks {}", m.order, m.types.join(","))
        });
        println!(
            "{} order {} position {} types {types} key {}{middleware}",
            l.name, l.order, l.position, l.key_id
        );
    }
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
        Err(kernable_store::Error::Format(0)) => {
            println!(
                "no migration from format 0: it comes from a development build before format 1; \
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

async fn submit(addr: &str, key: &str, mut cmd: Command) -> Result<bool> {
    let key = signing::read_signing_key(key)?;
    cmd.client_sig = Some(signing::sign(&key, &signing::command_message(&cmd)));
    println!("command {}", cmd.command_id);
    let mut client = CommandServiceClient::new(transport::connect(addr).await?);
    let request = SubmitRequest {
        command: Some(cmd),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
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
