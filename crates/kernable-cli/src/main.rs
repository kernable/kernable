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
  kernable verify <store> <kernel.pub>      checks hash chain, checkpoints and signatures
                                          (store: sqlite:<path>, file:<path> or a bare SQLite path)
  kernable certs new <dir> <client>...    writes a development CA, a kernel certificate and one
                                          client certificate per name, for mTLS
  kernable listeners <store>              lists registered listeners with order and position
  kernable listeners remove <store> <name>  removes a listener (kernel stopped)
  kernable submit <addr> <client.key> <tenant> <aggregate_type> <aggregate_id> <command_type> <payload>
                                          signs and submits a command (addr: unix:/path or host:port)";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match args.as_slice() {
        ["keys", "new", name] => keys_new(name),
        ["checkpoint", store, key] => checkpoint(store, key),
        ["verify", store, key] => verify(store, key),
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
        ["listeners", "remove", store, name] => remove_listener(store, name),
        [
            "submit",
            addr,
            key,
            tenant,
            agg_type,
            agg_id,
            cmd_type,
            payload,
        ] => {
            let cmd = Command {
                command_id: random_id(),
                tenant_id: tenant.to_string(),
                aggregate_type: agg_type.to_string(),
                aggregate_id: agg_id.to_string(),
                command_type: cmd_type.to_string(),
                payload: payload.as_bytes().to_vec(),
                client_sig: None,
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

fn verify(store: &str, key: &str) -> Result<bool> {
    let key_id = signing::key_id(&signing::read_verifying_key(key)?);
    let report = kernable_store::open_read_only(store)?.verify(&key_id)?;
    println!(
        "{} events, {} commands, {} checkpoints, {} snapshots, {} events after the last checkpoint",
        report.events, report.commands, report.checkpoints, report.snapshots, report.unanchored
    );
    for p in &report.problems {
        println!("PROBLEM {p}");
    }
    println!("{}", if report.is_ok() { "OK" } else { "TAMPERED" });
    Ok(report.is_ok())
}

fn listeners(store: &str) -> Result<bool> {
    for l in kernable_store::open_read_only(store)?.listeners()? {
        let types = match l.event_types.as_slice() {
            [] => "*".to_owned(),
            types => types.join(","),
        };
        println!(
            "{} order {} position {} types {types} key {}",
            l.name, l.order, l.position, l.key_id
        );
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
