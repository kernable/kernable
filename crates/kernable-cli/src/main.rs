use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use kernable_core::{
    proto::{Command, command_result::Outcome, command_service_client::CommandServiceClient},
    signing, transport,
};
use kernable_store_sqlite::SqliteStore;

const USAGE: &str = "usage:
  kernable keys new <name>                writes <name>.key (secret) and <name>.pub (key ID)
  kernable checkpoint <db> <kernel.key>   signs the current head of the event log
  kernable verify <db> <kernel.pub>       checks hash chain, checkpoints and signatures
  kernable submit <addr> <client.key> <tenant> <aggregate_type> <aggregate_id> <command_type> <payload>
                                          signs and submits a command (addr: unix:/path or host:port)";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match args.as_slice() {
        ["keys", "new", name] => keys_new(name),
        ["checkpoint", db, key] => checkpoint(db, key),
        ["verify", db, key] => verify(db, key),
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

fn checkpoint(db: &str, key: &str) -> Result<bool> {
    let key = signing::read_signing_key(key)?;
    match SqliteStore::open(db)?.checkpoint(&key, now_ms())? {
        Some(cp) => println!("checkpoint at position {}", cp.global_position),
        None => println!("no events yet"),
    }
    Ok(true)
}

fn verify(db: &str, key: &str) -> Result<bool> {
    let key_id = signing::key_id(&signing::read_verifying_key(key)?);
    let report = SqliteStore::open(db)?.verify(&key_id)?;
    println!(
        "{} events, {} commands, {} checkpoints, {} events after the last checkpoint",
        report.events, report.commands, report.checkpoints, report.unanchored
    );
    for p in &report.problems {
        println!("PROBLEM {p}");
    }
    println!("{}", if report.is_ok() { "OK" } else { "TAMPERED" });
    Ok(report.is_ok())
}

async fn submit(addr: &str, key: &str, mut cmd: Command) -> Result<bool> {
    let key = signing::read_signing_key(key)?;
    cmd.client_sig = Some(signing::sign(&key, &signing::command_message(&cmd)));
    println!("command {}", cmd.command_id);
    let mut client = CommandServiceClient::new(transport::connect(addr).await?);
    match client.submit(cmd).await?.into_inner().outcome {
        Some(Outcome::Committed(c)) => {
            for e in c.events {
                let payload = String::from_utf8_lossy(&e.payload);
                println!(
                    "committed #{} v{} {} {payload}",
                    e.global_position, e.version, e.event_type
                );
            }
            Ok(true)
        }
        Some(Outcome::Rejected(r)) => {
            println!("rejected {}: {}", r.code, r.message);
            Ok(false)
        }
        None => Ok(false),
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
