use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use kernable_core::signing;
use kernable_store_sqlite::SqliteStore;

const USAGE: &str = "usage:
  kernable keys new <name>                writes <name>.key (secret) and <name>.pub (key ID)
  kernable checkpoint <db> <kernel.key>   signs the current head of the event log
  kernable verify <db> <kernel.pub>       checks hash chain, checkpoints and signatures";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match args.as_slice() {
        ["keys", "new", name] => keys_new(name),
        ["checkpoint", db, key] => checkpoint(db, key),
        ["verify", db, key] => verify(db, key),
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}
