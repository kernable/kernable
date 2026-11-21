//! The kit cell: the kit's domains under the same faults, on a kernel of their own (the kit's
//! authorization middleware decides every request of its kernel, and the bank's clients hold
//! no identity). Its host runs the kit in one language at a time, and the language changes at
//! every check round: TypeScript, Python, Rust, Go (the parity runners' host mode, each with
//! its own authorization store and mail file). Clients drive people through the Rust kit's
//! `KitApi`, over and over: register, verify the email with the mailed token, sign in, refresh,
//! reuse an old refresh token, sign in again, sign out, refresh after it.
//!
//! - K1: a refresh token works once: refreshing with one that was rotated is refused.
//! - K2: a reused refresh token ends its session: its newest refresh token is refused after it.
//! - K3: after a sign-out, the session's refresh token is refused.
//! - K4 (the log): in every session, nothing follows `SessionEnded`, each `SessionRefreshed`
//!   is the generation after the one before, and no refresh token hash is used twice.
//! - S1–S9, the store's invariants, over the kit kernel's log.
//!
//! A call the faults cut off (the kernel away, a timeout) ends that person's flow without a
//! verdict; only an answer that must not be is a violation.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kernable::{Kernable, Options};
use kernable_kit::api::{ApiError, KitApi, new_signing_keys};
use kernable_kit::tenancy::{AccountSignUp, SignUpPolicy, TenantSignUp};
use kernable_store::invariants::{Carry, Log};
use serde_json::Value;

use crate::{Rng, procs::Proc, say};

pub const LANGUAGES: [&str; 4] = ["ts", "py", "rs", "go"];

#[derive(Clone, Default)]
pub struct KitLoad {
    pub found: Arc<Mutex<Vec<String>>>,
    pub stats: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl KitLoad {
    fn count(&self, what: &str) {
        *self
            .stats
            .lock()
            .unwrap()
            .entry(what.to_owned())
            .or_default() += 1;
    }

    fn found(&self, what: String) {
        say(&format!("kit: {what}"));
        self.found.lock().unwrap().push(what);
    }
}

/// What the kit checks carry from round to round.
#[derive(Default)]
pub struct KitChecked {
    carry: Carry,
    /// Per session: its last generation, and whether it ended.
    sessions: BTreeMap<String, (u64, bool)>,
    hashes: BTreeSet<String>,
}

/// The host of the kit in `lang`, as the parity runners run it.
pub fn host(lang: &str, repo: &Path, exe: &Path, dir: &Path, url: &str) -> Proc {
    let data = dir.join(format!("kit-{lang}"));
    let _ = std::fs::create_dir_all(&data);
    let log = dir.join("logs").join(format!("kit-{lang}.log"));
    let examples = exe.parent().map(|p| p.join("examples")).unwrap_or_default();
    let p = match lang {
        "ts" => Proc::new("kit-host", "node", repo.join("kits/typescript"), log).args(&[
            "--conditions=source",
            "--no-warnings",
            "test/parity.ts",
            "host",
        ]),
        "py" => Proc::new(
            "kit-host",
            repo.join("sdks/python/.venv/bin/python"),
            repo.join("kits/python"),
            log,
        )
        .args(&["-m", "tests.parity", "host"])
        .env(
            "PYTHONPATH",
            &format!(
                "{}:{}",
                repo.join("sdks/python").display(),
                repo.join("kits/python").display()
            ),
        ),
        "rs" => Proc::new(
            "kit-host",
            examples.join("parity"),
            repo.join("kits/rust"),
            log,
        )
        .args(&["host"]),
        _ => Proc::new(
            "kit-host",
            examples.join("parity-go"),
            repo.join("sdks/go"),
            log,
        )
        .args(&["host"]),
    };
    p.env("PARITY_URL", url)
        .env("PARITY_KEY", &dir.join("kit.key").display().to_string())
        .env("PARITY_DIR", &data.display().to_string())
        .env("KERNABLE_LOG", "warn")
}

/// The newest mail to `to` of `purpose` in any host's mail file.
fn mail_token(dir: &Path, to: &str, purpose: &str) -> Option<String> {
    let mut found = None;
    for lang in LANGUAGES {
        let path = dir.join(format!("kit-{lang}")).join("mails.jsonl");
        for line in std::fs::read_to_string(path).unwrap_or_default().lines() {
            if let Ok(m) = serde_json::from_str::<Value>(line)
                && m["to"] == to
                && m["purpose"] == purpose
            {
                found = m["token"].as_str().map(str::to_owned);
            }
        }
    }
    found
}

/// The kit's clients: after the bootstrap, `clients` people at a time go through their flow
/// while `running` is set.
pub fn drive(
    kit: KitLoad,
    url: String,
    dir: PathBuf,
    running: Arc<AtomicBool>,
    clients: u64,
    seed: u64,
    run: String,
) {
    tokio::spawn(async move {
        let options = || Options::new(dir.join("kit.key")).url(&url);
        let kernable = match Kernable::new(options()) {
            Ok(k) => k,
            Err(e) => return kit.found(format!("no client: {e}")),
        };
        for d in kernable_kit::kit_domains() {
            kernable.with_domain(&d);
        }
        let policy = SignUpPolicy {
            tenants: TenantSignUp::Active,
            accounts: AccountSignUp::Open,
        };
        // The bootstrap may run again; until the kit's host and kernel are up it fails.
        let booted = loop {
            match kernable_kit::bootstrap::bootstrap(&kernable, policy, "Platform", None).await {
                Ok(b) => break b,
                Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        };
        let api = KitApi::new(
            kernable,
            new_signing_keys(),
            &booted.api_identity,
            &booted.api_key_id,
        );
        say("kit: bootstrapped; people start signing up");
        for n in 0..clients {
            let (kit, api, dir, running, run) = (
                kit.clone(),
                api.clone(),
                dir.clone(),
                Arc::clone(&running),
                run.clone(),
            );
            tokio::spawn(async move {
                let mut rng = Rng::new(seed.wrapping_add(n + 101));
                for i in 0..u64::MAX {
                    while !running.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    let email = format!("p-{run}-{n}-{i}@kit.example");
                    let outcome = flow(&api, &dir, &email, &kit, &mut rng).await;
                    kit.count(&format!("flow {outcome}"));
                }
            });
        }
    });
}

/// Whether a call's error is a refusal (an answer) rather than a failure without one.
fn refused<T>(r: &Result<T, ApiError>) -> bool {
    matches!(r, Err(ApiError::Refused(_)))
}

/// One person's flow; what came of it.
async fn flow(api: &KitApi, dir: &Path, email: &str, kit: &KitLoad, rng: &mut Rng) -> &'static str {
    let password = "correct horse battery staple";
    if api.register(email, "Soak", Some(password)).await.is_err() {
        return "cut off at register";
    }
    let mut token = None;
    for _ in 0..240 {
        token = mail_token(dir, email, "verify");
        if token.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let Some(token) = token else {
        return "no verification mail in 60 s";
    };
    match api.verify_email(&token).await {
        Ok(true) => {}
        Ok(false) => return "verification refused",
        Err(_) => return "cut off at verify",
    }
    let Ok(first) = api.sign_in(email, password, None, None).await else {
        return "cut off at sign-in";
    };
    let Ok((_, second)) = api.refresh(&first.refresh_token, None).await else {
        return "cut off at refresh";
    };
    // K1: the rotated token is refused; K2: and that ends the session.
    match api.refresh(&first.refresh_token, None).await {
        Ok(_) => {
            kit.found(format!(
                "K1 refresh once: {email}'s rotated refresh token worked again"
            ));
            return "violation";
        }
        Err(ApiError::Kernel(_)) => return "cut off at reuse",
        Err(ApiError::Refused(_)) => {}
    }
    match api.refresh(&second, None).await {
        Ok(_) => {
            kit.found(format!(
                "K2 reuse ends the session: {email}'s newest refresh token worked after a reuse"
            ));
            return "violation";
        }
        Err(ApiError::Kernel(_)) => return "cut off after reuse",
        Err(ApiError::Refused(_)) => {}
    }
    if rng.below(2) == 0 {
        return "done (reuse)";
    }
    let Ok(again) = api.sign_in(email, password, None, None).await else {
        return "cut off at the second sign-in";
    };
    let Some(claims) = api.check(&again.access_token) else {
        kit.found(format!("K: {email}'s fresh access token does not check"));
        return "violation";
    };
    if api.sign_out(&claims).await.is_err() {
        return "cut off at sign-out";
    }
    // K3: nothing refreshes after the sign-out.
    let after = api.refresh(&again.refresh_token, None).await;
    if after.is_ok() {
        kit.found(format!(
            "K3 sign-out ends the session: {email}'s refresh token worked after it"
        ));
        return "violation";
    }
    if !refused(&after) {
        return "cut off after sign-out";
    }
    "done (sign-out)"
}

/// K4 and S1–S9 over the kit kernel's log since the round before.
pub async fn check(
    store: &str,
    kernel_key_ids: &[String],
    checked: &mut KitChecked,
    kit: &KitLoad,
) -> Vec<String> {
    let mut violations: Vec<String> = std::mem::take(&mut *kit.found.lock().unwrap());
    let (path, keys, after) = (
        store.to_owned(),
        kernel_key_ids.to_vec(),
        checked.carry.head,
    );
    let log = tokio::task::spawn_blocking(move || {
        kernable_store::open_read_only(&path)
            .and_then(|read| Log::read_since(read.as_ref(), &keys, &[], after))
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(|r| r);
    let log = match log {
        Ok(log) => log,
        Err(e) => {
            violations.push(format!("kit: reading its store: {e}"));
            return violations;
        }
    };
    violations.extend(
        log.check_carried(&mut checked.carry)
            .iter()
            .map(|v| format!("kit store: {v}")),
    );
    for e in log
        .events
        .iter()
        .filter(|e| e.domain == "account" && e.aggregate_type == "session")
    {
        let session = checked
            .sessions
            .entry(e.aggregate_id.clone())
            .or_insert((1, false));
        if session.1 {
            violations.push(format!(
                "K4 session ended: {} {} at {} after SessionEnded",
                e.aggregate_id, e.event_type, e.global_position
            ));
        }
        match e.event_type.as_str() {
            "SessionEnded" => session.1 = true,
            "SessionRefreshed" => {
                let data: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
                let generation = data["generation"].as_u64().unwrap_or(0);
                if generation != session.0 + 1 {
                    violations.push(format!(
                        "K4 generations: session {} refreshed to {generation} after {} at {}",
                        e.aggregate_id, session.0, e.global_position
                    ));
                }
                session.0 = generation;
                if let Some(hash) = data["refreshTokenHash"].as_str()
                    && !checked.hashes.insert(hash.to_owned())
                {
                    violations.push(format!(
                        "K4 refresh token used once: hash at {} seen before",
                        e.global_position
                    ));
                }
            }
            _ => {}
        }
    }
    violations
}

/// Waits until the kit kernel has nothing pending and every listener is at its head.
pub async fn quiet(store: &str, limit: Duration) -> Result<(), String> {
    let started = std::time::Instant::now();
    loop {
        let path = store.to_owned();
        let state = tokio::task::spawn_blocking(move || {
            let read = kernable_store::open_read_only(&path).map_err(|e| e.to_string())?;
            let head = read.head_position().map_err(|e| e.to_string())?;
            let behind = read
                .listeners()
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter(|l| !l.removed && l.position != head)
                .count();
            let pending = read.pending().map_err(|e| e.to_string())?.len();
            Ok::<_, String>((behind, pending))
        })
        .await
        .map_err(|e| e.to_string())?;
        if let Ok((0, 0)) = state {
            return Ok(());
        }
        if started.elapsed() > limit {
            return Err(format!("kit: not quiet after {limit:?}: {state:?}"));
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}
