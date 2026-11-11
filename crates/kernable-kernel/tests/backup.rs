//! Continuous backup: a backup follower writes a kernel started with `--backup on` to a
//! target, continues where it left off after a restart, and a restore builds the same store
//! again, whole or up to a record.

mod common;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_kernel::{
    backup::{
        self, Follower, Until,
        crypt::BackupKey,
        signer::Reference,
        target::{self, Keys, Op, Signer},
    },
    standby::Stores,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The follower's key, granted `backup`.
fn follower(env: &Env, to: &str) -> Follower {
    let key = key(9);
    let grant = grant(&key, &[("backup", &["*"])]);
    env.kernel.grant(grant.clone()).unwrap();
    Follower {
        addrs: vec![env.addr.clone()],
        key,
        grant: hex(&grant.encode_to_vec()),
        target: Arc::from(target::open(to, None).unwrap()),
        crypt: Arc::new(BackupKey::new(&[7; 32])),
        segment_every: Duration::from_millis(100),
        base_every: Duration::from_secs(3600),
        keep: 3,
        check_every: None,
        status_file: None,
    }
}

/// Where the test's backup goes: a directory, or `KERNABLE_TEST_BACKUP_TARGET` (an
/// `s3://<bucket>/<prefix>` with the `KERNABLE_S3_*` and `AWS_*` variables), under a prefix of
/// the test's own.
fn target_of(env: &Env) -> String {
    let name = env.dir.file_name().unwrap().to_string_lossy();
    match std::env::var("KERNABLE_TEST_BACKUP_TARGET") {
        Ok(base) if !base.is_empty() => format!("{}/{name}", base.trim_end_matches('/')),
        _ => format!("file:{}", env.dir.join("backup").display()),
    }
}

/// How far the backup in `to` reaches, and in how many generations.
fn reach(to: &str) -> (u64, usize) {
    let t = target::open(to, None).unwrap();
    let gens = backup::generations(t.as_ref(), &BackupKey::new(&[7; 32])).unwrap();
    let last = gens.iter().rev().find_map(|g| {
        let b = g.base.as_ref()?;
        Some(g.segments.last().map_or(b.seq, |s| s.last))
    });
    (last.unwrap_or(0), gens.len())
}

async fn backed_up(env: &Env, to: &str) {
    let head = env.kernel_head();
    for _ in 0..500 {
        if reach(to).0 >= head {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the backup did not reach record {head}: {:?}", reach(to));
}

trait Head {
    fn kernel_head(&self) -> u64;
}

impl Head for Env {
    fn kernel_head(&self) -> u64 {
        let store = kernable_store::open_read_only(&self.store).unwrap();
        store.record_head().unwrap().seq
    }
}

async fn place(env: &Env, from: usize, to: usize) {
    for i in from..to {
        let cmd = signed(
            command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
            &key(1),
        );
        submit(&env.addr, cmd).await.unwrap();
    }
}

/// New stores in `dir`, named after `name`.
fn stores(env: &Env, name: &str) -> Stores {
    let path = env.dir.join(name);
    std::fs::create_dir_all(&path).unwrap();
    Stores {
        store: kernable_store::open(&format!("sqlite:{}", path.join("k.db").display())).unwrap(),
        vault: None,
        data: kernable_store::data::open(&format!("file:{}", path.join("data").display())).unwrap(),
        snapshots: kernable_store::snapshots::open("memory:").unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backup_restores_the_same_store_and_continues_after_a_restart() {
    let env = start_custom("backup", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = target_of(&env);
    place(&env, 0, 10).await;
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    backed_up(&env, &to).await;

    // Stopped and started again, it continues the same generation.
    running.abort();
    place(&env, 10, 20).await;
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    backed_up(&env, &to).await;
    running.abort();
    assert_eq!(reach(&to).1, 1, "one generation");

    let restored = stores(&env, "restored");
    let t = target::open(&to, None).unwrap();
    let done = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &restored,
        Until::End,
        &[id(&key(3))],
    )
    .unwrap();
    assert_eq!(done.record, env.kernel_head());
    let source = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(
        restored.store.read_all(0, 1_000).unwrap(),
        source.read_all(0, 1_000).unwrap(),
        "the same events"
    );
    assert!(restored.store.verify(&[id(&key(3))]).unwrap().is_ok());

    // Another key opens nothing.
    let wrong = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[8; 32]),
        &stores(&env, "wrong"),
        Until::End,
        &[],
    );
    assert!(wrong.unwrap_err().to_string().contains("another key"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_stops_at_a_record() {
    let env = start_custom("backup-at", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = target_of(&env);
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    place(&env, 0, 5).await;
    backed_up(&env, &to).await;
    let middle = env.kernel_head();
    place(&env, 5, 10).await;
    backed_up(&env, &to).await;
    running.abort();

    let restored = stores(&env, "restored");
    let t = target::open(&to, None).unwrap();
    let done = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &restored,
        Until::Record(middle),
        &[id(&key(3))],
    )
    .unwrap();
    assert_eq!(done.record, middle);
    assert_eq!(restored.store.record_head().unwrap().seq, middle);
    let events = restored.store.read_all(0, 1_000).unwrap();
    let source = kernable_store::open_read_only(&env.store).unwrap();
    let all = source.read_all(0, 1_000).unwrap();
    assert!(done.short.is_none(), "{:?}", done.short);
    assert!(events.len() < all.len(), "later orders are not restored");
    // Asked for more than the backup holds: everything, and it says so.
    let beyond = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &stores(&env, "beyond"),
        Until::Record(1_000_000),
        &[id(&key(3))],
    )
    .unwrap();
    assert!(
        beyond
            .short
            .as_deref()
            .is_some_and(|s| s.contains("the backup ends")),
        "{:?}",
        beyond.short
    );
    assert_eq!(
        events,
        all[..events.len()],
        "what is restored is the beginning"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_without_backup_on_refuses_the_follower() {
    let env = start("backup-off").await;
    let to = target_of(&env);
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.abort();
    assert_eq!(reach(&to), (0, 0), "nothing written");
}

/// `backup check` reads the newest generation through without a store: it reaches the kernel's
/// head; a segment changed in the target, or another key, does not check out.
#[tokio::test(flavor = "multi_thread")]
async fn a_backup_check_reads_it_through_and_finds_a_damaged_segment() {
    let env = start_custom("backup-check", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = format!("file:{}", env.dir.join("backup").display());
    place(&env, 0, 5).await;
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    backed_up(&env, &to).await;
    place(&env, 5, 15).await;
    backed_up(&env, &to).await;
    running.abort();

    let t = target::open(&to, None).unwrap();
    let checked = backup::check(t.as_ref(), &BackupKey::new(&[7; 32])).unwrap();
    // What the target holds (the kernel's head moves on with its checkpoints).
    assert_eq!(checked.record, reach(&to).0);
    assert!(checked.record >= 15);
    assert!(checked.segments > 0, "{checked:?}");
    assert!(backup::check(t.as_ref(), &BackupKey::new(&[8; 32])).is_err());

    // One byte of the newest segment changed: the key's authentication refuses it.
    let gens = backup::generations(t.as_ref(), &BackupKey::new(&[7; 32])).unwrap();
    let segment = &gens.last().unwrap().segments.last().unwrap().name;
    let path = env.dir.join("backup").join(segment);
    let mut bytes = std::fs::read(&path).unwrap();
    let at = bytes.len() / 2;
    bytes[at] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(backup::check(t.as_ref(), &BackupKey::new(&[7; 32])).is_err());
}

/// `--status-file`: the follower notes how far it uploaded and what its checks found.
#[tokio::test(flavor = "multi_thread")]
async fn the_status_file_says_how_far_the_backup_reached_and_what_its_check_found() {
    let env = start_custom("backup-status", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = format!("file:{}", env.dir.join("backup").display());
    let status = env.dir.join("backup.status");
    place(&env, 0, 5).await;
    let mut f = follower(&env, &to);
    f.status_file = Some(status.clone());
    f.check_every = Some(Duration::from_millis(200));
    let running = tokio::spawn(backup::run(f));
    backed_up(&env, &to).await;
    let head = env.kernel_head();
    let read = || -> backup::Status {
        serde_json::from_slice(&std::fs::read(&status).unwrap_or_default()).unwrap_or_default()
    };
    for _ in 0..100 {
        let s = read();
        if s.record >= head && s.checked_at > 0 && s.current_at > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    running.abort();
    let s = read();
    assert_eq!(s.record, head);
    assert!(
        s.uploaded_at > 0 && s.checked_at > 0 && s.current_at > 0,
        "{s:?}"
    );
    assert_eq!(s.check_error, None);
}

/// S3 as far as the follower uses it, in memory, path style, taking only requests presigned
/// with `keys` for the bucket `kernable-test` that have not expired: ListObjectsV2 in pages of
/// two, so listings take continuation tokens.
struct FakeS3 {
    endpoint: String,
    keys: Keys,
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    /// Requests taken, and refused (not presigned, or not right).
    taken: AtomicUsize,
    refused: AtomicUsize,
}

const BUCKET: &str = "kernable-test";

impl FakeS3 {
    async fn start() -> Arc<FakeS3> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let s3 = Arc::new(FakeS3 {
            keys: Keys::new(&endpoint, "eu-central", "AKTEST", "SKTEST", None, true).unwrap(),
            endpoint,
            objects: Mutex::new(BTreeMap::new()),
            taken: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
        });
        let serving = Arc::clone(&s3);
        tokio::spawn(async move {
            loop {
                let (conn, _) = listener.accept().await.unwrap();
                let s3 = Arc::clone(&serving);
                tokio::spawn(async move { s3.connection(conn).await });
            }
        });
        s3
    }

    async fn connection(&self, mut conn: tokio::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 65536];
        let end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i;
            }
            match conn.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..end]).into_owned();
        let mut lines = head.split("\r\n");
        let mut first = lines.next().unwrap().split(' ');
        let (method, target) = (first.next().unwrap(), first.next().unwrap());
        let length: usize = lines
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .map_or(0, |(_, v)| v.trim().parse().unwrap());
        let mut body = buf.split_off(end + 4);
        while body.len() < length {
            match conn.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => body.extend_from_slice(&chunk[..n]),
            }
        }
        let (status, answer) = match self.verify(method, target) {
            Ok(request) => {
                self.taken.fetch_add(1, Ordering::Relaxed);
                self.answer(request, body)
            }
            Err(why) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                (
                    403,
                    format!("<Error><Code>AccessDenied</Code><Message>{why}</Message></Error>")
                        .into_bytes(),
                )
            }
        };
        let head = format!(
            "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            answer.len()
        );
        let _ = conn.write_all(head.as_bytes()).await;
        let _ = conn.write_all(&answer).await;
        let _ = conn.shutdown().await;
    }

    /// The operation, key and continuation of a request, if it is presigned as `keys` would
    /// presign it, and not expired.
    fn verify(&self, method: &str, target: &str) -> Result<(Op, String, String), String> {
        let (path, query) = target.split_once('?').ok_or("not presigned")?;
        let params: Vec<(String, String)> = query
            .split('&')
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                (decode(k), decode(v))
            })
            .collect();
        let get = |name: &str| {
            params
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let object = decode(path)
            .strip_prefix(&format!("/{BUCKET}/"))
            .ok_or("another bucket")?
            .to_owned();
        let (op, key, continuation) = match (method, get("list-type")) {
            ("GET", Some(_)) => (
                Op::List,
                get("prefix").unwrap_or_default(),
                get("continuation-token").unwrap_or_default(),
            ),
            ("GET", None) => (Op::Get, object, String::new()),
            ("PUT", _) => (Op::Put, object, String::new()),
            ("DELETE", _) => (Op::Delete, object, String::new()),
            _ => return Err(format!("{method}?")),
        };
        let stamp = get("X-Amz-Date").ok_or("no X-Amz-Date")?;
        let at = kernable_kernel::config::utc_ms(&format!(
            "{}-{}-{}T{}:{}:{}Z",
            &stamp[..4],
            &stamp[4..6],
            &stamp[6..8],
            &stamp[9..11],
            &stamp[11..13],
            &stamp[13..15]
        ))
        .ok_or("a bad X-Amz-Date")?;
        let expires: u64 = get("X-Amz-Expires")
            .ok_or("no X-Amz-Expires")?
            .parse()
            .unwrap();
        if at + expires as i64 * 1000 < kernable_kernel::now_ms() {
            return Err("expired".into());
        }
        let expected = self.keys.presign(
            op,
            BUCKET,
            &key,
            &continuation,
            Duration::from_secs(expires),
            at,
        );
        if expected.url != format!("{}{target}", self.endpoint) {
            return Err("SignatureDoesNotMatch".into());
        }
        Ok((op, key, continuation))
    }

    fn answer(
        &self,
        (op, key, continuation): (Op, String, String),
        body: Vec<u8>,
    ) -> (u16, Vec<u8>) {
        let mut objects = self.objects.lock().unwrap();
        match op {
            Op::Put => {
                objects.insert(key, body);
                (200, Vec::new())
            }
            Op::Get => match objects.get(&key) {
                Some(bytes) => (200, bytes.clone()),
                None => (404, b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
            },
            Op::Delete => {
                objects.remove(&key);
                (204, Vec::new())
            }
            Op::List => {
                let page: Vec<&String> = objects
                    .keys()
                    .filter(|k| k.starts_with(&key) && **k > continuation)
                    .take(3)
                    .collect();
                let more = page.len() > 2;
                let mut xml = format!("<ListBucketResult><IsTruncated>{more}</IsTruncated>");
                for k in page.iter().take(2) {
                    xml.push_str(&format!(
                        "<Contents><Key>{}</Key></Contents>",
                        k.replace('&', "&amp;")
                    ));
                }
                if more {
                    xml.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        page[1]
                    ));
                }
                xml.push_str("</ListBucketResult>");
                (200, xml.into_bytes())
            }
        }
    }
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

/// The reference signer for `s3://kernable-test/shop`, signing with `s3`'s keys for key 9 (the
/// follower's), on `addr` (any free port if `None`); its address, and the task to stop it.
async fn start_signer(
    s3: &FakeS3,
    addr: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(addr.unwrap_or("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let reference = Reference::new(
        s3.keys.clone(),
        &format!("s3://{BUCKET}/shop"),
        [id(&key(9))],
        Duration::from_secs(60),
    )
    .unwrap();
    let task = tokio::spawn(async move {
        let _ = Arc::new(reference).serve(listener).await;
    });
    (addr, task)
}

/// How far the backup in `target` reaches, read on a blocking thread.
async fn reach_of(target: Arc<dyn target::Target>) -> Result<u64, String> {
    tokio::task::spawn_blocking(move || {
        let gens = backup::generations(target.as_ref(), &BackupKey::new(&[7; 32]))
            .map_err(|e| e.to_string())?;
        Ok(gens
            .iter()
            .rev()
            .find_map(|g| {
                let b = g.base.as_ref()?;
                Some(g.segments.last().map_or(b.seq, |s| s.last))
            })
            .unwrap_or(0))
    })
    .await
    .unwrap()
}

/// A follower whose S3 target holds no credentials: a reference signer presigns every request
/// for it. While the signer is away, the backup falls behind and says so in its status file;
/// once it is back, it catches up, and a restore through the signer builds the same store. A key
/// the signer does not sign for is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_backup_through_a_signer_falls_behind_while_it_is_away_and_catches_up() {
    let env = start_custom("backup-signer", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let s3 = FakeS3::start().await;
    let (addr, signing) = start_signer(&s3, None).await;
    let url = format!("http://{addr}/sign");
    let to = format!("s3://{BUCKET}/shop/a");
    let target = |key: SigningKey| -> Arc<dyn target::Target> {
        Arc::from(target::open(&to, Some(Signer::new(&url, key).unwrap())).unwrap())
    };
    let status = env.dir.join("backup.status");
    let read = || -> backup::Status {
        serde_json::from_slice(&std::fs::read(&status).unwrap_or_default()).unwrap_or_default()
    };
    async fn until(what: &str, mut done: impl AsyncFnMut() -> bool) {
        for _ in 0..600 {
            if done().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{what}");
    }

    place(&env, 0, 10).await;
    let mut f = follower(&env, &format!("file:{}", env.dir.join("unused").display()));
    f.target = target(key(9));
    f.status_file = Some(status.clone());
    let running = tokio::spawn(backup::run(f));
    let head = env.kernel_head();
    until("backed up", async || {
        reach_of(target(key(9))).await.is_ok_and(|r| r >= head)
    })
    .await;

    // The signer goes away: nothing more reaches the bucket, and the status file says why.
    signing.abort();
    let _ = signing.await;
    place(&env, 10, 20).await;
    until("the upload fails", async || read().upload_error.is_some()).await;
    let s = read();
    assert!(s.failing_since > 0, "{s:?}");
    assert!(
        s.upload_error
            .as_deref()
            .unwrap()
            .contains("the backup signer"),
        "{s:?}"
    );
    assert!(s.record < env.kernel_head(), "the backup is behind: {s:?}");

    // Back: the follower catches up with what the kernel kept.
    let (_, signing) = start_signer(&s3, Some(addr)).await;
    let head = env.kernel_head();
    until("caught up", async || {
        reach_of(target(key(9))).await.is_ok_and(|r| r >= head)
    })
    .await;
    until("the status is clear", async || {
        read().upload_error.is_none()
    })
    .await;
    assert_eq!(read().failing_since, 0);
    running.abort();

    let restored = stores(&env, "restored");
    let t = target(key(9));
    let done = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &restored,
        Until::End,
        &[id(&key(3))],
    )
    .unwrap();
    assert!(done.record >= head);
    let source = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(
        restored.store.read_all(0, 1_000).unwrap(),
        source.read_all(0, 1_000).unwrap(),
        "the same events"
    );
    assert!(
        s3.objects
            .lock()
            .unwrap()
            .keys()
            .all(|k| k.starts_with("shop/a/g")),
        "everything under the prefix"
    );
    assert!(
        s3.objects.lock().unwrap().len() > 2,
        "listings took more than one page"
    );
    assert!(s3.taken.load(Ordering::Relaxed) > 10);
    assert_eq!(
        s3.refused.load(Ordering::Relaxed),
        0,
        "every request presigned right"
    );

    // A key the signer does not sign for gets nothing.
    let error = reach_of(target(key(8))).await.unwrap_err();
    assert!(error.contains("refused list /: HTTP 403"), "{error}");
    drop(signing);
}
