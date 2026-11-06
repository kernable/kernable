//! The witness: a small third process that decides which kernel of a pair is
//! the primary.
//!
//! It keeps three things on its disk: the term, the primary of that term (its key and address),
//! and the standby in sync, if the primary has one. The primary renews a lease every second or
//! so. A standby may take the primary's place (`CLAIM`) only if it is the one in sync and the
//! lease has expired here, plus a margin for clocks that run at slightly different rates. The
//! primary itself stops telling anyone of a write it commits alone once its lease runs out,
//! measured from when it asked, a margin earlier: so the two never both decide.
//!
//! A restarted witness grants no claim for a lease's length plus the margin: it does not know
//! when the primary renewed last.

use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable_core::{
    proto::{
        Grant, WitnessAnswer, WitnessOp, WitnessRequest, WitnessState,
        witness_service_server::{WitnessService, WitnessServiceServer},
    },
    signing, transport,
};
use prost::Message;
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

use crate::{BoxError, GRANT_HEADER, MAX_REQUEST_AGE_MS, Permissions, config, now_ms, protocol};

/// How long a lease lasts by default.
pub const LEASE_MS: u64 = 3_000;

/// What `kernable witness` runs with.
#[derive(Debug, Clone)]
pub struct WitnessConfig {
    pub listen: String,
    /// Root key IDs: the nodes' grants must be signed by one of them.
    pub roots: Vec<String>,
    /// The file it keeps its state in.
    pub state: PathBuf,
    pub lease_ms: u64,
    pub tls: Option<config::Tls>,
}

/// What a request asks, without its signature: what [`Witness::decide`] decides on.
#[derive(Debug, Clone)]
pub struct Ask {
    pub op: WitnessOp,
    pub node: String,
    pub term: u64,
    pub address: String,
    pub standby: String,
}

/// The witness's decisions, apart from the network.
pub struct Witness {
    inner: Mutex<Inner>,
    path: PathBuf,
    lease: Duration,
}

struct Inner {
    kept: WitnessState,
    /// Until when the primary's lease runs, here.
    lease_until: Instant,
    /// The primary handed its place over (HANDOVER): its standby claims without waiting,
    /// and it renews no more. Not kept on the disk: a restarted witness waits a lease anyway.
    released: bool,
}

impl Witness {
    /// Opens the state at `path`, or starts without a primary if there is none.
    pub fn open(path: &Path, lease: Duration) -> Result<Self, BoxError> {
        let kept = match std::fs::read(path) {
            Ok(bytes) => WitnessState::decode(bytes.as_slice())
                .map_err(|e| format!("{}: not a witness state ({e})", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => WitnessState::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Witness {
            // As if the primary had just renewed: no claim before its lease could have run out.
            inner: Mutex::new(Inner {
                kept,
                lease_until: Instant::now() + lease,
                released: false,
            }),
            path: path.to_owned(),
            lease,
        })
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The margin for clocks: a claim waits it beyond the lease, and the primary stops telling
    /// it before the lease ends.
    pub fn margin(lease: Duration) -> Duration {
        lease / 3
    }

    /// What the witness holds now.
    pub fn state(&self) -> WitnessState {
        self.inner().kept.clone()
    }

    /// Decides `ask` at `now`; a change is on the disk before the answer.
    pub fn decide(&self, ask: &Ask, now: Instant) -> Result<WitnessAnswer, BoxError> {
        let mut inner = self.inner();
        let kept = inner.kept.clone();
        let refuse = |message: String| Ok(answer(&kept, false, &message, self.lease));
        let primary = !kept.primary.is_empty() && ask.node == kept.primary;
        let leased = now < inner.lease_until;
        let mut next = kept.clone();
        match ask.op {
            WitnessOp::Lease if kept.term == 0 => {
                next = WitnessState {
                    term: 1,
                    primary: ask.node.clone(),
                    primary_address: ask.address.clone(),
                    in_sync: String::new(),
                };
            }
            WitnessOp::Lease if primary && inner.released => {
                return refuse("the primary handed its place over to its standby".into());
            }
            WitnessOp::Lease if primary => next.primary_address = ask.address.clone(),
            WitnessOp::Lease => {
                return refuse(format!(
                    "the primary of term {} is {} at {}",
                    kept.term, kept.primary, kept.primary_address
                ));
            }
            WitnessOp::Handover if !primary || ask.term != kept.term || !leased => {
                return refuse(format!(
                    "only the primary of term {} may hand over, while its lease holds",
                    kept.term
                ));
            }
            WitnessOp::Handover if kept.in_sync.is_empty() => {
                return refuse("no standby is in sync: there is nobody to hand over to".into());
            }
            WitnessOp::Handover => {
                inner.released = true;
                inner.lease_until = now;
                tracing::warn!(term = kept.term, primary = %kept.primary, standby = %kept.in_sync, "the primary hands its place over");
                return Ok(answer(&kept, true, "", self.lease));
            }
            WitnessOp::Alone | WitnessOp::InSync if !primary || ask.term != kept.term => {
                return refuse(format!("only the primary of term {} may say so", kept.term));
            }
            WitnessOp::Alone | WitnessOp::InSync if !leased => {
                return refuse("the primary's lease ran out; it renews first".into());
            }
            WitnessOp::Alone => next.in_sync = String::new(),
            WitnessOp::InSync => next.in_sync = ask.standby.clone(),
            WitnessOp::Claim if primary => {
                return refuse("the node is the primary already".into());
            }
            WitnessOp::Claim if kept.in_sync.is_empty() || ask.node != kept.in_sync => {
                return refuse(if kept.in_sync.is_empty() {
                    "the primary commits alone: no standby holds all its writes".into()
                } else {
                    format!("the standby in sync is {}", kept.in_sync)
                });
            }
            WitnessOp::Claim
                if !inner.released && now < inner.lease_until + Self::margin(self.lease) =>
            {
                return refuse(format!(
                    "the primary {} still holds its lease",
                    kept.primary
                ));
            }
            WitnessOp::Claim => {
                next = WitnessState {
                    term: kept.term + 1,
                    primary: ask.node.clone(),
                    primary_address: ask.address.clone(),
                    in_sync: String::new(),
                };
            }
            WitnessOp::Unspecified => return refuse("no op".into()),
        }
        if next != kept {
            write_state(&self.path, &next)?;
            if next.term != kept.term {
                tracing::warn!(term = next.term, primary = %next.primary, address = %next.primary_address, "a new primary");
            } else if next.in_sync != kept.in_sync {
                match next.in_sync.as_str() {
                    "" => tracing::warn!(term = next.term, "the primary commits alone"),
                    standby => tracing::info!(term = next.term, %standby, "the standby is in sync"),
                }
            }
            inner.kept = next.clone();
        }
        if matches!(ask.op, WitnessOp::Lease | WitnessOp::Claim) {
            inner.lease_until = now + self.lease;
        }
        if next.term != kept.term {
            inner.released = false;
        }
        Ok(answer(&next, true, "", self.lease))
    }
}

fn answer(kept: &WitnessState, granted: bool, message: &str, lease: Duration) -> WitnessAnswer {
    WitnessAnswer {
        granted,
        term: kept.term,
        primary: kept.primary.clone(),
        primary_address: kept.primary_address.clone(),
        in_sync: kept.in_sync.clone(),
        lease_ms: lease.as_millis() as u64,
        message: message.to_owned(),
    }
}

/// Writes the state next to its place, flushed, then renames it over: a crash leaves the old
/// state or the new one.
fn write_state(path: &Path, state: &WitnessState) -> Result<(), BoxError> {
    let partial = path.with_extension("partial");
    let mut file = std::fs::File::create(&partial)?;
    file.write_all(&state.encode_to_vec())?;
    file.sync_all()?;
    std::fs::rename(&partial, path)?;
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

struct Api {
    witness: Witness,
    roots: Vec<String>,
}

#[tonic::async_trait]
impl WitnessService for Api {
    async fn ask(
        &self,
        request: Request<WitnessRequest>,
    ) -> Result<Response<WitnessAnswer>, Status> {
        let presented = request
            .metadata()
            .get(GRANT_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| hex::decode(v.trim()).ok())
            .and_then(|b| Grant::decode(b.as_slice()).ok());
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("the request is not signed"))?;
        let msg =
            signing::witness_message(req.op, req.term, &req.address, &req.standby, req.issued_at);
        if sig.key_id != req.node || signing::verify(sig, &msg).is_err() {
            return Err(Status::unauthenticated("invalid signature"));
        }
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }
        // The witness keeps no grants: every request presents its own.
        let grant = presented
            .filter(|g| g.key_id == req.node)
            .filter(|g| {
                g.root_sig.as_ref().is_some_and(|s| {
                    self.roots.contains(&s.key_id)
                        && signing::verify(s, &signing::grant_message(g)).is_ok()
                })
            })
            .ok_or_else(|| {
                Status::permission_denied("present the node's grant, signed by a trusted root")
            })?;
        let grants = HashMap::from([(grant.key_id.clone(), grant)]);
        if !config::allowed(
            &Permissions::new(&grants, &HashMap::new()).replicate,
            &req.node,
            "*",
        ) {
            return Err(Status::permission_denied(
                "the node's grant needs replicate for *",
            ));
        }
        let ask = Ask {
            op: req.op(),
            node: req.node,
            term: req.term,
            address: req.address,
            standby: req.standby,
        };
        self.witness
            .decide(&ask, Instant::now())
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

/// Serves the witness until it fails.
pub async fn run(config: WitnessConfig) -> Result<(), BoxError> {
    let lease = Duration::from_millis(config.lease_ms);
    let witness = Witness::open(&config.state, lease)?;
    let state = witness.state();
    tracing::info!(listen = %config.listen, term = state.term, primary = %state.primary, "witness starting");
    let api = Api {
        witness,
        roots: config.roots.clone(),
    };
    let listen = config.listen.clone();
    let mut builder = Server::builder();
    if config.tls.is_none() && transport::unix_path(&listen).is_none() && !crate::loopback(&listen)
    {
        return Err(format!(
            "{listen} is reachable from other machines: use mutual TLS (--tls-cert, --tls-key, --tls-client-ca)"
        )
        .into());
    }
    if let Some(tls) = &config.tls {
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let client_ca = Certificate::from_pem(std::fs::read(&tls.client_ca)?);
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )?;
    }
    let router = builder.add_service(WitnessServiceServer::new(api));
    match transport::unix_path(&listen) {
        #[cfg(unix)]
        Some(path) => {
            use std::os::unix::fs::FileTypeExt;
            if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                std::fs::remove_file(path)?;
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            // Its owner and group connect, as to a kernel's socket: a proxy in front of it too,
            // through an ACL.
            crate::socket_mode(path)?;
            router
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await?;
        }
        #[cfg(not(unix))]
        Some(_) => return Err("unix: sockets need Unix; listen on host:port".into()),
        None => {
            let addr = tokio::net::lookup_host(&listen)
                .await?
                .next()
                .ok_or_else(|| format!("{listen}: no address"))?;
            router.serve(addr).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(op: WitnessOp, node: &str, term: u64) -> Ask {
        Ask {
            op,
            node: node.into(),
            term,
            address: format!("{node}:7233"),
            standby: String::new(),
        }
    }

    fn open(name: &str) -> (Witness, PathBuf) {
        let path = std::env::temp_dir().join(format!("kn-witness-{name}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        (Witness::open(&path, Duration::from_secs(3)).unwrap(), path)
    }

    #[test]
    fn the_first_node_becomes_the_primary_and_the_other_learns_of_it() {
        let (w, path) = open("first");
        let now = Instant::now();
        let a = w.decide(&ask(WitnessOp::Lease, "a", 0), now).unwrap();
        assert!(a.granted);
        assert_eq!((a.term, a.primary.as_str()), (1, "a"));
        let b = w.decide(&ask(WitnessOp::Lease, "b", 0), now).unwrap();
        assert!(!b.granted);
        assert_eq!(
            (b.term, b.primary.as_str(), b.primary_address.as_str()),
            (1, "a", "a:7233")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn only_the_standby_in_sync_claims_and_only_once_the_lease_ran_out() {
        let (w, path) = open("claim");
        let t0 = Instant::now();
        w.decide(&ask(WitnessOp::Lease, "a", 0), t0).unwrap();
        let claim =
            |node: &str, at: Instant| w.decide(&ask(WitnessOp::Claim, node, 1), at).unwrap();
        let late = t0 + Duration::from_secs(10);
        assert!(!claim("b", late).granted, "no standby is in sync yet");

        let mut in_sync = ask(WitnessOp::InSync, "a", 1);
        in_sync.standby = "b".into();
        assert!(w.decide(&in_sync, t0).unwrap().granted);
        assert!(!claim("c", late).granted, "c is not the standby in sync");
        assert!(
            !claim("b", t0 + Duration::from_secs(3)).granted,
            "the lease and the margin run"
        );
        let won = claim("b", t0 + Duration::from_secs(5));
        assert!(won.granted);
        assert_eq!(
            (won.term, won.primary.as_str(), won.in_sync.as_str()),
            (2, "b", "")
        );

        // The old primary learns it was deposed, and may no longer speak for the term.
        let old = w.decide(&ask(WitnessOp::Lease, "a", 1), late).unwrap();
        assert!(!old.granted);
        assert_eq!(old.primary, "b");
        assert!(
            !w.decide(&ask(WitnessOp::Alone, "a", 1), late)
                .unwrap()
                .granted
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_primary_that_commits_alone_has_no_standby_to_be_replaced_by() {
        let (w, path) = open("alone");
        let t0 = Instant::now();
        w.decide(&ask(WitnessOp::Lease, "a", 0), t0).unwrap();
        let mut in_sync = ask(WitnessOp::InSync, "a", 1);
        in_sync.standby = "b".into();
        w.decide(&in_sync, t0).unwrap();
        assert!(
            w.decide(&ask(WitnessOp::Alone, "a", 1), t0)
                .unwrap()
                .granted
        );
        let claim = w
            .decide(&ask(WitnessOp::Claim, "b", 1), t0 + Duration::from_secs(10))
            .unwrap();
        assert!(!claim.granted, "{claim:?}");
        // Without a lease, the primary may not go alone.
        let expired = w
            .decide(&ask(WitnessOp::Alone, "a", 1), t0 + Duration::from_secs(10))
            .unwrap();
        assert!(!expired.granted);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn the_state_survives_a_restart_and_a_restarted_witness_waits_a_lease_before_a_claim() {
        let (w, path) = open("restart");
        let t0 = Instant::now();
        w.decide(&ask(WitnessOp::Lease, "a", 0), t0).unwrap();
        let mut in_sync = ask(WitnessOp::InSync, "a", 1);
        in_sync.standby = "b".into();
        w.decide(&in_sync, t0).unwrap();
        drop(w);
        let w = Witness::open(&path, Duration::from_secs(3)).unwrap();
        let kept = w.state();
        assert_eq!(
            (kept.term, kept.primary.as_str(), kept.in_sync.as_str()),
            (1, "a", "b")
        );
        let now = Instant::now();
        assert!(
            !w.decide(&ask(WitnessOp::Claim, "b", 1), now)
                .unwrap()
                .granted
        );
        assert!(
            w.decide(&ask(WitnessOp::Claim, "b", 1), now + Duration::from_secs(5))
                .unwrap()
                .granted
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_handover_lets_the_standby_in_sync_claim_at_once_and_ends_the_old_lease() {
        let (w, path) = open("handover");
        let t0 = Instant::now();
        w.decide(&ask(WitnessOp::Lease, "a", 0), t0).unwrap();
        let handover = |node: &str, at: Instant| {
            w.decide(&ask(WitnessOp::Handover, node, 1), at)
                .unwrap()
                .granted
        };
        assert!(!handover("a", t0), "no standby in sync to hand over to");
        let mut in_sync = ask(WitnessOp::InSync, "a", 1);
        in_sync.standby = "b".into();
        assert!(w.decide(&in_sync, t0).unwrap().granted);
        assert!(!handover("b", t0), "only the primary hands over");
        let t1 = t0 + Duration::from_millis(500);
        assert!(handover("a", t1));
        // The old primary renews no more; the standby claims well before the lease would end.
        assert!(
            !w.decide(&ask(WitnessOp::Lease, "a", 1), t1)
                .unwrap()
                .granted
        );
        let won = w.decide(&ask(WitnessOp::Claim, "b", 1), t1).unwrap();
        assert!(won.granted, "{}", won.message);
        assert_eq!((won.term, won.primary.as_str()), (2, "b"));
        // The new primary renews as any primary does.
        assert!(
            w.decide(&ask(WitnessOp::Lease, "b", 2), t1)
                .unwrap()
                .granted
        );
        let _ = std::fs::remove_file(path);
    }
}
