//! A kernel of a pair: the witness decides which one is the primary.
//!
//! [`become_primary`] asks the witness for this kernel's role. If it is not the primary of
//! the newest term, it follows the primary as a standby until it takes the primary's place
//! (it claims it once the primary's lease ran out, or an operator promotes it), then it opens
//! its stores as the primary. [`NodePrimary::lease`] then keeps the lease, and decides with
//! the witness when the primary commits alone (its standby is away or behind for
//! `--standby-timeout-ms`) and when the standby is in sync again.
//!
//! A primary whose lease the witness gave to the other kernel was deposed: the lease future
//! ends with an error, and the process stops; started again, it follows the new primary.

use std::{
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        WitnessAnswer, WitnessOp, WitnessRequest, witness_service_client::WitnessServiceClient,
    },
    signing,
};
use tonic::transport::Channel;

use crate::{
    BoxError, GRANT_HEADER, Kernel, config::Config, config::Follow, now_ms, standby,
    witness::Witness,
};

/// This kernel towards the witness.
pub(crate) struct Link {
    witness: String,
    key: SigningKey,
    /// This kernel's key ID.
    pub(crate) me: String,
    grant: String,
    advertise: String,
    channel: Mutex<Option<Channel>>,
    /// The lease the witness grants, from its last answer.
    lease_ms: Mutex<u64>,
}

impl Link {
    fn open(config: &Config) -> Result<Self, BoxError> {
        let pair = config
            .replication
            .pair
            .as_ref()
            .ok_or("run with --witness")?;
        let key = signing::read_signing_key(&pair.node_key)
            .map_err(|e| format!("--node-key {}: {e}", pair.node_key.display()))?;
        let grant_path = pair.node_key.with_extension("grant");
        let grant = std::fs::read_to_string(&grant_path)
            .map_err(|e| format!("the node's grant {}: {e}", grant_path.display()))?
            .trim()
            .to_owned();
        Ok(Link {
            witness: pair.witness.clone(),
            me: signing::key_id(&key.verifying_key()),
            key,
            grant,
            advertise: pair.advertise.clone(),
            channel: Mutex::default(),
            lease_ms: Mutex::new(crate::witness::LEASE_MS),
        })
    }

    /// How long a lease lasts, as the witness said last.
    pub(crate) fn lease(&self) -> Duration {
        Duration::from_millis(*self.lease_ms.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The margin for clocks (see [`Witness::margin`]).
    pub(crate) fn margin(&self) -> Duration {
        Witness::margin(self.lease())
    }

    /// Asks the witness once.
    pub(crate) async fn ask(
        &self,
        op: WitnessOp,
        term: u64,
        standby: &str,
    ) -> Result<WitnessAnswer, BoxError> {
        let issued_at = now_ms();
        let msg = signing::witness_message(op.into(), term, &self.advertise, standby, issued_at);
        let mut request = tonic::Request::new(WitnessRequest {
            op: op.into(),
            node: self.me.clone(),
            term,
            address: self.advertise.clone(),
            standby: standby.to_owned(),
            issued_at,
            sig: Some(signing::sign(&self.key, &msg)),
            protocol: kernable_core::PROTOCOL,
        });
        request
            .metadata_mut()
            .insert(GRANT_HEADER, self.grant.parse()?);
        let channel = {
            let cached = self
                .channel
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            match cached {
                Some(channel) => channel,
                None => {
                    let channel = standby::connect(&self.witness).await?;
                    *self.channel.lock().unwrap_or_else(PoisonError::into_inner) =
                        Some(channel.clone());
                    channel
                }
            }
        };
        match WitnessServiceClient::new(channel).ask(request).await {
            Ok(answer) => {
                let answer = answer.into_inner();
                if answer.lease_ms > 0 {
                    *self.lease_ms.lock().unwrap_or_else(PoisonError::into_inner) = answer.lease_ms;
                }
                Ok(answer)
            }
            Err(status) => {
                *self.channel.lock().unwrap_or_else(PoisonError::into_inner) = None;
                Err(status.into())
            }
        }
    }
}

/// The primary of a pair, opened: serve `kernel` and run `lease` beside it; when `lease` ends,
/// this kernel was deposed and must stop.
pub struct NodePrimary {
    pub kernel: Arc<Kernel>,
    pub lease: Pin<Box<dyn Future<Output = BoxError> + Send>>,
}

/// Follows the primary until this kernel is the primary (see the module), then opens it.
pub async fn become_primary(config: Config) -> Result<NodePrimary, BoxError> {
    let pair = config
        .replication
        .pair
        .clone()
        .ok_or("run the pair with --witness")?;
    let link = Arc::new(Link::open(&config)?);
    loop {
        let sent = Instant::now();
        let answer = match link.ask(WitnessOp::Lease, 0, "").await {
            Ok(answer) => answer,
            Err(e) => {
                tracing::warn!(error = %e, witness = %pair.witness, "the witness does not answer; asking again");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if answer.granted {
            let kernel = Kernel::open(config.clone())?;
            let primary = kernel
                .replication
                .clone()
                .expect("a kernel of a pair runs with --replication sync");
            primary.set_lease(sent + link.lease() - link.margin());
            tracing::warn!(term = answer.term, "this kernel is the primary");
            let lease = Box::pin(keep_lease(
                Arc::clone(&kernel),
                link,
                answer.term,
                Duration::from_millis(pair.standby_timeout_ms),
                answer.in_sync,
            ));
            return Ok(NodePrimary { kernel, lease });
        }
        let mut follow = config.clone();
        follow.replication.follow = Some(Follow {
            primary: answer.primary_address.clone(),
            key: pair.node_key.clone(),
            primary_kernel_keys: pair.kernel_keys.clone(),
        });
        tracing::info!(term = answer.term, primary = %answer.primary, address = %answer.primary_address, "this kernel follows the primary");
        standby::run_with(follow, Some(Arc::clone(&link))).await?;
    }
}

/// Keeps the primary's lease of `term`, and tells the witness when the primary commits alone
/// and when its standby is in sync again. Ends with why, once the witness gave the lease to
/// the other kernel.
async fn keep_lease(
    kernel: Arc<Kernel>,
    link: Arc<Link>,
    term: u64,
    timeout: Duration,
    mut reported: String,
) -> BoxError {
    let primary = kernel
        .replication
        .clone()
        .expect("a kernel of a pair runs with --replication sync");
    let (mut last_acked, mut progress) = (primary.acked_seq(), Instant::now());
    let head_now = || kernel.store.record_head().map_or(0, |h| h.seq);
    // Writes up to this record may have been told without the standby (alone, or before this
    // kernel restarted): the witness hears of a standby in sync only once it holds them.
    let mut must_reach = head_now();
    let mut last_head = must_reach;
    loop {
        tokio::time::sleep(link.lease() / 3).await;
        let sent = Instant::now();
        match link.ask(WitnessOp::Lease, term, "").await {
            Ok(a) if a.granted && a.term == term => {
                primary.set_lease(sent + link.lease() - link.margin());
                reported = a.in_sync;
            }
            Ok(a) => {
                return format!(
                    "deposed: the witness made {} the primary of term {}; start this kernel again to follow it",
                    a.primary, a.term
                )
                .into();
            }
            // The lease runs out by itself: writes committed alone wait from then on.
            Err(e) => tracing::warn!(error = %e, "renewing the lease failed"),
        }
        let head = head_now();
        let (acked, standby) = (primary.acked_seq(), primary.standby());
        if standby.is_some() && (acked > last_acked || acked >= head) {
            progress = Instant::now();
        }
        last_acked = acked;
        if primary.alone() {
            // Under load the standby is always a little behind the head: it has caught up once
            // it holds what existed at the last look.
            if standby.is_some() && acked >= last_head {
                // First the writes wait for the standby again, then the witness hears of it,
                // once the standby holds everything told alone.
                primary.set_alone(false);
                must_reach = head_now();
                progress = Instant::now();
                tracing::info!("the standby caught up: writes wait for it again");
            }
        } else if let Some(standby) = standby.filter(|_| acked >= must_reach)
            && reported != standby
        {
            match link.ask(WitnessOp::InSync, term, &standby).await {
                Ok(a) if a.granted => {
                    reported = standby;
                    tracing::info!(standby = %reported, "the witness knows the standby is in sync");
                }
                Ok(a) => tracing::warn!(why = %a.message, "the witness refused in sync"),
                Err(e) => tracing::warn!(error = %e, "telling the witness failed"),
            }
        }
        // A standby the witness names in sync whose key was revoked must not take over: the
        // primary commits alone, which the witness records first.
        let revoked = !reported.is_empty() && kernel.revoked(&reported);
        if revoked && primary.alone() {
            reported = String::new();
        }
        if !primary.alone() && (progress.elapsed() >= timeout || revoked) {
            match link.ask(WitnessOp::Alone, term, "").await {
                Ok(a) if a.granted => {
                    reported = String::new();
                    primary.set_alone(true);
                    tracing::warn!(
                        away_ms = progress.elapsed().as_millis() as u64,
                        "the standby is away or behind: committing alone"
                    );
                }
                Ok(a) => tracing::warn!(why = %a.message, "the witness refused alone"),
                Err(e) => tracing::warn!(error = %e, "telling the witness failed"),
            }
        }
        last_head = head;
    }
}
