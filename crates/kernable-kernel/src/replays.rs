//! Writes that run at most once. Data and cache writes are signed over their content and
//! `issued_at` and valid for 60 s, so a captured one could be sent again within that window and
//! undo a newer write. The kernel remembers each signature until its request expires, and
//! refuses writes signed before it started: it cannot know whether they ran.

use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard, PoisonError},
};

use sha2::{Digest, Sha256};
use tonic::Status;

use crate::MAX_REQUEST_AGE_MS;

/// A signature, shortened: 128 bits are plenty to tell signatures apart.
type Id = [u8; 16];

pub(crate) struct Replays {
    started_at: i64,
    seen: Mutex<Seen>,
}

#[derive(Default)]
struct Seen {
    /// When each signature's request expires (ms since epoch).
    expires: HashMap<Id, i64>,
    /// Expired entries are dropped once this many are held, and once per time window.
    prune_at: usize,
    pruned: i64,
}

impl Replays {
    pub(crate) fn new(started_at: i64) -> Self {
        Replays {
            started_at,
            seen: Mutex::default(),
        }
    }

    fn seen(&self) -> MutexGuard<'_, Seen> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Claims the write signed with `sig` at `issued_at` (checked against the time window
    /// already). Dropping the claim without [`Claim::done`] gives it back, so a write that
    /// failed can be sent again.
    pub(crate) fn claim(&self, sig: &[u8], issued_at: i64, now: i64) -> Result<Claim<'_>, Status> {
        if issued_at < self.started_at {
            return Err(Status::failed_precondition(
                "the write was signed before the kernel started, so it may have run already; \
                 sign it again",
            ));
        }
        let id: Id = Sha256::digest(sig)[..16].try_into().expect("16 bytes");
        let mut seen = self.seen();
        if seen.expires.len() >= seen.prune_at || now - seen.pruned >= MAX_REQUEST_AGE_MS {
            seen.expires.retain(|_, expires| *expires >= now);
            seen.prune_at = (seen.expires.len() * 2).max(1024);
            seen.pruned = now;
        }
        if seen.expires.contains_key(&id) {
            return Err(Status::already_exists(
                "this write was carried out already: a replay",
            ));
        }
        seen.expires.insert(id, issued_at + MAX_REQUEST_AGE_MS);
        Ok(Claim {
            replays: self,
            id,
            done: false,
        })
    }
}

pub(crate) struct Claim<'a> {
    replays: &'a Replays,
    id: Id,
    done: bool,
}

impl Claim<'_> {
    /// The write took effect: its signature stays refused until the request expires.
    pub(crate) fn done(mut self) {
        self.done = true;
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.replays.seen().expires.remove(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_runs_once_within_its_window() {
        let replays = Replays::new(1_000);
        replays.claim(b"a", 1_000, 1_000).unwrap().done();
        let again = replays.claim(b"a", 1_000, 1_001).err().unwrap();
        assert_eq!(again.code(), tonic::Code::AlreadyExists);
        // Another write is not affected.
        replays.claim(b"b", 1_000, 1_001).unwrap().done();
    }

    #[test]
    fn a_failed_write_can_be_sent_again() {
        let replays = Replays::new(1_000);
        drop(replays.claim(b"a", 1_000, 1_000).unwrap());
        replays.claim(b"a", 1_000, 1_001).unwrap().done();
    }

    #[test]
    fn writes_signed_before_the_start_are_refused() {
        let replays = Replays::new(1_000);
        let refused = replays.claim(b"a", 999, 1_000).err().unwrap();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn expired_signatures_are_forgotten() {
        let start = 1_700_000_000_000;
        let replays = Replays::new(start);
        for i in 0..2_000u32 {
            replays
                .claim(&i.to_be_bytes(), start, start)
                .unwrap()
                .done();
        }
        // Long after they expired, the next claim drops them.
        let late = start + MAX_REQUEST_AGE_MS * 3;
        replays.claim(b"late", late, late).unwrap().done();
        assert_eq!(replays.seen().expires.len(), 1);
    }
}
