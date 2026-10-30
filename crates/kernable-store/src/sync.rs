//! Group commit: one flush to the disk makes every write durable that returned before it
//! began. A caller that arrives while a flush runs waits for the next one, which covers it and
//! everyone else who arrived meanwhile, so concurrent commits share flushes instead of queueing
//! for one each.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use crate::{Error, Result};

#[derive(Default)]
struct State {
    /// Writes so far.
    written: u64,
    /// Writes a finished flush covered.
    synced: u64,
    flushing: bool,
    /// Set once a flush failed: what the OS still buffers can no longer be trusted.
    failed: Option<String>,
}

#[derive(Default)]
pub(crate) struct Syncer {
    state: Mutex<State>,
    flushed: Condvar,
}

impl Syncer {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Refuses a write once a flush failed.
    pub(crate) fn check(&self) -> Result<()> {
        match &self.state().failed {
            Some(why) => Err(failed(why)),
            None => Ok(()),
        }
    }

    /// Counts a write that reached the OS.
    pub(crate) fn wrote(&self) {
        self.state().written += 1;
    }

    /// Counts writes by a counter that only grows (SQLite's total changes).
    pub(crate) fn wrote_up_to(&self, written: u64) {
        let mut state = self.state();
        state.written = state.written.max(written);
    }

    /// Returns once every write counted before the call is on the disk, flushing with `flush`
    /// if no running flush covers it.
    pub(crate) fn sync(&self, flush: impl FnOnce() -> std::io::Result<()>) -> Result<()> {
        let mut state = self.state();
        let mine = state.written;
        let mut flush = Some(flush);
        loop {
            if let Some(why) = &state.failed {
                return Err(failed(why));
            }
            if state.synced >= mine {
                return Ok(());
            }
            if !state.flushing {
                break;
            }
            state = self
                .flushed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        // This caller flushes for everyone who wrote so far.
        state.flushing = true;
        let target = state.written;
        drop(state);
        let flushed = (flush.take().expect("flushes once"))();
        let mut state = self.state();
        state.flushing = false;
        let result = match flushed {
            Ok(()) => {
                state.synced = state.synced.max(target);
                Ok(())
            }
            Err(e) => {
                state.failed = Some(e.to_string());
                Err(Error::Io(e))
            }
        };
        self.flushed.notify_all();
        result
    }
}

fn failed(why: &str) -> Error {
    Error::Io(std::io::Error::other(format!(
        "the store refuses writes since a flush to the disk failed ({why}); restart the kernel"
    )))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering::SeqCst},
        },
        thread,
        time::Duration,
    };

    use super::*;

    #[test]
    fn concurrent_callers_share_flushes() {
        let syncer = Arc::new(Syncer::default());
        let flushes = Arc::new(AtomicU32::new(0));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let (syncer, flushes) = (Arc::clone(&syncer), Arc::clone(&flushes));
                thread::spawn(move || {
                    syncer.wrote();
                    syncer
                        .sync(|| {
                            flushes.fetch_add(1, SeqCst);
                            thread::sleep(Duration::from_millis(20));
                            Ok(())
                        })
                        .unwrap();
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let n = flushes.load(SeqCst);
        assert!((1..16).contains(&n), "{n} flushes for 16 writers");
        assert!(syncer.sync(|| panic!("nothing new to flush")).is_ok());
    }

    #[test]
    fn a_growing_counter_counts_writes() {
        let syncer = Syncer::default();
        let flushes = AtomicU32::new(0);
        let flush = || {
            flushes.fetch_add(1, SeqCst);
            Ok(())
        };
        syncer.wrote_up_to(3);
        syncer.sync(flush).unwrap();
        assert_eq!(flushes.load(SeqCst), 1);
        syncer.wrote_up_to(2); // read before the last flush
        assert!(syncer.sync(|| panic!("nothing new to flush")).is_ok());
        syncer.wrote_up_to(4);
        syncer.sync(flush).unwrap();
        assert_eq!(flushes.load(SeqCst), 2);
    }

    #[test]
    fn a_failed_flush_refuses_everything_after() {
        let syncer = Syncer::default();
        syncer.wrote();
        assert!(syncer.sync(|| Err(std::io::Error::other("EIO"))).is_err());
        assert!(syncer.check().is_err());
        assert!(syncer.sync(|| Ok(())).is_err());
    }
}
