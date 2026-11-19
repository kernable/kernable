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

    /// Counts one write that reached the OS (in tests; the store counts by SQLite's total
    /// changes, [`Self::wrote_up_to`]).
    #[cfg(test)]
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
            mpsc,
        },
        thread,
    };

    use super::*;

    /// Writers that arrive while a flush runs share the next one, whatever the timing: the
    /// first flush covers the first write only, the second every other.
    #[test]
    fn concurrent_callers_share_flushes() {
        let syncer = Arc::new(Syncer::default());
        let flushes = Arc::new(AtomicU32::new(0));
        let flush = |flushes: &AtomicU32| {
            flushes.fetch_add(1, SeqCst);
            Ok(())
        };
        let (flushing, flush_started) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let first = {
            let (syncer, flushes) = (Arc::clone(&syncer), Arc::clone(&flushes));
            thread::spawn(move || {
                syncer.wrote();
                syncer
                    .sync(|| {
                        flushing.send(()).unwrap();
                        released.recv().unwrap();
                        flush(&flushes)
                    })
                    .unwrap();
            })
        };
        flush_started.recv().unwrap();
        let (wrote, written) = mpsc::channel();
        let others: Vec<_> = (0..15)
            .map(|_| {
                let (syncer, flushes, wrote) =
                    (Arc::clone(&syncer), Arc::clone(&flushes), wrote.clone());
                thread::spawn(move || {
                    syncer.wrote();
                    wrote.send(()).unwrap();
                    syncer.sync(|| flush(&flushes)).unwrap();
                })
            })
            .collect();
        for _ in 0..15 {
            written.recv().unwrap();
        }
        release.send(()).unwrap();
        first.join().unwrap();
        for t in others {
            t.join().unwrap();
        }
        assert_eq!(flushes.load(SeqCst), 2);
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
        assert!(syncer.sync(|| Ok(())).is_err());
    }
}
