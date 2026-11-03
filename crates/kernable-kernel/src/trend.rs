//! How far each listener was behind over the last hour, for the UI: the head and every
//! listener's position, sampled every [`EVERY`], kept in memory only (a restart starts over).

use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex, PoisonError},
    time::Duration,
};

use serde_json::{Value, json};

use crate::listeners::ListenerStatus;

/// How often the kernel samples.
pub(crate) const EVERY: Duration = Duration::from_secs(5);

/// Samples kept per listener: one hour.
const KEEP: usize = 720;

/// One sample: when (ms since epoch), the head, and the listener's acknowledged position.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Sample {
    at: i64,
    head: u64,
    position: u64,
}

#[derive(Default)]
pub(crate) struct Trend {
    samples: Mutex<HashMap<String, VecDeque<Sample>>>,
}

impl Trend {
    /// Adds one sample per listener; a listener that is gone loses its samples.
    pub(crate) fn record(&self, at: i64, head: u64, listeners: &[ListenerStatus]) {
        let mut samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        samples.retain(|name, _| listeners.iter().any(|l| &l.name == name));
        for l in listeners {
            let kept = samples.entry(l.name.clone()).or_default();
            if kept.len() == KEEP {
                kept.pop_front();
            }
            kept.push_back(Sample {
                at,
                head,
                position: l.position,
            });
        }
    }

    /// Per listener, its samples oldest first as `[at, head, position]`.
    pub(crate) fn view(&self) -> Value {
        let samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        let listeners: serde_json::Map<String, Value> = samples
            .iter()
            .map(|(name, kept)| {
                let rows: Vec<Value> = kept
                    .iter()
                    .map(|s| json!([s.at, s.head, s.position]))
                    .collect();
                (name.clone(), Value::Array(rows))
            })
            .collect();
        json!({ "everyMs": EVERY.as_millis() as u64, "listeners": listeners })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(name: &str, position: u64) -> ListenerStatus {
        ListenerStatus {
            name: name.into(),
            position,
            connected: true,
            parked: 0,
        }
    }

    #[test]
    fn keeps_an_hour_per_listener_and_forgets_removed_ones() {
        let trend = Trend::default();
        for i in 0..(KEEP as u64 + 3) {
            trend.record(i as i64, 10 + i, &[status("a", i), status("b", 2 * i)]);
        }
        let view = trend.view();
        let a = view["listeners"]["a"].as_array().unwrap();
        assert_eq!(a.len(), KEEP);
        assert_eq!(a[0], json!([3, 13, 3]), "the oldest ones were dropped");
        assert_eq!(view["everyMs"], 5_000);

        trend.record(1_000, 1_000, &[status("b", 900)]);
        let view = trend.view();
        assert!(view["listeners"].get("a").is_none());
        assert_eq!(
            view["listeners"]["b"].as_array().unwrap().last().unwrap(),
            &json!([1_000, 1_000, 900])
        );
    }
}
