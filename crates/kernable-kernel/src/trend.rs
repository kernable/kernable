//! How far each listener was behind over the last hour, for the UI: the head and every
//! listener's position, sampled every [`EVERY`]; and the kernel's activity per minute over the
//! last day. Kept in memory only (a restart starts over).

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

/// Activity samples kept, one per minute: a day, and the minute before it.
const ACTIVITY_KEEP: usize = 24 * 60 + 1;

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
    /// At the first sample of each minute: when, and the counts so far (events, committed
    /// commands, rejected commands, answered queries).
    activity: Mutex<VecDeque<[i64; 5]>>,
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

    /// The counts so far, kept once per minute: the UI shows their differences.
    pub(crate) fn record_activity(&self, at: i64, counts: [u64; 4]) {
        let mut kept = self.activity.lock().unwrap_or_else(PoisonError::into_inner);
        if kept
            .back()
            .is_some_and(|last| last[0] / 60_000 == at / 60_000)
        {
            return;
        }
        if kept.len() == ACTIVITY_KEEP {
            kept.pop_front();
        }
        let [events, commands, rejected, queries] = counts.map(|n| n as i64);
        kept.push_back([at, events, commands, rejected, queries]);
    }

    /// The commands committed since `since` (ms since epoch), as far back as kept.
    pub(crate) fn commands_since(&self, since: i64) -> u64 {
        let kept = self.activity.lock().unwrap_or_else(PoisonError::into_inner);
        let (Some(from), Some(last)) = (
            kept.iter().find(|row| row[0] >= since).or(kept.front()),
            kept.back(),
        ) else {
            return 0;
        };
        u64::try_from(last[2] - from[2]).unwrap_or(0)
    }

    /// Per listener, its samples oldest first as `[at, head, position]`; and the activity,
    /// oldest first as `[at, events, commands, rejected, queries]`, counted since the start.
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
        let activity: Vec<Value> = self
            .activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|row| json!(row))
            .collect();
        json!({ "everyMs": EVERY.as_millis() as u64, "listeners": listeners, "activity": activity })
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

    #[test]
    fn activity_is_kept_once_a_minute_for_a_day() {
        let trend = Trend::default();
        trend.record_activity(60_000, [1, 1, 0, 0]);
        trend.record_activity(61_000, [2, 2, 0, 0]);
        trend.record_activity(120_000, [5, 4, 1, 2]);
        let view = trend.view();
        assert_eq!(
            view["activity"],
            serde_json::json!([[60_000, 1, 1, 0, 0], [120_000, 5, 4, 1, 2]])
        );
        for minute in 3..(ACTIVITY_KEEP as i64 + 10) {
            trend.record_activity(minute * 60_000, [0; 4]);
        }
        assert_eq!(
            trend.view()["activity"].as_array().unwrap().len(),
            ACTIVITY_KEEP
        );
    }
}
