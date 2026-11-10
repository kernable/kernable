//! How far each listener was behind over the last hour, for the UI: the head and every
//! listener's position, sampled every [`EVERY`]; and the kernel's activity per minute over the
//! last day; and per event type, how many events the log holds ([`Tally`]). Kept in memory
//! only (a restart starts over).

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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

/// Minutes each event type's count per minute goes back.
pub(crate) const TALLY_MINUTES: i64 = 60;

/// How many events of each type (`domain.EventType`) the log holds, when the newest was
/// committed, and how many per minute over the last [`TALLY_MINUTES`]. Counted the first time
/// someone asks, from the start of the log, and from then on from where it stopped: the UI
/// shows it, nothing depends on it. Events a tenant takes along when it moves away stay
/// counted until the kernel restarts.
#[derive(Default)]
pub(crate) struct Tally {
    state: Mutex<TallyState>,
}

#[derive(Default)]
struct TallyState {
    /// The last log position counted.
    position: u64,
    types: BTreeMap<String, TypeCount>,
}

#[derive(Default)]
struct TypeCount {
    count: u64,
    last_at: i64,
    /// Per minute (ms since epoch / 60 000), oldest first, only minutes with events.
    minutes: VecDeque<(i64, u64)>,
}

impl Tally {
    /// Counts the events up to `head`, at most `budget` of them: a long log is counted over
    /// several calls, each saying how far it got.
    pub(crate) fn catch_up(
        &self,
        store: &dyn kernable_store::Store,
        head: u64,
        budget: u64,
        now: i64,
    ) -> kernable_store::Result<u64> {
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let stop = head.min(st.position.saturating_add(budget));
        let oldest = now / 60_000 - TALLY_MINUTES;
        while st.position < stop {
            let want = u32::try_from((stop - st.position).min(1_000)).unwrap_or(1_000);
            let events = store.read_all(st.position, want)?;
            let Some(last) = events.last() else { break };
            st.position = last.global_position.min(stop).max(st.position + 1);
            for e in &events {
                st.count(
                    &format!("{}.{}", e.domain, e.event_type),
                    e.created_at,
                    oldest,
                );
            }
        }
        Ok(st.position)
    }

    /// The counts of the types in `domain`: `{type: {count, lastAt, perMinute}}`, with
    /// `perMinute` the last [`TALLY_MINUTES`] minutes, oldest first, the current one last.
    pub(crate) fn view(&self, domain: &str, now: i64) -> Value {
        let st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let prefix = format!("{domain}.");
        let minute = now / 60_000;
        let types: serde_json::Map<String, Value> = st
            .types
            .iter()
            .filter(|(t, _)| t.starts_with(&prefix))
            .map(|(t, c)| {
                let per_minute: Vec<u64> = (minute - TALLY_MINUTES + 1..=minute)
                    .map(|m| {
                        c.minutes
                            .iter()
                            .find(|(at, _)| *at == m)
                            .map_or(0, |(_, n)| *n)
                    })
                    .collect();
                (
                    t.clone(),
                    json!({ "count": c.count, "lastAt": c.last_at, "perMinute": per_minute }),
                )
            })
            .collect();
        Value::Object(types)
    }
}

impl TallyState {
    fn count(&mut self, event_type: &str, at: i64, oldest: i64) {
        let c = self.types.entry(event_type.to_owned()).or_default();
        c.count += 1;
        c.last_at = c.last_at.max(at);
        let minute = at / 60_000;
        if minute > oldest {
            match c.minutes.iter_mut().find(|(m, _)| *m == minute) {
                Some((_, n)) => *n += 1,
                None => {
                    c.minutes.push_back((minute, 1));
                    c.minutes.make_contiguous().sort_unstable();
                }
            }
        }
        while c.minutes.front().is_some_and(|(m, _)| *m <= oldest) {
            c.minutes.pop_front();
        }
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

    #[test]
    fn tallies_each_event_type_and_its_last_hour() {
        let mut st = TallyState::default();
        let now = 100 * 60_000;
        let oldest = now / 60_000 - TALLY_MINUTES;
        st.count("shop.OrderPlaced", 10 * 60_000, oldest);
        st.count("shop.OrderPlaced", now - 1, oldest);
        st.count("shop.OrderPlaced", now, oldest);
        st.count("billing.InvoiceIssued", now, oldest);
        let tally = Tally {
            state: Mutex::new(st),
        };
        let view = tally.view("shop", now);
        let placed = &view["shop.OrderPlaced"];
        assert_eq!(placed["count"], 3);
        assert_eq!(placed["lastAt"], now);
        let per_minute = placed["perMinute"].as_array().unwrap();
        assert_eq!(per_minute.len(), TALLY_MINUTES as usize);
        assert_eq!(per_minute[59], 1, "the current minute");
        assert_eq!(per_minute[58], 1, "the minute before");
        assert_eq!(
            per_minute.iter().map(|n| n.as_u64().unwrap()).sum::<u64>(),
            2,
            "an hour back only"
        );
        assert!(view.get("billing.InvoiceIssued").is_none());
    }
}
