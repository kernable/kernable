//! The request context, times and IDs.

use std::{collections::BTreeMap, fmt};

use kernable_core::proto;

/// Who sends a request and in which context: signed with the command and stored with every
/// event it causes. `attributes` are business data only, never security decisions.
///
/// Set a default for a connection with [`crate::Options::context`], or one per request with
/// [`crate::Submit::context`] and [`crate::Query::context`]. Inside a listener's handler,
/// submit follow-up commands with `ctx.follow()` ([`crate::ListenerContext::follow`]). All
/// fields default to empty; set the ones you need:
/// `RequestContext { sender_identity: "customer-7".into(), expected_version: Some(3), ..Default::default() }`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestContext {
    /// The tenant the sender belongs to.
    pub sender_tenant: String,
    /// Who sends the request, e.g. a user or service ID; middlewares check permissions on it.
    pub sender_identity: String,
    pub sender_account: String,
    pub sender_session: String,
    /// Authorize as this identity of the target tenant; the sender stays the real principal.
    pub on_behalf_of: String,
    /// Reject the command (`version_mismatch`) unless the aggregate is at this version.
    pub expected_version: Option<u64>,
    /// Shared by everything one original request causes; defaults to the command ID.
    pub correlation_id: String,
    /// The command that caused this one.
    pub causation_id: String,
    pub trace_id: String,
    /// Privileged: only keys granted `privileged` for it may set it.
    pub bypass_authorization: bool,
    pub attributes: BTreeMap<String, String>,
}

impl RequestContext {
    /// A context for a command caused by command `command_id` with this context: same sender
    /// and correlation, without an expected version or privileges.
    pub(crate) fn follow(&self, command_id: &str) -> RequestContext {
        RequestContext {
            expected_version: None,
            bypass_authorization: false,
            correlation_id: if self.correlation_id.is_empty() {
                command_id.to_owned()
            } else {
                self.correlation_id.clone()
            },
            causation_id: command_id.to_owned(),
            ..self.clone()
        }
    }

    pub(crate) fn to_wire(&self) -> proto::RequestContext {
        proto::RequestContext {
            sender_tenant: self.sender_tenant.clone(),
            sender_identity: self.sender_identity.clone(),
            sender_account: self.sender_account.clone(),
            sender_session: self.sender_session.clone(),
            on_behalf_of: self.on_behalf_of.clone(),
            expected_version: self.expected_version,
            correlation_id: self.correlation_id.clone(),
            causation_id: self.causation_id.clone(),
            trace_id: self.trace_id.clone(),
            bypass_authorization: self.bypass_authorization,
            attributes: self
                .attributes
                .iter()
                .map(|(key, value)| proto::Attribute {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    pub(crate) fn from_wire(c: Option<&proto::RequestContext>) -> RequestContext {
        let Some(c) = c else {
            return RequestContext::default();
        };
        RequestContext {
            sender_tenant: c.sender_tenant.clone(),
            sender_identity: c.sender_identity.clone(),
            sender_account: c.sender_account.clone(),
            sender_session: c.sender_session.clone(),
            on_behalf_of: c.on_behalf_of.clone(),
            expected_version: c.expected_version,
            correlation_id: c.correlation_id.clone(),
            causation_id: c.causation_id.clone(),
            trace_id: c.trace_id.clone(),
            bypass_authorization: c.bypass_authorization,
            attributes: c
                .attributes
                .iter()
                .map(|a| (a.key.clone(), a.value.clone()))
                .collect(),
        }
    }
}

/// A point in time in milliseconds since the epoch, shown as RFC 3339 in UTC with
/// milliseconds (`2026-10-27T09:00:00.000Z`), like JavaScript's `toISOString()`: format it
/// with `to_string()`. `Timestamp(ms)` makes one from milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// The current time. In a command handler, use `ctx.now()` instead
    /// ([`crate::CommandContext::now`]), which is the same every time the command runs.
    pub fn now() -> Timestamp {
        Timestamp(now_ms())
    }

    /// Milliseconds since the Unix epoch.
    pub fn millis(self) -> i64 {
        self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (days, ms) = (self.0.div_euclid(86_400_000), self.0.rem_euclid(86_400_000));
        // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        write!(
            f,
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
            ms / 3_600_000,
            ms / 60_000 % 60,
            ms / 1000 % 60,
            ms % 1000
        )
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A random UUID (version 4).
pub(crate) fn random_id() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("OS randomness unavailable");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_read_like_iso_strings() {
        assert_eq!(Timestamp(0).to_string(), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            Timestamp(1_793_091_600_123).to_string(),
            "2026-10-27T09:00:00.123Z"
        );
        assert_eq!(
            Timestamp(951_782_400_000).to_string(),
            "2000-02-29T00:00:00.000Z"
        );
    }
}
