//! Connection and activity lines on stderr, filtered by `KERNABLE_LOG`.

use std::{path::Path, sync::Arc, time::Duration};

/// How much the SDK writes to stderr: `KERNABLE_LOG` is `off`, `error`, `warn`, `info`
/// (default) or `debug`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Level {
    Off,
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    /// The level a `KERNABLE_LOG` value names; `info` when unset or unknown.
    pub(crate) fn parse(value: Option<&str>) -> Level {
        match value.map(str::trim) {
            Some("off") => Level::Off,
            Some("error") => Level::Error,
            Some("warn") => Level::Warn,
            Some("debug") => Level::Debug,
            _ => Level::Info,
        }
    }

    /// Whether a line at `line` is printed with this level configured.
    pub(crate) fn allows(self, line: Level) -> bool {
        line != Level::Off && line <= self
    }
}

/// Where and as whom this process connects, for the log lines, and which of them to print.
#[derive(Clone, Debug)]
pub(crate) struct Log {
    pub(crate) level: Level,
    pub(crate) url: Arc<str>,
    pub(crate) key_name: Arc<str>,
}

impl Log {
    pub(crate) fn new(url: &str, key_file: &Path) -> Log {
        Log {
            level: Level::parse(std::env::var("KERNABLE_LOG").ok().as_deref()),
            url: url.into(),
            key_name: key_file
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default()
                .into(),
        }
    }

    /// Prints `kernable <what>: <message>` to stderr if `level` is enabled.
    pub(crate) fn line(&self, level: Level, what: &str, message: impl FnOnce() -> String) {
        if self.level.allows(level) {
            eprintln!("kernable {what}: {}", message());
        }
    }

    pub(crate) fn error(&self, what: &str, message: impl FnOnce() -> String) {
        self.line(Level::Error, what, message);
    }

    pub(crate) fn warn(&self, what: &str, message: impl FnOnce() -> String) {
        self.line(Level::Warn, what, message);
    }

    pub(crate) fn info(&self, what: &str, message: impl FnOnce() -> String) {
        self.line(Level::Info, what, message);
    }

    pub(crate) fn debug(&self, what: &str, message: impl FnOnce() -> String) {
        self.line(Level::Debug, what, message);
    }

    /// `connected to <url> as <keyname> (<id12>…)`.
    pub(crate) fn connected(&self, key_id: &str) -> String {
        connected(&self.url, &self.key_name, key_id)
    }
}

/// `connected to <url> as <keyname> (<first 12 chars of the key ID>…)`.
pub(crate) fn connected(url: &str, key_name: &str, key_id: &str) -> String {
    let short: String = key_id.chars().take(12).collect();
    format!("connected to {url} as {key_name} ({short}…)")
}

/// The items sorted and joined with `, `.
pub(crate) fn sorted_list<'a>(items: impl IntoIterator<Item = &'a String>) -> String {
    let mut items: Vec<&str> = items.into_iter().map(String::as_str).collect();
    items.sort_unstable();
    items.join(", ")
}

/// The worker's connected line after the prefix.
pub(crate) fn worker_connected(connected: &str, command_types: &[String]) -> String {
    format!("{connected}, runs {}", sorted_list(command_types))
}

/// The listener's connected line after the prefix: where it resumes, what it reads, and what
/// it answers and checks, if anything.
pub(crate) fn listener_connected(
    connected: &str,
    position: u64,
    event_types: &[String],
    query_types: &[String],
    checks: Option<&[String]>,
) -> String {
    let mut line = format!(
        "{connected}, at position {position}, reads {}",
        sorted_list(event_types)
    );
    if !query_types.is_empty() {
        line.push_str(&format!(", answers {}", sorted_list(query_types)));
    }
    if let Some(checks) = checks {
        line.push_str(&format!(", checks {}", sorted_list(checks)));
    }
    line
}

/// `connection lost (<error>), reconnecting in <seconds> s`.
/// What ended a session, for [`connection_lost`]: a gRPC status by its message only (as the
/// other SDKs print it), anything else as it displays; `None` when the stream just ended.
pub(crate) fn reason(error: Option<&crate::BoxError>) -> String {
    let Some(error) = error else {
        return "stream ended".into();
    };
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return status.message().to_owned();
    }
    if let Some(crate::Error::Status(status)) = error.downcast_ref::<crate::Error>() {
        return status.message().to_owned();
    }
    error.to_string()
}

pub(crate) fn connection_lost(error: &str, delay: Duration) -> String {
    format!(
        "connection lost ({error}), reconnecting in {} s",
        delay.as_secs_f64()
    )
}

/// What became of a command a worker ran.
pub(crate) fn command_done(
    command: &str,
    aggregate_type: &str,
    aggregate_id: &str,
    outcome: Result<usize, &str>,
) -> String {
    let what = match outcome {
        Ok(events) => format!("committed {events} events"),
        Err(code) => format!("rejected {code}"),
    };
    format!("{command} {aggregate_type}/{aggregate_id} {what}")
}

/// What became of a query a listener was asked.
pub(crate) fn query_done(query: &str, rejected: Option<&str>) -> String {
    match rejected {
        None => format!("answered {query}"),
        Some(code) => format!("rejected {query} {code}"),
    }
}

/// An event a listener processed.
pub(crate) fn event_processed(event: &str, position: u64) -> String {
    format!("{event} at {position}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn parses_levels() {
        assert_eq!(Level::parse(None), Level::Info);
        assert_eq!(Level::parse(Some("off")), Level::Off);
        assert_eq!(Level::parse(Some("error")), Level::Error);
        assert_eq!(Level::parse(Some("warn")), Level::Warn);
        assert_eq!(Level::parse(Some("info")), Level::Info);
        assert_eq!(Level::parse(Some("debug")), Level::Debug);
        assert_eq!(Level::parse(Some("verbose")), Level::Info);
    }

    #[test]
    fn filters_by_level() {
        assert!(!Level::Off.allows(Level::Error));
        assert!(Level::Error.allows(Level::Error));
        assert!(!Level::Error.allows(Level::Warn));
        assert!(Level::Warn.allows(Level::Error));
        assert!(Level::Info.allows(Level::Warn));
        assert!(!Level::Info.allows(Level::Debug));
        assert!(Level::Debug.allows(Level::Debug));
        assert!(!Level::Debug.allows(Level::Off));
    }

    #[test]
    fn takes_the_key_name_from_the_file_stem() {
        let log = Log::new("http://127.0.0.1:7233", Path::new("keys/app.key"));
        assert_eq!(&*log.key_name, "app");
    }

    #[test]
    fn formats_connected_lines() {
        let connected = connected("http://127.0.0.1:7233", "app", "0123456789abcdef0123");
        assert_eq!(
            connected,
            "connected to http://127.0.0.1:7233 as app (0123456789ab…)"
        );
        assert_eq!(
            worker_connected(&connected, &strings(&["shop.Checkout", "shop.AddItem"])),
            "connected to http://127.0.0.1:7233 as app (0123456789ab…), \
             runs shop.AddItem, shop.Checkout"
        );
        assert_eq!(
            listener_connected(
                &connected,
                42,
                &strings(&["shop.*", "billing.Paid"]),
                &[],
                None
            ),
            "connected to http://127.0.0.1:7233 as app (0123456789ab…), at position 42, \
             reads billing.Paid, shop.*"
        );
        assert_eq!(
            listener_connected(
                &connected,
                0,
                &strings(&["shop.ItemAdded"]),
                &strings(&["shop.Cart", "shop.Carts"]),
                Some(&strings(&["shop.*", "*"])),
            ),
            "connected to http://127.0.0.1:7233 as app (0123456789ab…), at position 0, \
             reads shop.ItemAdded, answers shop.Cart, shop.Carts, checks *, shop.*"
        );
    }

    #[test]
    fn formats_connection_lost() {
        assert_eq!(
            connection_lost("broken pipe", Duration::from_secs(1)),
            "connection lost (broken pipe), reconnecting in 1 s"
        );
        assert_eq!(
            connection_lost("broken pipe", Duration::from_millis(100)),
            "connection lost (broken pipe), reconnecting in 0.1 s"
        );
    }

    #[test]
    fn formats_activity() {
        assert_eq!(
            command_done("shop.AddItem", "cart", "c1", Ok(2)),
            "shop.AddItem cart/c1 committed 2 events"
        );
        assert_eq!(
            command_done("shop.AddItem", "cart", "c1", Err("checked_out")),
            "shop.AddItem cart/c1 rejected checked_out"
        );
        assert_eq!(event_processed("shop.ItemAdded", 7), "shop.ItemAdded at 7");
        assert_eq!(query_done("shop.Cart", None), "answered shop.Cart");
        assert_eq!(
            query_done("shop.Cart", Some("not_found")),
            "rejected shop.Cart not_found"
        );
    }
}
