//! Connection and activity lines on stderr, filtered by `KERNABLE_LOG`, and an app's overview.

use std::{
    io::{IsTerminal, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::When;

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

/// ANSI styles of the lines.
const BOLD: u8 = 1;
const DIM: u8 = 2;
const RED: u8 = 31;
const YELLOW: u8 = 33;

/// Which lines to print, whether in color, and the kernel and key this process connects with.
#[derive(Clone, Debug)]
pub(crate) struct Log {
    pub(crate) level: Level,
    /// Colors on a terminal, unless `NO_COLOR` is set.
    pub(crate) color: bool,
    /// `<url> · key <name> (<key id>…)`.
    pub(crate) peer: Arc<str>,
    /// Every address of the kernel is on this machine (a Unix socket, loopback): reconnecting
    /// costs nothing there, so it waits at most [`LOCAL_RETRY`].
    pub(crate) local: bool,
}

impl Log {
    pub(crate) fn new(url: &str, key_file: &Path, key_id: &str) -> Log {
        let key_name = key_file
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        Log {
            level: Level::parse(std::env::var("KERNABLE_LOG").ok().as_deref()),
            color: std::io::stderr().is_terminal()
                && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()),
            peer: peer(url, &key_name, key_id).into(),
            local: local(url),
        }
    }

    pub(crate) fn enabled(&self, level: Level) -> bool {
        self.level.allows(level)
    }

    /// Prints `HH:MM:SS  <what>  <line>` to stderr if `level` is enabled, warnings in yellow
    /// and errors in red.
    pub(crate) fn line(&self, level: Level, what: &str, message: impl FnOnce() -> String) {
        if self.enabled(level) {
            let line = format_line(&clock(), level, what, &message(), self.color);
            write(&format!("{line}\n"));
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
}

/// `text` to stderr in one write.
pub(crate) fn write(text: &str) {
    let _ = std::io::stderr().lock().write_all(text.as_bytes());
}

/// `text` in the ANSI style `code` (1 bold, 2 dim, 31 red, 33 yellow) if `color`.
pub(crate) fn style(color: bool, code: u8, text: &str) -> String {
    if color {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_owned()
    }
}

/// `<time>  <what>  <line>`: the time dim, `what` bold, warnings yellow and errors red.
pub(crate) fn format_line(time: &str, level: Level, what: &str, line: &str, color: bool) -> String {
    let line = match level {
        Level::Error => style(color, RED, line),
        Level::Warn => style(color, YELLOW, line),
        _ => line.to_owned(),
    };
    format!(
        "{}  {}  {line}",
        style(color, DIM, time),
        style(color, BOLD, what)
    )
}

/// The local time of day, `HH:MM:SS`.
fn clock() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let secs = (now as i64 + utc_offset(now)).rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

/// Seconds the local time is ahead of UTC at `now` (Unix seconds); 0 if unknown. Asked of
/// SQLite (bundled already), which follows `TZ` and the system's zone, once per hour.
fn utc_offset(now: u64) -> i64 {
    static CACHE: Mutex<Option<(u64, i64)>> = Mutex::new(None);
    let hour = now / 3600;
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((cached, offset)) = *cache
        && cached == hour
    {
        return offset;
    }
    let offset = rusqlite::Connection::open_in_memory()
        .and_then(|db| {
            db.query_row(
                "SELECT CAST(strftime('%s', ?1, 'unixepoch', 'localtime') AS INTEGER) - ?1",
                [now as i64],
                |row| row.get::<_, i64>(0),
            )
        })
        .unwrap_or(0);
    *cache = Some((hour, offset));
    offset
}

/// `<url> · key <keyname> (<first 12 chars of the key ID>…)`.
pub(crate) fn peer(url: &str, key_name: &str, key_id: &str) -> String {
    let short: String = key_id.chars().take(12).collect();
    format!("{url} · key {key_name} ({short}…)")
}

/// The items sorted and joined with `, `.
pub(crate) fn sorted_list<'a>(items: impl IntoIterator<Item = &'a String>) -> String {
    let mut items: Vec<&str> = items.into_iter().map(String::as_str).collect();
    items.sort_unstable();
    items.join(", ")
}

/// `shop.ItemAdded` as `ItemAdded` when `shop` is the only domain in `domains`.
pub(crate) fn relative(t: &str, domains: &[String]) -> String {
    match t.split_once('.') {
        Some((domain, name)) if domains.len() == 1 && domains[0] == domain && !name.is_empty() => {
            name.to_owned()
        }
        _ => t.to_owned(),
    }
}

/// What a listener does, one part per entry: where it starts, what it reads (in definition
/// order), answers and checks. `name` shortens the types (see [`relative`]).
pub(crate) fn listener_details(
    position: u64,
    reads: &[String],
    answers: &[String],
    checks: &[(When, Vec<String>)],
    name: &dyn Fn(&str) -> String,
) -> Vec<String> {
    let list = |types: &[String]| types.iter().map(|t| name(t)).collect::<Vec<_>>().join(", ");
    let mut details = vec![if reads.is_empty() {
        format!("from {position}")
    } else {
        format!("from {position} · reads {}", list(reads))
    }];
    if !answers.is_empty() {
        details.push(format!("answers {}", list(answers)));
    }
    for (when, types) in checks {
        details.push(format!("checks {} {when}", list(types)));
    }
    details
}

/// A listener's line on its first connect: `connected to <peer> · <details>`.
pub(crate) fn listener_connected(peer: &str, details: &[String]) -> String {
    let mut parts = vec![format!("connected to {peer}")];
    parts.extend(details.iter().cloned());
    parts.join(" · ")
}

/// A worker's line on its first connect: `connected to <peer> · runs <types, sorted>`.
pub(crate) fn worker_connected(peer: &str, command_types: &[String]) -> String {
    format!("connected to {peer} · runs {}", sorted_list(command_types))
}

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

/// `connection lost (<error>), reconnecting in <seconds> s`.
pub(crate) fn connection_lost(error: &str, delay: Duration) -> String {
    format!(
        "connection lost ({error}), reconnecting in {} s",
        delay.as_secs_f64()
    )
}

/// The pause before try `attempt` (from 1): `base`, doubled each time up to 30 s, ±20% jitter.
pub(crate) fn backoff(base: Duration, attempt: u32) -> Duration {
    let doubled = base.saturating_mul(1 << attempt.saturating_sub(1).min(16));
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() % 401);
    let delay = doubled
        .min(Duration::from_secs(30))
        .mul_f64(0.8 + f64::from(jitter) / 1000.0);
    Duration::from_millis(delay.as_millis() as u64) // readable in the log
}

/// The longest wait before trying a kernel of a pair again that was not the primary.
pub(crate) const PAIR_RETRY: Duration = Duration::from_secs(1);

/// The longest wait before trying a kernel on this machine again: a restart (a deploy) is over
/// within seconds, and the processes using it should be back with it, not up to 30 s later.
pub(crate) const LOCAL_RETRY: Duration = Duration::from_secs(2);

/// Whether every address in `url` (comma-separated for a pair) is on this machine: `unix:`, or
/// a loopback host (127.0.0.0/8, `localhost`, `::1`).
pub(crate) fn local(url: &str) -> bool {
    url.split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .all(|a| {
            if a.starts_with("unix:") {
                return true;
            }
            let rest = a.split_once("://").map_or(a, |(_, r)| r);
            let host = if let Some(v6) = rest.strip_prefix('[') {
                v6.split(']').next().unwrap_or_default()
            } else {
                rest.split([':', '/']).next().unwrap_or_default()
            };
            host == "localhost" || host == "::1" || host.starts_with("127.")
        })
}

/// Whether `reason` says the kernel is one of a pair that is not the primary right now.
pub(crate) fn switching_roles(reason: &str) -> bool {
    reason.starts_with("not_primary")
        || reason.contains("lost its lease at the witness")
        || reason.contains("hands its role over")
}

/// Logs the end of a session that was not stopped and says how long to wait before attempt
/// `attempt` (1: the first after it ended): `base`, twice as long after each further failed
/// attempt, at most 30 s, give or take a fifth, so processes do not reconnect in lockstep after
/// a kernel restart. A refusal that will not go away by itself is an error, not a warning.
pub(crate) fn reconnecting(
    log: &Log,
    what: &str,
    error: Option<&crate::BoxError>,
    base: Duration,
    attempt: u32,
) -> Duration {
    let reason = reason(error);
    // A kernel of a pair that is not the primary right now (a standby, one that lost its lease or
    // hands its role over) may be the primary in a moment: try again soon, not after up to 30 s.
    let delay = if switching_roles(&reason) {
        backoff(base, attempt).min(PAIR_RETRY)
    } else if log.local {
        backoff(base, attempt).min(LOCAL_RETRY)
    } else {
        backoff(base, attempt)
    };
    if refused(error) {
        log.error(what, || {
            format!(
                "refused ({reason}), reconnecting in {} s",
                delay.as_secs_f64()
            )
        });
    } else {
        log.warn(what, || connection_lost(&reason, delay));
    }
    delay
}

/// Awaits `f`, logging `line()` as an error if it still runs after `after`: a handler cannot be
/// stopped from outside, but a stuck one shows.
pub(crate) async fn watched<T>(
    log: &Log,
    what: &str,
    after: Duration,
    line: impl FnOnce() -> String,
    f: impl std::future::Future<Output = T>,
) -> T {
    let mut f = std::pin::pin!(f);
    tokio::select! {
        value = &mut f => return value,
        () = tokio::time::sleep(after) => log.error(what, line),
    }
    f.await
}

/// Whether the kernel refused in a way a retry does not change: INVALID_ARGUMENT,
/// PERMISSION_DENIED, FAILED_PRECONDITION, UNIMPLEMENTED, UNAUTHENTICATED.
fn refused(error: Option<&crate::BoxError>) -> bool {
    use tonic::Code::{
        FailedPrecondition, InvalidArgument, PermissionDenied, Unauthenticated, Unimplemented,
    };
    let status = error.and_then(|e| {
        e.downcast_ref::<tonic::Status>()
            .or(match e.downcast_ref::<crate::Error>() {
                Some(crate::Error::Status(status)) => Some(status),
                _ => None,
            })
    });
    status.is_some_and(|s| {
        matches!(
            s.code(),
            InvalidArgument
                | PermissionDenied
                | FailedPrecondition
                | Unimplemented
                | Unauthenticated
        )
    })
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

/// An app once everything is connected, for [`render_overview`].
pub(crate) struct Overview {
    pub(crate) service: String,
    pub(crate) version: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) peer: String,
    /// What the membership lists.
    pub(crate) workers: usize,
    pub(crate) listeners: usize,
    pub(crate) middlewares: usize,
    pub(crate) rows: Vec<Row>,
    /// Milliseconds from `run()` to everything connected.
    pub(crate) ms: u128,
}

/// One aggregate (`worker`) or listener (`listener` or `live`) of an app's overview.
pub(crate) struct Row {
    pub(crate) kind: &'static str,
    pub(crate) name: String,
    /// One line each; the first goes next to the name.
    pub(crate) details: Vec<String>,
}

/// An app's overview: the header, the kernel and membership, a row per aggregate and listener,
/// and how long it took to be ready; ends with an empty line.
pub(crate) fn render_overview(o: &Overview, color: bool) -> String {
    let count = |n: usize, what: &str| format!("{n} {what}{}", if n == 1 { "" } else { "s" });
    let mut listed = vec![count(o.workers, "worker"), count(o.listeners, "listener")];
    if o.middlewares > 0 {
        listed.push(count(o.middlewares, "middleware"));
    }
    let service = match &o.version {
        Some(version) if !version.is_empty() => format!("{} {version}", o.service),
        _ => o.service.clone(),
    };
    let mut header = vec!["kernable".to_owned(), service];
    header.extend(o.description.clone());
    let header: Vec<String> = header.into_iter().filter(|s| !s.is_empty()).collect();
    let mut lines = vec![
        style(color, BOLD, &header.join(" · ")),
        format!("  {:<10}  {}", "kernel", o.peer),
        format!("  {:<10}  applied: {}", "membership", listed.join(", ")),
        String::new(),
    ];
    let width = o
        .rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    for row in &o.rows {
        let mut details = row.details.iter();
        let first = details.next().map_or("", String::as_str);
        lines.push(format!("  {:<10}  {:<width$}  {first}", row.kind, row.name));
        for more in details {
            lines.push(format!("  {:<10}  {:<width$}  {more}", "", ""));
        }
    }
    lines.push(String::new());
    lines.push(format!(
        "  {}",
        style(color, DIM, &format!("ready in {} ms", o.ms))
    ));
    lines.push(String::new());
    lines.join("\n") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_kernel_on_this_machine_is_retried_within_two_seconds() {
        for url in [
            "unix:/srv/account/run/kernel.sock",
            "http://127.0.0.1:7233",
            "127.0.0.1:7233",
            "http://localhost:7233",
            "http://[::1]:7233",
            "http://127.0.0.1:7233,http://127.0.0.1:7235",
        ] {
            assert!(local(url), "{url}");
        }
        for url in [
            "https://kernel.example.com:7233",
            "http://10.0.0.5:7233",
            "http://127.0.0.1:7233,https://b.example:7233",
            "http://localhost.example.com:7233",
        ] {
            assert!(!local(url), "{url}");
        }
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
    fn names_the_peer_by_url_and_key_file_stem() {
        let log = Log::new(
            "http://127.0.0.1:7233",
            Path::new("keys/app.key"),
            "0123456789abcdef0123",
        );
        assert_eq!(
            &*log.peer,
            "http://127.0.0.1:7233 · key app (0123456789ab…)"
        );
    }

    #[test]
    fn formats_lines_with_and_without_color() {
        assert_eq!(
            format_line("13:12:10", Level::Warn, "listener carts", "lost", false),
            "13:12:10  listener carts  lost"
        );
        assert_eq!(
            format_line("13:12:10", Level::Info, "worker", "connected again", true),
            "\x1b[2m13:12:10\x1b[0m  \x1b[1mworker\x1b[0m  connected again"
        );
        assert_eq!(
            format_line("13:12:10", Level::Warn, "worker", "lost", true),
            "\x1b[2m13:12:10\x1b[0m  \x1b[1mworker\x1b[0m  \x1b[33mlost\x1b[0m"
        );
        assert_eq!(
            format_line("13:12:10", Level::Error, "worker", "bad", true),
            "\x1b[2m13:12:10\x1b[0m  \x1b[1mworker\x1b[0m  \x1b[31mbad\x1b[0m"
        );
        let time = clock();
        assert_eq!(time.len(), 8);
        assert_eq!(&time[2..3], ":");
    }

    #[test]
    fn formats_connected_lines() {
        let peer = peer("http://127.0.0.1:7233", "app", "0123456789abcdef0123");
        assert_eq!(peer, "http://127.0.0.1:7233 · key app (0123456789ab…)");
        assert_eq!(
            worker_connected(&peer, &strings(&["shop.Checkout", "shop.AddItem"])),
            "connected to http://127.0.0.1:7233 · key app (0123456789ab…) \
             · runs shop.AddItem, shop.Checkout"
        );
        let same = |t: &str| t.to_owned();
        let details = listener_details(42, &strings(&["shop.*", "billing.Paid"]), &[], &[], &same);
        assert_eq!(
            listener_connected(&peer, &details),
            "connected to http://127.0.0.1:7233 · key app (0123456789ab…) · from 42 \
             · reads shop.*, billing.Paid"
        );
        let details = listener_details(
            0,
            &strings(&["shop.ItemAdded"]),
            &strings(&["shop.Cart", "shop.Carts"]),
            &[
                (When::Before, strings(&["shop.*", "*"])),
                (When::After, strings(&["shop.Cart"])),
            ],
            &same,
        );
        assert_eq!(
            listener_connected(&peer, &details),
            "connected to http://127.0.0.1:7233 · key app (0123456789ab…) · from 0 \
             · reads shop.ItemAdded · answers shop.Cart, shop.Carts · checks shop.*, * before \
             · checks shop.Cart after"
        );
    }

    #[test]
    fn shortens_types_of_the_only_domain() {
        let shop = strings(&["shop"]);
        assert_eq!(relative("shop.ItemAdded", &shop), "ItemAdded");
        assert_eq!(relative("shop.*", &shop), "*");
        assert_eq!(relative("billing.Paid", &shop), "billing.Paid");
        assert_eq!(relative("*", &shop), "*");
        let both = strings(&["shop", "billing"]);
        assert_eq!(relative("shop.ItemAdded", &both), "shop.ItemAdded");
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

    fn shop() -> Overview {
        Overview {
            service: "shop".into(),
            version: Some("1.0.0".into()),
            description: Some("Carts and checkout".into()),
            peer: "http://127.0.0.1:7233 · key app (69212a19f36f…)".into(),
            workers: 1,
            listeners: 1,
            middlewares: 0,
            rows: vec![
                Row {
                    kind: "worker",
                    name: "cart".into(),
                    details: strings(&["runs AddItem, CheckOut"]),
                },
                Row {
                    kind: "listener",
                    name: "carts".into(),
                    details: strings(&["from 0 · reads ItemAdded, CheckedOut", "answers GetCart"]),
                },
            ],
            ms: 84,
        }
    }

    #[test]
    fn renders_an_app_overview() {
        assert_eq!(
            render_overview(&shop(), false),
            "kernable · shop 1.0.0 · Carts and checkout\n\
             \x20 kernel      http://127.0.0.1:7233 · key app (69212a19f36f…)\n\
             \x20 membership  applied: 1 worker, 1 listener\n\
             \n\
             \x20 worker      cart   runs AddItem, CheckOut\n\
             \x20 listener    carts  from 0 · reads ItemAdded, CheckedOut\n\
             \x20                    answers GetCart\n\
             \n\
             \x20 ready in 84 ms\n\
             \n"
        );
    }

    #[test]
    fn renders_an_overview_without_version_or_description_with_middlewares_in_color() {
        let o = Overview {
            version: None,
            description: None,
            workers: 0,
            listeners: 2,
            middlewares: 1,
            rows: vec![],
            ..shop()
        };
        assert_eq!(
            render_overview(&o, true),
            "\x1b[1mkernable · shop\x1b[0m\n\
             \x20 kernel      http://127.0.0.1:7233 · key app (69212a19f36f…)\n\
             \x20 membership  applied: 0 workers, 2 listeners, 1 middleware\n\
             \n\
             \n\
             \x20 \x1b[2mready in 84 ms\x1b[0m\n\
             \n"
        );
        // An empty version is no version.
        let o = Overview {
            version: Some(String::new()),
            ..o
        };
        assert!(render_overview(&o, false).starts_with("kernable · shop\n"));
    }

    #[test]
    fn reconnects_back_off_to_30_seconds_and_refusals_are_errors() {
        // A kernel on this machine: at most two seconds.
        let near = Log::new("unix:/k.sock", std::path::Path::new("app.key"), "ab");
        for attempt in [4, 12, 40] {
            let delay = reconnecting(&near, "worker", None, Duration::from_millis(100), attempt);
            assert!(delay <= LOCAL_RETRY, "attempt {attempt}: {delay:?}");
        }
        let log = Log::new(
            "https://kernel.example.com:7233",
            std::path::Path::new("app.key"),
            "ab",
        );
        let base = Duration::from_millis(100);
        for (attempt, expected) in [
            (1, 100),
            (2, 200),
            (3, 400),
            (4, 800),
            (12, 30_000),
            (40, 30_000),
        ] {
            let delay = reconnecting(&log, "worker", None, base, attempt).as_millis();
            assert!(
                (expected * 8 / 10..=expected * 12 / 10).contains(&delay),
                "attempt {attempt}: {delay} ms"
            );
        }
        let denied: crate::BoxError = Box::new(tonic::Status::permission_denied("no grant"));
        let unavailable: crate::BoxError = Box::new(tonic::Status::unavailable("gone"));
        assert!(refused(Some(&denied)));
        assert!(!refused(Some(&unavailable)));
        assert!(!refused(None));
        // A refusal the SDK's own calls return, wrapped in its error.
        let wrapped: crate::BoxError = Box::new(crate::Error::Status(
            tonic::Status::unauthenticated("unknown key"),
        ));
        assert!(refused(Some(&wrapped)));
    }
}

#[cfg(test)]
mod pair_retry_tests {
    use super::*;

    #[test]
    fn a_kernel_of_a_pair_that_is_not_the_primary_is_tried_again_within_a_second() {
        assert!(switching_roles(
            "not_primary: this kernel is a standby of x; connect to the primary"
        ));
        assert!(switching_roles(
            "this kernel lost its lease at the witness and no standby holds its writes: ask the other kernel"
        ));
        assert!(switching_roles(
            "the kernel hands its role over to its standby; retry"
        ));
        assert!(!switching_roles("the key may not run orders.Order"));
        for attempt in 1..20 {
            assert!(backoff(Duration::from_millis(500), attempt).min(PAIR_RETRY) <= PAIR_RETRY);
            assert!(backoff(Duration::from_millis(500), attempt).min(LOCAL_RETRY) <= LOCAL_RETRY);
        }
    }
}
