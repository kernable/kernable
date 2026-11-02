//! Rejections and errors.

use std::fmt;

use crate::StoredEvent;

/// Any error a listener, query or middleware handler may return; `?` converts most errors
/// into it, a [`Rejected`] too: `return Err(Rejected::new("no such cart").code("not_found").into())`.
/// A query or middleware handler that returns a [`Rejected`] (also one wrapped in
/// [`Error::Rejected`], e.g. by a query it asked) refuses with its code; any other error
/// refuses with `handler_error` (`middleware_error` from a middleware). An event handler's
/// error makes the listener reconnect and get the event again; a reactor parks it at once if
/// it is a [`Rejected`], else after a few attempts (see [`crate::Listener::park_after`]).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A business reason to refuse a command or query: return it from a handler. `code` (default
/// `rejected`) is what callers act on, `message` what they show.
///
/// A command handler returns `Err(Rejected::new("Cart is checked out"))`; a listener's query
/// or middleware handler returns it as a [`BoxError`] (`.into()` or `?`). Callers see it as
/// [`crate::Outcome::Rejected`] from `submit` and as [`Error::Rejected`] from `query`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    pub code: String,
    pub message: String,
    /// Set by the kernel for `unique_taken`: the name of the key another aggregate holds.
    pub key: Option<String>,
    /// Set by the kernel for `requirement_missing`: the first requirement that does not hold.
    /// Boxed: it keeps every `Result` with an [`Error`] small.
    pub requirement: Option<Box<crate::Requirement>>,
}

impl Rejected {
    /// A rejection with `message` and the code `rejected`.
    pub fn new(message: impl Into<String>) -> Rejected {
        Rejected {
            code: "rejected".into(),
            message: message.into(),
            key: None,
            requirement: None,
        }
    }

    /// This rejection with another code, e.g. `Rejected::new("no such cart").code("not_found")`.
    pub fn code(mut self, code: impl Into<String>) -> Rejected {
        self.code = code.into();
        self
    }

    pub(crate) fn from_wire(r: kernable_core::proto::Rejected) -> Rejected {
        Rejected {
            code: r.code,
            message: r.message,
            key: Some(r.key).filter(|k| !k.is_empty()),
            requirement: r
                .requirement
                .map(|q| Box::new(crate::Requirement::from_wire(*q))),
        }
    }

    pub(crate) fn to_wire(&self) -> kernable_core::proto::Rejected {
        // The key and requirement are the kernel's to set.
        kernable_core::proto::Rejected {
            code: self.code.clone(),
            message: self.message.clone(),
            ..Default::default()
        }
    }

    /// The rejection a failed handler reports: `err` itself if it is a [`Rejected`] (also one a
    /// query inside the handler returned), else `code` with the error's text.
    pub(crate) fn of(err: &BoxError, code: &str) -> Rejected {
        if let Some(r) = err.downcast_ref::<Rejected>() {
            return r.clone();
        }
        if let Some(Error::Rejected(r)) = err.downcast_ref::<Error>() {
            return r.clone();
        }
        Rejected::new(err.to_string()).code(code)
    }
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Rejected {}

/// Why a call to the kernel failed. It implements `std::error::Error`, so `?` turns it into a
/// [`BoxError`] in handlers and `main`.
#[derive(Debug)]
pub enum Error {
    /// A rejection: the read model or a middleware refused the query
    /// ([`crate::Kernable::query`]), or a command handler refused the command in
    /// [`crate::Aggregate::handle`]. A rejected submit is not an error but
    /// [`crate::Outcome::Rejected`].
    Rejected(Rejected),
    /// The command was committed, but not every listener that counts processed its events
    /// within the kernel's `--settle-timeout-ms`. Read models may not show the change yet; the
    /// committed events are attached. Submitting again with the same ID returns the same
    /// outcome.
    NotSettled(Vec<StoredEvent>),
    /// The kernel refused the call or could not be reached: the gRPC status, e.g.
    /// `PERMISSION_DENIED` without a grant (see Errors in the reference).
    Status(tonic::Status),
    /// A key, grant or certificate could not be read, or an option is invalid; also a failing
    /// handler in [`crate::Aggregate::fold`] and [`crate::Aggregate::handle`].
    Config(String),
    /// Data could not be encoded or decoded as JSON.
    Json(serde_json::Error),
    /// A read model's store failed.
    Store(rusqlite::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Rejected(r) => write!(f, "{} ({})", r.message, r.code),
            Error::NotSettled(_) => f.write_str(
                "command committed, but not all listeners have processed its events yet",
            ),
            Error::Status(s) => write!(f, "{}: {}", s.code(), s.message()),
            Error::Config(m) => f.write_str(m),
            Error::Json(e) => write!(f, "JSON: {e}"),
            Error::Store(e) => write!(f, "store: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Error {
        Error::Status(s)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::Json(e)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::Store(e)
    }
}

impl From<Rejected> for Error {
    fn from(r: Rejected) -> Error {
        Error::Rejected(r)
    }
}
