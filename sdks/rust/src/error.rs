//! Rejections and errors.

use std::fmt;

use crate::StoredEvent;

/// Any error a handler may return; `?` converts most errors into it.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A business reason to refuse a command or query: return it from a handler. `code` (default
/// `rejected`) is what callers act on, `message` what they show.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    pub code: String,
    pub message: String,
}

impl Rejected {
    pub fn new(message: impl Into<String>) -> Rejected {
        Rejected {
            code: "rejected".into(),
            message: message.into(),
        }
    }

    /// This rejection with another code.
    pub fn code(mut self, code: impl Into<String>) -> Rejected {
        self.code = code.into();
        self
    }

    pub(crate) fn from_wire(r: kernable_core::proto::Rejected) -> Rejected {
        Rejected {
            code: r.code,
            message: r.message,
        }
    }

    pub(crate) fn to_wire(&self) -> kernable_core::proto::Rejected {
        kernable_core::proto::Rejected {
            code: self.code.clone(),
            message: self.message.clone(),
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

/// Why a call to the kernel failed.
#[derive(Debug)]
pub enum Error {
    /// The read model refused the query.
    Rejected(Rejected),
    /// The command was committed, but not every listener processed its events within the
    /// kernel's `--settle-timeout-ms`. Read models may not show the change yet.
    NotSettled(Vec<StoredEvent>),
    /// The kernel refused the call or could not be reached.
    Status(tonic::Status),
    /// A key, grant or certificate could not be read, or an option is invalid.
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
