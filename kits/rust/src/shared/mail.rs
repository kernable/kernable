//! Sending mail: a small provider interface; the mail reactors call it.

use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    Verify,
    Reset,
    EmailChange,
    Invitation,
    AccountExists,
    /// A notice to a person from the application: no token (`token` empty).
    Notice,
}

#[derive(Clone, Debug)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub text: String,
    /// What the mail is for, and the token it carries: tests and logs read these.
    pub purpose: Purpose,
    pub token: String,
}

pub trait Mailer: Send + Sync {
    fn send(&self, mail: Mail);
}

/// Prints mails instead of sending them: for development.
pub struct ConsoleMailer;

impl Mailer for ConsoleMailer {
    fn send(&self, mail: Mail) {
        eprintln!("mail to {}: {}\n{}", mail.to, mail.subject, mail.text);
    }
}

/// Keeps mails in memory: for tests.
#[derive(Default)]
pub struct MemoryMailer {
    pub sent: Mutex<Vec<Mail>>,
}

impl MemoryMailer {
    pub fn last(&self, to: &str) -> Option<Mail> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|m| m.to == to)
            .cloned()
    }
}

impl Mailer for MemoryMailer {
    fn send(&self, mail: Mail) {
        self.sent.lock().unwrap().push(mail);
    }
}

static MAILER: RwLock<Option<Arc<dyn Mailer>>> = RwLock::new(None);

/// Sets the mailer the kit's reactors send with (an SMTP one in production).
pub fn use_mailer(mailer: Arc<dyn Mailer>) {
    *MAILER.write().unwrap() = Some(mailer);
}

pub fn current_mailer() -> Arc<dyn Mailer> {
    MAILER
        .read()
        .unwrap()
        .clone()
        .unwrap_or_else(|| Arc::new(ConsoleMailer))
}
