//! The scenes of the Stage: what each step does and what the Stage says about it.

use super::bank::Parts;

/// Intentions by node: `("worker", "Open an account only once.")`.
pub(super) type Intents = &'static [(&'static str, &'static str)];

/// A scene of the Stage: steps on the bank parts it runs, shown with one read model.
pub(super) struct Scene {
    pub title: &'static str,
    /// The question the scene answers, on its opening slide.
    pub question: &'static str,
    /// What to remember, on its closing slide.
    pub takeaway: &'static str,
    /// What the Stage says before the first step.
    pub intro: &'static str,
    /// The aggregate type the Stage shows, and the IDs of those it shows in the store.
    pub aggregate: &'static str,
    pub ids: &'static [&'static str],
    pub read_model: &'static str,
    /// The read model's query for one aggregate, `{ "id": … }`.
    pub query: &'static str,
    pub parts: Parts,
    /// Nodes the flow diagram shows besides client, kernel, worker, store and read model.
    pub nodes: &'static [&'static str],
    /// What each part of the system wants in this scene, in plain words, by node; a step's own
    /// intentions come first.
    pub intents: Intents,
    pub steps: &'static [Step],
}

/// One step of a scene: what it does, and what the Stage says about it.
pub(super) struct Step {
    pub title: &'static str,
    pub says: &'static str,
    /// What a part wants in this step only, by node (the worker's rule for this command).
    pub intents: Intents,
    pub does: Does,
}

pub(super) enum Does {
    /// Commands the client sends, one after the other, or all at the same moment.
    Send {
        sends: &'static [Send],
        together: bool,
    },
    /// Something the demo does besides sending a command; the Stage narrates what happened.
    Act(Act),
}

/// One command of a step, sent by `who` (the request context's sender identity).
pub(super) struct Send {
    pub who: &'static str,
    pub id: &'static str,
    pub command: &'static str,
    pub data: &'static str,
}

#[derive(Clone, Copy)]
pub(super) enum Act {
    /// `n` deposits at the same moment, all into `accounts[0]` or spread over `accounts`.
    Burst {
        accounts: &'static [&'static str],
        n: usize,
    },
    /// `kill -9` of the kernel while `n` deposits are on their way, a restart on the same store,
    /// and the clients sending what failed again.
    Crash { n: usize },
    /// `kernable verify` on the store.
    Verify,
    /// One event's payload changed in the database file.
    Tamper { position: u64 },
    /// The payments reactor fails from now on.
    Break,
    /// The payments reactor works again, and its parked events are retried.
    FixAndRetry,
    /// A new key for a mobile app, granted to deposit, and a deposit with it.
    NewKey,
    /// The mobile app's key revoked, and a deposit with it and with the app's key.
    Revoke,
    /// The rbac middleware stopped, a withdrawal tried, the middleware started again.
    FailClosed,
    /// A new read model built from the whole history, compared with the live one.
    Rebuild,
}

const NONE: Parts = Parts {
    accounts: false,
    customers: false,
    people: false,
    payments: false,
    rbac: false,
};
const ACCOUNTS: Parts = Parts {
    accounts: true,
    ..NONE
};

const fn send(id: &'static str, command: &'static str, data: &'static str) -> Send {
    Send {
        who: "client",
        id,
        command,
        data,
    }
}

const fn send_as(
    who: &'static str,
    id: &'static str,
    command: &'static str,
    data: &'static str,
) -> Send {
    Send {
        who,
        id,
        command,
        data,
    }
}

const fn sends(sends: &'static [Send]) -> Does {
    Does::Send {
        sends,
        together: false,
    }
}

pub(super) const SCENES: &[Scene] = &[
    // A command becomes an event, the event a row in a read model, and the client reads its own
    // write.
    Scene {
        title: "Command → event → read model",
        question: "When a command returns, does the next read see it?",
        takeaway: "The submit returns once the read side has the event: read your own write, without polling.",
        intro: "A client sends a command, a worker decides, the kernel stores the event, a read model shows it.",
        aggregate: "account",
        ids: &["a1"],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            (
                "kernel",
                "Run one command per aggregate at a time; store what the worker decides.",
            ),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "Open an account",
                says: "A client opens an account for Ada. The worker decides, the kernel stores the event, the read model shows it.",
                intents: &[("worker", "Open an account only once.")],
                does: sends(&[send("a1", "OpenAccount", r#"{"owner":"Ada"}"#)]),
            },
            Step {
                title: "Deposit 100",
                says: "The submit returns only once the read model has the event, so the query right after it already sees the new balance.",
                intents: &[("worker", "Deposit positive amounts into open accounts.")],
                does: sends(&[send("a1", "Deposit", r#"{"amount":100}"#)]),
            },
            Step {
                title: "Withdraw 500",
                says: "The worker rejects the command. Nothing is stored: no event, no change in the read model.",
                intents: &[("worker", "Never withdraw more than the balance.")],
                does: sends(&[send("a1", "Withdraw", r#"{"amount":500}"#)]),
            },
            Step {
                title: "Withdraw 30",
                says: "The balance comes from the events alone: 100 deposited, 30 withdrawn. Every event is signed and chained to the one before.",
                intents: &[("worker", "Never withdraw more than the balance.")],
                does: sends(&[send("a1", "Withdraw", r#"{"amount":30}"#)]),
            },
        ],
    },
    // A unique key: never two holders, also when two commands race for it.
    Scene {
        title: "Two customers, one username",
        question: "Can two customers ever end up with the same username?",
        takeaway: "Never two holders: the kernel checks unique keys at the commit, also when commands race.",
        intro: "A username belongs to one customer only. The worker never checks it: the kernel does, when it commits, also when two commands race for it.",
        aggregate: "customer",
        ids: &["c1", "c2", "c3", "c4"],
        read_model: "customers",
        query: "GetCustomer",
        parts: Parts {
            customers: true,
            ..NONE
        },
        nodes: &[],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            ("kernel", "Never let two customers hold one username."),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each customer with their username."),
            (
                "worker",
                "Register a customer once. The username is the kernel's job.",
            ),
        ],
        steps: &[
            Step {
                title: "Ada registers as ada",
                says: "The customer aggregate declares its username unique. With the event, the kernel stores that c1 now holds the key.",
                intents: &[],
                does: sends(&[send("c1", "Register", r#"{"name":"Ada","username":"ada"}"#)]),
            },
            Step {
                title: "Bob wants ada too",
                says: "The worker accepts: it never looks the username up. The kernel refuses the commit, because c1 holds the key.",
                intents: &[],
                does: sends(&[send("c2", "Register", r#"{"name":"Bob","username":"ADA"}"#)]),
            },
            Step {
                title: "The race: Cleo and Dan, one username",
                says: "Two customers claim cleo at the same moment. Both workers accept; the kernel commits one at a time, so exactly one wins.",
                intents: &[],
                does: Does::Send {
                    sends: &[
                        send("c3", "Register", r#"{"name":"Cleo","username":"cleo"}"#),
                        send("c4", "Register", r#"{"name":"Dan","username":"cleo"}"#),
                    ],
                    together: true,
                },
            },
            Step {
                title: "Ada changes her username",
                says: "The key follows the state: once c1 is ada.lovelace, it no longer holds ada, in the same commit as the event.",
                intents: &[("worker", "Let a registered customer change their username.")],
                does: sends(&[send(
                    "c1",
                    "ChangeUsername",
                    r#"{"username":"ada.lovelace"}"#,
                )]),
            },
            Step {
                title: "Bob tries again",
                says: "The same command as before now commits: ada is free. No cleanup job, no claims table, no compensation.",
                intents: &[],
                does: sends(&[send("c2", "Register", r#"{"name":"Bob","username":"ada"}"#)]),
            },
        ],
    },
    // A requirement on another aggregate, checked when the command commits.
    Scene {
        title: "Only open accounts get paid",
        question: "How does one aggregate depend on another without locking both?",
        takeaway: "Requirements are checked at the commit: the payer's worker never sees the other account.",
        intro: "A payment requires the receiving account to be open. The paying account's worker cannot see the other account: the kernel checks it when the payment commits.",
        aggregate: "account",
        ids: &["a1", "a2"],
        read_model: "accounts",
        query: "GetAccount",
        parts: Parts {
            payments: true,
            ..ACCOUNTS
        },
        nodes: &["reactor"],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            (
                "kernel",
                "Commit a payment only while the receiving account is open.",
            ),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            (
                "model",
                "Show each account, its balance, and whether it is open.",
            ),
            (
                "reactor",
                "Credit every payment to its receiver, exactly once.",
            ),
        ],
        steps: &[
            Step {
                title: "Two accounts",
                says: "Ada opens a1 and deposits 100; Bob opens a2.",
                intents: &[("worker", "Open accounts and take deposits.")],
                does: sends(&[
                    send("a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send("a1", "Deposit", r#"{"amount":100}"#),
                    send("a2", "OpenAccount", r#"{"owner":"Bob"}"#),
                ]),
            },
            Step {
                title: "Ada pays Bob 30",
                says: "The payment requires a2 to hold the fact open; it does. A reactor then credits a2, with a command of its own.",
                intents: &[("worker", "Pay only what the balance covers.")],
                does: sends(&[send("a1", "Pay", r#"{"to":"a2","amount":30}"#)]),
            },
            Step {
                title: "Bob closes a2",
                says: "With AccountClosed, a2 no longer holds the fact open: the kernel stores that with the event.",
                intents: &[("worker", "Close an account that is open.")],
                does: sends(&[send("a2", "Close", "{}")]),
            },
            Step {
                title: "Ada pays Bob again",
                says: "The worker of a1 accepts: it knows nothing about a2. The kernel refuses the commit: a2 does not hold open.",
                intents: &[("worker", "Pay only what the balance covers.")],
                does: sends(&[send("a1", "Pay", r#"{"to":"a2","amount":30}"#)]),
            },
        ],
    },
    // Many commands on one aggregate: queued, one at a time, no conflicts.
    Scene {
        title: "One hot account",
        question: "What happens when everyone writes to the same aggregate at once?",
        takeaway: "One queue per aggregate: no version conflicts, no retries, and different aggregates run in parallel.",
        intro: "Two hundred clients deposit into the same account at once. There are no version conflicts to retry: the kernel runs one command per aggregate at a time.",
        aggregate: "account",
        ids: &[
            "h1", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13",
            "s14", "s15", "s16", "s17", "s18", "s19", "s20",
        ],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Send all deposits at once."),
            (
                "kernel",
                "Queue commands per account: one at a time, in order; accounts in parallel.",
            ),
            ("worker", "Add each deposit to the balance it sees."),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "200 deposits into one account",
                says: "All at the same moment, into h1. The kernel queues them per aggregate and hands h1 to the worker one command at a time.",
                intents: &[],
                does: Does::Act(Act::Burst {
                    accounts: &["h1"],
                    n: 200,
                }),
            },
            Step {
                title: "200 deposits into 20 accounts",
                says: "Different aggregates run in parallel: the same 200 deposits, spread over s1 to s20.",
                intents: &[],
                does: Does::Act(Act::Burst {
                    accounts: &[
                        "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12",
                        "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20",
                    ],
                    n: 200,
                }),
            },
        ],
    },
    // A crash in the middle of the load: nothing lost, nothing twice.
    Scene {
        title: "Kill the kernel",
        question: "What do you lose if the kernel dies in the middle of a write?",
        takeaway: "Nothing. Commands are idempotent by ID: resend after a crash, and every one is stored exactly once.",
        intro: "300 deposits are on their way when the kernel is killed with kill -9. It restarts on the same store; the clients send again what failed, with the same command IDs.",
        aggregate: "account",
        ids: &["a1", "a2", "a3"],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Resend what failed, with the same command ID."),
            (
                "kernel",
                "Answer a command ID seen before with its stored result; run it only once.",
            ),
            ("worker", "Add each deposit to the balance it sees."),
            ("store", "Keep what was committed through any crash."),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "Three accounts",
                says: "a1, a2 and a3 are opened.",
                intents: &[("worker", "Open an account only once.")],
                does: sends(&[
                    send("a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send("a2", "OpenAccount", r#"{"owner":"Bob"}"#),
                    send("a3", "OpenAccount", r#"{"owner":"Cleo"}"#),
                ]),
            },
            Step {
                title: "kill -9 during 300 deposits",
                says: "The kernel dies mid-way, with commands queued, leased to the worker, and half committed. Then it starts again on the same store.",
                intents: &[],
                does: Does::Act(Act::Crash { n: 300 }),
            },
            Step {
                title: "Verify the store",
                says: "kernable verify recomputes the hash chain and checks every signature: the crash left no gap and no torn event.",
                intents: &[],
                does: Does::Act(Act::Verify),
            },
        ],
    },
    // The store is tamper-evident.
    Scene {
        title: "Change one byte",
        question: "Would you notice if someone edited the database by hand?",
        takeaway: "Yes: every event is signed and hash-chained, and verify names the one that was changed.",
        intro: "Every event is signed by the worker that produced it and chained to the one before by its hash. Someone changes one stored event directly in the database file.",
        aggregate: "account",
        ids: &["a1"],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            (
                "kernel",
                "Run one command per aggregate at a time; store what the worker decides.",
            ),
            ("store", "Sign and chain every event, so any change shows."),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "An account with history",
                says: "a1 is opened, 100 deposited, 30 withdrawn.",
                intents: &[(
                    "worker",
                    "Open, deposit, withdraw: each checked against the balance.",
                )],
                does: sends(&[
                    send("a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send("a1", "Deposit", r#"{"amount":100}"#),
                    send("a1", "Withdraw", r#"{"amount":30}"#),
                ]),
            },
            Step {
                title: "Change event #2 in the database",
                says: "With SQL, straight in the file: the deposit of 100 becomes 1,000,000. SQLite accepts it; nothing else notices.",
                intents: &[],
                does: Does::Act(Act::Tamper { position: 2 }),
            },
            Step {
                title: "Verify the store",
                says: "kernable verify recomputes every hash and checks every signature, and names the event that was changed.",
                intents: &[],
                does: Does::Act(Act::Verify),
            },
        ],
    },
    // Personal data in the vault, erased for good while the events stay.
    Scene {
        title: "Forget a customer",
        question: "How do you delete personal data from an append-only log?",
        takeaway: "Personal data lives in the vault. Erase the subject: the values are gone, the history stays intact.",
        intro: "Names and emails are personal data. They go to the vault, encrypted per person; the events keep references. Forgetting a person erases the values, the history stays intact.",
        aggregate: "person",
        ids: &["p1", "p2"],
        read_model: "people",
        query: "GetPerson",
        parts: Parts {
            people: true,
            ..NONE
        },
        nodes: &[],
        intents: &[
            (
                "client",
                "Send name and email; the SDK seals them for the vault.",
            ),
            (
                "kernel",
                "Keep personal fields in the vault, per person; erase them on request.",
            ),
            ("store", "Keep references to personal data, never the data."),
            (
                "model",
                "Show each person while they exist; forget them when asked.",
            ),
        ],
        steps: &[
            Step {
                title: "Ada is onboarded",
                says: "The handler gets name and email in clear. The stored command and event hold references; the values are in the vault.",
                intents: &[("worker", "Onboard a person once.")],
                does: sends(&[send(
                    "p1",
                    "Onboard",
                    r#"{"name":"Ada Lovelace","email":"ada@example.com"}"#,
                )]),
            },
            Step {
                title: "Bob is onboarded",
                says: "Each person's values are encrypted on their own, so one can be erased without touching the others.",
                intents: &[("worker", "Onboard a person once.")],
                does: sends(&[send(
                    "p2",
                    "Onboard",
                    r#"{"name":"Bob Babbage","email":"bob@example.com"}"#,
                )]),
            },
            Step {
                title: "Forget Ada",
                says: "PersonForgotten erases everything of p1 in the vault: in every event and command, at once. The events themselves stay.",
                intents: &[("worker", "Forget a person who exists.")],
                does: sends(&[send("p1", "Forget", "{}")]),
            },
        ],
    },
    // A middleware decides before anything is queued, and fails closed.
    Scene {
        title: "Who may take money out",
        question: "Where does authorization run, and what if it is down?",
        takeaway: "Before the queue, in a middleware: refused commands are never stored, and it fails closed.",
        intro: "A middleware checks every withdrawal before the kernel queues it: only tellers may withdraw. A refused command is never stored, and while the middleware is away, nothing it covers passes.",
        aggregate: "account",
        ids: &["a1"],
        read_model: "accounts",
        query: "GetAccount",
        parts: Parts {
            rbac: true,
            ..ACCOUNTS
        },
        nodes: &["middleware"],
        intents: &[
            (
                "client",
                "Say who sends the command: it travels signed with it.",
            ),
            ("middleware", "Only tellers may withdraw."),
            (
                "kernel",
                "Ask the middleware before queuing; refuse while it is away.",
            ),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "An account with 100",
                says: "A teller opens a1 and deposits 100.",
                intents: &[("worker", "Open an account, take deposits.")],
                does: sends(&[
                    send_as("teller", "a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send_as("teller", "a1", "Deposit", r#"{"amount":100}"#),
                ]),
            },
            Step {
                title: "Ada withdraws herself",
                says: "The middleware refuses: ada is not a teller. The kernel never queues the command; no worker sees it.",
                intents: &[("worker", "Never withdraw more than the balance.")],
                does: sends(&[send_as("ada", "a1", "Withdraw", r#"{"amount":30}"#)]),
            },
            Step {
                title: "A teller withdraws",
                says: "The same command from a teller passes the middleware, then runs as usual.",
                intents: &[("worker", "Never withdraw more than the balance.")],
                does: sends(&[send_as("teller", "a1", "Withdraw", r#"{"amount":30}"#)]),
            },
            Step {
                title: "The middleware goes away",
                says: "With the middleware stopped, the kernel refuses everything it covers, also from a teller: it fails closed, never open.",
                intents: &[],
                does: Does::Act(Act::FailClosed),
            },
        ],
    },
    // A reactor that fails parks its event, and the rest goes on.
    Scene {
        title: "A reactor that breaks",
        question: "What happens to an event a reactor cannot handle?",
        takeaway: "It is parked, not lost and not blocking: the rest goes on, and a retry runs it exactly once.",
        intro: "A reactor credits every payment to the receiving account. When it fails, the kernel parks the event, the rest of the system goes on, and an operator retries it later.",
        aggregate: "account",
        ids: &["a1", "a2"],
        read_model: "accounts",
        query: "GetAccount",
        parts: Parts {
            payments: true,
            ..ACCOUNTS
        },
        nodes: &["reactor"],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            (
                "kernel",
                "Park what a reactor cannot handle; keep everything else moving.",
            ),
            (
                "reactor",
                "Credit every payment to its receiver, exactly once.",
            ),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "Two accounts",
                says: "Ada opens a1 and deposits 100; Bob opens a2.",
                intents: &[("worker", "Open accounts and take deposits.")],
                does: sends(&[
                    send("a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send("a1", "Deposit", r#"{"amount":100}"#),
                    send("a2", "OpenAccount", r#"{"owner":"Bob"}"#),
                ]),
            },
            Step {
                title: "Ada pays Bob 30",
                says: "The payments reactor sees Paid and credits a2 with a command of its own, under a fixed ID.",
                intents: &[("worker", "Pay only what the balance covers.")],
                does: sends(&[send("a1", "Pay", r#"{"to":"a2","amount":30}"#)]),
            },
            Step {
                title: "The payments service fails",
                says: "From now on, the reactor's handler throws, like a service that is down.",
                intents: &[],
                does: Does::Act(Act::Break),
            },
            Step {
                title: "Ada pays Bob 20",
                says: "The payment commits. The reactor fails, so the kernel parks the event for it; the read model goes on.",
                intents: &[("worker", "Pay only what the balance covers.")],
                does: sends(&[send("a1", "Pay", r#"{"to":"a2","amount":20}"#)]),
            },
            Step {
                title: "Fix it and retry",
                says: "The service is back; an operator retries the parked event. The credit happens exactly once.",
                intents: &[],
                does: Does::Act(Act::FixAndRetry),
            },
        ],
    },
    // Keys can be revoked while everything runs.
    Scene {
        title: "Revoke a key",
        question: "A client key leaks. How fast can you shut it out?",
        takeaway: "At once, while everything runs: revocations are signed by the root key and kept in the store.",
        intro: "Every client signs with its own key, granted by a root key. A mobile app's key leaks: it is revoked while the kernel runs, and from that moment on the kernel refuses it.",
        aggregate: "account",
        ids: &["a1"],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Sign every command with this app's own key."),
            (
                "kernel",
                "Accept only keys the root key granted and nobody revoked.",
            ),
            (
                "store",
                "Keep every event: append only, signed, chained to the one before.",
            ),
            ("model", "Show each account with its balance."),
        ],
        steps: &[
            Step {
                title: "An account",
                says: "Ada opens a1.",
                intents: &[("worker", "Open an account only once.")],
                does: sends(&[send("a1", "OpenAccount", r#"{"owner":"Ada"}"#)]),
            },
            Step {
                title: "A mobile app gets a key",
                says: "kernable keys new mobile, then kernable grant: it may deposit and query in bank, nothing else. It deposits 10.",
                intents: &[],
                does: Does::Act(Act::NewKey),
            },
            Step {
                title: "The key leaks: revoke it",
                says: "kernable revoke, signed by the root key. The kernel refuses the mobile key at once; the other keys go on.",
                intents: &[],
                does: Does::Act(Act::Revoke),
            },
        ],
    },
    // The read side can always be rebuilt from the events.
    Scene {
        title: "The read side is disposable",
        question: "What if a read model is wrong, or you need a new one?",
        takeaway: "Rebuild it from the events: same rows, and new questions answered from old history.",
        intro: "Read models hold nothing the events do not. Throw one away, and a new one built from the history arrives at the same rows; a new question gets its answer from old events too.",
        aggregate: "account",
        ids: &["a1", "a2"],
        read_model: "accounts",
        query: "GetAccount",
        parts: ACCOUNTS,
        nodes: &[],
        intents: &[
            ("client", "Send a signed command, then read right after it."),
            (
                "kernel",
                "Run one command per aggregate at a time; store what the worker decides.",
            ),
            ("store", "Keep the whole history, in order."),
            (
                "model",
                "Hold nothing the events do not: rebuild from them at any time.",
            ),
        ],
        steps: &[
            Step {
                title: "Some history",
                says: "Two accounts with deposits and withdrawals.",
                intents: &[("worker", "Open accounts, take deposits and withdrawals.")],
                does: sends(&[
                    send("a1", "OpenAccount", r#"{"owner":"Ada"}"#),
                    send("a1", "Deposit", r#"{"amount":100}"#),
                    send("a2", "OpenAccount", r#"{"owner":"Bob"}"#),
                    send("a2", "Deposit", r#"{"amount":250}"#),
                    send("a1", "Withdraw", r#"{"amount":40}"#),
                    send("a2", "Withdraw", r#"{"amount":75}"#),
                ]),
            },
            Step {
                title: "Rebuild it from the events",
                says: "A new read model subscribes from position 0, replays the whole history and compares its rows with the live read model.",
                intents: &[],
                does: Does::Act(Act::Rebuild),
            },
        ],
    },
];
