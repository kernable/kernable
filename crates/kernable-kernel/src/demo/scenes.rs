//! The Stage's story: one day at the bank, in chapters. The bank grows all day, so each chapter
//! works on what the ones before left; the Stage steps through it command by command.

use super::bank::Parts;

/// Intentions by node: `("worker", "Open an account only once.")`.
pub(super) type Intents = &'static [(&'static str, &'static str)];

/// The story: every chapter's steps in one list, run on every part of the bank.
pub(super) struct Scene {
    pub title: &'static str,
    pub intro: &'static str,
    /// The aggregates the Stage shows in the store, by type and ID.
    pub ids: &'static [(&'static str, &'static str)],
    pub parts: Parts,
    /// Nodes the flow diagram shows besides client, kernel, worker, store and read model.
    pub nodes: &'static [&'static str],
    /// What each part of the system wants all day, in plain words, by node; a chapter's and a
    /// step's own intentions come first.
    pub intents: Intents,
    pub chapters: &'static [Chapter],
    pub steps: &'static [Step],
}

/// A chapter: the steps from `from` up to the next chapter's.
pub(super) struct Chapter {
    pub title: &'static str,
    /// The question the chapter answers, on its opening card.
    pub question: &'static str,
    /// What to remember, on its closing card.
    pub takeaway: &'static str,
    pub from: usize,
    pub intents: Intents,
}

/// One step: what it does, and what the Stage says about it.
pub(super) struct Step {
    pub title: &'static str,
    pub says: &'static str,
    /// What a part wants in this step only, by node (the worker's rule for this command).
    pub intents: Intents,
    pub does: Does,
}

pub(super) enum Does {
    /// Commands clients send, one after the other, or all at the same moment.
    Send {
        sends: &'static [Send],
        together: bool,
    },
    /// Something the demo does besides sending a command; the Stage narrates what happened.
    Act(Act),
}

/// One command of a step, sent by `who` (the request context's sender identity) to an
/// aggregate of type `aggregate`.
pub(super) struct Send {
    pub who: &'static str,
    pub aggregate: &'static str,
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
    /// `kill -9` of the kernel while `n` deposits into `accounts` are on their way, a restart on
    /// the same store, and the clients sending what failed again.
    Crash {
        accounts: &'static [&'static str],
        n: usize,
    },
    /// `kernable verify` on the store.
    Verify,
    /// The payload of the first deposit changed in the database file.
    Tamper,
    /// The payments reactor fails from now on.
    Break,
    /// The payments reactor works again, and its parked events are retried; they credit `to`.
    FixAndRetry { to: &'static str },
    /// A new key for a mobile app, granted to deposit, and a deposit with it.
    NewKey,
    /// The mobile app's key revoked, and a deposit with it and with the app's key.
    Revoke,
    /// Bob, then Ada, ask for Ada's account: the middleware answers only its owner.
    Peek,
    /// The rbac middleware stopped, a withdrawal tried, the middleware started again.
    FailClosed,
    /// A new read model built from the whole history, compared with the live one.
    Rebuild,
}

/// The read model and the query of each aggregate type the Stage shows.
pub(super) fn read_side(aggregate: &str) -> (&'static str, &'static str) {
    match aggregate {
        "customer" => ("customers", "GetCustomer"),
        "person" => ("people", "GetPerson"),
        _ => ("accounts", "GetAccount"),
    }
}

const fn send(
    who: &'static str,
    aggregate: &'static str,
    id: &'static str,
    command: &'static str,
    data: &'static str,
) -> Send {
    Send {
        who,
        aggregate,
        id,
        command,
        data,
    }
}

const fn account(
    who: &'static str,
    id: &'static str,
    command: &'static str,
    data: &'static str,
) -> Send {
    send(who, "account", id, command, data)
}

const fn sends(sends: &'static [Send]) -> Does {
    Does::Send {
        sends,
        together: false,
    }
}

const fn step(title: &'static str, says: &'static str, intents: Intents, does: Does) -> Step {
    Step {
        title,
        says,
        intents,
        does,
    }
}

const SPREAD: &[&str] = &[
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14", "s15",
    "s16", "s17", "s18", "s19", "s20",
];

const STORE: &str = "Keep every event: append only, signed, chained to the one before.";

pub(super) const SCENES: &[Scene] = &[Scene {
    title: "A day at the bank",
    intro: "One bank, one day, one store. Every chapter works on what the ones before left behind.",
    ids: &[
        ("account", "a1"),
        ("account", "a2"),
        ("account", "a3"),
        ("account", "h1"),
        ("account", "s1"),
        ("account", "s2"),
        ("account", "s3"),
        ("account", "s4"),
        ("account", "s5"),
        ("account", "s6"),
        ("account", "s7"),
        ("account", "s8"),
        ("account", "s9"),
        ("account", "s10"),
        ("account", "s11"),
        ("account", "s12"),
        ("account", "s13"),
        ("account", "s14"),
        ("account", "s15"),
        ("account", "s16"),
        ("account", "s17"),
        ("account", "s18"),
        ("account", "s19"),
        ("account", "s20"),
        ("customer", "c1"),
        ("customer", "c2"),
        ("customer", "c3"),
        ("customer", "c4"),
        ("person", "p1"),
        ("person", "p2"),
    ],
    parts: Parts {
        accounts: true,
        customers: true,
        people: true,
        payments: true,
        rbac: true,
    },
    nodes: &["middleware", "reactor"],
    intents: &[
        ("client", "Send a signed command, then read right after it."),
        (
            "kernel",
            "Run one command per aggregate at a time; store what the worker decides.",
        ),
        ("store", STORE),
        ("model", "Show each account with its balance."),
        (
            "middleware",
            "Only tellers may withdraw; customers see only their own accounts.",
        ),
        (
            "reactor",
            "Credit every payment to its receiver, exactly once.",
        ),
    ],
    chapters: &[
        Chapter {
            title: "Opening",
            question: "When a command returns, does the next read see it?",
            takeaway: "The submit returns once the read side has the event: read your own write, without polling.",
            from: 0,
            intents: &[],
        },
        Chapter {
            title: "Usernames",
            question: "Can two customers ever end up with the same username?",
            takeaway: "Never two holders: the kernel checks unique keys at the commit, also when commands race.",
            from: 3,
            intents: &[
                ("kernel", "Never let two customers hold one username."),
                ("model", "Show each customer with their username."),
                (
                    "worker",
                    "Register a customer once. The username is the kernel's job.",
                ),
            ],
        },
        Chapter {
            title: "Payments",
            question: "How does one account depend on another without locking both?",
            takeaway: "Requirements are checked at the commit: the payer's worker never sees the other account.",
            from: 7,
            intents: &[
                (
                    "kernel",
                    "Commit a payment only while the receiving account is open.",
                ),
                (
                    "model",
                    "Show each account, its balance, and whether it is open.",
                ),
            ],
        },
        Chapter {
            title: "Rush hour",
            question: "What happens when everyone writes to the same account at once?",
            takeaway: "One queue per aggregate: no version conflicts, no retries, and different aggregates run in parallel.",
            from: 11,
            intents: &[
                ("client", "Send all deposits at once."),
                (
                    "kernel",
                    "Queue commands per account: one at a time, in order; accounts in parallel.",
                ),
                ("worker", "Add each deposit to the balance it sees."),
            ],
        },
        Chapter {
            title: "Tellers",
            question: "Who may do what, and what if the one who decides is down?",
            takeaway: "A middleware decides before anything is queued or answered, and it fails closed.",
            from: 13,
            intents: &[
                (
                    "client",
                    "Say who sends: the identity travels signed with every request.",
                ),
                (
                    "kernel",
                    "Ask the middleware first; refuse what it covers while it is away.",
                ),
            ],
        },
        Chapter {
            title: "Outage",
            question: "What happens to an event a reactor cannot handle?",
            takeaway: "It is parked, not lost and not blocking: the rest goes on, and a retry runs it exactly once.",
            from: 17,
            intents: &[(
                "kernel",
                "Park what a reactor cannot handle; keep everything else moving.",
            )],
        },
        Chapter {
            title: "Leaked key",
            question: "A client key leaks. How fast can you shut it out?",
            takeaway: "At once, while everything runs: revocations are signed by the root key and kept in the store.",
            from: 20,
            intents: &[
                ("client", "Sign every command with this app's own key."),
                (
                    "kernel",
                    "Accept only keys the root key granted and nobody revoked.",
                ),
            ],
        },
        Chapter {
            title: "Disaster",
            question: "What do you lose if the kernel dies in the middle of a write?",
            takeaway: "Nothing. Commands are idempotent by ID: resend after a crash, and every one is stored exactly once.",
            from: 22,
            intents: &[
                ("client", "Resend what failed, with the same command ID."),
                (
                    "kernel",
                    "Answer a command ID seen before with its stored result; run it only once.",
                ),
                ("store", "Keep what was committed through any crash."),
            ],
        },
        Chapter {
            title: "Forget me",
            question: "How do you delete personal data from an append-only log?",
            takeaway: "Personal data lives in the vault. Erase the person: the values are gone, the history stays intact.",
            from: 24,
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
        },
        Chapter {
            title: "New report",
            question: "What if a read model is wrong, or you need a new one?",
            takeaway: "Rebuild it from the events: the same rows, and new questions answered from old history.",
            from: 26,
            intents: &[
                ("store", "Keep the whole history, in order."),
                (
                    "model",
                    "Hold nothing the events do not: rebuild from them at any time.",
                ),
            ],
        },
        Chapter {
            title: "Tampering",
            question: "Would you notice if someone edited the database by hand?",
            takeaway: "Yes: every event is signed and hash-chained, and verify names the one that was changed.",
            from: 27,
            intents: &[("store", "Sign and chain every event, so any change shows.")],
        },
    ],
    steps: &[
        // Opening
        step(
            "Ada opens an account",
            "A client opens an account for Ada. The worker decides, the kernel stores the event, the read model shows it.",
            &[("worker", "Open an account only once.")],
            sends(&[account("ada", "a1", "OpenAccount", r#"{"owner":"Ada"}"#)]),
        ),
        step(
            "Ada deposits 100",
            "The submit returns only once the read model has the event, so the query right after it already sees the new balance.",
            &[("worker", "Deposit positive amounts into open accounts.")],
            sends(&[account("ada", "a1", "Deposit", r#"{"amount":100}"#)]),
        ),
        step(
            "A teller withdraws 500 for Ada",
            "The middleware lets the teller through; the worker rejects: the balance is 100. Nothing is stored.",
            &[("worker", "Never withdraw more than the balance.")],
            sends(&[account("teller", "a1", "Withdraw", r#"{"amount":500}"#)]),
        ),
        // Usernames
        step(
            "Ada registers as ada",
            "The customer declares its username unique. With the event, the kernel stores that c1 now holds the key.",
            &[],
            sends(&[send(
                "ada",
                "customer",
                "c1",
                "Register",
                r#"{"name":"Ada","username":"ada"}"#,
            )]),
        ),
        step(
            "Bob wants ADA too",
            "The worker accepts: it never looks the username up. The kernel refuses the commit, because c1 holds the key.",
            &[],
            sends(&[send(
                "bob",
                "customer",
                "c2",
                "Register",
                r#"{"name":"Bob","username":"ADA"}"#,
            )]),
        ),
        step(
            "The race: Cleo and Dan, one username",
            "Two customers claim cleo at the same moment. Both workers accept; the kernel commits one at a time, so exactly one wins.",
            &[],
            Does::Send {
                sends: &[
                    send(
                        "cleo",
                        "customer",
                        "c3",
                        "Register",
                        r#"{"name":"Cleo","username":"cleo"}"#,
                    ),
                    send(
                        "dan",
                        "customer",
                        "c4",
                        "Register",
                        r#"{"name":"Dan","username":"cleo"}"#,
                    ),
                ],
                together: true,
            },
        ),
        step(
            "Ada renames, Bob gets ada",
            "Once c1 is ada.lovelace, it no longer holds ada, in the same commit. Bob's same command now commits: no cleanup, no claims table.",
            &[(
                "worker",
                "Let a customer change their username; register once.",
            )],
            sends(&[
                send(
                    "ada",
                    "customer",
                    "c1",
                    "ChangeUsername",
                    r#"{"username":"ada.lovelace"}"#,
                ),
                send(
                    "bob",
                    "customer",
                    "c2",
                    "Register",
                    r#"{"name":"Bob","username":"ada"}"#,
                ),
            ]),
        ),
        // Payments
        step(
            "Bob and Cleo open accounts",
            "a2 for Bob, a3 for Cleo.",
            &[("worker", "Open an account only once.")],
            sends(&[
                account("bob", "a2", "OpenAccount", r#"{"owner":"Bob"}"#),
                account("cleo", "a3", "OpenAccount", r#"{"owner":"Cleo"}"#),
            ]),
        ),
        step(
            "Ada pays Bob 30",
            "The payment requires a2 to hold the fact open; it does. The payments reactor then credits a2, with a command of its own.",
            &[("worker", "Pay only what the balance covers.")],
            sends(&[account("ada", "a1", "Pay", r#"{"to":"a2","amount":30}"#)]),
        ),
        step(
            "Bob closes his account",
            "With AccountClosed, a2 no longer holds the fact open: the kernel stores that with the event.",
            &[("worker", "Close an account that is open.")],
            sends(&[account("bob", "a2", "Close", "{}")]),
        ),
        step(
            "Ada pays Bob again",
            "The worker of a1 accepts: it knows nothing about a2. The kernel refuses the commit: a2 does not hold open.",
            &[("worker", "Pay only what the balance covers.")],
            sends(&[account("ada", "a1", "Pay", r#"{"to":"a2","amount":30}"#)]),
        ),
        // Rush hour
        step(
            "200 deposits into one account",
            "All at the same moment, into h1. The kernel queues them per aggregate and hands h1 to the worker one command at a time.",
            &[],
            Does::Act(Act::Burst {
                accounts: &["h1"],
                n: 200,
            }),
        ),
        step(
            "200 deposits into 20 accounts",
            "Different aggregates run in parallel: the same 200 deposits, spread over s1 to s20.",
            &[],
            Does::Act(Act::Burst {
                accounts: SPREAD,
                n: 200,
            }),
        ),
        // Tellers
        step(
            "Ada withdraws by herself",
            "The middleware refuses: ada is not a teller. The kernel never queues the command; no worker sees it.",
            &[],
            sends(&[account("ada", "a1", "Withdraw", r#"{"amount":20}"#)]),
        ),
        step(
            "A teller withdraws 20 for Ada",
            "The same command from a teller passes the middleware, then runs as usual.",
            &[("worker", "Never withdraw more than the balance.")],
            sends(&[account("teller", "a1", "Withdraw", r#"{"amount":20}"#)]),
        ),
        step(
            "Bob looks at Ada's account",
            "Queries pass the middleware too: Bob may not see a1, Ada may. Authorization covers the read side.",
            &[],
            Does::Act(Act::Peek),
        ),
        step(
            "The middleware goes away",
            "With the middleware stopped, the kernel refuses everything it covers, also from a teller: it fails closed, never open.",
            &[],
            Does::Act(Act::FailClosed),
        ),
        // Outage
        step(
            "The payments service fails",
            "From now on, the reactor's handler throws, like a service that is down.",
            &[],
            Does::Act(Act::Break),
        ),
        step(
            "Ada pays Cleo 20",
            "The payment commits. The reactor fails, so the kernel parks the event for it; the read model goes on.",
            &[("worker", "Pay only what the balance covers.")],
            sends(&[account("ada", "a1", "Pay", r#"{"to":"a3","amount":20}"#)]),
        ),
        step(
            "Fix it and retry",
            "The service is back; an operator retries the parked event. The credit happens exactly once.",
            &[],
            Does::Act(Act::FixAndRetry { to: "a3" }),
        ),
        // Leaked key
        step(
            "A mobile app gets a key",
            "kernable keys new mobile, then kernable grant: it may deposit and query in bank, nothing else. It deposits 10.",
            &[],
            Does::Act(Act::NewKey),
        ),
        step(
            "The key leaks: revoke it",
            "kernable revoke, signed by the root key. The kernel refuses the mobile key at once; the other keys go on.",
            &[],
            Does::Act(Act::Revoke),
        ),
        // Disaster
        step(
            "kill -9 during 300 deposits",
            "The kernel dies mid-way, with commands queued, leased to the worker, and half committed. Then it starts again on the same store.",
            &[],
            Does::Act(Act::Crash {
                accounts: &["a1", "a3", "h1"],
                n: 300,
            }),
        ),
        step(
            "Verify the store",
            "kernable verify recomputes the hash chain and checks every signature: the crash left no gap and no torn event.",
            &[],
            Does::Act(Act::Verify),
        ),
        // Forget me
        step(
            "Ada and Bob leave their contacts",
            "Name and email go to the vault, encrypted per person; the events keep references.",
            &[("worker", "Onboard a person once.")],
            sends(&[
                send(
                    "ada",
                    "person",
                    "p1",
                    "Onboard",
                    r#"{"name":"Ada Lovelace","email":"ada@example.com"}"#,
                ),
                send(
                    "bob",
                    "person",
                    "p2",
                    "Onboard",
                    r#"{"name":"Bob Babbage","email":"bob@example.com"}"#,
                ),
            ]),
        ),
        step(
            "Forget Ada",
            "PersonForgotten erases everything of p1 in the vault: in every event and command, at once. The events themselves stay.",
            &[("worker", "Forget a person who exists.")],
            sends(&[send("ada", "person", "p1", "Forget", "{}")]),
        ),
        // New report
        step(
            "Rebuild the accounts from the events",
            "A new read model subscribes from position 0, replays the whole day and compares its rows with the live read model.",
            &[],
            Does::Act(Act::Rebuild),
        ),
        // Tampering
        step(
            "Change a deposit in the database",
            "With SQL, straight in the file: Ada's deposit of 100 becomes 1,000,000. SQLite accepts it; nothing else notices.",
            &[],
            Does::Act(Act::Tamper),
        ),
        step(
            "Verify the store",
            "kernable verify recomputes every hash and checks every signature, and names the event that was changed.",
            &[],
            Does::Act(Act::Verify),
        ),
    ],
}];
