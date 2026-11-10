# How the pieces fit

A kernable application is made of a few kinds of code: aggregates, read models, reactors, middleware and processes. You always write aggregates; the rest you add when you need them. **A process is optional**: most commands come straight from your app, and many applications have no process at all. This page shows how the pieces work together, on an example almost every product has: onboarding a new account.

<FlowDiagram name="pieces" loop bare silent />

## Three kinds of messages

- **A command is a request:** "open account a-17", "activate account a-17". It goes to one aggregate, which may refuse it (`Rejected`, with a code). Your app sends most commands; a reactor or a process may send them too.
- **An event is a fact:** "account a-17 was opened". Only the aggregate writes it, when it accepts a command. Events are never changed or deleted, and every reader gets them in order.
- **A query is a question:** "what is the state of account a-17?". A read model answers it from the events it has read.

## The pieces, in the order you build them

| Piece | Turns | You write it | Tutorial |
|---|---|---|---|
| **Aggregate** | commands into events: it checks the rules and decides | always: it is the core of every application | [Commands and events](../tutorial/shop/2-commands) |
| **Read model** | events into answers to queries | when anything reads: your frontend, your API | [Queries](../tutorial/shop/3-queries) |
| **Reactor** | an event into an action: an email, a call to another service, a command | when one event should lead to one thing | [A reactor](../tutorial/shop/4-reactor) |
| **Middleware** | nothing: it lets a command or query through, or refuses it | when who may do what depends on more than the key | [Authorization](../tutorial/shop/6-authorization) |
| **Process** | events into the next command, over several steps and over time | when a flow waits for things, has deadlines, and you want to see where each case stands | [A business process](../tutorial/shop/10-process) |

The aggregate stays the authority over its data. A reactor or a process sends ordinary commands to it, exactly as your app would, and the aggregate checks its rules every time: neither can do anything the aggregate does not allow.

## Do you need a process?

| If you think | Write |
|---|---|
| "When X happens, do Y." (When an account is opened, send a welcome email.) | a **reactor** |
| "After X, wait for Y; if it does not come within a deadline, do Z." (After the KYC check is requested, wait for a decision; after 3 days, escalate.) | a **process** |
| "Show me how many onboardings are stuck, and where." | a **process**: its cases and statistics are in the UI |

A reactor keeps no state between events. A process keeps one **case** per key (here: per account), knows which step it is at, and sets timers. If you notice you are building a read model that remembers "where each account stands" so that a reactor knows what to do next, that is a process.

## An example: onboarding an account

The onboarding: a KYC check that a person approves or rejects, the account activated, the email address confirmed, with deadlines and reminders. Each part does one job:

| Part | Does |
|---|---|
| Aggregate `account` | takes `Open`, `RequestKyc`, `DecideKyc`, `Activate`, `SendVerification`, `ConfirmEmail`, `Close`, and writes `AccountOpened`, `KycRequested`, `KycDecided`, `AccountActivated`, … |
| Reactor `kyc-provider` | on `KycRequested`, calls the KYC provider; its result (a webhook) comes back into your API |
| Your back office | a reviewer approves or rejects: it sends `DecideKyc { approved }` |
| Reactor `mailer` | on `VerificationSent`, sends the email with the confirmation link |
| Your API | the user clicks the link: it sends `ConfirmEmail` |
| **Process `accounts.onboarding`** | says what comes next: sends the commands, waits for the events, keeps the deadlines |

The process, defined next to the domain:

::: code-group

```ts [TypeScript]
export const onboarding = accounts.process("onboarding")
  .start("AccountOpened", { key: "aggregateId", data: { accountId: "aggregateId" } })
  .command("request-kyc", "account.RequestKyc", { aggregate: "case.accountId" })
  // A person decides: the process waits for the event their command writes.
  .wait("kyc", "KycDecided", { key: "aggregateId", data: { approved: "data.approved" }, timeout: "3d", onTimeout: "kyc-overdue" })
  .decide("kyc-result", { when: "case.approved == true", then: "activate", otherwise: "reject" })
  .command("activate", "account.Activate", { aggregate: "case.accountId" })
  .command("verify-email", "account.SendVerification", { aggregate: "case.accountId" })
  .wait("email", "EmailConfirmed", { key: "aggregateId", timeout: "2d", onTimeout: "remind", next: "onboarded" })
  .command("remind", "account.SendVerification", { aggregate: "case.accountId" })
  .wait("email-late", "EmailConfirmed", { key: "aggregateId", timeout: "7d", onTimeout: "abandoned", next: "onboarded" })
  .command("kyc-overdue", "account.EscalateKyc", { aggregate: "case.accountId", next: "kyc" })
  .command("reject", "account.Close", { aggregate: "case.accountId", next: "rejected" })
  .end("onboarded")
  .end("rejected")
  .end("abandoned")
  .end("closed")
  // The customer closes the account during onboarding: the case ends at once.
  .cancelOn("AccountClosed", { key: "aggregateId", end: "closed" });
```

```python [Python]
onboarding = (
    accounts.process("onboarding")
    .start("AccountOpened", key="aggregate_id", data={"account_id": "aggregate_id"})
    .command("request-kyc", "account.RequestKyc", aggregate="case.account_id")
    # A person decides: the process waits for the event their command writes.
    .wait("kyc", "KycDecided", key="aggregate_id", data={"approved": "data.approved"}, timeout="3d", on_timeout="kyc-overdue")
    .decide("kyc-result", when="case.approved == true", then="activate", otherwise="reject")
    .command("activate", "account.Activate", aggregate="case.account_id")
    .command("verify-email", "account.SendVerification", aggregate="case.account_id")
    .wait("email", "EmailConfirmed", key="aggregate_id", timeout="2d", on_timeout="remind", next="onboarded")
    .command("remind", "account.SendVerification", aggregate="case.account_id")
    .wait("email-late", "EmailConfirmed", key="aggregate_id", timeout="7d", on_timeout="abandoned", next="onboarded")
    .command("kyc-overdue", "account.EscalateKyc", aggregate="case.account_id", next="kyc")
    .command("reject", "account.Close", aggregate="case.account_id", next="rejected")
    .end("onboarded")
    .end("rejected")
    .end("abandoned")
    .end("closed")
    # The customer closes the account during onboarding: the case ends at once.
    .cancel_on("AccountClosed", key="aggregate_id", end="closed")
)
```

```rust [Rust]
let onboarding = accounts
    .process("onboarding")
    .start_named("AccountOpened", Start::key("aggregate_id").data([("account_id", "aggregate_id")]))
    .command_named("request-kyc", "account.RequestKyc", Send::aggregate("case.account_id"))
    // A person decides: the process waits for the event their command writes.
    .wait_named("kyc", "KycDecided", Wait::key("aggregate_id").data([("approved", "data.approved")]).timeout("3d", "kyc-overdue"))
    .decide("kyc-result", Decide::when("case.approved == true", "activate").otherwise("reject"))
    .command_named("activate", "account.Activate", Send::aggregate("case.account_id"))
    .command_named("verify-email", "account.SendVerification", Send::aggregate("case.account_id"))
    .wait_named("email", "EmailConfirmed", Wait::key("aggregate_id").timeout("2d", "remind").next("onboarded"))
    .command_named("remind", "account.SendVerification", Send::aggregate("case.account_id"))
    .wait_named("email-late", "EmailConfirmed", Wait::key("aggregate_id").timeout("7d", "abandoned").next("onboarded"))
    .command_named("kyc-overdue", "account.EscalateKyc", Send::aggregate("case.account_id").next("kyc"))
    .command_named("reject", "account.Close", Send::aggregate("case.account_id").next("rejected"))
    .end("onboarded")
    .end("rejected")
    .end("abandoned")
    .end("closed")
    // The customer closes the account during onboarding: the case ends at once.
    .cancel_on_named("AccountClosed", "aggregate_id", "closed");
```

:::

One account, from start to end. Each line is a command (`──Name──▶`) or an event (`▶ Name`):

```
Your API      ──Open──────────────▶ account a-17  ▶ AccountOpened
Kernel        AccountOpened starts the case a-17#1
Process       ──RequestKyc────────▶ account a-17  ▶ KycRequested
Reactor       KycRequested: calls the KYC provider
Back office   ──DecideKyc─────────▶ account a-17  ▶ KycDecided { approved: true }
Process       the wait "kyc" completes; decides; ──Activate──▶ account a-17  ▶ AccountActivated
Process       ──SendVerification──▶ account a-17  ▶ VerificationSent
Reactor       VerificationSent: sends the email
Your API      the user clicks the link; ──ConfirmEmail──▶ account a-17  ▶ EmailConfirmed
Process       the wait "email" completes: the case ends at "onboarded"
```

The process never writes `AccountActivated` itself and never changes the account: it asks, and the account decides. Its own notes, where each case stands, are events of the case (domain `process`), which the UI reads ([Cases in the log](./processes#cases-in-the-log)).

## When something else happens meanwhile

A case reacts only to the events it waits for at its current step, with its key. Everything else in the log goes past it. Three situations:

- **The awaited event comes early.** The user confirms the email before the case reaches the step `email`. The kernel keeps the event for the case; when the case gets there, the wait completes at once.
- **An event the process does not care about.** Support changes the account's address. The case goes on as before.
- **An event that should stop the case.** The customer closes the account during onboarding. Name it with `cancelOn`: the case ends at once at the end step you give (`closed`), whatever step it is at. Its timer is withdrawn, a command it still awaits is ignored, and an incident ends with it. Without `cancelOn`, the case would notice only at its next command, which the closed account refuses; give that rejection a path (`rejected: { closed: "closed" }`), or the case stops as an incident.

Events the case's own commands caused do not cancel it: the step `reject` sends `Close`, the account writes `AccountClosed`, and the case still ends at `rejected`, where it was going.

## What a process does not do

- **Talk to the outside.** Calls to other services, emails and webhooks belong in reactors and your API; their results come back as commands. So the log stays the record of what happened, and a case can be replayed.
- **Write your events or change your data.** Only aggregates do, through commands.
- **Give people a task list.** Approving a KYC check is your back office's screen; the process waits for the command it sends. A read model, or the cases waiting at `kyc`, give that screen its list.
- **Move running cases to a new version.** A changed process is a new version; running cases finish on theirs ([Versions](./processes#registering-versions-and-removing)).

Next: [Business processes](./processes) has every step and option; the [tutorial's part 10](../tutorial/shop/10-process) builds one in the shop.
