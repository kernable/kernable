// The soak's TypeScript app: domain bank_ts, the same as bank.py and the Rust app. Accounts take
// deposits and withdrawals (never below zero); a read model keeps balances in a store; a reactor
// flags every withdrawal of 50 or more, once. An account holds its number as a unique key and,
// while open, the fact open, which a withdrawal sent to an account requires of that account. The
// soak checks all three against the log.
import { Kernable, Rejected, domain } from "../../../sdks/typescript/src/index.ts";

const bank = domain("bank_ts");
type Account = { balance: number; open: boolean; number: string | null };
type Withdrawal = { amount: number; to?: string | null };

bank.aggregate<Account>("account", { balance: 0, open: false, number: null }, { snapshotEvery: 5 })
  .apply("Deposited", (s, d: { amount: number }) => { s.balance += d.amount; })
  .apply("Withdrawn", (s, d: { amount: number }) => { s.balance -= d.amount; })
  .apply("Opened", (s) => { s.open = true; })
  .apply("Closed", (s) => { s.open = false; })
  .apply("Numbered", (s, d: { number: string | null }) => { s.number = d.number; })
  .unique("number", (s) => s.number)
  .fact("open", (s) => s.open)
  .command("Deposit", (_s, d: { amount: number }) => {
    if (!(d.amount > 0)) throw new Rejected("amount must be positive", "invalid");
    return [{ type: "Deposited", data: { amount: d.amount } }];
  })
  .command("Withdraw", (s, d: Withdrawal) => {
    if (s.balance < d.amount) throw new Rejected("insufficient funds", "insufficient");
    return [{ type: "Withdrawn", data: { amount: d.amount, to: d.to ?? null } }];
  }, {
    requires: (d: Withdrawal) =>
      d.to ? [{ domain: "bank_ts", aggregateType: "account", aggregateId: d.to, fact: "open" }] : [],
  })
  .command("Open", () => [{ type: "Opened", data: {} }])
  .command("Close", () => [{ type: "Closed", data: {} }])
  .command("Number", (_s, d: { number: string | null }) => [{ type: "Numbered", data: { number: d.number } }])
  .command("Flag", (_s, d: { position: number }) => [{ type: "Flagged", data: { position: d.position } }]);

type Row = { balance: number; flags: number };
const row = (store: any, id: string): Row => store.get("accounts", id) ?? { balance: 0, flags: 0 };

bank.readModel("balances-ts", { store: `sqlite:${process.env.SOAK_STORE}`, schema: "1" })
  .on("Deposited", (d: { amount: number }, evt, ctx) => {
    const r = row(ctx.store, evt.aggregateId);
    ctx.store!.set("accounts", evt.aggregateId, { ...r, balance: r.balance + d.amount });
  })
  .on("Withdrawn", (d: { amount: number }, evt, ctx) => {
    const r = row(ctx.store, evt.aggregateId);
    ctx.store!.set("accounts", evt.aggregateId, { ...r, balance: r.balance - d.amount });
  })
  .on("Flagged", (_d, evt, ctx) => {
    const r = row(ctx.store, evt.aggregateId);
    ctx.store!.set("accounts", evt.aggregateId, { ...r, flags: r.flags + 1 });
  })
  .query("GetAccount", (d: { id: string }, _q, ctx) => row(ctx.store, d.id));

const kernable = new Kernable();

bank.reactor("flagger-ts")
  .on("Withdrawn", async (d: { amount: number }, evt, ctx) => {
    if (d.amount < 50) return;
    await kernable.submit({
      domain: "bank_ts", aggregateType: "account", aggregateId: evt.aggregateId, type: "Flag",
      data: { position: evt.position }, id: `flag-${evt.position}`, wait: "committed", context: ctx.follow(),
    });
  });

await kernable.app({ service: "bank-ts", domains: [bank] }).run();
