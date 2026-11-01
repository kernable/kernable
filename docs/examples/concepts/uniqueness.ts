import { Rejected, domain } from "@kernable/sdk";

// #region fund
export const fundsDomain = domain("funds");

type Fund = { status: "none" | "open" | "wound_up"; pmdxId?: string; name?: string };

export const fundAggregate = fundsDomain.aggregate<Fund>("fund", { status: "none" })
  .apply("FundLaunched", (_s, data: { pmdxId: string; name: string }) => ({ status: "open", ...data }))
  .apply("FundWoundUp", (s) => { s.status = "wound_up"; })
  .command("Launch", (s, data: { pmdxId: string; name: string }) => {
    if (s.status !== "none") throw new Rejected("Fund already exists", "exists");
    return [{ type: "FundLaunched", data: { pmdxId: data.pmdxId, name: data.name } }];
  })
  .command("WindUp", (s) => {
    if (s.status !== "open") throw new Rejected(`Cannot wind up a ${s.status} fund`, "invalid_state");
    return [{ type: "FundWoundUp" }];
  })
  // One PMDX ID per fund in all tenants, kept after winding up.
  .unique("pmdxId", (s) => s.pmdxId?.trim().toUpperCase(), { scope: "global" })
  // One name per open fund in each tenant: winding up frees it.
  .unique("name", (s) => (s.status === "open" ? s.name?.trim().toLowerCase() : null))
  .fact("open", (s) => s.status === "open");
// #endregion fund

// #region account
type Account = { status: "none" | "open"; fundId?: string };

export const accountAggregate = fundsDomain.aggregate<Account>("account", { status: "none" })
  .apply("AccountOpened", (_s, data: { fundId: string }) => ({ status: "open", fundId: data.fundId }))
  .command(
    "OpenAccount",
    (s, data: { fundId: string }) => {
      if (s.status !== "none") throw new Rejected("Account already exists", "exists");
      return [{ type: "AccountOpened", data: { fundId: data.fundId } }];
    },
    // Commits only while the fund is open, checked by the kernel at commit.
    { requires: (data) => [{ domain: "funds", aggregateType: "fund", aggregateId: data.fundId, fact: "open" }] },
  );
// #endregion account

// #region investor
type Investor = { investorId?: string; email?: string };

export const investorAggregate = fundsDomain.aggregate<Investor>("investor", {})
  .apply("InvestorRegistered", (_s, data: { investorId: string; email: string }) => ({ ...data }))
  .command("RegisterInvestor", (s, data: { email: string }, cmd) => {
    if (s.investorId) throw new Rejected("Investor already registered", "exists");
    return [{ type: "InvestorRegistered", data: { investorId: cmd.aggregateId, email: data.email } }];
  })
  // A personal key: kept in the vault, freed when the investor is erased.
  .unique("email", (s) => s.email?.trim().toLowerCase(), { personal: (s) => s.investorId });

fundsDomain.command("RegisterInvestor").personal(["email"], (_d, id) => id);
fundsDomain.event("InvestorRegistered", 1).personal(["email"], (_d, id) => id);
// #endregion investor
