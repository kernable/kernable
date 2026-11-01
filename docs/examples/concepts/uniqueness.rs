//! The funds of "Unique keys and conditions".

use kernable::{Aggregate, Domain, NewEvent, Rejected, Requirement, Unique, domain};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The domain with its aggregates.
pub struct Funds {
    pub domain: Domain,
    pub fund_aggregate: Aggregate<Fund>,
    pub account_aggregate: Aggregate<Account>,
    pub investor_aggregate: Aggregate<Investor>,
}

pub fn funds() -> Funds {
    let funds_domain = domain("funds");
    Funds {
        fund_aggregate: fund(&funds_domain),
        account_aggregate: account(&funds_domain),
        investor_aggregate: investor(&funds_domain),
        domain: funds_domain,
    }
}

// #region fund
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fund {
    status: String, // "", "open" or "wound_up"
    pmdx_id: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Launch {
    pmdx_id: String,
    name: String,
}

fn fund(funds_domain: &Domain) -> Aggregate<Fund> {
    let fund_aggregate = funds_domain.aggregate("fund", Fund::default());
    fund_aggregate.apply("FundLaunched", |s, data: Launch, _evt, _ctx| {
        s.status = "open".into();
        s.pmdx_id = Some(data.pmdx_id);
        s.name = Some(data.name);
    });
    fund_aggregate.apply("FundWoundUp", |s, _: Value, _evt, _ctx| {
        s.status = "wound_up".into()
    });
    fund_aggregate.command("Launch", |s, data: Launch, _cmd, _ctx| {
        if !s.status.is_empty() {
            return Err(Rejected::new("Fund already exists").code("exists"));
        }
        Ok(vec![NewEvent::new(
            "FundLaunched",
            json!({ "pmdxId": data.pmdx_id, "name": data.name }),
        )])
    });
    fund_aggregate.command("WindUp", |s, _: Value, _cmd, _ctx| {
        if s.status != "open" {
            return Err(Rejected::new("Cannot wind up this fund").code("invalid_state"));
        }
        Ok(vec![NewEvent::new("FundWoundUp", json!({}))])
    });
    // One PMDX ID per fund in all tenants, kept after winding up.
    fund_aggregate.unique(
        "pmdxId",
        |s| s.pmdx_id.as_ref().map(|id| id.trim().to_uppercase()),
        Unique::global(),
    );
    // One name per open fund in each tenant: winding up frees it.
    fund_aggregate.unique(
        "name",
        |s| {
            s.name
                .as_ref()
                .filter(|_| s.status == "open")
                .map(|n| n.trim().to_lowercase())
        },
        Unique::tenant(),
    );
    fund_aggregate.fact("open", |s| s.status == "open");
    fund_aggregate
}
// #endregion fund

// #region account
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    fund_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenAccount {
    fund_id: String,
}

fn account(funds_domain: &Domain) -> Aggregate<Account> {
    let account_aggregate = funds_domain.aggregate("account", Account::default());
    account_aggregate.apply("AccountOpened", |s, data: OpenAccount, _evt, _ctx| {
        s.fund_id = Some(data.fund_id)
    });
    account_aggregate.command("OpenAccount", |s, data: OpenAccount, _cmd, _ctx| {
        if s.fund_id.is_some() {
            return Err(Rejected::new("Account already exists").code("exists"));
        }
        Ok(vec![NewEvent::new(
            "AccountOpened",
            json!({ "fundId": data.fund_id }),
        )])
    });
    // Commits only while the fund is open, checked by the kernel at commit.
    account_aggregate.requires("OpenAccount", |_s, data: OpenAccount| {
        [Requirement::new("funds", "fund", &data.fund_id, "open")]
    });
    account_aggregate
}
// #endregion account

// #region investor
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Investor {
    investor_id: Option<String>,
    email: Option<String>,
}

#[derive(Deserialize)]
struct RegisterInvestor {
    email: String,
}

fn investor(funds_domain: &Domain) -> Aggregate<Investor> {
    let investor_aggregate = funds_domain.aggregate("investor", Investor::default());
    investor_aggregate.apply("InvestorRegistered", |s, data: Investor, _evt, _ctx| {
        *s = data
    });
    investor_aggregate.command(
        "RegisterInvestor",
        |s, data: RegisterInvestor, cmd, _ctx| {
            if s.investor_id.is_some() {
                return Err(Rejected::new("Investor already registered").code("exists"));
            }
            Ok(vec![NewEvent::new(
                "InvestorRegistered",
                json!({ "investorId": cmd.aggregate_id, "email": data.email }),
            )])
        },
    );
    // A personal key: kept in the vault, freed when the investor is erased.
    investor_aggregate.unique(
        "email",
        |s| s.email.as_ref().map(|e| e.trim().to_lowercase()),
        Unique::tenant().personal(|s: &Investor| s.investor_id.clone()),
    );
    let aggregate = |_: &Value, id: &str| id.to_owned();
    funds_domain
        .command("RegisterInvestor")
        .personal(&["email"], aggregate);
    funds_domain
        .event("InvestorRegistered", 1)
        .personal(&["email"], aggregate);
    investor_aggregate
}
// #endregion investor
