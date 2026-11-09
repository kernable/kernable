//! Who may send which of the kit's types at all: the levels, kept as data. Types not listed
//! here need a group permission (or an owner). Your own types need nothing here: give groups
//! permissions for them.
//!
//! Levels: `Public` (anyone; the command carries its proof), `Authenticated` (a session),
//! `Internal` (the kit's reactors), `Owners`, `Platform`, `Group` (a group permission, or an
//! owner). `self`: also allowed when the target is the sender itself (its identity, account or
//! session).

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Public,
    Authenticated,
    Internal,
    Owners,
    Platform,
    Group,
}

#[derive(Clone, Copy, Debug)]
pub struct Rule {
    pub level: Level,
    /// Also allowed for the sender itself.
    pub itself: bool,
}

use Level::*;

const fn r(level: Level) -> Rule {
    Rule {
        level,
        itself: false,
    }
}

const fn me(level: Level) -> Rule {
    Rule {
        level,
        itself: true,
    }
}

pub const RULES: &[(&str, Rule)] = &[
    ("tenancy.CreateTenant", r(Authenticated)), // self-service; the platform's groups otherwise
    ("tenancy.ActivateTenant", r(Platform)),
    ("tenancy.SuspendTenant", r(Platform)), // owners too, with owner_requested: see policy.rs
    ("tenancy.ReactivateTenant", r(Platform)), // owners too, after owner_requested
    ("tenancy.DeleteTenant", r(Platform)),
    ("tenancy.ChangeSignUpPolicy", r(Platform)),
    ("tenancy.AddOwner", r(Owners)),
    ("tenancy.RemoveOwner", r(Owners)),
    ("tenancy.RequireMfa", r(Owners)),
    ("tenancy.ListTenants", r(Platform)),
    ("account.RegisterAccount", r(Public)), // `verified` only from the API: see policy.rs
    ("account.VerifyEmail", r(Public)),
    ("account.SignIn", r(Public)),
    ("account.RefreshSession", r(Public)),
    ("account.ProveSecondFactor", r(Public)),
    ("account.ResetPassword", r(Public)),
    ("account.ConfirmEmailChange", r(Public)),
    ("account.OpenSession", r(Internal)),
    ("account.AddSessionFactor", r(Internal)),
    ("account.SuspendAccount", r(Platform)), // the person too, with owner_requested: see policy.rs
    ("account.ReactivateAccount", r(Platform)),
    ("account.DeleteAccount", r(Platform)),
    ("account.EndSession", me(Group)),
    ("account.ChangePassword", me(Group)),
    ("account.RequestEmailChange", me(Group)),
    ("account.StartTotp", me(Group)),
    ("account.ConfirmTotp", me(Group)),
    ("account.DisableTotp", me(Group)),
    ("account.RegenerateRecoveryCodes", me(Group)),
    ("account.AddPasskey", me(Group)),
    ("account.RemovePasskey", me(Group)),
    ("account.UnlinkProvider", me(Group)),
    ("account.CreateSso", r(Owners)),
    ("account.UpdateSso", r(Owners)),
    ("account.AddSsoDomain", r(Owners)),
    ("account.RemoveSsoDomain", r(Owners)),
    ("account.EnforceSso", r(Owners)),
    ("account.DisableSso", r(Owners)),
    ("account.EnableSso", r(Owners)),
    ("account.DeleteSso", r(Owners)),
    ("account.GetSso", r(Owners)),
    ("account.GetAccount", me(Group)),
    ("account.ListSessions", me(Group)),
    ("identity.CreateIdentity", r(Internal)),
    ("identity.CreateProcessIdentity", r(Internal)),
    ("identity.UpdateProfile", me(Group)),
    ("identity.GetIdentity", me(Group)),
    ("identity.DeleteIdentity", me(Group)),
    ("identity.ListMyIdentities", me(Authenticated)),
    ("group.Explain", me(Group)),
    ("invitation.DeclineInvitation", r(Public)),
    ("invitation.RecordInvitationMailSent", r(Internal)),
    ("invitation.EraseInvitation", r(Platform)),
];

/// Requests for which the sender must hold every permission of the group concerned.
pub const ESCALATION: &[&str] = &[
    "identity.GrantMembership",
    "invitation.CreateInvitation",
    "invitation.CreateInvitationLink",
    "group.CreateGroup",
    "group.SetPermissions",
];

pub fn rule_of(r#type: &str) -> Rule {
    RULES
        .iter()
        .find(|(t, _)| *t == r#type)
        .map_or(r(Group), |(_, rule)| *rule)
}
