"""Who may send which of the kit's types at all: the levels, kept as data. Types not listed
here need a group permission (or an owner). Your own types need nothing here: give groups
permissions for them.

Levels: `public` (anyone; the command carries its proof), `authenticated` (a session),
`internal` (the kit's reactors), `owners`, `platform`, `group` (a group permission, or an owner).
`self`: also allowed when the target is the sender itself (its identity, account or session)."""

RULES: dict[str, dict] = {
    "tenancy.CreateTenant": {"level": "authenticated"},  # self-service; the platform's groups otherwise
    "tenancy.ActivateTenant": {"level": "platform"},
    "tenancy.SuspendTenant": {"level": "platform"},  # owners too, with owner_requested: see policy.py
    "tenancy.ReactivateTenant": {"level": "platform"},  # owners too, after owner_requested
    "tenancy.DeleteTenant": {"level": "platform"},
    "tenancy.ChangeSignUpPolicy": {"level": "platform"},
    "tenancy.AddOwner": {"level": "owners"},
    "tenancy.RemoveOwner": {"level": "owners"},
    "tenancy.RequireMfa": {"level": "owners"},
    "tenancy.ListTenants": {"level": "platform"},

    "account.RegisterAccount": {"level": "public"},  # `verified` only from the API: see policy.py
    "account.VerifyEmail": {"level": "public"},
    "account.SignIn": {"level": "public"},
    "account.RefreshSession": {"level": "public"},
    "account.ProveSecondFactor": {"level": "public"},
    "account.ResetPassword": {"level": "public"},
    "account.ConfirmEmailChange": {"level": "public"},
    "account.OpenSession": {"level": "internal"},
    "account.AddSessionFactor": {"level": "internal"},
    "account.SuspendAccount": {"level": "platform"},  # the person too, with owner_requested: see policy.py
    "account.ReactivateAccount": {"level": "platform"},
    "account.DeleteAccount": {"level": "platform"},
    "account.EndSession": {"level": "group", "self": True},
    "account.ChangePassword": {"level": "group", "self": True},
    "account.RequestEmailChange": {"level": "group", "self": True},
    "account.StartTotp": {"level": "group", "self": True},
    "account.ConfirmTotp": {"level": "group", "self": True},
    "account.DisableTotp": {"level": "group", "self": True},
    "account.RegenerateRecoveryCodes": {"level": "group", "self": True},
    "account.AddPasskey": {"level": "group", "self": True},
    "account.RemovePasskey": {"level": "group", "self": True},
    "account.UnlinkProvider": {"level": "group", "self": True},
    "account.CreateSso": {"level": "owners"},
    "account.UpdateSso": {"level": "owners"},
    "account.AddSsoDomain": {"level": "owners"},
    "account.RemoveSsoDomain": {"level": "owners"},
    "account.EnforceSso": {"level": "owners"},
    "account.DisableSso": {"level": "owners"},
    "account.EnableSso": {"level": "owners"},
    "account.DeleteSso": {"level": "owners"},
    "account.GetSso": {"level": "owners"},
    "account.GetAccount": {"level": "group", "self": True},
    "account.ListSessions": {"level": "group", "self": True},

    "identity.CreateIdentity": {"level": "internal"},
    "identity.CreateProcessIdentity": {"level": "internal"},
    "identity.UpdateProfile": {"level": "group", "self": True},
    "identity.GetIdentity": {"level": "group", "self": True},
    "identity.DeleteIdentity": {"level": "group", "self": True},
    "identity.ListMyIdentities": {"level": "authenticated", "self": True},
    "group.Explain": {"level": "group", "self": True},

    "invitation.DeclineInvitation": {"level": "public"},
    "invitation.RecordInvitationMailSent": {"level": "internal"},
    "invitation.EraseInvitation": {"level": "platform"},
}

#: Requests for which the sender must hold every permission of the group concerned.
ESCALATION = ["identity.GrantMembership", "invitation.CreateInvitation", "invitation.CreateInvitationLink", "group.CreateGroup", "group.SetPermissions"]


def rule_of(type_: str) -> dict:
    return RULES.get(type_, {"level": "group"})
