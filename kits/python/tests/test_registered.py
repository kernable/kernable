"""Every command and query of the catalogue (design/kits.md) is registered: a handler that its
domain's module does not import would silently not exist."""

import unittest

from kit import kit_domains

COMMANDS = {
    "tenancy.tenant": [
        "CreateTenant", "ActivateTenant", "RenameTenant", "ChangeTenantSlug", "SuspendTenant", "ReactivateTenant",
        "DeleteTenant", "AddOwner", "RemoveOwner", "RequireMfa", "ChangeSignUpPolicy",
    ],
    "account.account": [
        "RegisterAccount", "RequestVerification", "VerifyEmail", "SignIn", "SuspendAccount", "ReactivateAccount", "DeleteAccount",
        "ChangePassword", "RequestPasswordReset", "ResetPassword", "RequestEmailChange", "ConfirmEmailChange", "NoteRegistrationAttempt",
        "StartTotp", "ConfirmTotp", "DisableTotp", "RegenerateRecoveryCodes", "ProveSecondFactor",
        "AddPasskey", "RemovePasskey", "SignInWithPasskey", "ProvePasskey", "SignInWithProvider", "LinkProvider", "UnlinkProvider",
    ],
    "account.sso": ["CreateSso", "UpdateSso", "AddSsoDomain", "VerifySsoDomain", "RemoveSsoDomain", "EnforceSso", "DisableSso", "EnableSso", "DeleteSso"],
    "account.session": ["OpenSession", "RefreshSession", "AddSessionFactor", "EndSession"],
    "identity.identity": [
        "CreateIdentity", "CreateServiceIdentity", "CreateProcessIdentity", "UpdateProfile", "GrantMembership", "SuspendMembership",
        "ResumeMembership", "RevokeMembership", "CreateApiKey", "RevokeApiKey", "SuspendIdentity",
        "ReactivateIdentity", "DeleteIdentity",
    ],
    "group.group": ["CreateGroup", "UpdateGroup", "SetPermissions", "DeleteGroup"],
    "invitation.invitation": [
        "CreateInvitation", "ResendInvitation", "RevokeInvitation", "DeclineInvitation", "AcceptInvitation", "RecordInvitationMailSent", "EraseInvitation",
    ],
    "invitation.link": ["CreateInvitationLink", "RevokeInvitationLink", "JoinWithInvitationLink"],
}

QUERIES = {
    "tenancy.tenants": ["GetTenant", "GetTenantBySlug", "ListTenants", "GetSignUpPolicy"],
    "account.accounts": ["FindAccountByEmail", "GetAccount", "ListSessions", "FindAccountByLink"],
    "account.ssos": ["FindSsoByDomain", "GetSsoSecret", "GetSso"],
    "identity.identities": ["GetIdentity", "ListIdentities", "ListMyIdentities", "FindIdentityByAccount", "FindApiKey"],
    "group.groups": ["ListGroups", "GetGroup"],
    "group.authorization": ["Explain"],
    "invitation.invitations": ["ListInvitations", "PeekInvitation", "ListInvitationLinks", "PeekInvitationLink"],
}


class RegisteredTest(unittest.TestCase):
    def test_every_command_of_the_catalogue_is_registered_on_its_aggregate(self):
        found = {f"{d.name}.{a.name}": sorted(c for c in a.handlers if not c.startswith("_")) for d in kit_domains() for a in d.aggregates}
        for aggregate, commands in COMMANDS.items():
            self.assertEqual(found.get(aggregate), sorted(commands), aggregate)

    def test_every_query_of_the_catalogue_is_registered_on_its_read_model(self):
        found = {l.name: sorted(q.split(".")[1] for q in l._queries) for d in kit_domains() for l in d.listeners}
        for read_model, queries in QUERIES.items():
            self.assertEqual(found.get(read_model), sorted(queries), read_model)


if __name__ == "__main__":
    unittest.main()
