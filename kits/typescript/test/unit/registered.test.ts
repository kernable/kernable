// Every command and query of the catalogue (design/kits.md) is registered: a file in command/ or
// query/ that its domain's index.ts does not import would silently not exist.

import assert from "node:assert/strict";
import { test } from "node:test";

import type { Domain } from "@kernable/sdk";

import { kitDomains } from "../../src/kit.ts";

const COMMANDS: Record<string, string[]> = {
  "tenancy.tenant": [
    "CreateTenant", "ActivateTenant", "RenameTenant", "ChangeTenantSlug", "SuspendTenant", "ReactivateTenant",
    "DeleteTenant", "AddOwner", "RemoveOwner", "RequireMfa", "ChangeSignUpPolicy",
  ],
  "account.account": ["RegisterAccount", "RequestVerification", "VerifyEmail", "SignIn", "SuspendAccount", "ReactivateAccount", "DeleteAccount",
    "ChangePassword", "RequestPasswordReset", "ResetPassword", "RequestEmailChange", "ConfirmEmailChange", "NoteRegistrationAttempt"],
  "account.session": ["OpenSession", "RefreshSession", "AddSessionFactor", "EndSession"],
  "identity.identity": [
    "CreateIdentity", "CreateServiceIdentity", "UpdateProfile", "GrantMembership", "SuspendMembership",
    "ResumeMembership", "RevokeMembership", "CreateApiKey", "RevokeApiKey", "SuspendIdentity",
    "ReactivateIdentity", "DeleteIdentity",
  ],
  "group.group": ["CreateGroup", "UpdateGroup", "SetPermissions", "DeleteGroup"],
  "invitation.invitation": [
    "CreateInvitation", "ResendInvitation", "RevokeInvitation", "DeclineInvitation", "AcceptInvitation", "RecordInvitationMailSent", "EraseInvitation",
  ],
};

const QUERIES: Record<string, string[]> = {
  "tenancy.tenants": ["GetTenant", "GetTenantBySlug", "ListTenants", "GetSignUpPolicy"],
  "account.accounts": ["FindAccountByEmail", "GetAccount", "ListSessions"],
  "identity.identities": ["GetIdentity", "ListIdentities", "ListMyIdentities", "FindIdentityByAccount"],
  "group.groups": ["ListGroups", "GetGroup"],
  "invitation.invitations": ["ListInvitations", "PeekInvitation"],
};

const domains: Domain[] = kitDomains();

test("every command of the catalogue is registered on its aggregate", () => {
  const found = Object.fromEntries(domains.flatMap((d) => d.aggregates.map((a) => [`${d.name}.${a.name}`, [...a.handlers.keys()].filter((c) => !c.startsWith("_")).sort()])));
  for (const [aggregate, commands] of Object.entries(COMMANDS)) assert.deepEqual(found[aggregate], [...commands].sort(), aggregate);
});

test("every query of the catalogue is registered on its read model", () => {
  const found = Object.fromEntries(domains.flatMap((d) => d.listeners.map((l) => [l.name, [...l.queries.keys()].map((q) => q.split(".")[1]).sort()])));
  for (const [readModel, queries] of Object.entries(QUERIES)) assert.deepEqual(found[readModel], [...queries].sort(), readModel);
});
