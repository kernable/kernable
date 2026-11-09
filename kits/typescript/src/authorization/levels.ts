// Who may send which of the kit's types at all: the levels, kept as data. Types not
// listed here need a group permission (or an owner). Your own types need nothing here: give
// groups permissions for them.

export type Level =
  /** Anyone, also without a session: the command carries its proof (password, token). */
  | "public"
  /** A signed-in session, no identity in a tenant needed. */
  | "authenticated"
  /** Only the kit's reactors (with `bypassAuthorization`, which only privileged keys may set). */
  | "internal"
  /** Only the tenant's owners (and platform owners). */
  | "owners"
  /** Only platform groups and platform owners. */
  | "platform"
  /** A group permission, or an owner. */
  | "group";

export interface Rule {
  level: Level;
  /** Also allowed when the target is the sender itself: its identity, account or session. */
  self?: boolean;
}

export const RULES: Record<string, Rule> = {
  "tenancy.CreateTenant": { level: "authenticated" }, // self-service; the platform's groups otherwise
  "tenancy.ActivateTenant": { level: "platform" },
  "tenancy.SuspendTenant": { level: "platform" }, // owners too, with owner_requested: see policy.ts
  "tenancy.ReactivateTenant": { level: "platform" }, // owners too, after owner_requested
  "tenancy.DeleteTenant": { level: "platform" },
  "tenancy.ChangeSignUpPolicy": { level: "platform" },
  "tenancy.AddOwner": { level: "owners" },
  "tenancy.RemoveOwner": { level: "owners" },
  "tenancy.RequireMfa": { level: "owners" },
  "tenancy.ListTenants": { level: "platform" },

  "account.RegisterAccount": { level: "public" }, // `verified` only from the API: see policy.ts
  "account.VerifyEmail": { level: "public" },
  "account.SignIn": { level: "public" },
  "account.RefreshSession": { level: "public" },
  "account.ProveSecondFactor": { level: "public" },
  "account.ResetPassword": { level: "public" },
  "account.ConfirmEmailChange": { level: "public" },
  "account.OpenSession": { level: "internal" },
  "account.AddSessionFactor": { level: "internal" },
  "account.SuspendAccount": { level: "platform" }, // the person too, with owner_requested: see policy.ts
  "account.ReactivateAccount": { level: "platform" },
  "account.DeleteAccount": { level: "platform" },
  "account.EndSession": { level: "group", self: true },
  "account.ChangePassword": { level: "group", self: true },
  "account.StartTotp": { level: "group", self: true },
  "account.AddPasskey": { level: "group", self: true },
  "account.UnlinkProvider": { level: "group", self: true },
  "account.CreateSso": { level: "owners" },
  "account.UpdateSso": { level: "owners" },
  "account.AddSsoDomain": { level: "owners" },
  "account.RemoveSsoDomain": { level: "owners" },
  "account.EnforceSso": { level: "owners" },
  "account.DisableSso": { level: "owners" },
  "account.EnableSso": { level: "owners" },
  "account.DeleteSso": { level: "owners" },
  "account.GetSso": { level: "owners" },
  "account.RemovePasskey": { level: "group", self: true },
  "account.ConfirmTotp": { level: "group", self: true },
  "account.DisableTotp": { level: "group", self: true },
  "account.RegenerateRecoveryCodes": { level: "group", self: true },
  "account.RequestEmailChange": { level: "group", self: true },
  "account.GetAccount": { level: "group", self: true },
  "account.ListSessions": { level: "group", self: true },

  "identity.CreateIdentity": { level: "internal" },
  "identity.CreateProcessIdentity": { level: "internal" },
  "identity.UpdateProfile": { level: "group", self: true },
  "identity.GetIdentity": { level: "group", self: true },
  "identity.DeleteIdentity": { level: "group", self: true },
  "identity.ListMyIdentities": { level: "authenticated", self: true },
  "group.Explain": { level: "group", self: true },

  "invitation.DeclineInvitation": { level: "public" },
  "invitation.RecordInvitationMailSent": { level: "internal" },
  "invitation.EraseInvitation": { level: "platform" },
};

/** Requests for which the sender must hold every permission of the group concerned. */
export const ESCALATION = ["identity.GrantMembership", "invitation.CreateInvitation", "invitation.CreateInvitationLink", "group.CreateGroup", "group.SetPermissions"];

export function ruleOf(type: string): Rule {
  return RULES[type] ?? { level: "group" };
}
