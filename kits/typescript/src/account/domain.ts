// account: one Account per person and a Session per sign-in, in the platform tenant
// (design/kits.md "account"). The domain object and the types its parts share; `index.ts`
// registers the parts. Vault fields are declared on the event and command classes.

import { domain } from "@kernable/sdk";

export const accountDomain = domain("account");

export type AccountStatus = "none" | "pending" | "active" | "suspended" | "deleted";
export type TokenPurpose = "verify" | "reset" | "email_change";
export type SessionEnd = "signed_out" | "revoked" | "account_suspended" | "password_changed" | "account_deleted" | "reuse" | "idle" | "expired";
