// The bootstrap: run once, with a key granted `privileged` for `bypass_authorization`. It
// creates the platform tenant, the sign-up policy, and the API's service identity with its key.

import { randomBytes } from "node:crypto";

import type { Kernable, Outcome, RequestContext } from "@kernable/sdk";

import { PLATFORM_TENANT } from "./shared/constants.ts";
import type { SignUpPolicy } from "./tenancy/domain.ts";
import { apiKey } from "./identity/domain.ts";

/** What the API may ask for itself: lookups before anyone is signed in. */
export const API_PERMISSIONS = [
  { pattern: "account.FindAccountByEmail" },
  { pattern: "account.FindAccountByLink" },
  { pattern: "account.RegisterAccount" },
  { pattern: "account.RequestVerification" },
  { pattern: "account.RequestPasswordReset" },
  { pattern: "identity.FindIdentityByAccount" },
  { pattern: "identity.FindApiKey" },
  { pattern: "tenancy.GetTenantBySlug" },
  { pattern: "tenancy.GetSignUpPolicy" },
];

export interface Bootstrapped {
  /** The API's identity and key: give them to `KitApi`, keep the key secret. */
  api: { identityId: string; keyId: string; key: string };
}

function ok(outcome: Outcome): void {
  if (!outcome.committed && outcome.code !== "exists") throw new Error(`bootstrap: ${outcome.code}: ${outcome.message}`);
}

/** Sets up the platform: idempotent, so it may run again. */
export async function bootstrap(kernable: Kernable, options: { signUp: SignUpPolicy; platformName?: string }): Promise<Bootstrapped> {
  const context: RequestContext = { bypassAuthorization: true };
  const tenant = PLATFORM_TENANT;
  const submit = (domain: string, aggregateType: string, aggregateId: string, type: string, data: unknown) =>
    kernable.submit({ id: `bootstrap-${domain}-${aggregateId}-${type}`, domain, aggregateType, aggregateId, tenant, type, data, context }).then(ok);

  await submit("tenancy", "tenant", tenant, "CreateTenant", { slug: "platform", name: options.platformName ?? "Platform", owner: {} });
  await submit("tenancy", "tenant", tenant, "ChangeSignUpPolicy", options.signUp);
  const identityId = "api";
  await submit("identity", "identity", identityId, "CreateServiceIdentity", { displayName: "API" });
  await submit("group", "group", "api", "CreateGroup", { name: "api", description: "What the API asks for itself", permissions: API_PERMISSIONS });
  await submit("identity", "identity", identityId, "GrantMembership", { membershipId: "api", group: "api" });
  const keyId = randomBytes(8).toString("hex");
  const secret = randomBytes(32).toString("base64url");
  await kernable.submit({
    domain: "identity", aggregateType: "identity", aggregateId: identityId, tenant, type: "CreateApiKey",
    data: { keyId, secret, label: "API" }, context,
  }).then(ok);
  return { api: { identityId, keyId, key: apiKey(keyId, secret) } };
}
