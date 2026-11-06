// The bootstrap: run once, with a key granted `privileged` for `bypass_authorization`. It
// creates the platform tenant, the sign-up policy, and the API's service identity with its key.

import { randomBytes } from "node:crypto";

import type { Command, Kernable, Outcome, RequestContext } from "@kernable/sdk";

import { CreateGroup } from "./group/index.ts";
import { CreateApiKey, CreateServiceIdentity, GrantMembership, apiKey } from "./identity/index.ts";
import { PLATFORM_TENANT } from "./shared/constants.ts";
import { ChangeSignUpPolicy, CreateTenant, type SignUpPolicy } from "./tenancy/index.ts";

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
  const submit = (command: Command, aggregateId: string, id?: string) =>
    kernable.submit(command, { id: id ?? `bootstrap-${command.constructor.name}-${aggregateId}`, aggregateId, tenant, context }).then(ok);

  await submit(CreateTenant.new({ slug: "platform", name: options.platformName ?? "Platform", owner: { email: "" } }), tenant);
  await submit(ChangeSignUpPolicy.new(options.signUp), tenant);
  const identityId = "api";
  await submit(CreateServiceIdentity.new({ displayName: "API" }), identityId);
  await submit(CreateGroup.new({ name: "api", description: "What the API asks for itself", permissions: API_PERMISSIONS }), "api");
  await submit(GrantMembership.new({ membershipId: "api", group: "api" }), identityId);
  const keyId = randomBytes(8).toString("hex");
  const secret = randomBytes(32).toString("base64url");
  await submit(CreateApiKey.new({ keyId, secret, label: "API" }), identityId, randomBytes(8).toString("hex"));
  return { api: { identityId, keyId, key: apiKey(keyId, secret) } };
}
