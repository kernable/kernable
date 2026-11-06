// The kit as a whole: its domains, ready for `kernable.app({ domains: kitDomains() })`.

import type { Domain } from "@kernable/sdk";

import { accountDomain } from "./account/index.ts";
import { installRelations } from "./authorization/middleware.ts";
import { groupDomain } from "./group/index.ts";
import { identityDomain } from "./identity/index.ts";
import { invitationDomain } from "./invitation/index.ts";
import { tenancyDomain } from "./tenancy/index.ts";

export { relation, parent } from "./authorization/relations.ts";
export { PLATFORM_TENANT } from "./shared/constants.ts";

/**
 * Every domain of the kit, with the relations declared so far: declare yours (`relation`,
 * `parent`) before calling it.
 */
export function kitDomains(): Domain[] {
  installRelations();
  return [tenancyDomain, accountDomain, identityDomain, groupDomain, invitationDomain];
}

/** The kit's domains for `kernable.use(...)`, so clients seal the vault fields before signing. */
export function kitSchemas(): Domain[] {
  return [tenancyDomain, accountDomain, identityDomain, groupDomain, invitationDomain];
}
