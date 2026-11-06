// Registers every part of the identity domain: a part that is not imported here does not exist.

import "./aggregate/identity.ts";
import "./command/create.ts";
import "./command/create-service.ts";
import "./command/update-profile.ts";
import "./command/grant-membership.ts";
import "./command/suspend-membership.ts";
import "./command/resume-membership.ts";
import "./command/revoke-membership.ts";
import "./command/create-api-key.ts";
import "./command/revoke-api-key.ts";
import "./command/suspend.ts";
import "./command/reactivate.ts";
import "./command/delete.ts";
import "./readmodel/identities.ts";
import "./query/get.ts";
import "./query/list.ts";
import "./query/list-mine.ts";
import "./query/find-by-account.ts";
import "./reactor/owners.ts";

export { identityDomain } from "./domain.ts";
export type * from "./domain.ts";
