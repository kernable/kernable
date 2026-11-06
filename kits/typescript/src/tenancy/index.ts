// Registers every part of the tenancy domain: a part that is not imported here does not exist.

import "./aggregate/tenant.ts";
import "./command/create.ts";
import "./command/activate.ts";
import "./command/rename.ts";
import "./command/change-slug.ts";
import "./command/suspend.ts";
import "./command/reactivate.ts";
import "./command/delete.ts";
import "./command/add-owner.ts";
import "./command/remove-owner.ts";
import "./command/require-mfa.ts";
import "./command/change-sign-up-policy.ts";
import "./readmodel/tenants.ts";
import "./query/get.ts";
import "./query/get-by-slug.ts";
import "./query/list.ts";
import "./query/get-sign-up-policy.ts";

export { tenancyDomain } from "./domain.ts";
export type * from "./domain.ts";
