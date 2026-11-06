import { Rejected } from "@kernable/sdk";

import { PLATFORM_TENANT } from "../../shared/constants.ts";
import { normalEmail, validEmail } from "../../shared/secrets.ts";
import { tenantAggregate } from "../aggregate/tenant.ts";
import type { Owner } from "../domain.ts";
import { itself, name, slug } from "./checks.ts";

export interface CreateTenant {
  slug: string;
  name: string;
  /** `{ email }` when the platform creates it (an owner invitation follows), `{ accountId }` by self-service. */
  owner: Owner;
  /** `active`, or `pending` for a self-service tenant waiting for approval (the middleware holds it to the sign-up policy). */
  status?: "active" | "pending";
}

tenantAggregate.command("CreateTenant", (s, d: CreateTenant, cmd) => {
  itself(cmd);
  if (s.status !== "none") throw new Rejected("The tenant exists", "exists");
  const owner = d.owner as Partial<{ email: string; accountId: string }> | undefined;
  let first: Owner;
  if (typeof owner?.accountId === "string" && owner.accountId) first = { accountId: owner.accountId };
  else if (validEmail(normalEmail(owner?.email))) first = { email: normalEmail(owner?.email) };
  else if (cmd.aggregateId === PLATFORM_TENANT) first = { email: "" }; // its owners come from the bootstrap
  else throw new Rejected("A tenant needs an owner: an email or an account", "invalid_owner");
  return [{
    type: "TenantCreated",
    data: { slug: slug(d.slug, cmd.aggregateId), name: name(d.name), owner: first, status: d.status === "pending" ? "pending" : "active" },
  }];
});
