import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";
import { slug } from "./checks.ts";

tenantAggregate.command("ChangeTenantSlug", (s, d: { slug: string }, cmd) => {
  if (s.status !== "active") throw new Rejected("Only an active tenant changes its slug", "invalid_state");
  return [{ type: "TenantSlugChanged", data: { slug: slug(d.slug, cmd.aggregateId) } }];
});
