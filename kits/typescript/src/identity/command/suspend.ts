import { identityAggregate } from "../aggregate/identity.ts";
import { active } from "./checks.ts";

identityAggregate.command("SuspendIdentity", (s, d: { reason?: string }) => {
  active(s);
  return [{ type: "IdentitySuspended", data: { reason: d?.reason ?? "by_group" } }];
});
