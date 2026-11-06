import { identityAggregate } from "../aggregate/identity.ts";
import { active } from "./checks.ts";

identityAggregate.command("UpdateProfile", (s, d: { displayName?: string; title?: string; email?: string }) => {
  active(s);
  return [{ type: "ProfileUpdated", data: { displayName: d.displayName ?? null, title: d.title ?? null, email: d.email ?? null } }];
});
