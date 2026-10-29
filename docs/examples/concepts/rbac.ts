import { Rejected, domain } from "@kernable/sdk";

// #region rbac
export const identityDomain = domain("identity");
const known = new Set<string>();
const blocked = new Set<string>();

// A read model that is also a middleware: it learns from events, and decides on every shop request.
export const rbacReadmodel = identityDomain.readModel("rbac")
  .on("CustomerRegistered", (_data, evt) => void known.add(evt.aggregateId))
  .on("CustomerBlocked", (_data, evt) => void blocked.add(evt.aggregateId))
  .middleware({ types: ["shop.*"], order: 0 }, (check) => {
    const who = check.request.senderIdentity;
    if (!who) throw new Rejected("who are you?", "unauthenticated");
    if (!known.has(who)) throw new Rejected(`${who} is no customer`, "unknown_customer");
    if (blocked.has(who)) throw new Rejected(`${who} is blocked`, "blocked");
  });
// #endregion rbac
