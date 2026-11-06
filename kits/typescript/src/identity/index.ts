// Registers every part of the identity domain (a part not imported here does not exist) and
// exports what other domains and clients use.

export { identityDomain, apiKey, parseApiKey } from "./domain.ts";
export type * from "./domain.ts";
export * from "./events.ts";
export { identityAggregate, type Identity } from "./aggregates.ts";
export { CreateIdentity, CreateServiceIdentity, UpdateProfile, GrantMembership, SuspendMembership, ResumeMembership, RevokeMembership, CreateApiKey, RevokeApiKey, SuspendIdentity, ReactivateIdentity, DeleteIdentity } from "./commands.ts";
export { identitiesReadModel, type IdentityRow } from "./readmodels.ts";
export { GetIdentity, ListIdentities, ListMyIdentities, FindIdentityByAccount } from "./queries.ts";
export { ownersReactor } from "./reactors.ts";
