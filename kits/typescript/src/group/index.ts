// Registers every part of the group domain (a part not imported here does not exist) and exports
// what other domains and clients use.

export { groupDomain } from "./domain.ts";
export type * from "./domain.ts";
export * from "./events.ts";
export { groupAggregate, type Group } from "./aggregates.ts";
export { CreateGroup, UpdateGroup, SetPermissions, DeleteGroup } from "./commands.ts";
export { groupsReadModel, type GroupRow } from "./readmodels.ts";
export { ListGroups, GetGroup } from "./queries.ts";
