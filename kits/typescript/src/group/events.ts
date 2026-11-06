// The group events: what happened to groups, the vocabulary other domains read.

import { Event } from "@kernable/sdk";

import { groupDomain, type Permission } from "./domain.ts";

export class GroupCreated extends Event {
  name!: string;
  description!: string;
  permissions!: Permission[];
}

export class GroupUpdated extends Event {
  name?: string;
  description?: string;
}

/** The whole list in one event: simpler to read in the audit. */
export class GroupPermissionsSet extends Event {
  permissions!: Permission[];
}

export class GroupDeleted extends Event {}

// Declared on the domain, so a process that only reads them knows them too.
groupDomain.event(GroupCreated, GroupUpdated, GroupPermissionsSet, GroupDeleted);
