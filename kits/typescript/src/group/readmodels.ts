// The group read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { TenantDeleted } from "../tenancy/index.ts";
import { groupDomain, type Permission } from "./domain.ts";
import { GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated } from "./events.ts";

export interface GroupRow { id: string; tenant: string; name: string; description: string; permissions: Permission[] }

/** `tenant/groupId` */
export const groupRows = new Map<string, GroupRow>();
export const rowKey = (tenant: string, id: string) => `${tenant}/${id}`;

export const groupsReadModel = groupDomain
  .readModel("groups", { order: ORDERS.readModels })
  .on(GroupCreated, (d, evt) => void groupRows.set(rowKey(evt.tenant, evt.aggregateId), { id: evt.aggregateId, tenant: evt.tenant, ...d }))
  .on(GroupUpdated, (d, evt) => {
    const g = groupRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (g && d.name !== undefined) g.name = d.name;
    if (g && d.description !== undefined) g.description = d.description;
  })
  .on(GroupPermissionsSet, (d, evt) => {
    const g = groupRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (g) g.permissions = d.permissions;
  })
  .on(GroupDeleted, (_d, evt) => void groupRows.delete(rowKey(evt.tenant, evt.aggregateId)))
  .on(TenantDeleted, (_d, evt) => {
    for (const [k, g] of groupRows) if (g.tenant === evt.aggregateId) groupRows.delete(k);
  });
