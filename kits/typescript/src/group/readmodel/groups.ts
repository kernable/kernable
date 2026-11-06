// The groups read model. Its queries are in ../query/.

import { ORDERS } from "../../shared/constants.ts";
import { groupDomain, type GroupCreated, type Permission } from "../domain.ts";

export interface GroupRow { id: string; tenant: string; name: string; description: string; permissions: Permission[] }

/** `tenant/groupId` */
export const groupRows = new Map<string, GroupRow>();
export const rowKey = (tenant: string, id: string) => `${tenant}/${id}`;

export const groupsReadModel = groupDomain
  .readModel("groups", { order: ORDERS.readModels })
  .on("GroupCreated", (d: GroupCreated, evt) => void groupRows.set(rowKey(evt.tenant, evt.aggregateId), { id: evt.aggregateId, tenant: evt.tenant, ...d }))
  .on("GroupUpdated", (d: { name?: string; description?: string }, evt) => {
    const g = groupRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (g && d.name !== undefined) g.name = d.name;
    if (g && d.description !== undefined) g.description = d.description;
  })
  .on("GroupPermissionsSet", (d: { permissions: Permission[] }, evt) => {
    const g = groupRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (g) g.permissions = d.permissions;
  })
  .on("GroupDeleted", (_d, evt) => void groupRows.delete(rowKey(evt.tenant, evt.aggregateId)))
  .on("tenancy.TenantDeleted", (_d, evt) => {
    for (const [k, g] of groupRows) if (g.tenant === evt.aggregateId) groupRows.delete(k);
  });
