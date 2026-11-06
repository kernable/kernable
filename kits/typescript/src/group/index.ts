// Registers every part of the group domain: a part that is not imported here does not exist.

import "./aggregate/group.ts";
import "./command/create.ts";
import "./command/update.ts";
import "./command/set-permissions.ts";
import "./command/delete.ts";
import "./readmodel/groups.ts";
import "./query/list.ts";
import "./query/get.ts";

export { groupDomain } from "./domain.ts";
export type * from "./domain.ts";
