import { Rejected } from "@kernable/sdk";

import { accountRows, accountsReadModel } from "../readmodel/accounts.ts";

accountsReadModel.query("GetAccount", (d: { accountId: string }) => {
  const a = accountRows.get(d?.accountId);
  if (!a) throw new Rejected("No such account", "not_found");
  return a;
});
