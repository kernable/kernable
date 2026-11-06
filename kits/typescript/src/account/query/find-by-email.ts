import { Rejected } from "@kernable/sdk";

import { normalEmail } from "../../shared/secrets.ts";
import { accountRows, accountsByEmail, accountsReadModel } from "../readmodel/accounts.ts";

// The API's only: an open lookup would reveal who has an account.
accountsReadModel.query("FindAccountByEmail", (d: { email: string }) => {
  const id = accountsByEmail.get(normalEmail(d?.email));
  const a = id ? accountRows.get(id) : undefined;
  if (!a) throw new Rejected("No such account", "not_found");
  return { id: a.id, status: a.status };
});
