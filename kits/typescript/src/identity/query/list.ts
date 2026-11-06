import { identitiesReadModel, identityRows } from "../readmodel/identities.ts";

identitiesReadModel.query("ListIdentities", (_d, qry) => [...identityRows.values()].filter((r) => r.tenant === qry.tenant));
