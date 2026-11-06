import { policy, tenantsReadModel } from "../readmodel/tenants.ts";

tenantsReadModel.query("GetSignUpPolicy", () => policy.signUp);
