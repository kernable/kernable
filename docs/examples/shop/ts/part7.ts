// Part 7: personal data in the vault, passwords as secrets, erasure.
import { Kernable, type Outcome } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

const show = (what: string, o: Outcome) => console.log(`${what}: ${o.committed ? "committed" : `${o.code} (${o.message})`}`);

// #region try
// use(shopDomain): the client moves the fields the domain declares personal or secret to the vault.
const kernable = new Kernable({ context: { senderIdentity: "c1" } }).use(shopDomain);
const customer = (type: string, data?: object) => kernable.submit({ domain: "shop", aggregateType: "customer", aggregateId: "c1", type, data });

show("register", await customer("Register", { email: "ann@example.com", name: "Ann" }));
show("set password", await customer("SetPassword", { password: "correct horse" }));
show("sign in, wrong password", await customer("SignIn", { password: "wrong horse" }));
show("sign in", await customer("SignIn", { password: "correct horse" }));
console.log(await kernable.query({ domain: "shop", type: "GetCustomer", data: { id: "c1" } }));
show("erase", await customer("Erase"));
// Erased: the customer's data is gone, and with it the right to act.
await kernable.query({ domain: "shop", type: "GetCustomer", data: { id: "c1" } }).catch((err) => console.log(`read: ${err.code}`));
// #endregion try
kernable.close();
