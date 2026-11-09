// The shop's API (part 9): the kit's domains with the authorization middleware, and the kit's
// example HTTP server. People sign in here; the kernel and the kit decide what they may do.
import { mkdirSync, writeFileSync } from "node:fs";

import { Kernable } from "@kernable/sdk";

import { exampleServer } from "../../../../kits/typescript/example/server.ts";
import { KitApi } from "../../../../kits/typescript/src/api/api.ts";
import { newSigningKeys } from "../../../../kits/typescript/src/api/tokens.ts";
import { bootstrap } from "../../../../kits/typescript/src/bootstrap.ts";
import { kitDomains, kitSchemas, relation } from "../../../../kits/typescript/src/kit.ts";
import { useMailer } from "../../../../kits/typescript/src/shared/mail.ts";
import { shopDomain } from "./domain.ts";

// #region relation
// Who owns an order: the customer it was placed for. Declared before the kit's domains start.
relation("shop.order", "owner").set("shop.OrderPlaced", (e) => e.data.customerId);
// #endregion relation

// #region mail
// In development the kit's mails go to files in mail/, named by address, instead of a mail server.
mkdirSync("mail", { recursive: true });
useMailer({ async send(mail) { writeFileSync(`mail/${mail.to}`, mail.token); } });
// #endregion mail

// #region run
// The kit's domains: its aggregates, read models, reactors and the authorization middleware.
const kit = new Kernable().app({ service: "kit", domains: kitDomains() });
void kit.run();
await kit.ready;

// The API's client knows the shop's schemas too: GetOrder declares the order it is about.
const kernable = new Kernable().use(...kitSchemas(), shopDomain);
// Once: the platform tenant, the sign-up policy, and the API's own identity and key.
const { api: service } = await bootstrap(kernable, { signUp: { accounts: "open", tenants: "active" } });
const api = new KitApi({ kernable, keys: newSigningKeys(), service });
exampleServer(api, kernable).listen(Number(process.env.PORT ?? 3000));
// #endregion run
