// Part 9: people sign in through the shop's API, and the kit decides what they may do.
import { readFileSync } from "node:fs";

const API = process.env.API_URL ?? "http://localhost:3000";

// #region client
// A small client of the API: a person's access token and, inside the shop, its tenant.
type Person = { email: string; token?: string; tenant?: string };
async function call(method: string, path: string, body: object | null, as?: Person): Promise<[number, any]> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (as?.token) headers.authorization = `Bearer ${as.token}`;
  if (as?.tenant) headers["x-tenant"] = as.tenant;
  const res = await fetch(`${API}${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
  return [res.status, res.status === 204 ? null : await res.json()];
}

// Registers, confirms the email with the token from the mail (a file in development), signs in.
async function signUp(name: string): Promise<Person> {
  const email = `${name.toLowerCase()}@example.com`;
  await call("POST", "/register", { email, name, password: "correct horse battery" });
  await call("POST", "/verify", { token: readFileSync(`mail/${email}`, "utf8") });
  const [status, signedIn] = await call("POST", "/sign-in", { email, password: "correct horse battery" });
  console.log(`${name} signs up and signs in: ${status}`);
  return { email, token: signedIn.accessToken };
}
// #endregion client

// #region owner
const ann = await signUp("Ann");
const [opened, shop] = await call("POST", "/tenants", { slug: "anns-shop", name: "Ann's shop" }, ann);
ann.tenant = shop.tenant;
console.log(`Ann opens her shop: ${opened}`);

// Customers may place orders, and work only on orders they own.
const [grouped] = await call("POST", "/commands/group/group/CreateGroup", {
  aggregateId: "customers",
  data: {
    name: "Customers",
    permissions: [
      { pattern: "shop.Place" },
      { pattern: "shop.AddItem", when: "owner" },
      { pattern: "shop.Submit", when: "owner" },
      { pattern: "shop.GetOrder", when: "owner" },
    ],
  },
}, ann);
console.log(`Ann creates the group Customers: ${grouped}`);

const [linked, link] = await call("POST", "/invitation-links", { groups: [{ group: "customers" }], label: "the shop's website" }, ann);
console.log(`Ann creates an invitation link: ${linked}`);
// #endregion owner

// #region customers
const bob = await signUp("Bob");
const cleo = await signUp("Cleo");
for (const [name, person] of [["Bob", bob], ["Cleo", cleo]] as const) {
  const [joined] = await call("POST", "/invitation-links/join", { token: link.token }, person);
  person.tenant = shop.tenant;
  console.log(`${name} joins through the link: ${joined}`);
}

const [, me] = await call("GET", "/me", null, bob);
const [placed] = await call("POST", "/commands/shop/order/Place", { aggregateId: "o1", data: { customerId: me.senderIdentity } }, bob);
console.log(`Bob places o1: ${placed}`);
const [added] = await call("POST", "/commands/shop/order/AddItem", { aggregateId: "o1", data: { sku: "apple", qty: 2 } }, bob);
console.log(`Bob adds two apples: ${added}`);
const [cleoAdds, cleoAddsBody] = await call("POST", "/commands/shop/order/AddItem", { aggregateId: "o1", data: { sku: "pear", qty: 1 } }, cleo);
console.log(`Cleo adds to o1: ${cleoAdds} ${cleoAddsBody.error}`);

const [bobReads, order] = await call("POST", "/queries/shop/GetOrder", { data: { id: "o1" } }, bob);
console.log(`Bob reads o1: ${bobReads} ${order.status}, ${JSON.stringify(order.items)}`);
const [cleoReads, refused] = await call("POST", "/queries/shop/GetOrder", { data: { id: "o1" } }, cleo);
console.log(`Cleo reads o1: ${cleoReads} ${refused.error}`);
// #endregion customers

// #region explain
const [, cleoMe] = await call("GET", "/me", null, cleo);
const [, why] = await call("POST", "/queries/group/Explain", {
  data: { identityId: cleoMe.senderIdentity, type: "shop.GetOrder", aggregateType: "order", aggregateId: "o1" },
}, cleo);
console.log(`Why not? allowed: ${why.allowed}, ${why.code}`);

const [anonymous, nobody] = await call("POST", "/commands/shop/order/Place", { aggregateId: "o2", data: {} }, { email: "", tenant: shop.tenant });
console.log(`Without signing in: ${anonymous} ${nobody.error}`);
// #endregion explain
