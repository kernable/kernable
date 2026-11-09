# Groups

A group is a named set of permissions in one tenant: "Customers", "Support", "Accounting".
Identities get permissions only through groups (or by owning the tenant), so a tenant's admins
manage who may do what without code changes.

One aggregate, `group.group`, with a random ID, under its tenant. A group name is unique in its
tenant, ignoring case.

## Permissions

A permission is a pattern over request types, the same syntax as [grants](../concepts/trust#keys-and-grants),
optionally tied to a relation:

```ts
CreateGroup.new({
  name: "Customers",
  description: "People who buy in the shop",
  permissions: [
    { pattern: "shop.PlaceOrder" },                           // any order
    { pattern: "shop.CancelOrder", when: "owner" },           // only orders they own
    { pattern: "shop.GetOrder", when: "owner" },
    { pattern: "funds.UpdateVehicle", when: "manager", viaParent: true }, // vehicles of funds they manage
  ],
});
```

| Field | Means |
|---|---|
| `pattern` | a type (`shop.PlaceOrder`), every type of a domain (`shop.*`), or everything (`*`) |
| `when` | a relation the identity must have to the target aggregate; you declare relations in code, from your events ([Authorization](./authorization#relations)) |
| `viaParent` | the relation may hold on the target's parent instead, e.g. the fund a vehicle belongs to |

A permission with `when` needs a target: for a command, its aggregate; for a query, the field the
query names as its target. Without a target it never matches.

`SetPermissions` replaces the whole list in one event, which keeps the audit readable: each change
shows the complete list before and after.

## Owners and platform groups

- Every tenant lists a virtual group **`owners`** first: its members are the tenant's owners, it
  holds `*`, and it cannot be changed or deleted. Owners are managed on the
  [tenant](./tenancy#owners).
- Groups of the **platform tenant** hold their permissions in **every** tenant. That is how the
  people who run your application (support, billing) get access across customers. The `api` group
  that `bootstrap()` creates for your API is one of them.

## No escalation

Whoever creates a group or changes its permissions may only give permissions they hold
themselves. Otherwise anyone allowed to edit groups could give their own group `*`. The same rule
applies to granting memberships and to invitations. Only permissions the sender holds for the
whole tenant count, without scope or relation; owners and the platform hold all.

## Deleting a group

`DeleteGroup` makes its memberships stop counting at once; a reactor then revokes them on each
identity.

## Reference

<!--@include: ./generated/group.md-->
