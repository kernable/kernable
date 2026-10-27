# Keys per service, and changing a key

## One key per service

The tutorial uses one key for everything. In production, give every service its own key and allow it only what it needs:

```toml
[clients]
"<api>" = ["shop.Register", "shop.Place", "shop.AddItem", "shop.Submit"]
"<confirmations>" = ["shop.Confirm"]

[workers]
"<shop-worker>" = ["shop.*"]

[listeners]
"<read-models>" = ["shop.*"]
"<confirmations>" = ["shop.OrderSubmitted"]

[queriers]
"<api>" = ["shop.*"]
"<confirmations>" = ["shop.GetOrder", "shop.GetCustomer"]

[query_handlers]
"<read-models>" = ["shop.*"]

[vault.readers]
"<read-models>" = ["shop.*"]
```

A key that is not listed for a type gets `PERMISSION_DENIED`. The kernel reads the configuration at start: restart it (SIGTERM, then start) to apply a change.

## Changing a key

Signatures in the log stay verifiable after a key change: a key ID is the public key itself.

**A client or worker key:**

1. `kernable keys new <name>`, and add the new key ID to the allowlists next to the old one.
2. Restart the kernel; restart the process with the new key.
3. Remove the old key ID from the allowlists and restart the kernel.

**A listener's key** is part of its registration: a listener cannot reconnect under another key. Today:

1. Note the listener's position: `kernable listeners <store>` (kernel stopped).
2. Remove it: `kernable listeners remove <store> <name>`.
3. Allow the new key, start the kernel, and start the listener with the new key. A read model rebuilds anyway; give a **reactor** `start: <position>` (Python: `start=<position>`), or it would act on every event again.

Changing a listener's key without these steps is on the roadmap, as is revoking a key at a position in the log.
