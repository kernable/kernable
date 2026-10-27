# Change an event's format

Stored events are never rewritten. To change what an event carries, raise its version and tell every reader how to read the old one.

1. **Declare the new version and an upcast** on the domain, in every language that writes or reads the event:

   <<< @/examples/concepts/upcast.ts#upcast

   One upcast per old version: 1 → 2, then 2 → 3. Aggregates of the domain use them before `apply`; listeners get them with `use(shop)`.

2. **Write the new format** in the command handlers.

3. **Deploy the readers first**: read models, reactors and middleware that handle the event, then the workers. A reader that meets a version it does not know stops at that event (it logs and retries) instead of misreading it, and the listeners after it wait too.

4. **Check** with `kernable schemas <store>`: it lists the highest stored version per event type. A worker that would write an older version than the store holds is refused.

**Avoid a new version** when you can: adding an optional field that readers default does not need one. **Use a new event type** when the meaning changes, not only the shape.

Archives and exports that must see events exactly as stored mark their handler `raw`; they get every version unchanged, with `event.schema`.
