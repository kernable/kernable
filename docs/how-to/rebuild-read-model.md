# Rebuild a read model

A read model is derived from the events, so you can always build it again: after fixing a bug in a handler, or when it should hold something new.

- **In memory** (no `store`): restart it. It reads every event from the first on every start.
- **With a store:** raise its `schema` (`schema: "2"`). On start it clears the store and reads every event from the first. Or delete the store file.

While it rebuilds, it is behind: queries with `minPosition` wait for it, submits that settle wait for it up to `--settle-timeout-ms`, and `/ready` answers 503. Rebuild outside peak hours, or run the new version under a **new name** next to the old one, switch queries over once it caught up, then remove the old one ([Remove a listener, middleware or aggregate](./remove-listener)).

**Reactors are not rebuilt.** They act on events; reading every event again would act again. A reactor continues after its last acknowledged event.
