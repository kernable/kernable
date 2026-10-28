---
layout: home
hero:
  name: kernable
  text: Event sourcing with business logic in any language
  tagline: A Rust kernel stores, signs and orders; your services in TypeScript, Python or any language decide.
  actions:
    - theme: brand
      text: Get started
      link: /tutorial/getting-started
    - theme: alt
      text: Reference
      link: /reference/
features:
  - title: Any language
    details: Aggregates, read models and reactors run in your own processes and talk to the kernel over gRPC. TypeScript, Python and Rust SDKs share one API.
  - title: Read your own writes
    details: A submit returns once every listener has processed the command's events, in a fixed order, so the next query already sees them.
  - title: Signed and verifiable
    details: Every command, result and query is signed; stored events form a hash chain with signed checkpoints. kernable verify checks it all.
  - title: Personal data, deletable
    details: Marked fields go to an encrypted vault instead of the immutable log. An erasing event removes a person's data everywhere.
---
