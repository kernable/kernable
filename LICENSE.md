# License

Copyright 2026 Artur Susdorf

kernable comes under two licenses:

- **[FSL-1.1-ALv2](LICENSE-FSL.md)**, the Functional Source License, Version 1.1, with Apache 2.0 as its future license: the kernel and the `kernable` binary (`crates/kernable-kernel`), the store (`crates/kernable-store`), and the tools around them (`crates/kernable-bench`, `crates/kernable-soak`, `fuzz`). Each version becomes Apache 2.0 two years after its release.
- **[Apache-2.0](LICENSE-APACHE)**: the core types (`crates/kernable-core`) and the SDKs (`sdks/rust`, `sdks/typescript`, `sdks/python`), which your own code links against.

Each package carries its license in its own directory, and its manifest names it.
