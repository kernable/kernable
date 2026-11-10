# kernable-core

The kernable protocol for Rust: the gRPC messages, the signing layout, the hash chain and the
client transport, shared by the [kernable](https://github.com/kernable/kernable) kernel and the
Rust SDK.

You probably want [`kernable-sdk`](https://crates.io/crates/kernable-sdk), which builds on this
crate. The protocol is described in the [reference](https://kernable.com/docs/reference/protocol)
and [signatures](https://kernable.com/docs/reference/signing); every SDK is checked against the same
test vectors.

kernable is before 1.0: the protocol may still change between 0.x releases. MIT license.
