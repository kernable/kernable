//! Generates the protocol types from `proto/`: the repository's in a checkout, or the copy that
//! `pack_proto.sh` puts into the crate before `cargo publish`. protox compiles the `.proto` files
//! in Rust, so a build needs no `protoc` installed.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = if Path::new("proto").is_dir() {
        "proto"
    } else {
        "../../proto"
    };
    println!("cargo:rerun-if-changed={root}");
    let file = |name: &str| format!("{root}/kernable/v1/{name}");
    let descriptors = protox::compile(
        [
            file("kernable.proto"),
            file("storage.proto"),
            file("replication.proto"),
            file("export.proto"),
        ],
        [root],
    )?;
    tonic_prost_build::configure()
        // Boxed: a `Result<_, Rejected>` stays small.
        .boxed(".kernable.v1.Rejected.requirement")
        .compile_fds(descriptors)?;
    Ok(())
}
