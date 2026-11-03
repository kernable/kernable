fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto");
    tonic_prost_build::configure()
        // Boxed: a `Result<_, Rejected>` stays small.
        .boxed(".kernable.v1.Rejected.requirement")
        .compile_protos(
            &[
                "../../proto/kernable/v1/kernable.proto",
                "../../proto/kernable/v1/storage.proto",
                "../../proto/kernable/v1/replication.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
