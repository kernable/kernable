fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto");
    tonic_prost_build::configure().compile_protos(
        &[
            "../../proto/kernable/v1/kernable.proto",
            "../../proto/kernable/v1/storage.proto",
        ],
        &["../../proto"],
    )?;
    Ok(())
}
