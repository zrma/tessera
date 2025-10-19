use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir.join("../../proto");
    let orchestrator_proto = proto_root.join("orchestrator.proto");

    let proto_files = [orchestrator_proto];
    let include_dirs = [proto_root.clone()];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&proto_files, &include_dirs)?;

    println!("cargo:rerun-if-changed={}", proto_root.display());
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("build.rs").display()
    );

    Ok(())
}
