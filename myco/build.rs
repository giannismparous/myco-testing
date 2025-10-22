use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/proto/myco.proto");
    let proto_dir = proto_file.parent().unwrap();
    
    tonic_build::configure()
        .out_dir("src/proto")
        .compile(&[proto_file.as_path()], &[proto_dir])?;
    
    Ok(())
}