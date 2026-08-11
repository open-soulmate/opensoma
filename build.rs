fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate gRPC client code from proto definitions
    let proto_dir = "proto";
    let proto_file = format!("{}/soul.proto", proto_dir);

    if std::path::Path::new(&proto_file).exists() {
        tonic_build::configure()
            .build_server(false)
            .compile(&[proto_file], &[proto_dir])?;
    }
    Ok(())
}
