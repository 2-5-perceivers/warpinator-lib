fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/warpinator.proto")?;
    let out_dir = std::env::var("OUT_DIR")?;
    std::fs::rename(format!("{}/_.rs", out_dir), format!("{}/warpinator.rs", out_dir))?;
    Ok(())
}
